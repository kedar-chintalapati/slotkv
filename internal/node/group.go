package node

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

const (
	netMaxPool = 3
	// netTimeout bounds each Raft RPC. Ten seconds let one dead connection
	// stall a leadership handoff past its deadline.
	netTimeout      = 3 * time.Second
	boltOpenTimeout = 5 * time.Second
	logCacheSize    = 512
)

// group is this node's replica of one Raft group.
type group struct {
	id     int
	raft   *raft.Raft
	fsm    *fsm
	store  io.Closer // the group's own database; nil when the node's shared log holds it
	notify chan bool // Raft leadership notifications

	// readyTerm is the highest term in which this node, as leader, applied a
	// barrier. Terms only grow, so it needs no clearing on leadership loss:
	// readiness is "leader now, and readyTerm equals the current term".
	readyTerm atomic.Uint64

	// Balancer state, touched only by the balancer goroutine.
	prevCommit uint64    // commit index at the previous tick
	retryAt    time.Time // no transfer attempts before this
}

// ready reports whether g can serve linearizable reads. The state is read
// before the term, so a leadership change in between cannot yield a false yes.
func (g *group) ready() bool {
	return readReady(g.raft.State(), g.raft.CurrentTerm(), g.readyTerm.Load())
}

func readReady(state raft.RaftState, term, barrierTerm uint64) bool {
	return state == raft.Leader && barrierTerm == term
}

// status snapshots the group's Raft state.
func (g *group) status() *slotkvv1.GroupStatus {
	r := g.raft
	state := r.State()
	term := r.CurrentTerm()
	_, leader := r.LeaderWithID()
	return &slotkvv1.GroupStatus{
		GroupId:      uint32(g.id),
		State:        state.String(),
		LeaderId:     string(leader),
		Term:         term,
		CommitIndex:  r.CommitIndex(),
		AppliedIndex: r.AppliedIndex(),
		LastLogIndex: r.LastIndex(),
		ReadReady:    state == raft.Leader && g.readyTerm.Load() == term,
	}
}

// openGroup opens group id's stores and transport, bootstraps it if the data
// directory is empty, and starts its Raft instance.
func (n *Node) openGroup(id int) (g *group, err error) {
	dir := filepath.Join(n.cfg.DataDir, "g"+strconv.Itoa(id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var (
		logStore raft.LogStore
		stable   raft.StableStore
		closer   io.Closer
	)
	if n.shared != nil {
		gl := n.shared.group(id)
		logStore, stable = gl, gl
	} else {
		dbPath := filepath.Join(dir, "raft.db")
		var store *raftboltdb.BoltStore
		if store, err = raftboltdb.New(raftboltdb.Options{Path: dbPath, BoltOptions: &bbolt.Options{Timeout: boltOpenTimeout}}); err != nil {
			return nil, fmt.Errorf("open %s: %w", dbPath, err)
		}
		defer func() {
			if err != nil {
				store.Close()
			}
		}()
		logStore, stable, closer = store, store, store
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 2, n.raftLog)
	if err != nil {
		return nil, err
	}
	logs, err := raft.NewLogCache(logCacheSize, logStore)
	if err != nil {
		return nil, err
	}
	var trans raft.Transport
	if n.cfg.RaftTransport != nil {
		trans, err = n.cfg.RaftTransport(id)
	} else {
		trans = raft.NewNetworkTransportWithLogger(n.mux.layer(id), netMaxPool, netTimeout, n.raftLog)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if c, ok := trans.(raft.WithClose); ok && err != nil {
			c.Close()
		}
	}()

	g = &group{id: id, fsm: newFSM(!n.cfg.DisableDedup), store: closer, notify: make(chan bool, 16)}
	rc := n.raftConfig()
	rc.NotifyCh = g.notify
	rc.Logger = n.raftLog.With("group", id)

	has, err := raft.HasExistingState(logs, stable, snaps)
	if err != nil {
		return nil, err
	}
	if !has {
		if err = raft.BootstrapCluster(rc, logs, stable, snaps, trans, n.bootstrapConfig(id)); err != nil {
			return nil, err
		}
	}
	if g.raft, err = raft.NewRaft(rc, g.fsm, logs, stable, snaps, trans); err != nil {
		return nil, err
	}
	n.wg.Add(1)
	go n.watch(g)
	return g, nil
}

// bootstrapConfig lists every replica of group id as a voter, in topology
// order, so all replicas bootstrap with an identical configuration.
func (n *Node) bootstrapConfig(id int) raft.Configuration {
	var c raft.Configuration
	for _, r := range n.topo.Groups[id].Replicas {
		nd, _ := n.topo.Node(r)
		c.Servers = append(c.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(r),
			Address:  raft.ServerAddress(nd.RaftAddr),
		})
	}
	return c
}

// raftConfig returns the Raft settings shared by all groups.
func (n *Node) raftConfig() *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(n.id)
	c.HeartbeatTimeout = n.cfg.HeartbeatTimeout
	c.ElectionTimeout = n.cfg.ElectionTimeout
	c.LeaderLeaseTimeout = n.cfg.LeaderLeaseTimeout
	c.CommitTimeout = n.cfg.CommitTimeout
	c.MaxAppendEntries = 1024 // hashicorp/raft's maximum: the largest batch per append
	c.BatchApplyCh = true
	return c
}

// watch consumes g's leadership notifications. Each gain starts a barrier in
// its own goroutine, so a slow barrier never stalls Raft's notification send.
func (n *Node) watch(g *group) {
	defer n.wg.Done()
	for {
		select {
		case leader := <-g.notify:
			if leader {
				n.wg.Add(1)
				go n.confirmLeader(g)
			}
		case <-n.stop:
			return
		}
	}
}

// confirmLeader applies a barrier and, if this node is still leader of the
// same term, marks the group read-ready for that term.
func (n *Node) confirmLeader(g *group) {
	defer n.wg.Done()
	for g.raft.State() == raft.Leader {
		term := g.raft.CurrentTerm()
		err := await(context.Background(), g.raft.Barrier(n.cfg.ApplyTimeout), n.cfg.ApplyTimeout)
		if err == nil && g.raft.State() == raft.Leader && g.raft.CurrentTerm() == term {
			for old := g.readyTerm.Load(); old < term && !g.readyTerm.CompareAndSwap(old, term); old = g.readyTerm.Load() {
			}
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-n.stop:
			return
		}
	}
}
