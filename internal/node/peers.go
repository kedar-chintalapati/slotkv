package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

const (
	statusTTL     = 250 * time.Millisecond
	statusTimeout = 300 * time.Millisecond
)

var errPeerClosed = errors.New("node: stopped")

// peer is another node, reached over gRPC for Status calls. Results (and
// errors) are cached for statusTTL, and concurrent callers share one RPC.
type peer struct {
	addr string
	opts []grpc.DialOption

	mu     sync.Mutex
	cc     *grpc.ClientConn
	closed bool
	at     time.Time
	st     *slotkvv1.StatusResponse
	err    error
}

// status returns the peer's cached or freshly fetched Status.
func (p *peer) status() (*slotkvv1.StatusResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errPeerClosed
	}
	if !p.at.IsZero() && time.Since(p.at) < statusTTL {
		return p.st, p.err
	}
	return p.fetchLocked()
}

// statusFresh bypasses the cache for decisions that must observe a target's
// current draining state, such as a leadership transfer.
func (p *peer) statusFresh() (*slotkvv1.StatusResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errPeerClosed
	}
	return p.fetchLocked()
}

// fetchLocked runs a Status RPC and refreshes the cache. The caller holds p.mu.
func (p *peer) fetchLocked() (*slotkvv1.StatusResponse, error) {
	if p.cc == nil {
		cc, err := grpc.NewClient("passthrough:///"+p.addr, p.opts...)
		if err != nil {
			return nil, err
		}
		p.cc = cc
	}
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	p.st, p.err = slotkvv1.NewKVServiceClient(p.cc).Status(ctx, &slotkvv1.StatusRequest{})
	p.at = time.Now()
	return p.st, p.err
}

func (p *peer) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.cc != nil {
		p.cc.Close()
	}
}

// peerDialOptions are the options for connections to other nodes: short
// reconnect backoff, so a restarted node is found again quickly.
func peerDialOptions(extra []grpc.DialOption) []grpc.DialOption {
	return append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}),
	}, extra...)
}

// groupStatus finds group id in st.
func groupStatus(st *slotkvv1.StatusResponse, id int) *slotkvv1.GroupStatus {
	for _, gs := range st.GetGroups() {
		if int(gs.GetGroupId()) == id {
			return gs
		}
	}
	return nil
}

// Ready is the /readyz decision. A draining or non-serving node is never
// ready. In caught-up mode every local group needs a known leader, and this
// node must either lead or be within ReadyMaxLag entries of the leader's
// commit index, which comes from a (cached) Status call to the leader.
func (n *Node) Ready() bool {
	if n.draining.Load() || !n.serving.Load() {
		return false
	}
	if n.cfg.Readiness == ReadinessServing {
		return true
	}
	for _, id := range n.local {
		g := n.group(id)
		if g == nil {
			return false
		}
		_, leader := g.raft.LeaderWithID()
		if leader == "" {
			return false
		}
		if string(leader) == n.id {
			continue
		}
		p := n.peers[string(leader)]
		if p == nil {
			return false
		}
		st, err := p.status()
		if err != nil {
			return false
		}
		gs := groupStatus(st, id)
		if !followerCaughtUp(g, string(leader), gs, n.cfg.ReadyMaxLag) {
			return false
		}
	}
	return true
}

func followerCaughtUp(g *group, expectedLeader string, gs *slotkvv1.GroupStatus, maxLag uint64) bool {
	if gs == nil || gs.GetState() != "Leader" || gs.GetLeaderId() != expectedLeader {
		return false
	}
	_, currentLeader := g.raft.LeaderWithID()
	return string(currentLeader) == expectedLeader && withinLag(g.raft.AppliedIndex(), maxLag, gs.GetCommitIndex())
}

func withinLag(applied, maxLag, commit uint64) bool {
	return applied >= commit || maxLag >= commit-applied
}
