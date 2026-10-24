// Package sim runs slotkv clusters in-process with seeded fault choices.
package sim

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/hashicorp/raft"
	"github.com/kedar-chintalapati/slotkv/client"
	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
	"github.com/kedar-chintalapati/slotkv/internal/lincheck"
	"github.com/kedar-chintalapati/slotkv/internal/node"
	"github.com/kedar-chintalapati/slotkv/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Config controls one simulator run.
type Config struct {
	Seed            int64
	Duration        time.Duration
	Nodes, Groups   int
	ReadMode        string
	Dedup           bool
	DropProbability float64
	Faults          []string
	CheckTimeout    time.Duration
	VizPath         string
}

// Result is the summary of one run.
type Result struct {
	Seed             int64         `json:"seed"`
	Verdict          string        `json:"verdict"`
	Ops              int           `json:"ops"`
	FailedOps        int           `json:"failed_ops"`
	SuccessReads     int           `json:"success_reads"`
	SuccessWrites    int           `json:"success_writes"`
	CheckTime        time.Duration `json:"check_time"`
	Crashes          int           `json:"crashes"`
	Partitions       int           `json:"partitions"`
	DroppedResponses int           `json:"dropped_responses"`
	VizPath          string        `json:"viz_path,omitempty"`
	Err              string        `json:"error,omitempty"`
}

type incarnation struct {
	n     *node.Node
	ln    *bufconn.Listener
	trans map[int]*raft.InmemTransport
}

type cluster struct {
	mu   sync.RWMutex
	topo *topology.Topology
	inc  map[string]*incarnation
	all  []string
}

func shouldDrop(seed int64, probability float64, method string, req any, trials map[string]uint64, mu *sync.Mutex) bool {
	key, value := "", ""
	clientID := ""
	var seq uint64
	switch r := req.(type) {
	case *slotkvv1.PutRequest:
		key, value, seq = string(r.GetKey()), string(r.GetValue()), r.GetSession().GetSeq()
		clientID = r.GetSession().GetClientId()
	case *slotkvv1.DeleteRequest:
		key, seq = string(r.GetKey()), r.GetSession().GetSeq()
		clientID = r.GetSession().GetClientId()
	default:
		return false
	}
	identity := fmt.Sprintf("%s|%s|%s|%s|%d", method, clientID, key, value, seq)
	mu.Lock()
	attempt := trials[identity]
	trials[identity] = attempt + 1
	mu.Unlock()
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d|%s|%d", seed, identity, attempt)
	x := h.Sum64() + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return float64(x)/float64(^uint64(0)) < probability
}

func (c *cluster) dialer(ctx context.Context, addr string) (net.Conn, error) {
	c.mu.RLock()
	in := c.incByAddr(addr)
	c.mu.RUnlock()
	if in == nil || in.ln == nil {
		return nil, fmt.Errorf("sim: node %s is down", addr)
	}
	return in.ln.DialContext(ctx)
}
func (c *cluster) incByAddr(addr string) *incarnation {
	for _, nd := range c.topo.Nodes {
		if nd.GRPCAddr == addr {
			return c.inc[nd.ID]
		}
	}
	return nil
}

func Run(ctx context.Context, cfg Config) (res Result) {
	res = Result{Seed: cfg.Seed}
	if cfg.Nodes == 0 {
		cfg.Nodes = 3
	}
	if cfg.Groups == 0 {
		cfg.Groups = 4
	}
	if cfg.Duration == 0 {
		cfg.Duration = 6 * time.Second
	}
	if cfg.CheckTimeout == 0 {
		cfg.CheckTimeout = 30 * time.Second
	}
	if cfg.ReadMode == "" {
		cfg.ReadMode = "verify"
	}
	if cfg.Nodes < 3 || cfg.Groups < 1 || cfg.Duration < 0 || cfg.CheckTimeout < 0 || math.IsNaN(cfg.DropProbability) || math.IsInf(cfg.DropProbability, 0) || cfg.DropProbability < 0 || cfg.DropProbability > 1 {
		res.Err = "need at least 3 nodes, 1 group, nonnegative duration and check timeout, and drop probability from 0 to 1"
		return res
	}
	if cfg.ReadMode != "verify" && cfg.ReadMode != "any" {
		res.Err = "read mode must be verify or any"
		return res
	}
	for _, f := range cfg.Faults {
		if f != "crash" && f != "partition" && f != "drop" {
			res.Err = "unknown fault " + f
			return res
		}
	}
	root, err := os.MkdirTemp("", "slotkv-sim-")
	if err != nil {
		res.Err = err.Error()
		return res
	}
	defer os.RemoveAll(root)
	cl := &cluster{inc: map[string]*incarnation{}}
	for i := 0; i < cfg.Nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		cl.all = append(cl.all, id)
	}
	nds := make([]topology.Node, cfg.Nodes)
	for i, id := range cl.all {
		nds[i] = topology.Node{ID: id, GRPCAddr: id + ":7000", RaftAddr: id + ":7001"}
	}
	cl.topo, err = topology.Build(nds, cfg.Groups, 3, 1024)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	dropEnabled := false
	for _, f := range cfg.Faults {
		if f == "drop" {
			dropEnabled = true
		}
	}
	var dropMu sync.Mutex
	dropTrials := map[string]uint64{}
	var crashes, partitions, droppedResponses atomic.Int64
	defer func() {
		res.Crashes = int(crashes.Load())
		res.Partitions = int(partitions.Load())
		res.DroppedResponses = int(droppedResponses.Load())
	}()
	startNode := func(id string) error {
		ln := bufconn.Listen(1 << 20)
		tr := map[int]*raft.InmemTransport{}
		for _, g := range cl.topo.GroupsOn(id) {
			_, t := raft.NewInmemTransport(raft.ServerAddress(cl.topo.Nodes[mustIndex(cl.topo, id)].RaftAddr))
			tr[g] = t
		}
		cfgNode := node.Config{NodeID: id, Topology: cl.topo, DataDir: filepath.Join(root, id), GRPCListener: ln, RaftTransport: func(g int) (raft.Transport, error) {
			t := tr[g]
			if t == nil {
				return nil, fmt.Errorf("missing transport for group %d", g)
			}
			return t, nil
		}, PeerDialOptions: []grpc.DialOption{grpc.WithContextDialer(cl.dialer)}, HeartbeatTimeout: 100 * time.Millisecond, ElectionTimeout: 100 * time.Millisecond, LeaderLeaseTimeout: 50 * time.Millisecond, CommitTimeout: 25 * time.Millisecond, BalanceInterval: time.Second, DisableDedup: !cfg.Dedup, ReadMode: node.ReadMode(cfg.ReadMode), Logger: slog.New(slog.DiscardHandler), RaftLogLevel: "off", UnaryInterceptors: []grpc.UnaryServerInterceptor{func(c context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			out, e := handler(c, req)
			if e == nil && dropEnabled && cfg.DropProbability > 0 && (strings.HasSuffix(info.FullMethod, "/Put") || strings.HasSuffix(info.FullMethod, "/Delete")) {
				if shouldDrop(cfg.Seed, cfg.DropProbability, info.FullMethod, req, dropTrials, &dropMu) {
					droppedResponses.Add(1)
					return nil, status.Error(codes.Unavailable, "simulated lost response")
				}
			}
			return out, e
		}}}
		n, err := node.New(cfgNode)
		if err != nil {
			ln.Close()
			return err
		}
		if err = n.Start(); err != nil {
			ln.Close()
			for _, t := range tr {
				t.Close()
			}
			return err
		}
		cl.mu.Lock()
		cl.inc[id] = &incarnation{n: n, ln: ln, trans: tr}
		cl.mu.Unlock()
		cl.connectAll()
		return nil
	}
	for _, id := range cl.all {
		if err := startNode(id); err != nil {
			res.Err = err.Error()
			cl.stopAll()
			return res
		}
	}
	defer cl.stopAll()
	if err := cl.waitReady(ctx, 10*time.Second); err != nil {
		res.Err = err.Error()
		return res
	}
	// Client's dialer resolves each node to the currently live incarnation.
	seedCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var cli *client.Client
	for {
		cli, err = client.Dial(seedCtx, []string{cl.topo.Nodes[0].GRPCAddr}, client.Options{DialOptions: []grpc.DialOption{grpc.WithContextDialer(cl.dialer)}, ReadAnyReplica: cfg.ReadMode == "any"})
		if err == nil {
			break
		}
		if seedCtx.Err() != nil {
			res.Err = err.Error()
			return res
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer cli.Close()
	recorder := lincheck.NewRecorder()
	var failed atomic.Int64
	var successReads, successWrites atomic.Int64
	workCtx, stopWork := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() {
		stopWork()
		workers.Wait()
	}()
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(cid int) {
			defer workers.Done()
			s := cli.NewSession()
			wrng := rand.New(rand.NewSource(cfg.Seed + int64(cid) + 1))
			var opSeq uint64
			for {
				select {
				case <-workCtx.Done():
					return
				default:
				}
				key := []byte(fmt.Sprintf("key-%d", wrng.Intn(8)))
				opSeq++
				typ := wrng.Intn(10)
				var in lincheck.Input
				if typ < 5 {
					in = lincheck.Input{Op: lincheck.Get, Key: string(key)}
				} else if typ == 5 {
					in = lincheck.Input{Op: lincheck.Delete, Key: string(key)}
				} else {
					v := fmt.Sprintf("v-%d-%d-%d", cfg.Seed, cid, opSeq)
					in = lincheck.Input{Op: lincheck.Put, Key: string(key), Value: v}
				}
				h := recorder.Begin(cid, in)
				opctx, cc := context.WithTimeout(workCtx, time.Duration(1+wrng.Intn(2))*time.Second)
				var out lincheck.Output
				var e error
				if in.Op == lincheck.Get {
					v, found, getErr := s.Get(opctx, key)
					out.Value, out.Found, e = string(v), found, getErr
				} else if in.Op == lincheck.Delete {
					out.Existed, e = s.Delete(opctx, key)
				} else {
					e = s.Put(opctx, key, []byte(in.Value))
				}
				cc()
				if e != nil {
					failed.Add(1)
				} else if in.Op == lincheck.Get {
					successReads.Add(1)
				} else {
					successWrites.Add(1)
				}
				recorder.End(h, out, e)
				select {
				case <-workCtx.Done():
					return
				case <-time.After(120 * time.Millisecond):
				}
			}
		}(i)
	}
	// Fault loop uses seeded pauses. A partition/crash lasts 300–700ms.
	faultDone := make(chan struct{})
	go func() {
		defer close(faultDone)
		end := time.Now().Add(cfg.Duration)
		for time.Now().Before(end) {
			delay := time.Duration(300+rng.Intn(401)) * time.Millisecond
			select {
			case <-workCtx.Done():
				return
			case <-time.After(delay):
			}
			if len(cfg.Faults) == 0 {
				continue
			}
			f := cfg.Faults[rng.Intn(len(cfg.Faults))]
			idx := rng.Intn(len(cl.all))
			id := cl.all[idx]
			switch f {
			case "drop": // Interceptor controls this independently.
			case "partition":
				partitions.Add(1)
				cl.partition(id, true)
				time.Sleep(time.Duration(300+rng.Intn(401)) * time.Millisecond)
				cl.partition(id, false)
			case "crash":
				crashes.Add(1)
				cl.crash(id)
				time.Sleep(time.Duration(300+rng.Intn(401)) * time.Millisecond)
				if e := startNode(id); e != nil {
					res.Err = e.Error()
					return
				}
			}
		}
	}()
	timer := time.NewTimer(cfg.Duration)
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
	timer.Stop()
	<-faultDone
	if res.Err != "" {
		stopWork()
		workers.Wait()
		return res
	}
	if err := ctx.Err(); err != nil {
		stopWork()
		workers.Wait()
		res.Err = err.Error()
		return res
	}
	cl.partitionAll(false)
	// Restart nodes one at a time to exercise durable recovery after healing.
	for _, id := range cl.all {
		cl.crash(id)
		if e := startNode(id); e != nil {
			res.Err = e.Error()
			return res
		}
	}
	if err := cl.waitReady(ctx, 10*time.Second); err != nil {
		res.Err = err.Error()
		return res
	}
	settle := time.NewTimer(time.Second)
	select {
	case <-settle.C:
	case <-ctx.Done():
	}
	settle.Stop()
	if err := ctx.Err(); err != nil {
		stopWork()
		workers.Wait()
		res.Err = err.Error()
		return res
	}
	stopWork()
	workers.Wait()
	ops := recorder.Operations()
	res.Ops = len(ops)
	res.FailedOps = int(failed.Load())
	res.SuccessReads = int(successReads.Load())
	res.SuccessWrites = int(successWrites.Load())
	if res.SuccessReads == 0 || res.SuccessWrites == 0 {
		res.Err = fmt.Sprintf("sim: no successful operations of both kinds (reads=%d writes=%d)", res.SuccessReads, res.SuccessWrites)
		return res
	}
	started := time.Now()
	verdict, info := lincheck.Check(ops, cfg.CheckTimeout)
	res.CheckTime = time.Since(started)
	switch verdict {
	case porcupine.Ok:
		res.Verdict = "ok"
	case porcupine.Illegal:
		res.Verdict = "illegal"
	case porcupine.Unknown:
		res.Verdict = "unknown"
	}
	if verdict == porcupine.Illegal && cfg.VizPath != "" {
		if e := lincheck.Visualize(info, cfg.VizPath); e != nil {
			res.Err = e.Error()
		} else {
			res.VizPath = cfg.VizPath
		}
	}
	return res
}

func mustIndex(t *topology.Topology, id string) int {
	for i, n := range t.Nodes {
		if n.ID == id {
			return i
		}
	}
	panic("missing node")
}
func (c *cluster) connectAll() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, g := range c.topo.Groups {
		for i := 0; i < len(c.all); i++ {
			for j := i + 1; j < len(c.all); j++ {
				a, b := c.inc[c.all[i]], c.inc[c.all[j]]
				if a == nil || b == nil {
					continue
				}
				ta, tb := a.trans[g.ID], b.trans[g.ID]
				if ta != nil && tb != nil {
					ta.Connect(raft.ServerAddress(c.topo.Nodes[j].RaftAddr), tb)
					tb.Connect(raft.ServerAddress(c.topo.Nodes[i].RaftAddr), ta)
				}
			}
		}
	}
}

func (c *cluster) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		ready := make([]bool, len(c.topo.Groups))
		c.mu.RLock()
		for _, in := range c.inc {
			if in == nil || in.n == nil {
				continue
			}
			for _, g := range in.n.Status().Groups {
				if g.State == "Leader" && g.ReadReady {
					ready[g.GroupId] = true
				}
			}
		}
		c.mu.RUnlock()
		all := true
		for _, ok := range ready {
			all = all && ok
		}
		if all {
			return nil
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			return fmt.Errorf("sim: timed out waiting for ready Raft leaders")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (c *cluster) partition(id string, on bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	idx := mustIndex(c.topo, id)
	victim := c.inc[id]
	if victim == nil {
		return
	}
	for _, g := range c.topo.Groups {
		for j, peerID := range c.all {
			if j == idx {
				continue
			}
			peer := c.inc[peerID]
			if peer == nil {
				continue
			}
			a, b := victim.trans[g.ID], peer.trans[g.ID]
			if a == nil || b == nil {
				continue
			}
			addrA := raft.ServerAddress(c.topo.Nodes[idx].RaftAddr)
			addrB := raft.ServerAddress(c.topo.Nodes[j].RaftAddr)
			if on {
				a.Disconnect(addrB)
				b.Disconnect(addrA)
			} else {
				a.Connect(addrB, b)
				b.Connect(addrA, a)
			}
		}
	}
}
func (c *cluster) partitionAll(on bool) {
	if on {
		for _, id := range c.all {
			c.partition(id, true)
		}
	} else {
		for _, id := range c.all {
			c.partition(id, false)
		}
	}
}
func (c *cluster) crash(id string) {
	c.partition(id, true)
	c.mu.Lock()
	in := c.inc[id]
	delete(c.inc, id)
	c.mu.Unlock()
	if in != nil {
		in.n.Kill()
	}
}
func (c *cluster) stopAll() {
	for _, id := range c.all {
		c.crash(id)
	}
}
