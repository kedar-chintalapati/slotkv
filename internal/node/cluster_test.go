package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
	"github.com/kedar-chintalapati/slotkv/internal/topology"
)

const clusterSize = 3

// testCluster is clusterSize nodes in one process, talking over real
// localhost TCP, with short Raft timeouts.
type testCluster struct {
	t         *testing.T
	topo      *topology.Topology
	dirs      []string
	httpAddrs []string
	nodes     []*Node
	log       *slog.Logger
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// newTestCluster starts a three-node cluster hosting numGroups groups.
func newTestCluster(t *testing.T, numGroups int, tweak func(*Config)) *testCluster {
	t.Helper()
	logs := &syncBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			s := logs.String()
			t.Logf("node logs (tail):\n%s", s[max(0, len(s)-20000):])
		}
	})
	c := &testCluster{t: t, log: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}

	var gl, rl, hl [clusterSize]net.Listener
	peers := make([]topology.Node, clusterSize)
	for i := range peers {
		gl[i], rl[i], hl[i] = listenLocal(t), listenLocal(t), listenLocal(t)
		peers[i] = topology.Node{ID: fmt.Sprintf("n%d", i), GRPCAddr: gl[i].Addr().String(), RaftAddr: rl[i].Addr().String()}
		c.dirs = append(c.dirs, t.TempDir())
		c.httpAddrs = append(c.httpAddrs, hl[i].Addr().String())
	}
	topo, err := topology.Build(peers, numGroups, 3, 64)
	if err != nil {
		t.Fatal(err)
	}
	c.topo = topo
	for i := range peers {
		cfg := c.config(i, tweak)
		cfg.GRPCListener, cfg.RaftListener, cfg.HTTPListener = gl[i], rl[i], hl[i]
		c.nodes = append(c.nodes, c.start(cfg))
	}
	return c
}

func (c *testCluster) config(i int, tweak func(*Config)) Config {
	cfg := Config{
		NodeID:             c.topo.Nodes[i].ID,
		Topology:           c.topo,
		DataDir:            c.dirs[i],
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    100 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		BalanceInterval:    100 * time.Millisecond,
		Logger:             c.log,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	return cfg
}

func (c *testCluster) start(cfg Config) *Node {
	c.t.Helper()
	n, err := New(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(n.Kill)
	if err := n.Start(); err != nil {
		c.t.Fatal(err)
	}
	return n
}

// restart starts node i again on its old data directory and addresses.
func (c *testCluster) restart(i int, tweak func(*Config)) {
	c.t.Helper()
	cfg := c.config(i, tweak)
	cfg.GRPCListenAddr = c.topo.Nodes[i].GRPCAddr
	cfg.RaftListenAddr = c.topo.Nodes[i].RaftAddr
	c.nodes[i] = c.start(cfg)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *testCluster) index(id string) int {
	for i, nd := range c.topo.Nodes {
		if nd.ID == id {
			return i
		}
	}
	return -1
}

// leaderOf returns the index of the node leading group g, or -1.
func (c *testCluster) leaderOf(g int) int {
	for i, n := range c.nodes {
		if r := n.Raft(g); r != nil && r.State() == raft.Leader {
			return i
		}
	}
	return -1
}

// waitPreferred waits until each group is led, read-ready, by its preferred node.
func (c *testCluster) waitPreferred(timeout time.Duration) {
	c.t.Helper()
	waitFor(c.t, timeout, "every group led by its preferred node", func() bool {
		for _, g := range c.topo.Groups {
			n := c.nodes[c.index(c.topo.PreferredLeader(g.ID))]
			if gr := n.group(g.ID); gr == nil || !gr.ready() {
				return false
			}
		}
		return true
	})
}

func (c *testCluster) waitReady(i int) {
	c.t.Helper()
	waitFor(c.t, 15*time.Second, fmt.Sprintf("node %d to be ready", i), c.nodes[i].Ready)
}

// kvClient is a minimal client: it follows NotLeader hints and retries other
// errors until the deadline. A client has one request outstanding at a time.
type kvClient struct {
	topo   *topology.Topology
	id     string
	seq    uint64
	conns  map[string]*grpc.ClientConn
	leader map[int]string
}

func newKVClient(t *testing.T, topo *topology.Topology, id string) *kvClient {
	c := &kvClient{topo: topo, id: id, conns: map[string]*grpc.ClientConn{}, leader: map[int]string{}}
	t.Cleanup(func() {
		for _, cc := range c.conns {
			cc.Close()
		}
	})
	return c
}

func (c *kvClient) kv(addr string) slotkvv1.KVServiceClient {
	cc, ok := c.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient("passthrough:///"+addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 20 * time.Millisecond, Multiplier: 1.6, MaxDelay: 200 * time.Millisecond},
				MinConnectTimeout: time.Second,
			}))
		if err != nil {
			panic(err)
		}
		c.conns[addr] = cc
	}
	return slotkvv1.NewKVServiceClient(cc)
}

func leaderHint(err error) string {
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		return ""
	}
	for _, d := range st.Details() {
		if nl, ok := d.(*slotkvv1.NotLeader); ok {
			return nl.GetLeaderAddr()
		}
	}
	return ""
}

func (c *kvClient) call(key string, timeout time.Duration, fn func(context.Context, slotkvv1.KVServiceClient) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	gid := c.topo.GroupForKey([]byte(key))
	replicas := c.topo.Groups[gid].Replicas
	var last error
	for try := 0; ; try++ {
		addr := c.leader[gid]
		if addr == "" {
			nd, _ := c.topo.Node(replicas[try%len(replicas)])
			addr = nd.GRPCAddr
		}
		actx, acancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := fn(actx, c.kv(addr))
		acancel()
		if err == nil {
			return nil
		}
		last = err
		if ctx.Err() != nil {
			return fmt.Errorf("%w; last error: %v", ctx.Err(), last)
		}
		if status.Code(err) == codes.InvalidArgument {
			return err
		}
		if hint := leaderHint(err); hint != "" && hint != addr {
			c.leader[gid] = hint
			continue
		}
		delete(c.leader, gid)
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *kvClient) put(key, value string, timeout time.Duration) error {
	c.seq++
	req := &slotkvv1.PutRequest{Session: &slotkvv1.Session{ClientId: c.id, Seq: c.seq}, Key: []byte(key), Value: []byte(value)}
	return c.call(key, timeout, func(ctx context.Context, kv slotkvv1.KVServiceClient) error {
		_, err := kv.Put(ctx, req)
		return err
	})
}

func (c *kvClient) del(key string, timeout time.Duration) (existed bool, err error) {
	c.seq++
	req := &slotkvv1.DeleteRequest{Session: &slotkvv1.Session{ClientId: c.id, Seq: c.seq}, Key: []byte(key)}
	err = c.call(key, timeout, func(ctx context.Context, kv slotkvv1.KVServiceClient) error {
		resp, err := kv.Delete(ctx, req)
		existed = resp.GetExisted()
		return err
	})
	return existed, err
}

func (c *kvClient) get(key string, timeout time.Duration) (value string, found bool, err error) {
	req := &slotkvv1.GetRequest{Key: []byte(key)}
	err = c.call(key, timeout, func(ctx context.Context, kv slotkvv1.KVServiceClient) error {
		resp, err := kv.Get(ctx, req)
		value, found = string(resp.GetValue()), resp.GetFound()
		return err
	})
	return value, found, err
}

func mustPut(t *testing.T, c *kvClient, key, value string, timeout time.Duration) {
	t.Helper()
	if err := c.put(key, value, timeout); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func mustGet(t *testing.T, c *kvClient, key, want string) {
	t.Helper()
	v, found, err := c.get(key, 5*time.Second)
	if err != nil || !found || v != want {
		t.Fatalf("get %s = %q, %v, %v; want %q", key, v, found, err, want)
	}
}

// writer puts unique keys back to back until stopped, counting requests that
// failed to succeed within the per-request timeout.
type writer struct {
	cl      *kvClient
	timeout time.Duration
	stop    chan struct{}
	done    chan struct{}
	written atomic.Int64
	failed  atomic.Int64
	slowest atomic.Int64 // longest request, in nanoseconds
	mu      sync.Mutex
	errs    []error
}

func startWriter(t *testing.T, topo *topology.Topology, name string, timeout time.Duration) *writer {
	w := &writer{cl: newKVClient(t, topo, name), timeout: timeout, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for i := 0; ; i++ {
			select {
			case <-w.stop:
				return
			default:
			}
			key := fmt.Sprintf("%s-%d", name, i)
			start := time.Now()
			err := w.cl.put(key, "v"+key, timeout)
			if d := int64(time.Since(start)); d > w.slowest.Load() {
				w.slowest.Store(d)
			}
			if err != nil {
				w.failed.Add(1)
				w.mu.Lock()
				w.errs = append(w.errs, fmt.Errorf("%s: %w", key, err))
				w.mu.Unlock()
				continue
			}
			w.written.Add(1)
		}
	}()
	return w
}

// finish stops the writer and returns the keys it wrote.
func (w *writer) finish() int {
	close(w.stop)
	<-w.done
	return int(w.written.Load())
}

func TestClusterPutGetDelete(t *testing.T) {
	c := newTestCluster(t, 4, nil)
	c.waitPreferred(15 * time.Second)
	cl := newKVClient(t, c.topo, "client-1")

	seen := map[int]bool{}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key-%d", i)
		seen[c.topo.GroupForKey([]byte(key))] = true
		mustPut(t, cl, key, "value-"+key, 5*time.Second)
	}
	if len(seen) != 4 {
		t.Fatalf("keys covered %d groups, want 4", len(seen))
	}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key-%d", i)
		mustGet(t, cl, key, "value-"+key)
	}
	mustPut(t, cl, "key-0", "rewritten", 5*time.Second)
	mustGet(t, cl, "key-0", "rewritten")
	if existed, err := cl.del("key-0", 5*time.Second); err != nil || !existed {
		t.Fatalf("delete = %v, %v; want existed", existed, err)
	}
	if existed, err := cl.del("key-0", 5*time.Second); err != nil || existed {
		t.Fatalf("second delete = %v, %v; want absent", existed, err)
	}
	if _, found, err := cl.get("key-0", 5*time.Second); err != nil || found {
		t.Fatalf("get after delete = %v, %v", found, err)
	}

	// A write needs a session.
	g := c.topo.GroupForKey([]byte("key-1"))
	leader := c.nodes[c.leaderOf(g)]
	kv := cl.kv(c.topo.Nodes[c.index(leader.id)].GRPCAddr)
	_, err := kv.Put(context.Background(), &slotkvv1.PutRequest{Key: []byte("key-1"), Value: []byte("x")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("put without a session = %v, want InvalidArgument", err)
	}

	// A retry (same session and seq) takes effect once, and repeats its result.
	ctx := context.Background()
	retryPut := func(seq uint64, value string) error {
		_, err := kv.Put(ctx, &slotkvv1.PutRequest{Session: &slotkvv1.Session{ClientId: "retrier", Seq: seq}, Key: []byte("key-1"), Value: []byte(value)})
		return err
	}
	retryDel := func(seq uint64) bool {
		resp, err := kv.Delete(ctx, &slotkvv1.DeleteRequest{Session: &slotkvv1.Session{ClientId: "retrier", Seq: seq}, Key: []byte("key-1")})
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		return resp.Existed
	}
	if err := retryPut(1, "first"); err != nil {
		t.Fatal(err)
	}
	if err := retryPut(1, "second"); err != nil {
		t.Fatal(err)
	}
	mustGet(t, cl, "key-1", "first")
	if !retryDel(2) || !retryDel(2) {
		t.Fatal("a retried delete did not repeat its result")
	}
	if _, found, err := cl.get("key-1", 5*time.Second); err != nil || found {
		t.Fatalf("get after deduplicated delete = %v, %v", found, err)
	}

	// A follower answers NotLeader and names the leader.
	for i, n := range c.nodes {
		if n == leader {
			continue
		}
		kv := cl.kv(c.topo.Nodes[i].GRPCAddr)
		waitFor(t, 5*time.Second, "follower to learn the leader", func() bool {
			_, err := kv.Get(context.Background(), &slotkvv1.GetRequest{Key: []byte("key-1")})
			return leaderHint(err) != ""
		})
		wantAddr := c.topo.Nodes[c.index(leader.id)].GRPCAddr
		for _, call := range []func() error{
			func() error {
				_, err := kv.Get(context.Background(), &slotkvv1.GetRequest{Key: []byte("key-1")})
				return err
			},
			func() error {
				_, err := kv.Put(context.Background(), &slotkvv1.PutRequest{Session: &slotkvv1.Session{ClientId: "x", Seq: 1}, Key: []byte("key-1")})
				return err
			},
		} {
			err := call()
			if status.Code(err) != codes.FailedPrecondition || leaderHint(err) != wantAddr {
				t.Fatalf("follower %s answered %v, want NotLeader hinting %s", n.id, err, wantAddr)
			}
		}
	}

	// Topology and Status.
	topo, err := kv.Topology(context.Background(), &slotkvv1.TopologyRequest{})
	if err != nil || topo.NumSlots != 64 || len(topo.Groups) != 4 || len(topo.Nodes) != 3 || len(topo.SlotToGroup) != 64 {
		t.Fatalf("Topology = %v, %v", topo, err)
	}
	for _, grp := range topo.Groups {
		if want := c.topo.PreferredLeader(int(grp.Id)); grp.LeaderId != want {
			t.Fatalf("group %d leader as seen by %s = %q, want %q", grp.Id, leader.id, grp.LeaderId, want)
		}
	}
	for i, n := range c.nodes {
		st, err := cl.kv(c.topo.Nodes[i].GRPCAddr).Status(context.Background(), &slotkvv1.StatusRequest{})
		if err != nil || st.NodeId != n.id || st.Draining || len(st.Groups) != 4 {
			t.Fatalf("Status of %s = %v, %v", n.id, st, err)
		}
		for _, gs := range st.Groups {
			isLeader := c.topo.PreferredLeader(int(gs.GroupId)) == n.id
			if isLeader != (gs.State == "Leader") || isLeader != gs.ReadReady || gs.Term == 0 || gs.CommitIndex == 0 || gs.LastLogIndex == 0 {
				t.Fatalf("Status of %s group %d = %v", n.id, gs.GroupId, gs)
			}
		}
	}
}

func TestReadAnyServesFollowers(t *testing.T) {
	c := newTestCluster(t, 2, func(cfg *Config) { cfg.ReadMode = ReadAny })
	c.waitPreferred(15 * time.Second)
	cl := newKVClient(t, c.topo, "client-1")
	mustPut(t, cl, "k", "v", 5*time.Second)
	leader := c.leaderOf(c.topo.GroupForKey([]byte("k")))
	for i, nd := range c.topo.Nodes {
		if i == leader {
			continue
		}
		kv := cl.kv(nd.GRPCAddr)
		waitFor(t, 5*time.Second, "follower "+nd.ID+" to serve the value locally", func() bool {
			resp, err := kv.Get(context.Background(), &slotkvv1.GetRequest{Key: []byte("k")})
			return err == nil && resp.Found && string(resp.Value) == "v"
		})
	}
}

func TestHTTPEndpoints(t *testing.T) {
	c := newTestCluster(t, 4, nil)
	c.waitPreferred(15 * time.Second)
	get := func(i int, path string) (int, string) {
		resp, err := http.Get("http://" + c.httpAddrs[i] + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := get(0, "/healthz"); code != 200 {
		t.Fatalf("/healthz = %d", code)
	}
	waitFor(t, 10*time.Second, "/readyz to return 200", func() bool {
		code, _ := get(1, "/readyz")
		return code == 200
	})
	code, body := get(0, "/status")
	var st slotkvv1.StatusResponse
	if err := protojson.Unmarshal([]byte(body), &st); err != nil || code != 200 || st.NodeId != "n0" || len(st.Groups) != 4 {
		t.Fatalf("/status = %d %s (%v)", code, body, err)
	}
	// A draining node reports not-ready while still healthy.
	c.nodes[2].draining.Store(true)
	if code, _ := get(2, "/readyz"); code != 503 {
		t.Fatalf("/readyz while draining = %d, want 503", code)
	}
	if code, _ := get(2, "/healthz"); code != 200 {
		t.Fatalf("/healthz while draining = %d", code)
	}
}

func TestKillAndRestart(t *testing.T) {
	c := newTestCluster(t, 4, nil)
	c.waitPreferred(15 * time.Second)
	cl := newKVClient(t, c.topo, "client-1")
	for i := 0; i < 20; i++ {
		mustPut(t, cl, fmt.Sprintf("before-%d", i), "x", 5*time.Second)
	}
	// Take snapshots so the restart restores one and replays the log after it.
	for g := range c.topo.Groups {
		if err := c.nodes[0].Raft(g).Snapshot().Error(); err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
			t.Fatalf("snapshot group %d: %v", g, err)
		}
	}
	mustPut(t, cl, "after-snapshot", "y", 5*time.Second)

	w := startWriter(t, c.topo, "w", 5*time.Second)
	waitFor(t, 5*time.Second, "writer to make progress", func() bool { return w.written.Load() > 5 })

	c.nodes[0].Kill() // n0 leads groups 0 and 3
	if got := c.nodes[0].Ready(); got {
		t.Fatal("killed node reports ready")
	}
	// Stay down long enough for the leaders' replication backoff to grow.
	mark := w.written.Load()
	time.Sleep(3 * time.Second)
	if w.written.Load() < mark+10 {
		t.Fatalf("only %d writes in 3s with one node down", w.written.Load()-mark)
	}
	for i := 0; i < 20; i++ {
		mustPut(t, cl, fmt.Sprintf("during-%d", i), "z", 5*time.Second)
	}

	c.restart(0, nil)
	c.waitReady(0)
	mark = w.written.Load()
	waitFor(t, 10*time.Second, "writes after restart", func() bool { return w.written.Load() > mark+10 })
	c.waitPreferred(15 * time.Second) // the balancer hands leadership back

	n := w.finish()
	t.Logf("%d writes, slowest took %v", n, time.Duration(w.slowest.Load()))
	if f := w.failed.Load(); f != 0 {
		t.Fatalf("%d writes failed: %v", f, w.errs)
	}
	for i := 0; i < 20; i++ {
		mustGet(t, cl, fmt.Sprintf("before-%d", i), "x")
		mustGet(t, cl, fmt.Sprintf("during-%d", i), "z")
	}
	mustGet(t, cl, "after-snapshot", "y")
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("w-%d", i)
		mustGet(t, cl, key, "v"+key)
	}

	// The restarted node holds the same data once it has caught up.
	for g := range c.topo.Groups {
		leader := c.nodes[c.leaderOf(g)].Raft(g)
		waitFor(t, 5*time.Second, fmt.Sprintf("n0 to apply group %d", g), func() bool {
			return c.nodes[0].Raft(g).AppliedIndex() >= leader.CommitIndex()
		})
	}
}

func TestGracefulShutdownMovesLeadership(t *testing.T) {
	c := newTestCluster(t, 4, nil)
	c.waitPreferred(15 * time.Second)
	victim := c.nodes[0]
	var led []int
	for g := range c.topo.Groups {
		if c.leaderOf(g) == 0 {
			led = append(led, g)
		}
	}
	if len(led) == 0 {
		t.Fatal("n0 leads no groups")
	}

	w := startWriter(t, c.topo, "w", time.Second)
	waitFor(t, 5*time.Second, "writer to make progress", func() bool { return w.written.Load() > 10 })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := victim.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for _, g := range led {
		if r := victim.Raft(g); r.State() != raft.Shutdown {
			t.Fatalf("group %d on the stopped node is %v", g, r.State())
		}
		waitFor(t, 5*time.Second, fmt.Sprintf("group %d to have a new leader", g), func() bool { return c.leaderOf(g) > 0 })
	}
	mark := w.written.Load()
	waitFor(t, 10*time.Second, "writes to continue", func() bool { return w.written.Load() > mark+10 })

	n := w.finish()
	t.Logf("%d writes, slowest took %v", n, time.Duration(w.slowest.Load()))
	if f := w.failed.Load(); f != 0 {
		t.Fatalf("%d of %d writes failed: %v", f, n, w.errs)
	}
	cl := newKVClient(t, c.topo, "reader")
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("w-%d", i)
		mustGet(t, cl, key, "v"+key)
	}
	// Shutdown is idempotent, and Kill after it is harmless.
	if err := victim.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	victim.Kill()
}

func TestNewValidation(t *testing.T) {
	topo, err := topology.Build([]topology.Node{{ID: "a", GRPCAddr: "a:1", RaftAddr: "a:2"}}, 1, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	ok := Config{NodeID: "a", Topology: topo, DataDir: t.TempDir(), GRPCListenAddr: ":0", RaftListenAddr: ":0"}
	if _, err := New(ok); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mod := range map[string]func(*Config){
		"no topology":       func(c *Config) { c.Topology = nil },
		"unknown node":      func(c *Config) { c.NodeID = "zzz" },
		"no data dir":       func(c *Config) { c.DataDir = "" },
		"no grpc address":   func(c *Config) { c.GRPCListenAddr = "" },
		"no raft address":   func(c *Config) { c.RaftListenAddr = "" },
		"bad shutdown":      func(c *Config) { c.Shutdown = "sometimes" },
		"bad readiness":     func(c *Config) { c.Readiness = "maybe" },
		"bad read mode":     func(c *Config) { c.ReadMode = "eventually" },
		"lease > heartbeat": func(c *Config) { c.HeartbeatTimeout = 100 * time.Millisecond },
	} {
		cfg := ok
		mod(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

func TestKillBeforeStartReleasesListeners(t *testing.T) {
	topo, err := topology.Build([]topology.Node{{ID: "a", GRPCAddr: "a:1", RaftAddr: "a:2"}}, 1, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	ln := listenLocal(t)
	n, err := New(Config{NodeID: "a", Topology: topo, DataDir: t.TempDir(), GRPCListener: ln, RaftListenAddr: ":0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	n.Kill()
	if _, err := ln.Accept(); err == nil {
		t.Fatal("listener still open after Kill")
	}
	if err := n.Start(); err == nil {
		t.Fatal("Start succeeded after Kill")
	}
	if n.Ready() || n.Raft(0) != nil {
		t.Fatal("stopped node reports state")
	}
}
