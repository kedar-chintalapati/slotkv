package client_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kedar-chintalapati/slotkv/client"
	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
	"github.com/kedar-chintalapati/slotkv/internal/topology"
)

// fakeCluster is an in-memory slotkv cluster behind bufconn. Each group has one
// leader; other nodes answer NotLeader. Writes are deduplicated by session the
// way the real state machine does it, and every write RPC is logged.
type fakeCluster struct {
	t     *testing.T
	topo  *topology.Topology
	lis   map[string]*bufconn.Listener // by gRPC address
	nodes map[string]*fakeNode         // by node ID

	mu       sync.Mutex
	leader   map[int]string // actual leader per group
	reported map[int]string // leader per group as announced by Topology
	data     map[string]string
	applied  map[string]uint64 // client ID -> last applied seq
	existed  map[string]bool   // client ID -> result of its last applied Delete
	writes   []write
	applies  int
	noHint   atomic.Bool // NotLeader errors omit the leader
	anyRead  atomic.Bool // every node serves Gets
	badSlots atomic.Bool // Topology reports a slot map that does not match the placement
}

type write struct {
	node, client string
	seq          uint64
}

type fakeNode struct {
	slotkvv1.UnimplementedKVServiceServer
	id string
	c  *fakeCluster

	calls    atomic.Int64 // Get, Put, and Delete RPCs received
	fail     atomic.Int64 // fail this many upcoming calls with failCode
	failCode atomic.Int32
	drop     atomic.Int64 // lose the response of this many upcoming writes after applying them
	hang     atomic.Bool  // never answer
}

func newCluster(t *testing.T, numNodes, numGroups int) *fakeCluster {
	t.Helper()
	nodes := make([]topology.Node, numNodes)
	for i := range nodes {
		id := fmt.Sprintf("n%d", i)
		nodes[i] = topology.Node{ID: id, GRPCAddr: id + ":7000", RaftAddr: id + ":7001"}
	}
	topo, err := topology.Build(nodes, numGroups, min(3, numNodes), 64)
	if err != nil {
		t.Fatal(err)
	}
	c := &fakeCluster{
		t: t, topo: topo,
		lis:      map[string]*bufconn.Listener{},
		nodes:    map[string]*fakeNode{},
		leader:   map[int]string{},
		reported: map[int]string{},
		data:     map[string]string{},
		applied:  map[string]uint64{},
		existed:  map[string]bool{},
	}
	for g := range topo.Groups {
		c.leader[g] = topo.PreferredLeader(g)
		c.reported[g] = c.leader[g]
	}
	for _, n := range nodes {
		fn := &fakeNode{id: n.ID, c: c}
		lis := bufconn.Listen(1 << 16)
		srv := grpc.NewServer()
		slotkvv1.RegisterKVServiceServer(srv, fn)
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		c.lis[n.GRPCAddr] = lis
		c.nodes[n.ID] = fn
	}
	return c
}

// dial connects a client through bufconn. Without seeds it uses node n0.
func (c *fakeCluster) dial(opts client.Options, seeds ...string) (*client.Client, error) {
	opts.DialOptions = append(opts.DialOptions, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		lis, ok := c.lis[addr]
		if !ok {
			return nil, fmt.Errorf("no listener at %s", addr)
		}
		return lis.DialContext(ctx)
	}))
	if len(seeds) == 0 {
		seeds = []string{"n0:7000"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl, err := client.Dial(ctx, seeds, opts)
	if err == nil {
		c.t.Cleanup(func() { cl.Close() })
	}
	return cl, err
}

func (c *fakeCluster) mustDial(opts client.Options, seeds ...string) *client.Client {
	c.t.Helper()
	cl, err := c.dial(opts, seeds...)
	if err != nil {
		c.t.Fatal(err)
	}
	return cl
}

// replicasOf returns the group of key and its replica IDs, preferred leader first.
func (c *fakeCluster) replicasOf(key []byte) (group int, ids []string) {
	g := c.topo.GroupForKey(key)
	return g, c.topo.Groups[g].Replicas
}

func (c *fakeCluster) applyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applies
}

func (c *fakeCluster) setLeader(group int, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader[group] = id
}

func (c *fakeCluster) report(group int, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reported[group] = id
}

// calls returns how many KV RPCs each node has received.
func (c *fakeCluster) calls(ids ...string) []int64 {
	out := make([]int64, len(ids))
	for i, id := range ids {
		out[i] = c.nodes[id].calls.Load()
	}
	return out
}

func (c *fakeCluster) failAll(code codes.Code) {
	for _, n := range c.nodes {
		n.failCode.Store(int32(code))
		n.fail.Store(1 << 40)
	}
}

func (c *fakeCluster) writeLog() []write {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]write(nil), c.writes...)
}

// gate counts a call and applies any injected fault.
func (n *fakeNode) gate(ctx context.Context) error {
	n.calls.Add(1)
	if n.hang.Load() {
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	if n.fail.Add(-1) >= 0 {
		return status.Error(codes.Code(n.failCode.Load()), "injected failure")
	}
	return nil
}

// requireLeader returns the NotLeader error a follower would send.
func (n *fakeNode) requireLeader(key []byte) error {
	c := n.c
	g := c.topo.GroupForKey(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	lead := c.leader[g]
	if lead == n.id {
		return nil
	}
	nl := &slotkvv1.NotLeader{GroupId: uint32(g)}
	if lead != "" && !c.noHint.Load() {
		node, _ := c.topo.Node(lead)
		nl.LeaderId, nl.LeaderAddr = lead, node.GRPCAddr
	}
	st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(nl)
	if err != nil {
		c.t.Error(err)
	}
	return st.Err()
}

// apply logs a write and applies it unless the session already did.
func (n *fakeNode) apply(s *slotkvv1.Session, key []byte, value string, del bool) (existed bool) {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, write{n.id, s.ClientId, s.Seq})
	if s.Seq <= c.applied[s.ClientId] {
		return c.existed[s.ClientId]
	}
	c.applied[s.ClientId] = s.Seq
	c.applies++
	_, existed = c.data[string(key)]
	if del {
		delete(c.data, string(key))
	} else {
		c.data[string(key)] = value
	}
	c.existed[s.ClientId] = existed
	return existed
}

func (n *fakeNode) Put(ctx context.Context, req *slotkvv1.PutRequest) (*slotkvv1.PutResponse, error) {
	if err := n.gate(ctx); err != nil {
		return nil, err
	}
	if err := n.requireLeader(req.Key); err != nil {
		return nil, err
	}
	n.apply(req.Session, req.Key, string(req.Value), false)
	if n.drop.Add(-1) >= 0 {
		return nil, status.Error(codes.Unavailable, "response lost")
	}
	return &slotkvv1.PutResponse{}, nil
}

func (n *fakeNode) Delete(ctx context.Context, req *slotkvv1.DeleteRequest) (*slotkvv1.DeleteResponse, error) {
	if err := n.gate(ctx); err != nil {
		return nil, err
	}
	if err := n.requireLeader(req.Key); err != nil {
		return nil, err
	}
	existed := n.apply(req.Session, req.Key, "", true)
	return &slotkvv1.DeleteResponse{Existed: existed}, nil
}

func (n *fakeNode) Get(ctx context.Context, req *slotkvv1.GetRequest) (*slotkvv1.GetResponse, error) {
	if err := n.gate(ctx); err != nil {
		return nil, err
	}
	if !n.c.anyRead.Load() {
		if err := n.requireLeader(req.Key); err != nil {
			return nil, err
		}
	}
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	v, ok := n.c.data[string(req.Key)]
	return &slotkvv1.GetResponse{Value: []byte(v), Found: ok}, nil
}

func (n *fakeNode) Topology(context.Context, *slotkvv1.TopologyRequest) (*slotkvv1.TopologyResponse, error) {
	c := n.c
	c.mu.Lock()
	defer c.mu.Unlock()
	resp := &slotkvv1.TopologyResponse{NumSlots: uint32(c.topo.NumSlots)}
	for _, nd := range c.topo.Nodes {
		resp.Nodes = append(resp.Nodes, &slotkvv1.Node{Id: nd.ID, GrpcAddr: nd.GRPCAddr, RaftAddr: nd.RaftAddr})
	}
	for _, g := range c.topo.Groups {
		resp.Groups = append(resp.Groups, &slotkvv1.Group{Id: uint32(g.ID), Replicas: g.Replicas, LeaderId: c.reported[g.ID]})
	}
	for _, g := range c.topo.SlotToGroup {
		resp.SlotToGroup = append(resp.SlotToGroup, uint32(g))
	}
	if c.badSlots.Load() {
		resp.SlotToGroup[0]++
	}
	return resp, nil
}
