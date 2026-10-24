// Package client is the slotkv Go client. It sends each request to the Raft
// leader of the key's group, follows NotLeader hints, and retries until the
// caller's context ends. Writes carry session sequence numbers, so the
// duplicates that retries create are discarded by the server.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
	"github.com/kedar-chintalapati/slotkv/internal/topology"
)

const (
	defaultAttemptTimeout = 500 * time.Millisecond
	minRetryDelay         = 10 * time.Millisecond
	maxRetryDelay         = 100 * time.Millisecond
)

// ErrClosed is returned by requests made on, or interrupted by, a closed Client.
var ErrClosed = errors.New("slotkv: client closed")

// Options configures a Client. The zero value is usable.
type Options struct {
	AttemptTimeout time.Duration     // per-attempt RPC timeout; default 500ms
	DialOptions    []grpc.DialOption // appended to the defaults, so they can override them
	ReadAnyReplica bool              // send Gets to a random replica; a negative control only
	Logger         *slog.Logger
}

type replica struct {
	id string
	kv slotkvv1.KVServiceClient
}

// route is the client's view of one Raft group.
type route struct {
	replicas []replica
	leader   atomic.Int32 // index into replicas of the presumed leader
}

func (r *route) indexOf(id string) int {
	return slices.IndexFunc(r.replicas, func(x replica) bool { return x.id == id })
}

// Client routes requests to the nodes of a slotkv cluster. It is safe for
// concurrent use; the sessions it creates are not.
type Client struct {
	topo           *topology.Topology
	routes         []route // indexed by group ID
	conns          []*grpc.ClientConn
	attemptTimeout time.Duration
	readAny        bool
	log            *slog.Logger

	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// Dial asks the seed addresses in order, each for at most AttemptTimeout, for
// the cluster topology, and opens a connection to every node in it. It makes a
// single pass over the seeds; the caller may retry. The leader of each group is
// initially the one the answering seed reports, else the group's preferred
// replica.
func Dial(ctx context.Context, seeds []string, opts Options) (*Client, error) {
	if len(seeds) == 0 {
		return nil, errors.New("slotkv: no seed addresses")
	}
	c := &Client{
		attemptTimeout: opts.AttemptTimeout,
		readAny:        opts.ReadAnyReplica,
		log:            opts.Logger,
		done:           make(chan struct{}),
	}
	if c.attemptTimeout <= 0 {
		c.attemptTimeout = defaultAttemptTimeout
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  50 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
			MinConnectTimeout: time.Second,
		}),
	}, opts.DialOptions...)

	resp, err := fetchTopology(ctx, seeds, dialOpts, c.attemptTimeout)
	if err != nil {
		return nil, err
	}
	topo, err := buildTopology(resp)
	if err != nil {
		return nil, fmt.Errorf("slotkv: %w", err)
	}
	kvs := make(map[string]slotkvv1.KVServiceClient, len(topo.Nodes))
	for _, n := range topo.Nodes {
		cc, err := grpc.NewClient("passthrough:///"+n.GRPCAddr, dialOpts...)
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("slotkv: node %s: %w", n.ID, err)
		}
		cc.Connect()
		c.conns = append(c.conns, cc)
		kvs[n.ID] = slotkvv1.NewKVServiceClient(cc)
	}
	c.topo = topo
	c.routes = make([]route, len(topo.Groups))
	for g, grp := range topo.Groups {
		rt := &c.routes[g]
		for _, id := range grp.Replicas {
			rt.replicas = append(rt.replicas, replica{id: id, kv: kvs[id]})
		}
		rt.leader.Store(int32(max(0, slices.Index(grp.Replicas, resp.Groups[g].LeaderId))))
	}
	c.log.Info("connected", "nodes", len(topo.Nodes), "groups", len(topo.Groups), "slots", topo.NumSlots)
	return c, nil
}

// fetchTopology returns the first answer to Topology from the seeds.
func fetchTopology(ctx context.Context, seeds []string, opts []grpc.DialOption, timeout time.Duration) (*slotkvv1.TopologyResponse, error) {
	var errs []error
	for _, seed := range seeds {
		cc, err := grpc.NewClient("passthrough:///"+seed, opts...)
		if err != nil {
			errs = append(errs, fmt.Errorf("seed %s: %w", seed, err))
			continue
		}
		actx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := slotkvv1.NewKVServiceClient(cc).Topology(actx, &slotkvv1.TopologyRequest{})
		cancel()
		_ = cc.Close()
		if err == nil {
			return resp, nil
		}
		errs = append(errs, fmt.Errorf("seed %s: %w", seed, err))
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("slotkv: no seed answered Topology: %w", errors.Join(errs...))
}

// buildTopology rebuilds the placement from the node list and verifies that it
// is the one the server reported, so that client and server agree on where
// every key lives.
func buildTopology(r *slotkvv1.TopologyResponse) (*topology.Topology, error) {
	if r == nil {
		return nil, errors.New("topology response is nil")
	}
	if len(r.Groups) == 0 {
		return nil, errors.New("topology has no groups")
	}
	nodes := make([]topology.Node, len(r.Nodes))
	for i, n := range r.Nodes {
		if n == nil {
			return nil, fmt.Errorf("topology node %d is nil", i)
		}
		if n.GrpcAddr == "" || n.RaftAddr == "" {
			return nil, fmt.Errorf("topology node %q has an empty address", n.Id)
		}
		nodes[i] = topology.Node{ID: n.Id, GRPCAddr: n.GrpcAddr, RaftAddr: n.RaftAddr}
	}
	if r.Groups[0] == nil {
		return nil, errors.New("topology group 0 is nil")
	}
	t, err := topology.Build(nodes, len(r.Groups), len(r.Groups[0].Replicas), int(r.NumSlots))
	if err != nil {
		return nil, err
	}
	for i, g := range r.Groups {
		if g == nil {
			return nil, fmt.Errorf("topology group %d is nil", i)
		}
		if int(g.Id) != i || !slices.Equal(g.Replicas, t.Groups[i].Replicas) {
			return nil, fmt.Errorf("group %d replicas %v differ from computed placement %v", g.Id, g.Replicas, t.Groups[i].Replicas)
		}
		if g.LeaderId != "" && !slices.Contains(g.Replicas, g.LeaderId) {
			return nil, fmt.Errorf("group %d leader %q is not one of its replicas", g.Id, g.LeaderId)
		}
	}
	if !slices.EqualFunc(r.SlotToGroup, t.SlotToGroup, func(a uint32, b int) bool { return int(a) == b }) {
		return nil, errors.New("slot map differs from computed placement")
	}
	return t, nil
}

// Topology returns the placement the client routes by. It must not be modified.
func (c *Client) Topology() *topology.Topology { return c.topo }

// NewSession starts a session with a fresh random client ID.
func (c *Client) NewSession() *Session {
	return &Session{c: c, id: fmt.Sprintf("%016x", rand.Uint64())}
}

// Close closes all connections and aborts requests that are retrying.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		errs := make([]error, len(c.conns))
		for i, cc := range c.conns {
			errs[i] = cc.Close()
		}
		c.closeErr = errors.Join(errs...)
	})
	return c.closeErr
}

// do runs attempt against the replicas of group until it succeeds, fails
// permanently, or ctx ends. A NotLeader hint redirects the next attempt at
// once; any other failure moves on to the next replica after a short jittered
// pause. Gets with ReadAnyReplica use a random replica and leave the leader
// cache alone.
func (c *Client) do(ctx context.Context, op string, group int, read bool, attempt func(context.Context, slotkvv1.KVServiceClient) error) error {
	rt := &c.routes[group]
	n := len(rt.replicas)
	pinned := !(read && c.readAny)
	var last error
	redirects := 0
	for {
		if err := c.interrupted(ctx); err != nil {
			return giveUp(op, group, err, last)
		}
		i := int(rt.leader.Load())
		if !pinned {
			i = rand.IntN(n)
		}
		actx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
		err := attempt(actx, rt.replicas[i].kv)
		cancel()
		if err == nil {
			return nil
		}
		last = err
		st, _ := status.FromError(err)
		if permanent(st.Code()) {
			return fmt.Errorf("slotkv: %s (group %d): %w", op, group, err)
		}
		c.log.Debug("attempt failed", "op", op, "group", group, "node", rt.replicas[i].id, "err", err)
		// A hint is trusted for at most one lap of the group, which stops
		// replicas with stale views from redirecting a request in circles.
		if nl, ok := notLeader(st); ok && redirects < n {
			if j := rt.indexOf(nl.LeaderId); j >= 0 && j != i {
				if pinned {
					rt.leader.Store(int32(j))
				}
				redirects++
				continue
			}
		}
		redirects = 0
		if pinned {
			// Advance only if no other request already moved the cache.
			rt.leader.CompareAndSwap(int32(i), int32((i+1)%n))
		}
		if err := c.sleep(ctx, minRetryDelay+rand.N(maxRetryDelay-minRetryDelay)); err != nil {
			return giveUp(op, group, err, last)
		}
	}
}

// interrupted reports why a request must stop retrying, if it must.
func (c *Client) interrupted(ctx context.Context) error {
	select {
	case <-c.done:
		return ErrClosed
	default:
		return ctx.Err()
	}
}

func (c *Client) sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

// giveUp wraps both why the request stopped and the last attempt's error.
func giveUp(op string, group int, cause, last error) error {
	if last == nil {
		return fmt.Errorf("slotkv: %s (group %d): %w", op, group, cause)
	}
	return fmt.Errorf("slotkv: %s (group %d): %w (last attempt: %w)", op, group, cause, last)
}

// permanent reports whether retrying an error with this code cannot help.
func permanent(c codes.Code) bool {
	switch c {
	case codes.InvalidArgument, codes.Unimplemented, codes.PermissionDenied, codes.Unauthenticated:
		return true
	}
	return false
}

// notLeader extracts the NotLeader detail of a FailedPrecondition error.
func notLeader(st *status.Status) (*slotkvv1.NotLeader, bool) {
	if st.Code() != codes.FailedPrecondition {
		return nil, false
	}
	for _, d := range st.Details() {
		if nl, ok := d.(*slotkvv1.NotLeader); ok {
			return nl, true
		}
	}
	return nil, false
}
