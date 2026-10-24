// Package node implements the slotkv server: one Raft instance per hosted
// group, the replicated state machine, the gRPC KVService, readiness and
// graceful shutdown, and the leader balancer.
package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protojson"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
	"github.com/kedar-chintalapati/slotkv/internal/topology"
)

// ShutdownMode selects what Shutdown does.
type ShutdownMode string

// ReadinessMode selects the /readyz rule.
type ReadinessMode string

// ReadMode selects how Get is served.
type ReadMode string

const (
	// ShutdownGraceful hands leadership off before stopping. This is the default.
	ShutdownGraceful ShutdownMode = "graceful"
	// ShutdownNaive makes Shutdown behave like Kill.
	ShutdownNaive ShutdownMode = "naive"

	// ReadinessCaughtUp requires every local group to be caught up. This is the default.
	ReadinessCaughtUp ReadinessMode = "caught-up"
	// ReadinessServing reports ready as soon as the gRPC server is listening.
	ReadinessServing ReadinessMode = "serving"

	// ReadVerify serves linearizable Gets from the leader. This is the default.
	ReadVerify ReadMode = "verify"
	// ReadAny serves Gets from any replica's local state (stale reads).
	ReadAny ReadMode = "any"
)

// LogStoreMode selects how a node stores its groups' Raft logs.
type LogStoreMode string

const (
	// LogStoreShared keeps every local group's log in one database per node and
	// group-commits their writes, one fsync per batch. This is the default.
	LogStoreShared LogStoreMode = "shared"
	// LogStorePerGroup gives each group its own database, so each group fsyncs
	// on its own.
	LogStorePerGroup LogStoreMode = "per-group"
)

// Config configures a Node. Zero values select the defaults noted below.
// Listeners passed in belong to the Node from New on and are closed by
// Shutdown and Kill.
type Config struct {
	NodeID   string
	Topology *topology.Topology
	DataDir  string

	GRPCListener   net.Listener // optional; otherwise listen on GRPCListenAddr
	GRPCListenAddr string
	RaftListener   net.Listener // optional; otherwise listen on RaftListenAddr (TCP mux)
	RaftListenAddr string
	HTTPListener   net.Listener // optional; otherwise listen on HTTPListenAddr
	HTTPListenAddr string       // "" (and no HTTPListener) disables the HTTP server

	// RaftTransport, when non-nil, supplies each group's transport instead of
	// the TCP mux (the simulator passes raft.InmemTransports whose addresses
	// equal the topology RaftAddr). Raft listener settings are then unused.
	RaftTransport func(group int) (raft.Transport, error)
	// PeerDialOptions are appended when dialing other nodes' gRPC addresses for
	// readiness and balancer Status calls (the simulator passes a bufconn dialer).
	PeerDialOptions []grpc.DialOption
	// UnaryInterceptors are installed on the gRPC server (simulator fault injection).
	UnaryInterceptors []grpc.UnaryServerInterceptor

	Shutdown        ShutdownMode  // default graceful
	Readiness       ReadinessMode // default caught-up
	ReadMode        ReadMode      // default verify
	DisableDedup    bool
	LogStore        LogStoreMode  // default shared
	BalanceInterval time.Duration // 0 disables leader balancing
	ReadyMaxLag     uint64        // default 1000

	HeartbeatTimeout, ElectionTimeout, LeaderLeaseTimeout, CommitTimeout time.Duration // 0 = design defaults
	ApplyTimeout                                                         time.Duration // default 2s

	Logger       *slog.Logger // default slog.Default()
	RaftLogLevel string       // hclog level, default "warn"; "off" silences Raft
}

const (
	stateNew = iota
	stateRunning
	stateStopping
	stateStopped
)

const (
	listenRetry     = 5 * time.Second
	grpcStopTimeout = 5 * time.Second
	handoffTimeout  = 15 * time.Second // bound on all leadership handoff passes
)

// Node hosts this process's replica of every group it belongs to.
type Node struct {
	cfg     Config
	topo    *topology.Topology
	id      string
	local   []int // IDs of the groups hosted here
	log     *slog.Logger
	raftLog hclog.Logger
	peers   map[string]*peer // every other node, by ID

	stop   chan struct{} // closed when teardown begins
	killed chan struct{} // closed by Kill
	done   chan struct{} // closed when teardown ends
	wg     sync.WaitGroup

	mu          sync.Mutex // guards state
	state       int
	startMu     sync.Mutex // held by Start; teardown waits for it
	cleanupOnce sync.Once
	killOnce    sync.Once
	balanceMu   sync.Mutex // held during a balancer tick

	// Set by Start under startMu.
	mux     *mux
	opened  []*group
	shared  *sharedLog // nil with LogStorePerGroup
	grpcSrv *grpc.Server
	httpSrv *http.Server

	groups   atomic.Pointer[[]*group] // indexed by group ID; nil entries are not hosted
	draining atomic.Bool
	serving  atomic.Bool
}

// New validates cfg and returns a Node. It touches no files or sockets; Start does.
func New(cfg Config) (*Node, error) {
	if cfg.Topology == nil {
		return nil, errors.New("node: Topology is required")
	}
	self, ok := cfg.Topology.Node(cfg.NodeID)
	if !ok {
		return nil, fmt.Errorf("node: %q is not in the topology", cfg.NodeID)
	}
	if cfg.DataDir == "" {
		return nil, errors.New("node: DataDir is required")
	}
	if cfg.GRPCListener == nil && cfg.GRPCListenAddr == "" {
		return nil, errors.New("node: GRPCListener or GRPCListenAddr is required")
	}
	if cfg.RaftTransport == nil && cfg.RaftListener == nil && cfg.RaftListenAddr == "" {
		return nil, errors.New("node: RaftTransport, RaftListener or RaftListenAddr is required")
	}
	cfg.Shutdown = cmp.Or(cfg.Shutdown, ShutdownGraceful)
	cfg.Readiness = cmp.Or(cfg.Readiness, ReadinessCaughtUp)
	cfg.ReadMode = cmp.Or(cfg.ReadMode, ReadVerify)
	cfg.LogStore = cmp.Or(cfg.LogStore, LogStoreShared)
	switch {
	case cfg.LogStore != LogStoreShared && cfg.LogStore != LogStorePerGroup:
		return nil, fmt.Errorf("node: unknown log store %q", cfg.LogStore)
	case cfg.Shutdown != ShutdownGraceful && cfg.Shutdown != ShutdownNaive:
		return nil, fmt.Errorf("node: unknown shutdown mode %q", cfg.Shutdown)
	case cfg.Readiness != ReadinessCaughtUp && cfg.Readiness != ReadinessServing:
		return nil, fmt.Errorf("node: unknown readiness mode %q", cfg.Readiness)
	case cfg.ReadMode != ReadVerify && cfg.ReadMode != ReadAny:
		return nil, fmt.Errorf("node: unknown read mode %q", cfg.ReadMode)
	}
	cfg.HeartbeatTimeout = cmp.Or(cfg.HeartbeatTimeout, time.Second)
	cfg.ElectionTimeout = cmp.Or(cfg.ElectionTimeout, time.Second)
	cfg.LeaderLeaseTimeout = cmp.Or(cfg.LeaderLeaseTimeout, 500*time.Millisecond)
	cfg.CommitTimeout = cmp.Or(cfg.CommitTimeout, 50*time.Millisecond)
	cfg.ApplyTimeout = cmp.Or(cfg.ApplyTimeout, 2*time.Second)
	cfg.ReadyMaxLag = cmp.Or(cfg.ReadyMaxLag, 1000)
	cfg.RaftLogLevel = cmp.Or(cfg.RaftLogLevel, "warn")
	cfg.Logger = cmp.Or(cfg.Logger, slog.Default())

	n := &Node{
		cfg:     cfg,
		topo:    cfg.Topology,
		id:      cfg.NodeID,
		local:   cfg.Topology.GroupsOn(cfg.NodeID),
		log:     cfg.Logger.With("node", cfg.NodeID),
		raftLog: newRaftLogger(cfg.Logger.With("node", cfg.NodeID), cfg.RaftLogLevel),
		peers:   map[string]*peer{},
		stop:    make(chan struct{}),
		killed:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	rc := n.raftConfig()
	if err := raft.ValidateConfig(rc); err != nil {
		return nil, fmt.Errorf("node: %w", err)
	}
	opts := peerDialOptions(cfg.PeerDialOptions)
	for _, nd := range cfg.Topology.Nodes {
		if nd.ID != self.ID {
			n.peers[nd.ID] = &peer{addr: nd.GRPCAddr, opts: opts}
		}
	}
	return n, nil
}

// group returns the local replica of group id, or nil.
func (n *Node) group(id int) *group {
	gs := n.groups.Load()
	if gs == nil || id < 0 || id >= len(*gs) {
		return nil
	}
	return (*gs)[id]
}

// Raft returns the Raft instance for group, or nil if this node does not host it.
func (n *Node) Raft(group int) *raft.Raft {
	if g := n.group(group); g != nil {
		return g.raft
	}
	return nil
}

// Status reports the node's draining flag and the Raft state of each hosted group.
func (n *Node) Status() *slotkvv1.StatusResponse {
	resp := &slotkvv1.StatusResponse{NodeId: n.id, Draining: n.draining.Load()}
	for _, id := range n.local {
		if g := n.group(id); g != nil {
			resp.Groups = append(resp.Groups, g.status())
		}
	}
	return resp
}

// Start opens the stores, bootstraps fresh groups, starts Raft, the gRPC and
// HTTP servers and the balancer. On error everything opened is released.
func (n *Node) Start() error {
	n.startMu.Lock()
	n.mu.Lock()
	fresh := n.state == stateNew
	if fresh {
		n.state = stateRunning
	}
	n.mu.Unlock()
	if !fresh {
		n.startMu.Unlock()
		return errors.New("node: already started or stopped")
	}
	err := n.start()
	n.startMu.Unlock()
	if err != nil {
		n.cleanup()
	}
	return err
}

func (n *Node) start() error {
	cfg := &n.cfg
	self, _ := n.topo.Node(n.id)
	if cfg.RaftTransport == nil {
		ln := cfg.RaftListener
		if ln == nil {
			var err error
			if ln, err = listen(cfg.RaftListenAddr); err != nil {
				return fmt.Errorf("node: raft listen: %w", err)
			}
		}
		n.mux = newMux(ln, self.RaftAddr, n.local)
	}
	if cfg.LogStore == LogStoreShared {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return fmt.Errorf("node: %w", err)
		}
		s, err := openSharedLog(filepath.Join(cfg.DataDir, "raft-log.db"), n.local)
		if err != nil {
			return fmt.Errorf("node: %w", err)
		}
		n.shared = s
	}
	for _, id := range n.local {
		g, err := n.openGroup(id)
		if err != nil {
			return fmt.Errorf("node: group %d: %w", id, err)
		}
		n.opened = append(n.opened, g)
	}
	gs := make([]*group, len(n.topo.Groups))
	for _, g := range n.opened {
		gs[g.id] = g
	}
	n.groups.Store(&gs)

	ln := cfg.GRPCListener
	if ln == nil {
		var err error
		if ln, err = listen(cfg.GRPCListenAddr); err != nil {
			return fmt.Errorf("node: grpc listen: %w", err)
		}
	}
	opts := []grpc.ServerOption{
		// Clients may send keepalive pings on idle connections.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: time.Second, PermitWithoutStream: true}),
	}
	if len(cfg.UnaryInterceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(cfg.UnaryInterceptors...))
	}
	n.grpcSrv = grpc.NewServer(opts...)
	slotkvv1.RegisterKVServiceServer(n.grpcSrv, &server{n: n})
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.grpcSrv.Serve(ln); err != nil {
			n.log.Error("grpc server stopped", "err", err)
		}
	}()
	n.serving.Store(true)

	if hl := cfg.HTTPListener; hl != nil || cfg.HTTPListenAddr != "" {
		if hl == nil {
			var err error
			if hl, err = listen(cfg.HTTPListenAddr); err != nil {
				return fmt.Errorf("node: http listen: %w", err)
			}
		}
		n.httpSrv = &http.Server{Handler: n.httpHandler(), ReadHeaderTimeout: 5 * time.Second}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			if err := n.httpSrv.Serve(hl); !errors.Is(err, http.ErrServerClosed) {
				n.log.Error("http server stopped", "err", err)
			}
		}()
	}
	if cfg.BalanceInterval > 0 {
		n.wg.Add(1)
		go n.balanceLoop()
	}
	n.log.Info("started", "groups", len(n.local), "grpc", ln.Addr().String(), "log_store", cfg.LogStore, "shutdown", cfg.Shutdown,
		"readiness", cfg.Readiness, "read_mode", cfg.ReadMode, "dedup", !cfg.DisableDedup)
	return nil
}

// listen binds addr, retrying briefly: a restarted process may find the port
// still held by its predecessor.
func listen(addr string) (net.Listener, error) {
	deadline := time.Now().Add(listenRetry)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil || time.Now().After(deadline) {
			return ln, err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) httpHandler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	m.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !n.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	m.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(n.Status())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
	return m
}

// Shutdown stops the node as SIGTERM should. In graceful mode it marks the
// node draining, transfers the leadership of every group it leads, drains the
// gRPC server, then stops Raft and closes the stores. In naive mode it is Kill.
// It returns once everything is released, or early if ctx ends first, in which
// case the remaining steps are forced.
func (n *Node) Shutdown(ctx context.Context) error {
	if n.cfg.Shutdown == ShutdownNaive {
		n.Kill()
		return nil
	}
	n.mu.Lock()
	state := n.state
	if state == stateRunning {
		n.state = stateStopping
	}
	n.mu.Unlock()
	switch state {
	case stateNew:
		n.cleanup()
		return nil
	case stateStopping, stateStopped:
		select {
		case <-n.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	n.startMu.Lock() // wait for Start to finish
	n.startMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-n.killed:
			cancel()
		case <-ctx.Done():
		}
	}()

	n.log.Info("draining")
	n.draining.Store(true)
	n.balanceMu.Lock() // let an in-flight balancer tick finish
	n.balanceMu.Unlock()
	err := n.transferLeaderships(ctx)
	n.stopGRPC(ctx)
	n.cleanup()
	return errors.Join(err, ctx.Err())
}

// transferLeaderships hands off every group this node leads, in parallel, and
// repeats until the node leads none, handoffTimeout passes, or ctx ends. One
// pass is not enough: a transfer fails when its target is briefly behind, and
// an election can make this node leader of another group while it drains.
func (n *Node) transferLeaderships(ctx context.Context) error {
	deadline := time.Now().Add(handoffTimeout)
	for pass := 1; ; pass++ {
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
			led  int
		)
		for _, id := range n.local {
			g := n.group(id)
			if g == nil || g.raft.State() != raft.Leader {
				continue
			}
			led++
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := await(ctx, g.raft.LeadershipTransfer(), transferTimeout)
				if err != nil && g.raft.State() == raft.Leader {
					n.log.Warn("leadership transfer failed", "group", id, "pass", pass, "err", err)
					mu.Lock()
					errs = append(errs, fmt.Errorf("group %d: %w", id, err))
					mu.Unlock()
					return
				}
				n.log.Info("leadership transferred", "group", id, "pass", pass)
			}()
		}
		wg.Wait()
		if led == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.Join(errs...)
		}
		select {
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// stopGRPC drains the gRPC server, forcing it to stop after grpcStopTimeout.
func (n *Node) stopGRPC(ctx context.Context) {
	if n.grpcSrv == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		n.grpcSrv.GracefulStop()
		close(done)
	}()
	t := time.NewTimer(grpcStopTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		n.log.Warn("grpc graceful stop timed out")
		n.grpcSrv.Stop()
	case <-ctx.Done():
		n.grpcSrv.Stop()
	}
}

// Kill stops the node abruptly, like a crashed process: no handoff and no
// draining. It releases every file, socket and goroutine, so a new Node can
// reopen the same DataDir and addresses.
func (n *Node) Kill() {
	n.killOnce.Do(func() { close(n.killed) })
	n.cleanup()
}

// cleanup tears everything down, once. Concurrent callers wait for it.
func (n *Node) cleanup() {
	n.cleanupOnce.Do(func() {
		n.startMu.Lock()
		defer n.startMu.Unlock()
		n.serving.Store(false)
		close(n.stop)
		if n.grpcSrv != nil {
			n.grpcSrv.Stop()
		}
		var wg sync.WaitGroup
		for _, g := range n.opened {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g.raft.Shutdown().Error()
			}()
		}
		wg.Wait()
		if n.httpSrv != nil {
			n.httpSrv.Close()
		}
		if n.mux != nil {
			n.mux.Close()
		}
		for _, p := range n.peers {
			p.close()
		}
		n.wg.Wait()
		for _, g := range n.opened {
			if g.store != nil {
				g.store.Close()
			}
		}
		if n.shared != nil {
			n.shared.Close()
		}
		for _, ln := range []net.Listener{n.cfg.GRPCListener, n.cfg.RaftListener, n.cfg.HTTPListener} {
			if ln != nil {
				ln.Close()
			}
		}
		n.mu.Lock()
		n.state = stateStopped
		n.mu.Unlock()
		n.log.Info("stopped")
		close(n.done)
	})
}
