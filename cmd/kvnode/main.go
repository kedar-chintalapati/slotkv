// Command kvnode runs one slotkv server node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kedar-chintalapati/slotkv/internal/node"
	"github.com/kedar-chintalapati/slotkv/internal/topology"
)

// shutdownTimeout bounds a graceful shutdown, within Kubernetes'
// terminationGracePeriodSeconds of 30.
const shutdownTimeout = 25 * time.Second

func main() {
	var (
		id          = flag.String("id", "", "node ID (default $POD_NAME, then hostname)")
		peers       = flag.String("peers", "", "id=host:grpcPort:raftPort,... (required; same order on every node)")
		groups      = flag.Int("groups", 12, "number of Raft groups")
		replication = flag.Int("replication", 3, "replicas per group")
		slots       = flag.Int("slots", topology.DefaultNumSlots, "number of hash slots")
		dataDir     = flag.String("data-dir", "", "data directory (default ./data/<id>)")
		grpcListen  = flag.String("grpc-listen", ":7000", "gRPC listen address")
		raftListen  = flag.String("raft-listen", ":7001", "Raft listen address")
		httpListen  = flag.String("http-listen", ":8080", "HTTP listen address (empty disables)")
		shutdown    = flag.String("shutdown", "graceful", "on SIGTERM: graceful|naive")
		readiness   = flag.String("readiness", "caught-up", "/readyz rule: caught-up|serving")
		readMode    = flag.String("read-mode", "verify", "Get consistency: verify|any")
		dedup       = flag.Bool("dedup", true, "deduplicate retried writes (false is a negative control)")
		balance     = flag.Duration("balance-interval", time.Second, "leader balancing period (0 disables)")
		readyMaxLag = flag.Uint64("ready-max-lag", 1000, "entries a replica may trail its leader by and still be ready")
		logStore    = flag.String("log-store", "shared", "Raft log storage: shared (one group-committed database per node) or per-group")
		heartbeat   = flag.Duration("heartbeat-timeout", time.Second, "Raft heartbeat timeout")
		election    = flag.Duration("election-timeout", time.Second, "Raft election timeout")
		lease       = flag.Duration("leader-lease-timeout", 500*time.Millisecond, "Raft leader lease timeout")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fatal(fmt.Errorf("--log-level: %w", err))
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if *id == "" {
		*id = os.Getenv("POD_NAME")
	}
	if *id == "" {
		host, err := os.Hostname()
		if err != nil {
			fatal(err)
		}
		*id = host
	}
	if *dataDir == "" {
		*dataDir = filepath.Join(".", "data", *id)
	}
	nodes, err := topology.ParsePeers(*peers)
	if err != nil {
		fatal(fmt.Errorf("--peers: %w", err))
	}
	topo, err := topology.Build(nodes, *groups, *replication, *slots)
	if err != nil {
		fatal(err)
	}
	raftLevel := "warn"
	if level <= slog.LevelDebug {
		raftLevel = "debug"
	}
	n, err := node.New(node.Config{
		NodeID:             *id,
		Topology:           topo,
		DataDir:            *dataDir,
		GRPCListenAddr:     *grpcListen,
		RaftListenAddr:     *raftListen,
		HTTPListenAddr:     *httpListen,
		Shutdown:           node.ShutdownMode(*shutdown),
		Readiness:          node.ReadinessMode(*readiness),
		ReadMode:           node.ReadMode(*readMode),
		DisableDedup:       !*dedup,
		LogStore:           node.LogStoreMode(*logStore),
		BalanceInterval:    *balance,
		ReadyMaxLag:        *readyMaxLag,
		HeartbeatTimeout:   *heartbeat,
		ElectionTimeout:    *election,
		LeaderLeaseTimeout: *lease,
		Logger:             logger,
		RaftLogLevel:       raftLevel,
	})
	if err != nil {
		fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)
	if err := n.Start(); err != nil {
		fatal(err)
	}
	sig := <-sigs
	logger.Info("signal received", "signal", sig.String(), "shutdown", *shutdown)
	if node.ShutdownMode(*shutdown) == node.ShutdownNaive {
		os.Exit(0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := n.Shutdown(ctx); err != nil {
		logger.Error("shutdown", "err", err)
	}
	cancel()
	os.Exit(0)
}

func fatal(err error) {
	slog.Error("kvnode", "err", err)
	os.Exit(1)
}
