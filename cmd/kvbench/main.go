// Command kvbench is the slotkv load generator.
//
// It runs one of three workloads against a cluster and prints progress to
// stderr and, as its last line on stdout, "RESULT <json>":
//
//	throughput  closed loop: --workers sessions issue requests back to back
//	steady      open loop: requests start at --rate per second regardless of how
//	            fast they complete, and latency counts from the scheduled start
//	check       steady on a few keys with unique values; the recorded history is
//	            then checked for linearizability
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kedar-chintalapati/slotkv/client"
)

const (
	modeThroughput = "throughput"
	modeSteady     = "steady"
	modeCheck      = "check"
)

type config struct {
	seeds        []string
	mode         string
	label        string
	duration     time.Duration
	warmup       time.Duration
	workers      int
	rate         int
	maxInflight  int
	keys         int
	valueSize    int
	readRatio    float64
	deadline     time.Duration
	checkTimeout time.Duration
	history      string
	viz          string
	readAny      bool
	seed         int64
}

func parseFlags(args []string) (config, error) {
	var cfg config
	var seeds string
	fs := flag.NewFlagSet("kvbench", flag.ContinueOnError)
	fs.StringVar(&seeds, "seeds", "", "comma-separated `host:port` gRPC addresses of nodes to ask for the topology")
	fs.StringVar(&cfg.mode, "mode", modeThroughput, "throughput, steady, or check")
	fs.StringVar(&cfg.label, "label", "", "free-form label copied into the result")
	fs.DurationVar(&cfg.duration, "duration", 30*time.Second, "measured duration, after the warmup")
	fs.DurationVar(&cfg.warmup, "warmup", 5*time.Second, "warmup, excluded from the totals but not the timeline")
	fs.IntVar(&cfg.workers, "workers", 256, "closed-loop workers (throughput)")
	fs.IntVar(&cfg.rate, "rate", 2000, "requests per second (steady, check; check defaults to 200)")
	fs.IntVar(&cfg.maxInflight, "max-inflight", 4096, "requests allowed in flight; more count as overload (steady, check)")
	fs.IntVar(&cfg.keys, "keys", 100000, "size of the key space (check defaults to 16)")
	fs.IntVar(&cfg.valueSize, "value-size", 128, "bytes per value")
	fs.Float64Var(&cfg.readRatio, "read-ratio", 0, "fraction of requests that are Gets (check defaults to 0.5)")
	fs.DurationVar(&cfg.deadline, "deadline", time.Second, "deadline of each request; a request that misses it failed")
	fs.DurationVar(&cfg.checkTimeout, "check-timeout", 120*time.Second, "time limit of the linearizability check")
	fs.StringVar(&cfg.history, "history", "", "write the typed check history to this `path`, or - to emit HISTORY JSON on stdout")
	fs.StringVar(&cfg.viz, "viz", "", "write the Porcupine visualization to this `path` if the history is illegal")
	fs.BoolVar(&cfg.readAny, "read-any-replica", false, "send Gets to a random replica (negative control)")
	fs.Int64Var(&cfg.seed, "seed", 1, "seed of the workload's random number generators")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if cfg.mode == modeCheck {
		if !set["rate"] {
			cfg.rate = 200
		}
		if !set["keys"] {
			cfg.keys = 16
		}
		if !set["read-ratio"] {
			cfg.readRatio = 0.5
		}
	}
	for _, s := range strings.Split(seeds, ",") {
		if s = strings.TrimSpace(s); s != "" {
			cfg.seeds = append(cfg.seeds, s)
		}
	}
	return cfg, cfg.validate()
}

func (c config) validate() error {
	switch {
	case len(c.seeds) == 0:
		return errors.New("--seeds is required")
	case c.mode != modeThroughput && c.mode != modeSteady && c.mode != modeCheck:
		return fmt.Errorf("unknown --mode %q", c.mode)
	case c.duration <= 0 || c.warmup < 0 || c.deadline <= 0 || c.checkTimeout < 0:
		return errors.New("--duration and --deadline must be positive, --warmup and --check-timeout not negative")
	case c.workers < 1 || c.rate < 1 || c.maxInflight < 1 || c.keys < 1 || c.valueSize < 0:
		return errors.New("--workers, --rate, --max-inflight and --keys must be at least 1, --value-size not negative")
	case c.readRatio < 0 || c.readRatio > 1:
		return errors.New("--read-ratio must be between 0 and 1")
	case c.history != "" && c.mode != modeCheck:
		return errors.New("--history is only valid with --mode check")
	}
	return nil
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "kvbench: "+format+"\n", args...)
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		logf("%v", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := dial(ctx, cfg)
	if err != nil {
		logf("%v", err)
		os.Exit(1)
	}
	defer cl.Close()
	out, err := json.Marshal(run(ctx, cfg, cl))
	if err != nil {
		logf("%v", err)
		os.Exit(1)
	}
	fmt.Printf("RESULT %s\n", out)
}

// dial connects to the cluster, waiting up to 30s for a seed to answer.
func dial(ctx context.Context, cfg config) (*client.Client, error) {
	opts := client.Options{
		ReadAnyReplica: cfg.readAny,
		Logger:         slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	giveUp := time.Now().Add(30 * time.Second)
	for {
		cl, err := client.Dial(ctx, cfg.seeds, opts)
		if err == nil {
			t := cl.Topology()
			logf("connected to %d nodes, %d groups, %d slots", len(t.Nodes), len(t.Groups), t.NumSlots)
			return cl, nil
		}
		if ctx.Err() != nil || time.Now().After(giveUp) {
			return nil, err
		}
		logf("waiting for the cluster: %v", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
}
