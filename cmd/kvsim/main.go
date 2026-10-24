// Command kvsim runs seeded fault-injection histories against an in-process cluster.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kedar-chintalapati/slotkv/internal/sim"
)

type config struct {
	runs, parallel, nodes, groups int
	seed                          int64
	duration                      time.Duration
	readMode                      string
	dedup                         bool
	dropProb                      float64
	faults                        []string
	viz                           string
}

func parse(args []string) (config, error) {
	c := config{runs: 1, parallel: 1, nodes: 3, groups: 4, seed: 1, duration: 6 * time.Second, readMode: "verify", dedup: true, dropProb: 0.05, faults: []string{"crash", "partition", "drop"}, viz: "porcupine-kvsim-illegal.html"}
	var faults string
	fs := flag.NewFlagSet("kvsim", flag.ContinueOnError)
	fs.IntVar(&c.runs, "runs", 1, "independent runs")
	fs.Int64Var(&c.seed, "seed", 1, "base random seed")
	fs.IntVar(&c.parallel, "parallel", 1, "runs to execute concurrently")
	fs.DurationVar(&c.duration, "duration", 6*time.Second, "fault and workload duration")
	fs.IntVar(&c.nodes, "nodes", 3, "cluster nodes")
	fs.IntVar(&c.groups, "groups", 4, "Raft groups")
	fs.StringVar(&c.readMode, "read-mode", "verify", "verify or any")
	fs.BoolVar(&c.dedup, "dedup", true, "enable write deduplication")
	fs.Float64Var(&c.dropProb, "drop-prob", 0.05, "probability of dropping a successful write response")
	fs.StringVar(&faults, "faults", "crash,partition,drop", "comma-separated crash,partition,drop faults")
	fs.StringVar(&c.viz, "viz", "porcupine-kvsim-illegal.html", "path for the first illegal Porcupine visualization")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.faults = nil
	for _, f := range strings.Split(faults, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			c.faults = append(c.faults, f)
		}
	}
	if c.runs < 1 || c.parallel < 1 || c.nodes < 3 || c.groups < 1 || c.duration <= 0 {
		return c, errors.New("runs, parallel, groups, duration must be positive; nodes must be at least 3")
	}
	if strings.TrimSpace(c.viz) == "" {
		return c, errors.New("--viz path cannot be empty")
	}
	if c.readMode != "verify" && c.readMode != "any" {
		return c, errors.New("--read-mode must be verify or any")
	}
	if math.IsNaN(c.dropProb) || math.IsInf(c.dropProb, 0) || c.dropProb < 0 || c.dropProb > 1 {
		return c, errors.New("--drop-prob must be between 0 and 1")
	}
	for _, f := range c.faults {
		if f != "crash" && f != "partition" && f != "drop" {
			return c, fmt.Errorf("unknown fault %q", f)
		}
	}
	return c, nil
}

type summary struct {
	Runs      int `json:"runs"`
	OK        int `json:"ok"`
	Illegal   int `json:"illegal"`
	Unknown   int `json:"unknown"`
	Ops       int `json:"ops"`
	FailedOps int `json:"failed_ops"`
	Errors    int `json:"errors"`
}

func summarize(results []sim.Result) summary {
	s := summary{Runs: len(results)}
	for _, r := range results {
		switch r.Verdict {
		case "ok":
			s.OK++
		case "illegal":
			s.Illegal++
		case "unknown":
			s.Unknown++
		}
		if r.Err != "" {
			s.Errors++
		}
		s.Ops += r.Ops
		s.FailedOps += r.FailedOps
	}
	return s
}

func (s summary) exitCode() int {
	if s.Errors > 0 {
		return 1
	}
	return 0
}

func writeExclusive(src, desired string) (string, error) {
	dir, name := filepath.Dir(desired), filepath.Base(desired)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for suffix := 0; ; suffix++ {
		candidate := desired
		if suffix > 0 {
			candidate = filepath.Join(dir, fmt.Sprintf("%s-%d%s", stem, suffix+1, ext))
		}
		out, err := os.OpenFile(candidate, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		in, err := os.Open(src)
		if err == nil {
			_, err = io.Copy(out, in)
			_ = in.Close()
		}
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(candidate)
			return "", err
		}
		if err = os.Remove(src); err != nil {
			_ = os.Remove(candidate)
			return "", err
		}
		return candidate, nil
	}
}

func main() {
	c, err := parse(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvsim:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	type row struct {
		index int
		r     sim.Result
	}
	rows := make([]row, c.runs)
	for i := range rows {
		rows[i].index = i
	}
	jobs := make(chan int, c.runs)
	for i := range rows {
		jobs <- i
	}
	close(jobs)
	tmpDir, err := os.MkdirTemp("", "kvsim-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvsim:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)
	var wg sync.WaitGroup
	var outputMu sync.Mutex
	firstArtifact := false
	for w := 0; w < min(c.parallel, c.runs); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				var r sim.Result
				if ctx.Err() != nil {
					r = sim.Result{Seed: c.seed + int64(i), Err: "cancelled before this run started"}
				} else {
					viz := filepath.Join(tmpDir, fmt.Sprintf("run-%d.html", i+1))
					r = sim.Run(ctx, sim.Config{Seed: c.seed + int64(i), Duration: c.duration, Nodes: c.nodes, Groups: c.groups, ReadMode: c.readMode, Dedup: c.dedup, DropProbability: c.dropProb, Faults: c.faults, VizPath: viz})
				}
				outputMu.Lock()
				if r.VizPath != "" {
					if firstArtifact {
						_ = os.Remove(r.VizPath)
						r.VizPath = ""
					} else {
						path, moveErr := writeExclusive(r.VizPath, c.viz)
						if moveErr != nil {
							r.Err = fmt.Sprintf("write visualization: %v", moveErr)
							r.VizPath = ""
						} else {
							firstArtifact = true
							r.VizPath = path
						}
					}
				}
				rows[i] = row{i, r}
				b, _ := json.Marshal(r)
				fmt.Printf("RUN %d %s\n", i+1, b)
				outputMu.Unlock()
			}
		}()
	}
	wg.Wait()
	results := make([]sim.Result, len(rows))
	for i := range rows {
		if rows[i].r.Verdict == "" && rows[i].r.Err == "" {
			rows[i].r = sim.Result{Seed: c.seed + int64(i), Err: "cancelled before this run started"}
		}
		results[i] = rows[i].r
	}
	s := summarize(results)
	b, _ := json.Marshal(s)
	fmt.Printf("RESULT %s\n", b)
	_ = os.RemoveAll(tmpDir)
	os.Exit(s.exitCode())
}
