// Command kvcheck rechecks a saved kvbench history without contacting a cluster.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/kedar-chintalapati/slotkv/internal/lincheck"
)

type config struct {
	path    string
	timeout time.Duration
	viz     string
}

type result struct {
	Verdict string `json:"verdict"`
	Ops     int    `json:"ops"`
	Viz     string `json:"viz,omitempty"`
}

func parse(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("kvcheck", flag.ContinueOnError)
	fs.DurationVar(&c.timeout, "timeout", 120*time.Second, "maximum time for the linearizability check; 0 means no limit")
	fs.StringVar(&c.viz, "viz", "", "write an illegal-history visualization to this path")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if len(fs.Args()) != 1 {
		return c, errors.New("usage: kvcheck [--timeout duration] [--viz path] HISTORYFILE")
	}
	if c.timeout < 0 {
		return c, errors.New("--timeout cannot be negative")
	}
	c.path = fs.Arg(0)
	return c, nil
}

func evaluate(c config) (result, error) {
	f, err := os.Open(c.path)
	if err != nil {
		return result{}, err
	}
	ops, err := lincheck.ReadHistory(f)
	closeErr := f.Close()
	if err != nil {
		return result{}, err
	}
	if closeErr != nil {
		return result{}, closeErr
	}
	verdict, info := lincheck.Check(ops, c.timeout)
	out := result{Ops: len(ops)}
	switch verdict {
	case porcupine.Ok:
		out.Verdict = "ok"
	case porcupine.Illegal:
		out.Verdict = "illegal"
	case porcupine.Unknown:
		out.Verdict = "unknown"
	}
	if verdict == porcupine.Illegal && c.viz != "" {
		if err := lincheck.Visualize(info, c.viz); err != nil {
			return out, fmt.Errorf("write visualization: %w", err)
		}
		out.Viz = c.viz
	}
	return out, nil
}

func main() {
	c, err := parse(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvcheck:", err)
		os.Exit(2)
	}
	out, err := evaluate(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvcheck:", err)
		os.Exit(1)
	}
	b, _ := json.Marshal(out)
	fmt.Printf("RESULT %s\n", b)
}
