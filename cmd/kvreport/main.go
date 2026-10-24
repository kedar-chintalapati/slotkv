// Command kvreport renders kvbench result files (the JSON after "RESULT ") as
// markdown tables for the experiment write-ups.
//
//	kvreport throughput FILES...  ops/s and latency per file, speedup vs. the first
//	kvreport rollout FILES...     failures and tail latency of a rolling restart
//	kvreport zone FILES...        outcome and linearizability of a fault-injection run
package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// result holds the fields of kvbench's JSON output that the reports use.
type result struct {
	Label      string  `json:"label"`
	Ops        int64   `json:"ops"`
	OK         int64   `json:"ok"`
	Failed     int64   `json:"failed"`
	Throughput float64 `json:"throughput_ops_s"`
	Latency    struct {
		P50  float64 `json:"p50"`
		P99  float64 `json:"p99"`
		P999 float64 `json:"p999"`
		Max  float64 `json:"max"`
	} `json:"latency_ms"`
	Timeline     []point `json:"timeline"`
	Linearizable string  `json:"linearizable"`
}

// point is one second of the run's timeline.
type point struct {
	T      int   `json:"t_s"`
	Failed int64 `json:"failed"`
}

var reports = map[string]func(io.Writer, []result){
	"throughput": throughput,
	"rollout":    rollout,
	"zone":       zone,
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "kvreport:", err)
		os.Exit(1)
	}
}

func run(args []string, w io.Writer) error {
	if len(args) < 2 || reports[args[0]] == nil {
		return fmt.Errorf("usage: kvreport throughput|rollout|zone FILE...")
	}
	rs := make([]result, 0, len(args)-1)
	for _, path := range args[1:] {
		r, err := load(path)
		if err != nil {
			return err
		}
		rs = append(rs, r)
	}
	reports[args[0]](w, rs)
	return nil
}

// load reads one result file. An empty label falls back to the file name.
func load(path string) (result, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return result{}, err
	}
	var r result
	if err := json.Unmarshal(bytes.TrimPrefix(b, []byte("\xef\xbb\xbf")), &r); err != nil {
		return result{}, fmt.Errorf("%s: %w", path, err)
	}
	if r.Label == "" {
		r.Label = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	return r, nil
}

func throughput(w io.Writer, rs []result) {
	rows := make([][]string, len(rs))
	for i, r := range rs {
		speedup := "n/a"
		if base := rs[0].Throughput; base > 0 {
			speedup = fmt.Sprintf("%.2fx", r.Throughput/base)
		}
		rows[i] = []string{r.Label, fmt.Sprintf("%.0f", r.Throughput),
			ms(r.Latency.P50), ms(r.Latency.P99), ms(r.Latency.P999), speedup}
	}
	table(w, []string{"label", "ops/s", "p50 (ms)", "p99 (ms)", "p999 (ms)", "speedup"}, rows)
}

func rollout(w io.Writer, rs []result) {
	rows := make([][]string, len(rs))
	for i, r := range rs {
		errPct := "n/a"
		if r.Ops > 0 {
			errPct = fmt.Sprintf("%.3f%%", 100*float64(r.Failed)/float64(r.Ops))
		}
		rows[i] = []string{r.Label, fmt.Sprint(r.Failed), errPct,
			ms(r.Latency.P99), ms(r.Latency.P999), ms(r.Latency.Max), fmt.Sprint(longestFailRun(r.Timeline))}
	}
	table(w, []string{"label", "failed", "error %", "p99 (ms)", "p999 (ms)", "max (ms)", "longest failing run (s)"}, rows)
}

func zone(w io.Writer, rs []result) {
	rows := make([][]string, len(rs))
	for i, r := range rs {
		lin := r.Linearizable
		if lin == "" {
			lin = "-"
		}
		rows[i] = []string{r.Label, fmt.Sprint(r.Ops), fmt.Sprint(r.OK), fmt.Sprint(r.Failed), lin}
	}
	table(w, []string{"label", "ops", "ok", "failed", "linearizable"}, rows)
}

// longestFailRun returns the longest run of consecutive timeline seconds in
// which at least one request failed. A missing second ends a run.
func longestFailRun(tl []point) int {
	tl = slices.SortedFunc(slices.Values(tl), func(a, b point) int { return cmp.Compare(a.T, b.T) })
	best, run, prev := 0, 0, 0
	for _, p := range tl {
		switch {
		case p.Failed == 0:
			run = 0
		case run > 0 && p.T == prev+1:
			run++
		default:
			run = 1
		}
		prev = p.T
		best = max(best, run)
	}
	return best
}

func ms(v float64) string { return fmt.Sprintf("%.2f", v) }

// table writes a markdown table: the first column left-aligned, the rest right-aligned.
func table(w io.Writer, header []string, rows [][]string) {
	sep := make([]string, len(header))
	for i := range sep {
		sep[i] = "---:"
	}
	sep[0] = "---"
	fmt.Fprintf(w, "| %s |\n|%s|\n", strings.Join(header, " | "), strings.Join(sep, "|"))
	for _, row := range rows {
		row[0] = strings.ReplaceAll(row[0], "|", `\|`)
		fmt.Fprintf(w, "| %s |\n", strings.Join(row, " | "))
	}
}
