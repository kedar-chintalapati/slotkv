package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestStatsResult(t *testing.T) {
	cfg := config{
		mode: modeSteady, label: "unit", rate: 100, keys: 10, valueSize: 8, readRatio: 0.25,
		deadline: time.Second, duration: 3 * time.Second, warmup: 2 * time.Second,
	}
	st := newStats(cfg.warmup, cfg.warmup+cfg.duration)

	// The two seconds of warmup: successes and a failure, excluded from the totals.
	for range 10 {
		st.record(500*time.Millisecond, 5*time.Second, "")
	}
	st.record(1500*time.Millisecond, time.Second, classUnavailable)
	// Measured: latencies of 1..100ms in the third second, in the fifth a few
	// failures of each class.
	for i := 1; i <= 100; i++ {
		st.record(2*time.Second+ms(i), ms(i), "")
	}
	for _, class := range []string{classDeadline, classDeadline, classUnavailable, classNotLeader, classOverload, "bogus"} {
		st.record(4500*time.Millisecond, time.Second, class)
	}

	start := time.UnixMilli(1_700_000_000_000)
	res := st.result(cfg, start, start.Add(4*time.Second), cfg.duration)

	if res.OK != 100 || res.Failed != 6 || res.Ops != 106 {
		t.Errorf("ops %d, ok %d, failed %d; want 106, 100, 6 (warmup excluded)", res.Ops, res.OK, res.Failed)
	}
	if res.Throughput != math.Round(100.0/3*10)/10 {
		t.Errorf("throughput %v, want 100 successes over 3s", res.Throughput)
	}
	if want := (errCounts{DeadlineExceeded: 2, Unavailable: 1, NotLeader: 1, Overload: 1, Other: 1}); res.Errors != want {
		t.Errorf("errors %+v, want %+v", res.Errors, want)
	}
	near := func(name string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > want/500+0.001 { // histograms keep three significant digits
			t.Errorf("%s = %v ms, want %v", name, got, want)
		}
	}
	near("p50", res.Latency.P50, 50)
	near("p90", res.Latency.P90, 90)
	near("p99", res.Latency.P99, 99)
	near("p99.9", res.Latency.P999, 100)
	near("max", res.Latency.Max, 100)

	if len(res.Timeline) != 5 {
		t.Fatalf("timeline has %d entries, want one per second of warmup + duration = 5", len(res.Timeline))
	}
	for sec, want := range []point{
		{T: 0, OK: 10, Failed: 0, P99: 5000},
		{T: 1, OK: 0, Failed: 1},
		{T: 2, OK: 100, Failed: 0, P99: 99},
		{T: 3},
		{T: 4, OK: 0, Failed: 6},
	} {
		got := res.Timeline[sec]
		if got.T != want.T || got.OK != want.OK || got.Failed != want.Failed {
			t.Errorf("timeline[%d] = %+v, want %+v", sec, got, want)
		}
		near(fmt.Sprintf("timeline[%d].p99", sec), got.P99, want.P99)
	}
	if res.StartUnixMS != 1_700_000_000_000 || res.EndUnixMS != 1_700_000_004_000 {
		t.Errorf("start/end = %d/%d", res.StartUnixMS, res.EndUnixMS)
	}
}

func TestResultJSONSchema(t *testing.T) {
	cfg := config{mode: modeThroughput, workers: 8, keys: 5, deadline: time.Second, duration: time.Second, warmup: 0}
	st := newStats(0, time.Second)
	st.record(0, ms(3), "")
	res := st.result(cfg, time.Now(), time.Now(), time.Second)
	res.Linearizable = "ok"

	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	keys := func(m map[string]any) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		slices.Sort(out)
		return out
	}
	check := func(name string, got map[string]any, want ...string) {
		t.Helper()
		slices.Sort(want)
		if g := keys(got); !slices.Equal(g, want) {
			t.Errorf("%s keys = %v, want %v", name, g, want)
		}
	}
	check("result", doc, "label", "mode", "params", "start_unix_ms", "end_unix_ms", "ops", "ok", "failed",
		"throughput_ops_s", "latency_ms", "errors", "timeline", "linearizable")
	check("params", doc["params"].(map[string]any),
		"workers", "rate", "keys", "value_size", "read_ratio", "deadline_ms", "duration_s", "warmup_s")
	check("latency_ms", doc["latency_ms"].(map[string]any), "p50", "p90", "p99", "p999", "max")
	check("errors", doc["errors"].(map[string]any), "deadline_exceeded", "unavailable", "not_leader", "overload", "other")
	check("timeline[0]", doc["timeline"].([]any)[0].(map[string]any), "t_s", "ok", "failed", "p99_ms")
	if p := doc["params"].(map[string]any); p["workers"] != 8.0 || p["rate"] != 0.0 || p["deadline_ms"] != 1000.0 {
		t.Errorf("params = %v", p)
	}
}

func TestClassify(t *testing.T) {
	notLeader, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&slotkvv1.NotLeader{GroupId: 1})
	if err != nil {
		t.Fatal(err)
	}
	// What the client returns once the caller's deadline passes.
	gaveUp := func(last error) error {
		return fmt.Errorf("slotkv: put (group 1): %w (last attempt: %w)", context.DeadlineExceeded, last)
	}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, ""},
		{"last attempt unavailable", gaveUp(status.Error(codes.Unavailable, "down")), classUnavailable},
		{"last attempt not leader", gaveUp(notLeader.Err()), classNotLeader},
		{"last attempt timed out", gaveUp(status.Error(codes.DeadlineExceeded, "slow")), classDeadline},
		{"no attempt made", fmt.Errorf("slotkv: put: %w", context.DeadlineExceeded), classDeadline},
		{"bad request", status.Error(codes.InvalidArgument, "empty key"), classOther},
		{"precondition without detail", status.Error(codes.FailedPrecondition, "x"), classOther},
		{"plain error", errors.New("boom"), classOther},
	} {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("%s: classify = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"--seeds", "a:1, b:2", "--mode", "check"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.seeds, []string{"a:1", "b:2"}) || cfg.rate != 200 || cfg.keys != 16 || cfg.readRatio != 0.5 {
		t.Errorf("check defaults: %+v", cfg)
	}
	cfg, err = parseFlags([]string{"--seeds", "a:1", "--mode", "check", "--rate", "50", "--keys", "4", "--read-ratio", "0"})
	if err != nil || cfg.rate != 50 || cfg.keys != 4 || cfg.readRatio != 0 {
		t.Errorf("explicit flags must win over the check defaults: %+v, %v", cfg, err)
	}
	cfg, err = parseFlags([]string{"--seeds", "a:1"})
	if err != nil || cfg.mode != modeThroughput || cfg.workers != 256 || cfg.rate != 2000 || cfg.keys != 100000 ||
		cfg.duration != 30*time.Second || cfg.warmup != 5*time.Second || cfg.valueSize != 128 || cfg.deadline != time.Second ||
		cfg.maxInflight != 4096 || cfg.checkTimeout != 120*time.Second {
		t.Errorf("defaults: %+v, %v", cfg, err)
	}
	for _, args := range [][]string{
		nil,
		{"--seeds", "a:1", "--mode", "burst"},
		{"--seeds", "a:1", "--read-ratio", "1.5"},
		{"--seeds", "a:1", "--duration", "0s"},
		{"--seeds", "a:1", "--workers", "0"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("parseFlags(%v) succeeded", args)
		}
	}
}
