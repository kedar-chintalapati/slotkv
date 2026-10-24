package main

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

// Error classes. A failed request is classified by the error of its last
// attempt, or as deadline_exceeded if it never made an attempt.
const (
	classDeadline    = "deadline_exceeded"
	classUnavailable = "unavailable"
	classNotLeader   = "not_leader"
	classOverload    = "overload"
	classOther       = "other"
)

const maxLatencyUS = 60_000_000 // histograms cover 1µs to 60s

func newHistogram() *hdrhistogram.Histogram { return hdrhistogram.New(1, maxLatencyUS, 3) }

// classify returns the error class of err, or "" if err is nil.
func classify(err error) string {
	if err == nil {
		return ""
	}
	st, _ := status.FromError(err) // finds the status wrapped by the client's error
	switch st.Code() {
	case codes.Unavailable:
		return classUnavailable
	case codes.DeadlineExceeded:
		return classDeadline
	case codes.FailedPrecondition:
		for _, d := range st.Details() {
			if _, ok := d.(*slotkvv1.NotLeader); ok {
				return classNotLeader
			}
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return classDeadline
	}
	return classOther
}

// stats accumulates request outcomes. Latencies are of successful requests
// only; failures are counted by class. A request is attributed to the second,
// counted from the start of the run, in which it started or was scheduled to
// start, and requests from the warmup count in the timeline but not the totals.
type stats struct {
	mu     sync.Mutex
	warmup time.Duration

	hist   *hdrhistogram.Histogram // measured window, in µs
	ok     int
	failed int
	errs   errCounts

	allOK, allFailed int // including the warmup, for progress output
	secs             []second
}

type second struct {
	ok, failed int
	hist       *hdrhistogram.Histogram // allocated on first success
}

func newStats(warmup, total time.Duration) *stats {
	return &stats{
		warmup: warmup,
		hist:   newHistogram(),
		secs:   make([]second, max(1, int(math.Ceil(total.Seconds())))),
	}
}

// record adds a request that started (or was due to start) off into the run
// and took lat. A non-empty class marks it failed.
func (s *stats) record(off, lat time.Duration, class string) {
	us := min(max(lat.Microseconds(), 1), maxLatencyUS)
	s.mu.Lock()
	defer s.mu.Unlock()
	sec := &s.secs[min(int(off/time.Second), len(s.secs)-1)]
	measured := off >= s.warmup
	if class == "" {
		s.allOK++
		sec.ok++
		if sec.hist == nil {
			sec.hist = newHistogram()
		}
		_ = sec.hist.RecordValue(us)
		if measured {
			s.ok++
			_ = s.hist.RecordValue(us)
		}
		return
	}
	s.allFailed++
	sec.failed++
	if measured {
		s.failed++
		s.errs.add(class)
	}
}

// totals returns the successes and failures so far, warmup included.
func (s *stats) totals() (ok, failed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allOK, s.allFailed
}

type params struct {
	Workers    int     `json:"workers"`
	Rate       int     `json:"rate"`
	Keys       int     `json:"keys"`
	ValueSize  int     `json:"value_size"`
	ReadRatio  float64 `json:"read_ratio"`
	DeadlineMS int     `json:"deadline_ms"`
	DurationS  float64 `json:"duration_s"`
	WarmupS    float64 `json:"warmup_s"`
}

type latencyMS struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
}

type errCounts struct {
	DeadlineExceeded int `json:"deadline_exceeded"`
	Unavailable      int `json:"unavailable"`
	NotLeader        int `json:"not_leader"`
	Overload         int `json:"overload"`
	Other            int `json:"other"`
}

func (e *errCounts) add(class string) {
	switch class {
	case classDeadline:
		e.DeadlineExceeded++
	case classUnavailable:
		e.Unavailable++
	case classNotLeader:
		e.NotLeader++
	case classOverload:
		e.Overload++
	default:
		e.Other++
	}
}

type point struct {
	T      int     `json:"t_s"`
	OK     int     `json:"ok"`
	Failed int     `json:"failed"`
	P99    float64 `json:"p99_ms"`
}

// result is the document printed after "RESULT ".
type result struct {
	Label        string    `json:"label"`
	Mode         string    `json:"mode"`
	Params       params    `json:"params"`
	StartUnixMS  int64     `json:"start_unix_ms"`
	EndUnixMS    int64     `json:"end_unix_ms"`
	Ops          int       `json:"ops"`
	OK           int       `json:"ok"`
	Failed       int       `json:"failed"`
	Throughput   float64   `json:"throughput_ops_s"`
	Latency      latencyMS `json:"latency_ms"`
	Errors       errCounts `json:"errors"`
	Timeline     []point   `json:"timeline"`
	Linearizable string    `json:"linearizable"`
}

// result assembles the totals. window is how long the measured part of the run
// lasted, which is what throughput is divided by.
func (s *stats) result(cfg config, start, end time.Time, window time.Duration) *result {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := func(us int64) float64 { return float64(us) / 1000 }
	r := &result{
		Label: cfg.label,
		Mode:  cfg.mode,
		Params: params{
			Keys:       cfg.keys,
			ValueSize:  cfg.valueSize,
			ReadRatio:  cfg.readRatio,
			DeadlineMS: int(cfg.deadline.Milliseconds()),
			DurationS:  cfg.duration.Seconds(),
			WarmupS:    cfg.warmup.Seconds(),
		},
		StartUnixMS: start.UnixMilli(),
		EndUnixMS:   end.UnixMilli(),
		Ops:         s.ok + s.failed,
		OK:          s.ok,
		Failed:      s.failed,
		Latency: latencyMS{
			P50:  ms(s.hist.ValueAtQuantile(50)),
			P90:  ms(s.hist.ValueAtQuantile(90)),
			P99:  ms(s.hist.ValueAtQuantile(99)),
			P999: ms(s.hist.ValueAtQuantile(99.9)),
			Max:  ms(s.hist.Max()),
		},
		Errors:   s.errs,
		Timeline: make([]point, len(s.secs)),
	}
	if cfg.mode == modeThroughput {
		r.Params.Workers = cfg.workers
	} else {
		r.Params.Rate = cfg.rate
	}
	if window > 0 {
		r.Throughput = math.Round(float64(s.ok)/window.Seconds()*10) / 10
	}
	for t, sec := range s.secs {
		r.Timeline[t] = point{T: t, OK: sec.ok, Failed: sec.failed}
		if sec.hist != nil {
			r.Timeline[t].P99 = ms(sec.hist.ValueAtQuantile(99))
		}
	}
	return r
}
