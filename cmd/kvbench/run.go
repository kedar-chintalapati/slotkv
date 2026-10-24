package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/kedar-chintalapati/slotkv/client"
	"github.com/kedar-chintalapati/slotkv/internal/lincheck"
)

// deleteShare is the fraction of the writes in check mode that are Deletes.
const deleteShare = 0.2

// session is a client session with the number the recorded history knows it by.
type session struct {
	*client.Session
	id     int
	writes int
}

// sessionPool hands out sessions, most recently used first, creating them on
// demand. Reusing the latest one keeps the number of distinct clients small.
type sessionPool struct {
	cl   *client.Client
	mu   sync.Mutex
	free []*session
	made int
}

func (p *sessionPool) get() *session {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.free); n > 0 {
		s := p.free[n-1]
		p.free = p.free[:n-1]
		return s
	}
	p.made++
	return &session{Session: p.cl.NewSession(), id: p.made - 1}
}

func (p *sessionPool) put(s *session) {
	p.mu.Lock()
	p.free = append(p.free, s)
	p.mu.Unlock()
}

type runner struct {
	cfg      config
	pool     sessionPool
	st       *stats
	rec      *lincheck.Recorder // check mode only
	keys     [][]byte
	value    []byte
	t0       time.Time
	inflight atomic.Int64
	firstErr sync.Map // error class -> the first error of that class
}

// run executes the workload cfg describes against cl and assembles the result.
func run(ctx context.Context, cfg config, cl *client.Client) *result {
	total := cfg.warmup + cfg.duration
	r := &runner{
		cfg:   cfg,
		pool:  sessionPool{cl: cl},
		st:    newStats(cfg.warmup, total),
		keys:  make([][]byte, cfg.keys),
		value: make([]byte, cfg.valueSize),
	}
	rng := rand.New(rand.NewPCG(uint64(cfg.seed), math.MaxUint64))
	for i := range r.value {
		r.value[i] = byte(rng.Uint32())
	}
	prefix := "key"
	if cfg.mode == modeCheck {
		// The model assumes every key starts out absent, so a check must not see
		// keys left by an earlier run.
		prefix = fmt.Sprintf("chk-%08x", rand.Uint32())
		r.rec = lincheck.NewRecorder()
	}
	for i := range r.keys {
		r.keys[i] = fmt.Appendf(nil, "%s-%07d", prefix, i)
	}

	logf("%s: warmup %v, measuring %v", cfg.mode, cfg.warmup, cfg.duration)
	r.t0 = time.Now()
	stopProgress := r.progress()
	if cfg.mode == modeThroughput {
		r.closedLoop(ctx, total)
	} else {
		r.openLoop(ctx, total)
	}
	stopProgress()
	end := time.Now()

	// Interrupted runs report the part of the measured window that ran.
	window := max(0, min(end.Sub(r.t0), total)-cfg.warmup)
	res := r.st.result(cfg, r.t0, end, window)
	if cfg.mode == modeCheck {
		res.Linearizable = r.check()
	}
	r.summarize(res)
	return res
}

// closedLoop runs cfg.workers workers, each issuing requests back to back on
// its own session until total has elapsed.
func (r *runner) closedLoop(ctx context.Context, total time.Duration) {
	var wg sync.WaitGroup
	for w := range r.cfg.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := r.pool.get()
			rng := rand.New(rand.NewPCG(uint64(r.cfg.seed), uint64(w)+1))
			for ctx.Err() == nil {
				start := time.Now()
				off := start.Sub(r.t0)
				if off >= total {
					return
				}
				op, key := r.pick(rng)
				rctx, cancel := context.WithTimeout(ctx, r.cfg.deadline)
				r.inflight.Add(1)
				err := r.do(rctx, s, op, key)
				r.inflight.Add(-1)
				cancel()
				r.finish(off, time.Since(start), err)
			}
		}()
	}
	wg.Wait()
}

// openLoop starts cfg.rate requests per second at fixed intervals until total
// has elapsed. A request's latency counts from its scheduled start, so a slow
// server cannot slow the arrivals down and hide its own queueing.
func (r *runner) openLoop(ctx context.Context, total time.Duration) {
	var wg sync.WaitGroup
	rng := rand.New(rand.NewPCG(uint64(r.cfg.seed), 0))
	for i := int64(0); ; i++ {
		off := time.Duration(i * int64(time.Second) / int64(r.cfg.rate))
		if off >= total || !sleepUntil(ctx, r.t0.Add(off)) {
			break
		}
		op, key := r.pick(rng)
		if r.inflight.Add(1) > int64(r.cfg.maxInflight) {
			r.inflight.Add(-1)
			r.st.record(off, 0, classOverload)
			continue
		}
		due := r.t0.Add(off)
		s := r.pool.get()
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithDeadline(ctx, due.Add(r.cfg.deadline))
			err := r.do(rctx, s, op, key)
			cancel()
			lat := time.Since(due)
			r.pool.put(s)
			r.inflight.Add(-1)
			r.finish(off, lat, err)
		}()
	}
	wg.Wait()
}

// finish records the outcome of a request that started off into the run.
func (r *runner) finish(off, lat time.Duration, err error) {
	class := classify(err)
	if class != "" {
		r.firstErr.LoadOrStore(class, err)
	}
	r.st.record(off, lat, class)
}

// sleepUntil waits for t, and reports false if ctx ended first.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// pick chooses the next request.
func (r *runner) pick(rng *rand.Rand) (lincheck.Op, []byte) {
	key := r.keys[rng.IntN(len(r.keys))]
	switch {
	case rng.Float64() < r.cfg.readRatio:
		return lincheck.Get, key
	case r.rec != nil && rng.Float64() < deleteShare:
		return lincheck.Delete, key
	}
	return lincheck.Put, key
}

// do issues one request on s, recording it in the history in check mode.
func (r *runner) do(ctx context.Context, s *session, op lincheck.Op, key []byte) error {
	value := r.value
	var h *lincheck.Handle
	if r.rec != nil {
		in := lincheck.Input{Op: op, Key: string(key)}
		if op == lincheck.Put {
			s.writes++
			in.Value = fmt.Sprintf("%s-%d", s.ID(), s.writes)
			value = []byte(in.Value)
		}
		h = r.rec.Begin(s.id, in)
	}
	var out lincheck.Output
	var err error
	switch op {
	case lincheck.Get:
		var v []byte
		v, out.Found, err = s.Get(ctx, key)
		out.Value = string(v)
	case lincheck.Put:
		err = s.Put(ctx, key, value)
	case lincheck.Delete:
		out.Existed, err = s.Delete(ctx, key)
	}
	if r.rec != nil {
		r.rec.End(h, out, err)
	}
	return err
}

// check runs the linearizability check on the recorded history.
func (r *runner) check() string {
	ops := r.rec.Operations()
	if r.cfg.history != "" {
		if err := exportHistory(r.cfg.history, ops, os.Stdout); err != nil {
			logf("writing history: %v", err)
		}
	}
	unknown := 0
	for _, op := range ops {
		if op.Output.(lincheck.Output).Unknown {
			unknown++
		}
	}
	logf("checking %d operations (%d with unknown outcome), limit %v", len(ops), unknown, r.cfg.checkTimeout)
	start := time.Now()
	res, info := lincheck.Check(ops, r.cfg.checkTimeout)
	logf("check: %s in %v", res, time.Since(start).Round(time.Millisecond))
	if res == porcupine.Illegal && r.cfg.viz != "" {
		if err := lincheck.Visualize(info, r.cfg.viz); err != nil {
			logf("writing visualization: %v", err)
		} else {
			logf("wrote visualization to %s", r.cfg.viz)
		}
	}
	switch res {
	case porcupine.Ok:
		return "ok"
	case porcupine.Illegal:
		return "illegal"
	}
	return "unknown"
}

func exportHistory(path string, ops []porcupine.Operation, stdout io.Writer) error {
	if path == "-" {
		data, err := lincheck.MarshalHistory(ops)
		if err != nil {
			return err
		}
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(encoded, data)
		const chunkSize = 3072 // Keep each output write under the common 4 KiB log-record limit.
		for len(encoded) > 0 {
			n := min(chunkSize, len(encoded))
			if _, err := fmt.Fprintf(stdout, "HISTORY %s\n", encoded[:n]); err != nil {
				return err
			}
			encoded = encoded[n:]
		}
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	writeErr := lincheck.WriteHistory(f, ops)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// progress prints a line per second to stderr until the returned function is called.
func (r *runner) progress() (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var lastOK, lastFailed int
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				ok, failed := r.st.totals()
				logf("t=%3ds  ok/s=%-7d failed/s=%-6d failed=%-7d inflight=%d",
					int(time.Since(r.t0).Round(time.Second)/time.Second), ok-lastOK, failed-lastFailed, failed, r.inflight.Load())
				lastOK, lastFailed = ok, failed
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

// summarize prints the totals, and a sample error for each class of failure.
func (r *runner) summarize(res *result) {
	logf("%s %s: %d ops, %d ok, %d failed, %.1f ok/s", res.Mode, res.Label, res.Ops, res.OK, res.Failed, res.Throughput)
	logf("latency ms: p50 %.3f  p90 %.3f  p99 %.3f  p99.9 %.3f  max %.3f",
		res.Latency.P50, res.Latency.P90, res.Latency.P99, res.Latency.P999, res.Latency.Max)
	e := res.Errors
	logf("errors: deadline_exceeded %d, unavailable %d, not_leader %d, overload %d, other %d",
		e.DeadlineExceeded, e.Unavailable, e.NotLeader, e.Overload, e.Other)
	for _, class := range []string{classDeadline, classUnavailable, classNotLeader, classOther} {
		if err, ok := r.firstErr.Load(class); ok {
			logf("first %s error: %v", class, err)
		}
	}
	if res.Linearizable != "" {
		logf("linearizable: %s", res.Linearizable)
	}
}
