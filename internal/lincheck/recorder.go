package lincheck

import (
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Recorder collects the history of a concurrent run. It is safe for concurrent
// use, and its zero value is ready to use.
//
// Timestamps are nanoseconds on one monotonic clock, handed out under the
// recorder's lock so that they are strictly increasing in the order the
// recorded events happened.
type Recorder struct {
	mu      sync.Mutex
	start   time.Time
	last    int64
	handles []*Handle
}

// Handle identifies one call recorded by Begin.
type Handle struct {
	client int
	in     Input
	call   int64

	// Set by End.
	done   bool
	failed bool
	out    Output
	ret    int64
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// stamp returns the next timestamp. The caller holds r.mu.
func (r *Recorder) stamp() int64 {
	if r.start.IsZero() {
		r.start = time.Now()
	}
	t := int64(time.Since(r.start))
	if t <= r.last {
		t = r.last + 1
	}
	r.last = t
	return t
}

// Begin records the invocation of in by client and returns its handle. Call it
// immediately before issuing the request.
func (r *Recorder) Begin(clientID int, in Input) *Handle {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := &Handle{client: clientID, in: in, call: r.stamp()}
	r.handles = append(r.handles, h)
	return h
}

// End records the response to h. A non-nil err means the call failed: a failed
// Get is dropped from the history, and a failed Put or Delete is kept with an
// unknown outcome. Call it immediately after the request returns.
func (r *Recorder) End(h *Handle, out Output, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h.done {
		return
	}
	h.done, h.failed, h.out, h.ret = true, err != nil, out, r.stamp()
}

// Operations returns the history recorded so far. Writes that failed, or that
// have not returned yet, are marked Unknown and return after every other event.
func (r *Recorder) Operations() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	end := r.last + 1
	ops := make([]porcupine.Operation, 0, len(r.handles))
	for _, h := range r.handles {
		op := porcupine.Operation{ClientId: h.client, Input: h.in, Call: h.call, Output: h.out, Return: h.ret}
		if !h.done || h.failed {
			if h.in.Op == Get {
				continue
			}
			op.Output, op.Return = Output{Unknown: true}, end
		}
		ops = append(ops, op)
	}
	return ops
}
