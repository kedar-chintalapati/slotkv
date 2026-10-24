package lincheck_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/kedar-chintalapati/slotkv/internal/lincheck"
)

func put(k, v string) lincheck.Input { return lincheck.Input{Op: lincheck.Put, Key: k, Value: v} }
func get(k string) lincheck.Input    { return lincheck.Input{Op: lincheck.Get, Key: k} }
func del(k string) lincheck.Input    { return lincheck.Input{Op: lincheck.Delete, Key: k} }

func found(v string) lincheck.Output { return lincheck.Output{Value: v, Found: true} }

var (
	absent  = lincheck.Output{}
	existed = lincheck.Output{Existed: true}
	timeout = errors.New("timeout")
)

// at builds an operation with explicit timestamps.
func at(call, ret int64, in lincheck.Input, out lincheck.Output) porcupine.Operation {
	return porcupine.Operation{Input: in, Output: out, Call: call, Return: ret}
}

func check(t *testing.T, ops []porcupine.Operation) porcupine.CheckResult {
	t.Helper()
	res, _ := lincheck.Check(ops, 10*time.Second)
	return res
}

// history records calls made one after another, in order.
func history(steps func(do func(client int, in lincheck.Input, out lincheck.Output, err error))) []porcupine.Operation {
	r := lincheck.NewRecorder()
	steps(func(client int, in lincheck.Input, out lincheck.Output, err error) {
		r.End(r.Begin(client, in), out, err)
	})
	return r.Operations()
}

func TestConcurrentHistoryIsLinearizable(t *testing.T) {
	ops := []porcupine.Operation{
		at(0, 10, put("x", "1"), absent),
		at(5, 15, get("x"), found("1")), // overlaps the put and sees it
		at(2, 8, get("x"), absent),      // overlaps the put and does not
		at(20, 30, del("x"), existed),
		at(40, 50, get("x"), absent),
	}
	if got := check(t, ops); got != porcupine.Ok {
		t.Fatalf("Check = %v, want Ok", got)
	}
}

func TestStaleReadAfterCompletedPutIsIllegal(t *testing.T) {
	ops := []porcupine.Operation{
		at(0, 10, put("x", "1"), absent),
		at(20, 30, put("x", "2"), absent),
		at(40, 50, get("x"), found("1")),
	}
	if got := check(t, ops); got != porcupine.Illegal {
		t.Fatalf("Check = %v, want Illegal", got)
	}
	ops[2] = at(40, 50, get("x"), absent)
	if got := check(t, ops); got != porcupine.Illegal {
		t.Fatalf("read of a missing key after a put: Check = %v, want Illegal", got)
	}
}

func TestDeleteMustReportExistence(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []porcupine.Operation
		want porcupine.CheckResult
	}{
		{"existing key", []porcupine.Operation{at(0, 1, put("x", "1"), absent), at(2, 3, del("x"), existed)}, porcupine.Ok},
		{"existing key reported absent", []porcupine.Operation{at(0, 1, put("x", "1"), absent), at(2, 3, del("x"), absent)}, porcupine.Illegal},
		{"missing key", []porcupine.Operation{at(0, 1, del("x"), absent)}, porcupine.Ok},
		{"missing key reported existing", []porcupine.Operation{at(0, 1, del("x"), existed)}, porcupine.Illegal},
		{"deleted key reported existing", []porcupine.Operation{at(0, 1, put("x", "1"), absent), at(2, 3, del("x"), existed), at(4, 5, del("x"), existed)}, porcupine.Illegal},
		{"read after delete", []porcupine.Operation{at(0, 1, put("x", "1"), absent), at(2, 3, del("x"), existed), at(4, 5, get("x"), found("1"))}, porcupine.Illegal},
	} {
		if got := check(t, tc.ops); got != tc.want {
			t.Errorf("%s: Check = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestKeysAreIndependent(t *testing.T) {
	ops := []porcupine.Operation{
		at(0, 10, put("a", "1"), absent),
		at(20, 30, get("b"), absent),
		at(20, 30, get("a"), found("1")),
	}
	if got := check(t, ops); got != porcupine.Ok {
		t.Fatalf("Check = %v, want Ok", got)
	}
}

func TestUnknownPutMayOrMayNotHaveHappened(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reads []lincheck.Output
		want  porcupine.CheckResult
	}{
		{"happened", []lincheck.Output{found("2")}, porcupine.Ok},
		{"did not happen", []lincheck.Output{found("1")}, porcupine.Ok},
		{"happened later", []lincheck.Output{found("1"), found("2")}, porcupine.Ok},
		{"never happened", []lincheck.Output{found("1"), found("1")}, porcupine.Ok},
		{"cannot be undone", []lincheck.Output{found("2"), found("1")}, porcupine.Illegal},
		{"nobody wrote it", []lincheck.Output{found("3")}, porcupine.Illegal},
	} {
		ops := history(func(do func(int, lincheck.Input, lincheck.Output, error)) {
			do(0, put("x", "1"), absent, nil)
			do(1, put("x", "2"), absent, timeout) // outcome unknown
			for _, out := range tc.reads {
				do(0, get("x"), out, nil)
			}
		})
		if got := check(t, ops); got != tc.want {
			t.Errorf("%s: Check = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEarlyUnknownWriteCannotBeRemovedAsHistoryTail(t *testing.T) {
	ops := []porcupine.Operation{
		at(0, 1, put("x", "1"), absent),
		at(2, 3, put("x", "2"), lincheck.Output{Unknown: true}),
		at(4, 5, get("x"), found("1")),
	}
	if got := check(t, ops); got == porcupine.Ok {
		t.Fatalf("Check = %v, must not certify an early-returning unknown write as a tail operation", got)
	}
}

func TestLateUnknownWriteMayBePlacedAfterKnownHistory(t *testing.T) {
	ops := []porcupine.Operation{
		at(0, 1, put("x", "1"), absent),
		at(2, 3, get("x"), found("1")),
		at(2, 5, put("x", "2"), lincheck.Output{Unknown: true}),
	}
	if got := check(t, ops); got != porcupine.Ok {
		t.Fatalf("Check = %v, want Ok when every unknown write can be placed after known operations", got)
	}
}

func TestUnknownDeleteMayOrMayNotHaveHappened(t *testing.T) {
	for _, tc := range []struct {
		name string
		read lincheck.Output
		want porcupine.CheckResult
	}{
		{"happened", absent, porcupine.Ok},
		{"did not happen", found("1"), porcupine.Ok},
		{"nobody wrote it", found("2"), porcupine.Illegal},
	} {
		ops := history(func(do func(int, lincheck.Input, lincheck.Output, error)) {
			do(0, put("x", "1"), absent, nil)
			do(1, del("x"), absent, timeout)
			do(0, get("x"), tc.read, nil)
		})
		if got := check(t, ops); got != tc.want {
			t.Errorf("%s: Check = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestRecorderOperations(t *testing.T) {
	r := lincheck.NewRecorder()
	r.End(r.Begin(0, put("a", "1")), lincheck.Output{}, nil)
	r.End(r.Begin(1, get("a")), found("1"), nil)
	r.End(r.Begin(1, get("a")), lincheck.Output{}, timeout) // failed Get: dropped
	r.End(r.Begin(2, put("b", "2")), lincheck.Output{}, timeout)
	r.Begin(3, del("b"))                      // never returned: unknown
	r.Begin(3, get("b"))                      // never returned: dropped
	r.End(r.Begin(0, del("a")), existed, nil) // the latest event

	ops := r.Operations()
	if len(ops) != 5 {
		t.Fatalf("got %d operations, want 5: %+v", len(ops), ops)
	}
	var latest int64
	for _, op := range ops {
		if op.Output.(lincheck.Output).Unknown {
			continue
		}
		if op.Call >= op.Return {
			t.Errorf("%v: call %d is not before return %d", op.Input, op.Call, op.Return)
		}
		latest = max(latest, op.Return)
	}
	unknown := 0
	for _, op := range ops {
		if !op.Output.(lincheck.Output).Unknown {
			continue
		}
		unknown++
		if op.Return <= latest {
			t.Errorf("unknown %v returns at %d, want after every other event (%d)", op.Input, op.Return, latest)
		}
		if op.Input.(lincheck.Input).Op == lincheck.Get {
			t.Errorf("unknown Get kept: %+v", op)
		}
	}
	if unknown != 2 {
		t.Errorf("got %d unknown operations, want 2", unknown)
	}
	if got := check(t, ops); got != porcupine.Ok {
		t.Errorf("Check = %v, want Ok", got)
	}
}

func TestRecorderSupportsConcurrentUse(t *testing.T) {
	var (
		mu    sync.Mutex
		store = map[string]string{}
		rec   lincheck.Recorder // the zero value works
		wg    sync.WaitGroup
	)
	for c := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				k := fmt.Sprintf("k%d", i%3)
				in := lincheck.Input{Op: lincheck.Op(i % 3), Key: k, Value: fmt.Sprintf("%d-%d", c, i)}
				h := rec.Begin(c, in)
				mu.Lock()
				v, ok := store[k]
				var out lincheck.Output
				switch in.Op {
				case lincheck.Get:
					out = lincheck.Output{Value: v, Found: ok}
				case lincheck.Put:
					store[k] = in.Value
				case lincheck.Delete:
					delete(store, k)
					out = lincheck.Output{Existed: ok}
				}
				mu.Unlock()
				rec.End(h, out, nil)
			}
		}()
	}
	wg.Wait()
	ops := rec.Operations()
	if len(ops) != 8*200 {
		t.Fatalf("got %d operations, want %d", len(ops), 8*200)
	}
	if got := check(t, ops); got != porcupine.Ok {
		t.Fatalf("Check = %v, want Ok", got)
	}
}

func TestVisualizeWritesHTML(t *testing.T) {
	_, info := lincheck.Check([]porcupine.Operation{
		at(0, 10, put("x", "1"), absent),
		at(20, 30, get("x"), absent),
	}, time.Second)
	path := filepath.Join(t.TempDir(), "nested", "history.html")
	if err := lincheck.Visualize(info, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `get(\"x\")`) {
		t.Fatalf("visualization lacks the operation description; %d bytes", len(data))
	}
}

func TestManyUnknownWritesStillDetectViolation(t *testing.T) {
	// Each unknown put may have taken effect at any later time, which makes
	// proving a history illegal exponential in their number unless Check relaxes
	// them first.
	ops := []porcupine.Operation{at(0, 1, put("x", "base"), absent)}
	for i := range 24 {
		c := int64(10*i + 10)
		ops = append(ops,
			at(c, 1<<40, put("x", fmt.Sprintf("u%d", i)), lincheck.Output{Unknown: true}),
			at(c+2, c+3, put("x", fmt.Sprintf("k%d", i)), absent),
			at(c+4, c+5, get("x"), found(fmt.Sprintf("k%d", i))))
	}
	ops = append(ops,
		at(10000, 10001, put("x", "A"), absent),
		at(10002, 10003, put("x", "B"), absent),
		at(10004, 10005, get("x"), found("A")))

	start := time.Now()
	res, _ := lincheck.Check(ops, time.Minute)
	if res != porcupine.Illegal || time.Since(start) > 10*time.Second {
		t.Fatalf("Check = %v after %v, want Illegal in a few seconds", res, time.Since(start))
	}
}

// TestCheckAgreesWithUnrelaxedModel compares Check with the plain model on
// random small histories, some legal and some corrupted, that contain
// unknown-outcome writes.
func TestCheckAgreesWithUnrelaxedModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	illegal := 0
	for range 3000 {
		ops := randomHistory(rng)
		want := porcupine.CheckOperations(lincheck.Model, ops)
		if got, _ := lincheck.Check(ops, 0); (got == porcupine.Ok) != want {
			t.Fatalf("Check = %v, but the plain model says linearizable = %v for\n%+v", got, want, ops)
		}
		if !want {
			illegal++
		}
	}
	if illegal < 300 || illegal > 2700 {
		t.Fatalf("%d of 3000 histories were illegal; the generator should produce both kinds", illegal)
	}
}

// randomHistory executes random operations against a sequential model at
// random points inside their intervals, turns some writes into unknown ones
// (which take effect only half the time), and corrupts a few results.
func randomHistory(rng *rand.Rand) []porcupine.Operation {
	type step struct {
		at int64
		op porcupine.Operation
	}
	var steps []step
	for i := range 4 + rng.IntN(7) {
		in := lincheck.Input{Key: fmt.Sprintf("k%d", rng.IntN(2))}
		switch rng.IntN(3) {
		case 0:
			in.Op = lincheck.Get
		case 1:
			in.Op, in.Value = lincheck.Put, fmt.Sprintf("v%d", rng.IntN(3))
		default:
			in.Op = lincheck.Delete
		}
		p := int64(100 * (i + 1))
		p += int64(rng.IntN(100))
		steps = append(steps, step{p, porcupine.Operation{Input: in, Call: p - int64(rng.IntN(150)), Return: p + 1 + int64(rng.IntN(150))}})
	}
	slices.SortFunc(steps, func(a, b step) int { return int(a.at - b.at) })
	state := map[string]string{}
	var ops []porcupine.Operation
	for _, s := range steps {
		op := s.op
		in := op.Input.(lincheck.Input)
		v, present := state[in.Key]
		var out lincheck.Output
		switch in.Op {
		case lincheck.Get:
			out = lincheck.Output{Value: v, Found: present}
		case lincheck.Put:
			state[in.Key] = in.Value
		case lincheck.Delete:
			delete(state, in.Key)
			out.Existed = present
		}
		switch {
		case in.Op != lincheck.Get && rng.IntN(3) == 0:
			out = lincheck.Output{Unknown: true}
			op.Return = 10000
			if rng.IntN(2) == 0 { // never took effect: undo
				if present {
					state[in.Key] = v
				} else {
					delete(state, in.Key)
				}
			}
		case rng.IntN(8) == 0:
			out = lincheck.Output{Value: fmt.Sprintf("v%d", rng.IntN(3)), Found: rng.IntN(2) == 0, Existed: rng.IntN(2) == 0}
			if in.Op == lincheck.Put {
				out = lincheck.Output{}
			}
		}
		op.Output = out
		ops = append(ops, op)
	}
	return ops
}
