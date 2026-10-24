// Package lincheck checks recorded slotkv histories for linearizability with
// Porcupine. The sequential specification is a map from key to value, so a
// history is checked one key at a time.
//
// A Put or Delete that failed has an unknown outcome: it may have taken effect
// at any point after it was invoked, or never. Such operations are modeled as
// accepting any output and as not returning until after every other operation,
// which lets the checker place them anywhere after their call, including after
// the end of the history, where they have no effect.
package lincheck

import (
	"fmt"
	"hash/maphash"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/anishathalye/porcupine"
)

// Op is the kind of a key-value operation.
type Op uint8

const (
	Get Op = iota
	Put
	Delete
)

func (o Op) String() string {
	switch o {
	case Get:
		return "get"
	case Put:
		return "put"
	case Delete:
		return "delete"
	}
	return fmt.Sprintf("op(%d)", uint8(o))
}

// Input is the argument of an operation.
type Input struct {
	Op    Op
	Key   string
	Value string // the value written by a Put
}

// Output is the result of an operation.
type Output struct {
	Value   string // Get: the value read, if Found
	Found   bool   // Get: whether the key existed
	Existed bool   // Delete: whether the key existed
	Unknown bool   // Put or Delete: the call failed and may or may not have taken effect
}

// state is the state of one key.
type state struct {
	present bool
	value   string
}

var seed = maphash.MakeSeed()

// Model is the sequential specification of the store, partitioned by key.
var Model = porcupine.Model{
	Partition: partition,
	Init:      func() any { return state{} },
	Step:      step,
	Equal:     func(a, b any) bool { return a.(state) == b.(state) },
	Hash: func(s any) uint64 {
		st := s.(state)
		h := maphash.String(seed, st.value)
		if st.present {
			h = ^h
		}
		return h
	},
	DescribeOperation: describeOperation,
	DescribeState:     describeState,
}

// partition splits a history by key, in key order.
func partition(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	for _, op := range history {
		k := op.Input.(Input).Key
		byKey[k] = append(byKey[k], op)
	}
	parts := make([][]porcupine.Operation, 0, len(byKey))
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		parts = append(parts, byKey[k])
	}
	return parts
}

func step(st, in, out any) (bool, any) {
	s, i, o := st.(state), in.(Input), out.(Output)
	switch i.Op {
	case Get:
		return o.Unknown || (o.Found == s.present && (!o.Found || o.Value == s.value)), s
	case Put:
		return true, state{present: true, value: i.Value}
	case Delete:
		return o.Unknown || o.Existed == s.present, state{}
	}
	return false, s
}

func describeOperation(in, out any) string {
	i, o := in.(Input), out.(Output)
	switch {
	case i.Op == Get && o.Found:
		return fmt.Sprintf("get(%q) -> %q", i.Key, o.Value)
	case i.Op == Get && o.Unknown:
		return fmt.Sprintf("get(%q) -> any", i.Key)
	case i.Op == Get:
		return fmt.Sprintf("get(%q) -> absent", i.Key)
	case i.Op == Put && o.Unknown:
		return fmt.Sprintf("put(%q, %q) -> unknown", i.Key, i.Value)
	case i.Op == Put:
		return fmt.Sprintf("put(%q, %q)", i.Key, i.Value)
	case i.Op == Delete && o.Unknown:
		return fmt.Sprintf("delete(%q) -> unknown", i.Key)
	case i.Op == Delete && o.Existed:
		return fmt.Sprintf("delete(%q) -> existed", i.Key)
	case i.Op == Delete:
		return fmt.Sprintf("delete(%q) -> absent", i.Key)
	}
	return fmt.Sprintf("%v -> %v", i, o)
}

func describeState(st any) string {
	s := st.(state)
	if !s.present {
		return "absent"
	}
	return fmt.Sprintf("%q", s.value)
}

// Check reports whether ops, as returned by Recorder.Operations, are
// linearizable. A zero timeout means no limit; a check that runs out of time
// returns porcupine.Unknown.
//
// Refuting a history takes exponential time in its unknown-outcome writes, each
// of which may have taken effect at any later time. When there are any, Check
// first looks for a violation in a relaxed history (see relax), which is cheap.
func Check(ops []porcupine.Operation, timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	start := time.Now()
	remaining := timeout
	known, fastPath := unknownWritesAtHistoryTail(ops)
	if timeout > 0 {
		remaining = timeout - time.Since(start)
		if remaining <= 0 {
			return porcupine.Unknown, porcupine.LinearizationInfo{}
		}
	}
	if fastPath {
		budget := time.Second
		if remaining > 0 && remaining < budget {
			budget = remaining
		}
		res, info := porcupine.CheckOperationsVerbose(Model, known, budget)
		if res == porcupine.Ok {
			return res, info
		}
		if remaining > 0 {
			remaining = timeout - time.Since(start)
			if remaining <= 0 {
				return porcupine.Unknown, porcupine.LinearizationInfo{}
			}
		}
	}
	relaxed, hasUnknown := relax(ops)
	if timeout > 0 {
		remaining = timeout - time.Since(start)
		if remaining <= 0 {
			return porcupine.Unknown, porcupine.LinearizationInfo{}
		}
	}
	if hasUnknown {
		if res, info := porcupine.CheckOperationsVerbose(Model, relaxed, remaining); res == porcupine.Illegal {
			return res, info
		}
		if remaining > 0 {
			remaining = timeout - time.Since(start)
			if remaining <= 0 {
				return porcupine.Unknown, porcupine.LinearizationInfo{}
			}
		}
	}
	return porcupine.CheckOperationsVerbose(Model, ops, remaining)
}

// unknownWritesAtHistoryTail returns the known operations when every unknown
// write has a well-formed interval ending after all known operations. If the
// known subset is linearizable, placing every unknown write after it is a
// concrete linearization of the full history.
func unknownWritesAtHistoryTail(ops []porcupine.Operation) ([]porcupine.Operation, bool) {
	known := make([]porcupine.Operation, 0, len(ops))
	unknownCount := 0
	minUnknownReturn := int64(1<<63 - 1)
	maxKnownReturn := int64(-1 << 63)
	for _, op := range ops {
		in, inOK := op.Input.(Input)
		out, outOK := op.Output.(Output)
		if !inOK || !outOK {
			return nil, false
		}
		if out.Unknown && (in.Op == Put || in.Op == Delete) {
			if op.Call >= op.Return {
				return nil, false
			}
			unknownCount++
			minUnknownReturn = min(minUnknownReturn, op.Return)
			continue
		}
		known = append(known, op)
		maxKnownReturn = max(maxKnownReturn, op.Return)
	}
	if unknownCount == 0 || (len(known) > 0 && minUnknownReturn <= maxKnownReturn) {
		return nil, false
	}
	return known, true
}

// relax drops the unknown-outcome writes of ops and lets the operations that
// might have observed one return anything. Every history that is linearizable
// stays so, which makes an illegal verdict on the result conclusive. It reports
// false if ops has no unknown-outcome writes.
func relax(ops []porcupine.Operation) ([]porcupine.Operation, bool) {
	type keyed struct{ key, value string }
	firstPut, firstDelete := map[string]int64{}, map[string]int64{} // earliest unknown call per key
	unknownValues := map[keyed]bool{}
	for _, op := range ops {
		in, out := op.Input.(Input), op.Output.(Output)
		if !out.Unknown || in.Op == Get {
			continue
		}
		first := firstDelete
		if in.Op == Put {
			first, unknownValues[keyed{in.Key, in.Value}] = firstPut, true
		}
		if t, ok := first[in.Key]; !ok || op.Call < t {
			first[in.Key] = op.Call
		}
	}
	if len(firstPut)+len(firstDelete) == 0 {
		return nil, false
	}
	// maybe reports whether an unknown call to key started before t.
	maybe := func(first map[string]int64, key string, t int64) bool {
		c, ok := first[key]
		return ok && c < t
	}
	relaxed := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		in, out := op.Input.(Input), op.Output.(Output)
		switch {
		case out.Unknown && in.Op != Get:
			continue
		case in.Op == Get && out.Found && unknownValues[keyed{in.Key, out.Value}],
			in.Op == Get && !out.Found && maybe(firstDelete, in.Key, op.Return),
			in.Op == Delete && out.Existed && maybe(firstPut, in.Key, op.Return),
			in.Op == Delete && !out.Existed && maybe(firstDelete, in.Key, op.Return):
			op.Output = Output{Unknown: true}
		}
		relaxed = append(relaxed, op)
	}
	return relaxed, true
}

// Visualize writes an HTML rendering of a Check result to path.
func Visualize(info porcupine.LinearizationInfo, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return porcupine.VisualizePath(Model, info, path)
}
