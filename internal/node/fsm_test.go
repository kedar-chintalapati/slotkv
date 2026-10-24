package node

import (
	"bytes"
	"testing"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

func cmd(t *testing.T, op slotkvv1.Command_Op, key, value, client string, seq uint64) *raft.Log {
	t.Helper()
	data, err := proto.Marshal(&slotkvv1.Command{
		Op:      op,
		Key:     []byte(key),
		Value:   []byte(value),
		Session: &slotkvv1.Session{ClientId: client, Seq: seq},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Type: raft.LogCommand, Data: data}
}

func put(t *testing.T, f *fsm, key, value, client string, seq uint64) result {
	t.Helper()
	return mustResult(t, f.Apply(cmd(t, slotkvv1.Command_OP_PUT, key, value, client, seq)))
}

func del(t *testing.T, f *fsm, key, client string, seq uint64) result {
	t.Helper()
	return mustResult(t, f.Apply(cmd(t, slotkvv1.Command_OP_DELETE, key, "", client, seq)))
}

func mustResult(t *testing.T, v any) result {
	t.Helper()
	r, ok := v.(result)
	if !ok {
		t.Fatalf("Apply returned %#v, want a result", v)
	}
	return r
}

func want(t *testing.T, f *fsm, key, value string, found bool) {
	t.Helper()
	v, ok := f.get([]byte(key))
	if ok != found || string(v) != value {
		t.Fatalf("get %q = %q, %v; want %q, %v", key, v, ok, value, found)
	}
}

func TestFSMPutGetDelete(t *testing.T) {
	f := newFSM(true)
	want(t, f, "a", "", false)
	if r := put(t, f, "a", "1", "c", 1); r.existed {
		t.Fatal("first put reported an existing key")
	}
	want(t, f, "a", "1", true)
	if r := put(t, f, "a", "2", "c", 2); !r.existed {
		t.Fatal("overwrite did not report an existing key")
	}
	want(t, f, "a", "2", true)
	if r := del(t, f, "a", "c", 3); !r.existed {
		t.Fatal("delete did not report an existing key")
	}
	want(t, f, "a", "", false)
	if r := del(t, f, "a", "c", 4); r.existed {
		t.Fatal("second delete reported an existing key")
	}
	put(t, f, "empty", "", "c", 5)
	want(t, f, "empty", "", true)
}

func TestFSMApplyBatch(t *testing.T) {
	f := newFSM(true)
	out := f.ApplyBatch([]*raft.Log{
		cmd(t, slotkvv1.Command_OP_PUT, "a", "1", "c", 1),
		{Type: raft.LogConfiguration},
		cmd(t, slotkvv1.Command_OP_DELETE, "a", "", "c", 2),
	})
	if len(out) != 3 || out[1] != nil || mustResult(t, out[0]).existed || !mustResult(t, out[2]).existed {
		t.Fatalf("ApplyBatch = %#v", out)
	}
	want(t, f, "a", "", false)
}

func TestFSMDedup(t *testing.T) {
	f := newFSM(true)
	if put(t, f, "a", "1", "c", 1).existed {
		t.Fatal("first put reported an existing key")
	}
	put(t, f, "a", "2", "d", 1) // another session changes the key

	// A retry of c's seq 1 returns the cached result and changes nothing.
	if r := put(t, f, "a", "retry", "c", 1); r.existed {
		t.Fatal("retry returned a fresh result instead of the cached one")
	}
	want(t, f, "a", "2", true)

	// Same for a retried delete.
	if !del(t, f, "a", "c", 2).existed {
		t.Fatal("delete did not report an existing key")
	}
	put(t, f, "a", "back", "d", 2)
	if r := del(t, f, "a", "c", 2); !r.existed {
		t.Fatal("retried delete returned a fresh result instead of the cached one")
	}
	want(t, f, "a", "back", true)

	// A stale seq is also a retry; a newer one applies.
	put(t, f, "z", "stale", "c", 1)
	want(t, f, "z", "", false)
	put(t, f, "a", "3", "c", 3)
	want(t, f, "a", "3", true)

	// Without dedup, every command applies.
	g := newFSM(false)
	put(t, g, "a", "1", "c", 1)
	put(t, g, "a", "2", "c", 1)
	want(t, g, "a", "2", true)
	if len(g.sessions) != 0 {
		t.Fatal("sessions recorded with dedup disabled")
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	f := newFSM(true)
	for i, k := range []string{"a", "b", "c"} {
		put(t, f, k, k+k, "c1", uint64(i+1))
	}
	del(t, f, "b", "c2", 7)
	put(t, f, "big", string(bytes.Repeat([]byte("x"), 1<<20)), "c2", 8)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	put(t, f, "after", "snapshot", "c1", 9) // must not leak into the snapshot

	store := raft.NewInmemSnapshotStore()
	sink, err := store.Create(raft.SnapshotVersionMax, 10, 1, raft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snap.Release()
	_, rc, err := store.Open(sink.ID())
	if err != nil {
		t.Fatal(err)
	}

	g := newFSM(true)
	put(t, g, "stale", "x", "z", 1) // Restore must discard prior state
	if err := g.Restore(rc); err != nil {
		t.Fatal(err)
	}
	want(t, g, "a", "aa", true)
	want(t, g, "b", "", false)
	want(t, g, "c", "cc", true)
	want(t, g, "stale", "", false)
	want(t, g, "after", "", false)
	if v, _ := g.get([]byte("big")); len(v) != 1<<20 {
		t.Fatalf("big value has %d bytes", len(v))
	}
	if len(g.sessions) != 2 || g.sessions["c1"].seq != 3 || g.sessions["c2"].seq != 8 {
		t.Fatalf("sessions = %+v", g.sessions)
	}
	// The restored session table still deduplicates.
	if r := del(t, g, "b", "c2", 7); r.existed {
		t.Fatal("restored session lost its cached result")
	}
	want(t, g, "a", "aa", true)
}

func TestFSMRestoreRejectsGarbage(t *testing.T) {
	f := newFSM(true)
	put(t, f, "a", "1", "c", 1)
	for _, in := range [][]byte{nil, {9}, {snapshotVersion, 5}} {
		store := raft.NewInmemSnapshotStore()
		sink, _ := store.Create(raft.SnapshotVersionMax, 1, 1, raft.Configuration{}, 0, nil)
		sink.Write(in)
		sink.Close()
		_, rc, _ := store.Open(sink.ID())
		if err := f.Restore(rc); err == nil {
			t.Fatalf("Restore(%v) succeeded", in)
		}
	}
	want(t, f, "a", "1", true) // a failed restore leaves state alone
}
