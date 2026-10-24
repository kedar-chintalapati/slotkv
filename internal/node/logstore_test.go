package node

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func openTestLog(t *testing.T, path string, groups ...int) *sharedLog {
	t.Helper()
	s, err := openSharedLog(path, groups)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func entries(from, to uint64, term uint64) []*raft.Log {
	var out []*raft.Log
	for i := from; i <= to; i++ {
		out = append(out, &raft.Log{
			Index: i, Term: term, Type: raft.LogCommand,
			Data: []byte(fmt.Sprintf("cmd-%d", i)), Extensions: []byte{byte(i)},
			AppendedAt: time.Unix(0, int64(i)*1000),
		})
	}
	return out
}

func TestSharedLogKeepsGroupsApart(t *testing.T) {
	s := openTestLog(t, filepath.Join(t.TempDir(), "raft.db"), 0, 1)
	defer s.Close()
	g0, g1 := s.group(0), s.group(1)
	if err := g0.StoreLogs(entries(1, 10, 2)); err != nil {
		t.Fatal(err)
	}
	if err := g1.StoreLog(entries(5, 5, 3)[0]); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		g           *groupLog
		first, last uint64
	}{{g0, 1, 10}, {g1, 5, 5}} {
		if f, _ := c.g.FirstIndex(); f != c.first {
			t.Fatalf("first = %d, want %d", f, c.first)
		}
		if l, _ := c.g.LastIndex(); l != c.last {
			t.Fatalf("last = %d, want %d", l, c.last)
		}
	}
	var got raft.Log
	if err := g0.GetLog(7, &got); err != nil {
		t.Fatal(err)
	}
	want := entries(7, 7, 2)[0]
	if got.Index != 7 || got.Term != 2 || got.Type != raft.LogCommand || !bytes.Equal(got.Data, want.Data) ||
		!bytes.Equal(got.Extensions, want.Extensions) || !got.AppendedAt.Equal(want.AppendedAt) {
		t.Fatalf("GetLog(7) = %+v, want %+v", got, want)
	}
	if err := g1.GetLog(7, &got); !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("GetLog on the other group = %v, want ErrLogNotFound", err)
	}
	empty := s.group(1)
	if err := empty.DeleteRange(5, 5); err != nil {
		t.Fatal(err)
	}
	if f, _ := empty.FirstIndex(); f != 0 {
		t.Fatalf("first index of an empty log = %d, want 0", f)
	}
}

func TestSharedLogDeleteRange(t *testing.T) {
	s := openTestLog(t, filepath.Join(t.TempDir(), "raft.db"), 0)
	defer s.Close()
	g := s.group(0)
	if err := g.StoreLogs(entries(1, 20, 1)); err != nil {
		t.Fatal(err)
	}
	if err := g.DeleteRange(1, 15); err != nil {
		t.Fatal(err)
	}
	if f, _ := g.FirstIndex(); f != 16 {
		t.Fatalf("first = %d after compaction, want 16", f)
	}
	if err := g.DeleteRange(18, 30); err != nil { // a conflicting suffix
		t.Fatal(err)
	}
	if l, _ := g.LastIndex(); l != 17 {
		t.Fatalf("last = %d after truncation, want 17", l)
	}
}

func TestSharedLogStableStore(t *testing.T) {
	s := openTestLog(t, filepath.Join(t.TempDir(), "raft.db"), 0, 1)
	defer s.Close()
	g := s.group(0)
	// Raft recognizes a missing key by this exact text.
	if _, err := g.GetUint64([]byte("CurrentTerm")); err == nil || err.Error() != "not found" {
		t.Fatalf("missing key: err = %v, want \"not found\"", err)
	}
	if err := g.SetUint64([]byte("CurrentTerm"), 42); err != nil {
		t.Fatal(err)
	}
	if v, err := g.GetUint64([]byte("CurrentTerm")); err != nil || v != 42 {
		t.Fatalf("GetUint64 = %d, %v", v, err)
	}
	if err := g.Set([]byte("LastVoteCand"), []byte("kv-1")); err != nil {
		t.Fatal(err)
	}
	if v, _ := g.Get([]byte("LastVoteCand")); string(v) != "kv-1" {
		t.Fatalf("Get = %q", v)
	}
	if _, err := s.group(1).Get([]byte("LastVoteCand")); err == nil {
		t.Fatal("stable state leaked into another group")
	}
}

func TestSharedLogGroupCommitsConcurrentWriters(t *testing.T) {
	const groups, writers, perWriter = 8, 4, 50
	ids := make([]int, groups)
	for i := range ids {
		ids[i] = i
	}
	s := openTestLog(t, filepath.Join(t.TempDir(), "raft.db"), ids...)
	defer s.Close()
	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				gl := s.group(g)
				for i := 0; i < perWriter; i++ {
					idx := uint64(w*perWriter + i + 1)
					if err := gl.StoreLogs(entries(idx, idx, 1)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	for g := 0; g < groups; g++ {
		if l, _ := s.group(g).LastIndex(); l != writers*perWriter {
			t.Fatalf("group %d: last index %d, want %d", g, l, writers*perWriter)
		}
	}
	writes, commits := s.writes.Load(), s.commits.Load()
	if writes != groups*writers*perWriter {
		t.Fatalf("%d writes committed, want %d", writes, groups*writers*perWriter)
	}
	t.Logf("%d writes in %d commits", writes, commits)
}

func TestSharedLogSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	s := openTestLog(t, path, 3)
	if err := s.group(3).StoreLogs(entries(1, 5, 4)); err != nil {
		t.Fatal(err)
	}
	if err := s.group(3).SetUint64([]byte("CurrentTerm"), 4); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.group(3).StoreLog(entries(6, 6, 4)[0]); !errors.Is(err, errLogClosed) {
		t.Fatalf("write after Close = %v, want errLogClosed", err)
	}
	s = openTestLog(t, path, 3)
	defer s.Close()
	if l, _ := s.group(3).LastIndex(); l != 5 {
		t.Fatalf("last index after reopen = %d, want 5", l)
	}
	if v, _ := s.group(3).GetUint64([]byte("CurrentTerm")); v != 4 {
		t.Fatalf("term after reopen = %d, want 4", v)
	}
}

func TestDecodeLogRejectsTruncatedEntries(t *testing.T) {
	b := encodeLog(entries(1, 1, 1)[0])
	var l raft.Log
	for _, n := range []int{0, 10, 24, len(b) - 1} {
		if err := decodeLog(b[:n], &l); err == nil {
			t.Fatalf("decoded a %d-byte prefix of a %d-byte entry", n, len(b))
		}
	}
}
