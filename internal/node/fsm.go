package node

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

// result is the outcome of a write, cached per session for deduplication.
type result struct{ existed bool }

type session struct {
	seq uint64
	res result
}

// fsm is one group's state machine: a key-value map and a session table.
// Raft applies commands from a single goroutine; reads take the read lock.
type fsm struct {
	dedup    bool
	mu       sync.RWMutex
	kv       map[string][]byte
	sessions map[string]session
}

func newFSM(dedup bool) *fsm {
	return &fsm{dedup: dedup, kv: map[string][]byte{}, sessions: map[string]session{}}
}

// get returns the stored value. Values are never modified in place, so the
// slice stays valid after the lock is released.
func (f *fsm) get(key []byte) ([]byte, bool) {
	f.mu.RLock()
	v, ok := f.kv[string(key)]
	f.mu.RUnlock()
	return v, ok
}

// Apply implements raft.FSM.
func (f *fsm) Apply(l *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.apply(l.Data)
}

// ApplyBatch implements raft.BatchingFSM and locks once per batch.
func (f *fsm) ApplyBatch(logs []*raft.Log) []any {
	out := make([]any, len(logs))
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, l := range logs {
		if l.Type == raft.LogCommand {
			out[i] = f.apply(l.Data)
		}
	}
	return out
}

// apply runs one command and returns a result, or an error if the command is
// malformed. A command whose seq is not newer than its session's is a retry:
// it returns the cached result and changes nothing.
func (f *fsm) apply(data []byte) any {
	var c slotkvv1.Command
	if err := proto.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	id, seq := c.GetSession().GetClientId(), c.GetSession().GetSeq()
	track := f.dedup && id != ""
	if track {
		if s, ok := f.sessions[id]; ok && seq <= s.seq {
			return s.res
		}
	}
	key := string(c.Key)
	_, existed := f.kv[key]
	switch c.Op {
	case slotkvv1.Command_OP_PUT:
		f.kv[key] = c.Value
	case slotkvv1.Command_OP_DELETE:
		delete(f.kv, key)
	default:
		return fmt.Errorf("unknown op %v", c.Op)
	}
	res := result{existed}
	if track {
		f.sessions[id] = session{seq, res}
	}
	return res
}

// snapshot is an immutable copy of an fsm's state.
type snapshot struct {
	kv       map[string][]byte
	sessions map[string]session
}

// Snapshot implements raft.FSM. The copy is shallow: stored values are
// immutable, so only the maps are cloned.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &snapshot{maps.Clone(f.kv), maps.Clone(f.sessions)}, nil
}

// Restore implements raft.FSM and replaces all state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	s, err := readSnapshot(rc)
	if err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	f.mu.Lock()
	f.kv, f.sessions = s.kv, s.sessions
	f.mu.Unlock()
	return nil
}

// Snapshot encoding: a version byte, then uvarint-length-prefixed entries:
// count, (key, value)*, count, (client_id, seq, existed)*.
const (
	snapshotVersion = 1
	maxSnapshotItem = 1 << 30
)

// Persist implements raft.FSMSnapshot.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	w := bufio.NewWriterSize(sink, 256<<10)
	w.WriteByte(snapshotVersion)
	putUvarint(w, uint64(len(s.kv)))
	for k, v := range s.kv {
		putUvarint(w, uint64(len(k)))
		w.WriteString(k)
		putUvarint(w, uint64(len(v)))
		w.Write(v)
	}
	putUvarint(w, uint64(len(s.sessions)))
	for id, ss := range s.sessions {
		putUvarint(w, uint64(len(id)))
		w.WriteString(id)
		putUvarint(w, ss.seq)
		if ss.res.existed {
			w.WriteByte(1)
		} else {
			w.WriteByte(0)
		}
	}
	if err := w.Flush(); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *snapshot) Release() {}

func putUvarint(w *bufio.Writer, v uint64) {
	var b [binary.MaxVarintLen64]byte
	w.Write(b[:binary.PutUvarint(b[:], v)])
}

func readSnapshot(r io.Reader) (*snapshot, error) {
	br := bufio.NewReaderSize(r, 256<<10)
	if v, err := br.ReadByte(); err != nil || v != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d (%v)", v, err)
	}
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	s := &snapshot{kv: make(map[string][]byte, min(n, 1<<16))}
	for ; n > 0; n-- {
		k, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		v, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		s.kv[string(k)] = v
	}
	if n, err = binary.ReadUvarint(br); err != nil {
		return nil, err
	}
	s.sessions = make(map[string]session, min(n, 1<<16))
	for ; n > 0; n-- {
		id, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		seq, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		existed, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		s.sessions[string(id)] = session{seq, result{existed == 1}}
	}
	return s, nil
}

func readBytes(br *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	if n > maxSnapshotItem {
		return nil, errors.New("snapshot item too large")
	}
	b := make([]byte, n)
	_, err = io.ReadFull(br, b)
	return b, err
}
