package node

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
)

// errKeyNotFound is what Raft expects from a StableStore for a missing key; it
// compares the error text with "not found".
var errKeyNotFound = errors.New("not found")

var (
	errLogClosed  = errors.New("log store closed")
	errCorruptLog = errors.New("corrupt log entry")
)

// maxCommitBatch bounds the write requests merged into one transaction.
const maxCommitBatch = 4096

// sharedLog is one bbolt database holding the Raft log and stable state of
// every group on a node. All writes go through a single committer goroutine
// that merges whatever requests are waiting, from any group, into one
// transaction, so a single fsync makes the whole batch durable.
//
// With a database per group, each group pays for its own fsync, so sharding a
// node's writes over more groups multiplies the syncs it issues and shrinks
// the batch each one carries. On a disk where syncs are the bottleneck, that
// made 24 groups about seven times slower than one.
type sharedLog struct {
	db      *bbolt.DB
	reqs    chan *logWrite
	closing chan struct{}
	stopped chan struct{}
	once    sync.Once
	commits atomic.Uint64 // transactions committed
	writes  atomic.Uint64 // write requests committed
}

type logWrite struct {
	apply func(*bbolt.Tx) error
	done  chan error
}

func openSharedLog(path string, groups []int) (*sharedLog, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout:         boltOpenTimeout,
		NoFreelistSync:  true, // rebuilt on open; saves a page write per commit
		FreelistType:    bbolt.FreelistMapType,
		InitialMmapSize: 32 << 20,
	})
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, g := range groups {
			for _, name := range [][]byte{logsBucket(g), stableBucket(g)} {
				if _, err := tx.CreateBucketIfNotExists(name); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init %s: %w", path, err)
	}
	s := &sharedLog{db: db, reqs: make(chan *logWrite), closing: make(chan struct{}), stopped: make(chan struct{})}
	go s.commitLoop()
	return s, nil
}

// commitLoop is the only writer. While one transaction syncs, new requests
// queue on the unbuffered channel; the next iteration takes all of them.
func (s *sharedLog) commitLoop() {
	defer close(s.stopped)
	batch := make([]*logWrite, 0, 64)
	errs := make([]error, 0, 64)
	for {
		select {
		case w := <-s.reqs:
			batch = append(batch[:0], w)
		case <-s.closing:
			return
		}
	collect:
		for len(batch) < maxCommitBatch {
			select {
			case w := <-s.reqs:
				batch = append(batch, w)
			default:
				break collect
			}
		}
		errs = errs[:0]
		err := s.db.Update(func(tx *bbolt.Tx) error {
			for _, w := range batch {
				errs = append(errs, w.apply(tx))
			}
			return nil
		})
		if err == nil {
			s.commits.Add(1)
			s.writes.Add(uint64(len(batch)))
		}
		for i, w := range batch {
			if err != nil {
				w.done <- err
			} else {
				w.done <- errs[i]
			}
		}
	}
}

// write runs apply in the next group commit and waits until it is durable.
func (s *sharedLog) write(apply func(*bbolt.Tx) error) error {
	w := &logWrite{apply: apply, done: make(chan error, 1)}
	select {
	case s.reqs <- w:
	case <-s.closing:
		return errLogClosed
	}
	return <-w.done
}

// Close stops the committer after its current transaction and closes the database.
func (s *sharedLog) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closing)
		<-s.stopped
		err = s.db.Close()
	})
	return err
}

// group returns the LogStore and StableStore of group id.
func (s *sharedLog) group(id int) *groupLog {
	return &groupLog{s: s, logs: logsBucket(id), stable: stableBucket(id)}
}

func logsBucket(g int) []byte   { return []byte("logs/" + strconv.Itoa(g)) }
func stableBucket(g int) []byte { return []byte("stable/" + strconv.Itoa(g)) }

// groupLog is one group's view of a sharedLog.
type groupLog struct {
	s      *sharedLog
	logs   []byte
	stable []byte
}

var (
	_ raft.LogStore    = (*groupLog)(nil)
	_ raft.StableStore = (*groupLog)(nil)
)

func (g *groupLog) FirstIndex() (uint64, error) { return g.edge(true) }
func (g *groupLog) LastIndex() (uint64, error)  { return g.edge(false) }

func (g *groupLog) edge(first bool) (uint64, error) {
	var idx uint64
	err := g.s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(g.logs).Cursor()
		k, _ := c.Last()
		if first {
			k, _ = c.First()
		}
		if k != nil {
			idx = binary.BigEndian.Uint64(k)
		}
		return nil
	})
	return idx, err
}

func (g *groupLog) GetLog(index uint64, out *raft.Log) error {
	return g.s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(g.logs).Get(u64(index))
		if v == nil {
			return raft.ErrLogNotFound
		}
		return decodeLog(v, out)
	})
}

func (g *groupLog) StoreLog(l *raft.Log) error { return g.StoreLogs([]*raft.Log{l}) }

func (g *groupLog) StoreLogs(logs []*raft.Log) error {
	// Encode before queueing, so the committer only copies bytes.
	keys := make([][]byte, len(logs))
	vals := make([][]byte, len(logs))
	for i, l := range logs {
		keys[i], vals[i] = u64(l.Index), encodeLog(l)
	}
	return g.s.write(func(tx *bbolt.Tx) error {
		b := tx.Bucket(g.logs)
		for i := range keys {
			if err := b.Put(keys[i], vals[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (g *groupLog) DeleteRange(min, max uint64) error {
	return g.s.write(func(tx *bbolt.Tx) error {
		b := tx.Bucket(g.logs)
		// Collect first: deleting under a cursor can skip the next key.
		var keys [][]byte
		c := b.Cursor()
		for k, _ := c.Seek(u64(min)); k != nil && binary.BigEndian.Uint64(k) <= max; k, _ = c.Next() {
			keys = append(keys, bytes.Clone(k))
		}
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (g *groupLog) Set(key, val []byte) error {
	key, val = bytes.Clone(key), bytes.Clone(val)
	return g.s.write(func(tx *bbolt.Tx) error { return tx.Bucket(g.stable).Put(key, val) })
}

func (g *groupLog) Get(key []byte) ([]byte, error) {
	var out []byte
	err := g.s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(g.stable).Get(key)
		if v == nil {
			return errKeyNotFound
		}
		out = append([]byte{}, v...)
		return nil
	})
	return out, err
}

func (g *groupLog) SetUint64(key []byte, val uint64) error { return g.Set(key, u64(val)) }

func (g *groupLog) GetUint64(key []byte) (uint64, error) {
	v, err := g.Get(key)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("stable key %q holds %d bytes, want 8", key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

// encodeLog lays out a log entry as index, term, type and append time (Unix
// nanoseconds, 0 if unset), then the length-prefixed data and extensions.
func encodeLog(l *raft.Log) []byte {
	b := make([]byte, 0, 25+2*binary.MaxVarintLen64+len(l.Data)+len(l.Extensions))
	b = binary.BigEndian.AppendUint64(b, l.Index)
	b = binary.BigEndian.AppendUint64(b, l.Term)
	b = append(b, byte(l.Type))
	var at int64
	if !l.AppendedAt.IsZero() {
		at = l.AppendedAt.UnixNano()
	}
	b = binary.BigEndian.AppendUint64(b, uint64(at))
	b = binary.AppendUvarint(b, uint64(len(l.Data)))
	b = append(b, l.Data...)
	b = binary.AppendUvarint(b, uint64(len(l.Extensions)))
	return append(b, l.Extensions...)
}

// decodeLog parses an entry written by encodeLog, copying out of b, which
// bbolt only keeps valid for the transaction.
func decodeLog(b []byte, l *raft.Log) error {
	if len(b) < 25 {
		return errCorruptLog
	}
	l.Index = binary.BigEndian.Uint64(b)
	l.Term = binary.BigEndian.Uint64(b[8:])
	l.Type = raft.LogType(b[16])
	l.AppendedAt = time.Time{}
	if at := int64(binary.BigEndian.Uint64(b[17:])); at != 0 {
		l.AppendedAt = time.Unix(0, at)
	}
	var err error
	rest := b[25:]
	if l.Data, rest, err = lengthPrefixed(rest); err != nil {
		return err
	}
	l.Extensions, _, err = lengthPrefixed(rest)
	return err
}

func lengthPrefixed(b []byte) (val, rest []byte, err error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		return nil, nil, errCorruptLog
	}
	b = b[k:]
	if n == 0 {
		return nil, b, nil
	}
	return bytes.Clone(b[:n]), b[n:], nil
}
