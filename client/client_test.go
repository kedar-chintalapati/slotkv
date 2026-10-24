package client_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kedar-chintalapati/slotkv/client"
)

var key = []byte("alpha")

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func wantCalls(t *testing.T, fc *fakeCluster, ids []string, want ...int64) {
	t.Helper()
	if got := fc.calls(ids...); !slices.Equal(got, want) {
		t.Fatalf("calls to %v = %v, want %v", ids, got, want)
	}
}

func TestDialSkipsDeadSeedAndSeedsLeaderCache(t *testing.T) {
	fc := newCluster(t, 3, 4)
	g, reps := fc.replicasOf(key)
	fc.setLeader(g, reps[2])
	fc.report(g, reps[2])

	cl := fc.mustDial(client.Options{}, "ghost:7000", "n1:7000")
	if err := cl.NewSession().Put(testCtx(t), key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, fc, reps, 0, 0, 1)
}

func TestDialFailsWhenNoSeedAnswers(t *testing.T) {
	fc := newCluster(t, 3, 4)
	if _, err := fc.dial(client.Options{}, "ghost:1", "ghost:2"); err == nil {
		t.Fatal("Dial succeeded with unreachable seeds")
	}
}

func TestDialRejectsMismatchedPlacement(t *testing.T) {
	fc := newCluster(t, 3, 4)
	fc.badSlots.Store(true)
	if _, err := fc.dial(client.Options{}); err == nil {
		t.Fatal("Dial accepted a slot map that differs from the computed placement")
	}
}

func TestNotLeaderHintRedirectsAndUpdatesCache(t *testing.T) {
	fc := newCluster(t, 3, 4)
	g, reps := fc.replicasOf(key)
	cl := fc.mustDial(client.Options{}) // Topology names reps[0]
	fc.setLeader(g, reps[1])
	s := cl.NewSession()

	if err := s.Put(testCtx(t), key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, fc, reps, 1, 1, 0)

	if err := s.Put(testCtx(t), key, []byte("w")); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, fc, reps, 1, 2, 0)
}

func TestNotLeaderWithoutHintTriesNextReplica(t *testing.T) {
	fc := newCluster(t, 3, 4)
	fc.noHint.Store(true)
	g, reps := fc.replicasOf(key)
	cl := fc.mustDial(client.Options{})
	fc.setLeader(g, reps[2])
	s := cl.NewSession()

	start := time.Now()
	if err := s.Put(testCtx(t), key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 15*time.Millisecond {
		t.Fatalf("two hintless retries took %v, want a pause of at least 10ms each", d)
	}
	wantCalls(t, fc, reps, 1, 1, 1)

	if err := s.Put(testCtx(t), key, []byte("w")); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, fc, reps, 1, 1, 2)
}

func TestRetryReusesSessionSeq(t *testing.T) {
	fc := newCluster(t, 3, 4)
	_, reps := fc.replicasOf(key)
	s := fc.mustDial(client.Options{}).NewSession()
	fc.nodes[reps[0]].drop.Store(1) // applies the first Put, then answers Unavailable

	ctx := testCtx(t)
	if err := s.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if existed, err := s.Delete(ctx, key); err != nil || !existed {
		t.Fatalf("Delete = %v, %v; want true, nil", existed, err)
	}
	if err := s.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(ctx, key); err != nil {
		t.Fatal(err)
	}

	var seqs []uint64
	for _, w := range fc.writeLog() {
		if w.client != s.ID() {
			t.Fatalf("write from client %q, want %q", w.client, s.ID())
		}
		seqs = append(seqs, w.seq)
	}
	// The lost Put is retried with seq 1; the Delete and the next Put take 2 and 3.
	if want := []uint64{1, 1, 2, 3}; !slices.Equal(seqs, want) {
		t.Fatalf("write seqs = %v, want %v", seqs, want)
	}
	if n := fc.applyCount(); n != 3 {
		t.Fatalf("server applied %d writes, want 3 (the retry is a duplicate)", n)
	}
}

func TestSessionIDsAreRandom64BitHex(t *testing.T) {
	cl := newCluster(t, 3, 4).mustDial(client.Options{})
	seen := map[string]bool{}
	for range 100 {
		id := cl.NewSession().ID()
		var v uint64
		if _, err := fmt.Sscanf(id, "%x", &v); err != nil || len(id) != 16 || seen[id] {
			t.Fatalf("session ID %q is not a fresh 16-digit hex string (%v)", id, err)
		}
		seen[id] = true
	}
}

func TestGivesUpWhenContextExpires(t *testing.T) {
	fc := newCluster(t, 3, 4)
	_, reps := fc.replicasOf(key)
	cl := fc.mustDial(client.Options{})
	fc.failAll(codes.Unavailable)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := cl.NewSession().Put(ctx, key, []byte("v"))
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	// The last attempt either failed with Unavailable or was cut off by the deadline.
	if c := status.Code(err); c != codes.Unavailable && c != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want it to wrap the last attempt's error", err)
	}
	if elapsed < 250*time.Millisecond || elapsed > time.Second {
		t.Fatalf("gave up after %v, want about 300ms", elapsed)
	}
	var attempts int64
	for _, n := range fc.calls(reps...) {
		attempts += n
	}
	if attempts < 3 {
		t.Fatalf("%d attempts in 300ms, want the client to keep retrying", attempts)
	}
}

func TestAttemptTimeoutMovesToNextReplica(t *testing.T) {
	fc := newCluster(t, 3, 4)
	g, reps := fc.replicasOf(key)
	fc.setLeader(g, reps[1]) // Topology still names reps[0], which hangs
	fc.report(g, reps[0])
	fc.nodes[reps[0]].hang.Store(true)
	cl := fc.mustDial(client.Options{AttemptTimeout: 50 * time.Millisecond})

	start := time.Now()
	if err := cl.NewSession().Put(testCtx(t), key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > 2*time.Second {
		t.Fatalf("Put took %v, want one 50ms attempt timeout plus a short pause", d)
	}
	wantCalls(t, fc, reps, 1, 1, 0)
}

func TestAttemptNeverOutlivesContext(t *testing.T) {
	fc := newCluster(t, 3, 4)
	_, reps := fc.replicasOf(key)
	fc.nodes[reps[0]].hang.Store(true)
	cl := fc.mustDial(client.Options{AttemptTimeout: 10 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := cl.NewSession().Put(ctx, key, []byte("v"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("returned after %v, want about 100ms", d)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	fc := newCluster(t, 3, 4)
	_, reps := fc.replicasOf(key)
	cl := fc.mustDial(client.Options{})
	n := fc.nodes[reps[0]]
	n.failCode.Store(int32(codes.InvalidArgument))
	n.fail.Store(5)

	err := cl.NewSession().Put(testCtx(t), key, nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	wantCalls(t, fc, reps, 1, 0, 0)
}

func TestGetPutDeleteRoundTrip(t *testing.T) {
	s := newCluster(t, 3, 4).mustDial(client.Options{}).NewSession()
	ctx := testCtx(t)

	if _, found, err := s.Get(ctx, key); err != nil || found {
		t.Fatalf("Get on empty store = found %v, err %v", found, err)
	}
	if err := s.Put(ctx, key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, found, err := s.Get(ctx, key); err != nil || !found || string(v) != "v" {
		t.Fatalf("Get = %q, %v, %v; want v, true, nil", v, found, err)
	}
	if existed, err := s.Delete(ctx, key); err != nil || !existed {
		t.Fatalf("Delete = %v, %v; want true, nil", existed, err)
	}
	if existed, err := s.Delete(ctx, key); err != nil || existed {
		t.Fatalf("second Delete = %v, %v; want false, nil", existed, err)
	}
}

func TestReadAnyReplica(t *testing.T) {
	fc := newCluster(t, 3, 4)
	fc.anyRead.Store(true)
	_, reps := fc.replicasOf(key)
	ctx := testCtx(t)

	s := fc.mustDial(client.Options{}).NewSession()
	for range 30 {
		if _, _, err := s.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	wantCalls(t, fc, reps, 30, 0, 0)

	s = fc.mustDial(client.Options{ReadAnyReplica: true}).NewSession()
	for range 90 {
		if _, _, err := s.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if got := fc.calls(reps...); got[1] == 0 || got[2] == 0 {
		t.Fatalf("calls with ReadAnyReplica = %v, want followers to serve Gets", got)
	}
}

func TestCloseAbortsRetries(t *testing.T) {
	fc := newCluster(t, 3, 4)
	cl := fc.mustDial(client.Options{})
	fc.failAll(codes.Unavailable)

	errc := make(chan error, 1)
	go func() { errc <- cl.NewSession().Put(context.Background(), key, []byte("v")) }()
	time.Sleep(50 * time.Millisecond)
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, client.ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Put still retrying after Close")
	}
}

func TestConcurrentSessionsAcrossLeaderChanges(t *testing.T) {
	fc := newCluster(t, 3, 4)
	cl := fc.mustDial(client.Options{})
	ctx := testCtx(t)

	stop := make(chan struct{})
	var flipper sync.WaitGroup
	flipper.Add(1)
	go func() {
		defer flipper.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				for g := range cl.Topology().Groups {
					fc.setLeader(g, fmt.Sprintf("n%d", rand.IntN(3)))
				}
			}
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := cl.NewSession()
			for i := range 300 {
				k := fmt.Appendf(nil, "k%d", (w+i)%10)
				var err error
				switch i % 3 {
				case 0:
					err = s.Put(ctx, k, []byte("v"))
				case 1:
					_, _, err = s.Get(ctx, k)
				default:
					_, err = s.Delete(ctx, k)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	flipper.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var calls int64
	for _, n := range fc.nodes {
		calls += n.calls.Load()
	}
	if calls <= 8*300 {
		t.Errorf("%d RPCs for %d requests: leader changes caused no redirects", calls, 8*300)
	}
}
