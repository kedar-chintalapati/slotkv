package sim

import (
	"context"
	"sync"
	"testing"
	"time"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

func TestShortRunIsLinearizable(t *testing.T) {
	for _, seed := range []int64{17, 23} {
		t.Run(time.Duration(seed).String(), func(t *testing.T) {
			got := Run(context.Background(), Config{Seed: seed, Duration: 900 * time.Millisecond, Nodes: 3, Groups: 2, ReadMode: "verify", Dedup: true, Faults: []string{"partition"}, CheckTimeout: 5 * time.Second})
			if got.Err != "" {
				t.Fatalf("sim run: %s", got.Err)
			}
			if got.Verdict != "ok" {
				t.Fatalf("seed %d: verdict %q (%d ops, %d failed)", seed, got.Verdict, got.Ops, got.FailedOps)
			}
			if got.Ops == 0 {
				t.Fatal("run recorded no operations")
			}
			if got.SuccessReads == 0 || got.SuccessWrites == 0 {
				t.Fatalf("run needs successful reads and writes, got %d reads and %d writes", got.SuccessReads, got.SuccessWrites)
			}
			if got.Partitions == 0 {
				t.Fatal("run injected no partitions")
			}
		})
	}
}

func TestDroppedResponsesAreCounted(t *testing.T) {
	got := Run(context.Background(), Config{Seed: 31, Duration: 200 * time.Millisecond, Nodes: 3, Groups: 1, ReadMode: "verify", Dedup: true, DropProbability: 1, Faults: []string{"drop"}})
	if got.DroppedResponses == 0 {
		t.Fatal("run dropped no successful write responses")
	}
}

func TestDropDecisionIsSeededPerWriteAttempt(t *testing.T) {
	req := &slotkvv1.PutRequest{Session: &slotkvv1.Session{ClientId: "session", Seq: 4}, Key: []byte("key"), Value: []byte("value")}
	trialsA, trialsB := map[string]uint64{}, map[string]uint64{}
	var muA, muB sync.Mutex
	for attempt := 0; attempt < 4; attempt++ {
		a := shouldDrop(91, 0.5, "/slotkv.v1.KVService/Put", req, trialsA, &muA)
		b := shouldDrop(91, 0.5, "/slotkv.v1.KVService/Put", req, trialsB, &muB)
		if a != b {
			t.Fatalf("attempt %d got different decisions for the same seed: %v vs %v", attempt, a, b)
		}
	}
}

func TestRunRejectsNegativeTimeouts(t *testing.T) {
	for name, cfg := range map[string]Config{
		"duration":      {Nodes: 3, Groups: 1, Duration: -time.Millisecond},
		"check timeout": {Nodes: 3, Groups: 1, CheckTimeout: -time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Run(context.Background(), cfg); got.Err == "" {
				t.Fatal("Run accepted a negative duration")
			}
		})
	}
}

func TestRunHonorsCancellationDuringStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := Run(ctx, Config{Nodes: 3, Groups: 1, Duration: time.Second})
	if got.Err == "" {
		t.Fatal("cancelled run returned without an error")
	}
}
