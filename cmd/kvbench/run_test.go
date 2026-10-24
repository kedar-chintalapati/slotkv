package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/anishathalye/porcupine"
	"github.com/kedar-chintalapati/slotkv/client"
	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
	"github.com/kedar-chintalapati/slotkv/internal/lincheck"
)

// fakeKV is a single-node, single-group store. Operations take effect
// atomically, so it is linearizable unless stale makes Gets return the value a
// key had before its latest write.
type fakeKV struct {
	slotkvv1.UnimplementedKVServiceServer
	delay time.Duration
	stale float64

	mu        sync.Mutex
	cur, prev map[string]string
	rng       *rand.Rand
}

func newTestClient(t *testing.T, kv *fakeKV) *client.Client {
	t.Helper()
	kv.cur, kv.prev, kv.rng = map[string]string{}, map[string]string{}, rand.New(rand.NewPCG(7, 7))
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	slotkvv1.RegisterKVServiceServer(srv, kv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	cl, err := client.Dial(context.Background(), []string{"n0:7000"}, client.Options{DialOptions: []grpc.DialOption{dialer}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func (kv *fakeKV) Topology(context.Context, *slotkvv1.TopologyRequest) (*slotkvv1.TopologyResponse, error) {
	resp := &slotkvv1.TopologyResponse{
		NumSlots:    16,
		Nodes:       []*slotkvv1.Node{{Id: "n0", GrpcAddr: "n0:7000", RaftAddr: "n0:7001"}},
		Groups:      []*slotkvv1.Group{{Id: 0, Replicas: []string{"n0"}, LeaderId: "n0"}},
		SlotToGroup: make([]uint32, 16),
	}
	return resp, nil
}

func (kv *fakeKV) Put(_ context.Context, req *slotkvv1.PutRequest) (*slotkvv1.PutResponse, error) {
	time.Sleep(kv.delay)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	k := string(req.Key)
	if old, ok := kv.cur[k]; ok {
		kv.prev[k] = old
	}
	kv.cur[k] = string(req.Value)
	return &slotkvv1.PutResponse{}, nil
}

func (kv *fakeKV) Delete(_ context.Context, req *slotkvv1.DeleteRequest) (*slotkvv1.DeleteResponse, error) {
	time.Sleep(kv.delay)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	k := string(req.Key)
	old, existed := kv.cur[k]
	if existed {
		kv.prev[k] = old
	}
	delete(kv.cur, k)
	return &slotkvv1.DeleteResponse{Existed: existed}, nil
}

func (kv *fakeKV) Get(_ context.Context, req *slotkvv1.GetRequest) (*slotkvv1.GetResponse, error) {
	time.Sleep(kv.delay)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	k := string(req.Key)
	if old, ok := kv.prev[k]; ok && kv.rng.Float64() < kv.stale {
		return &slotkvv1.GetResponse{Value: []byte(old), Found: true}, nil
	}
	v, ok := kv.cur[k]
	return &slotkvv1.GetResponse{Value: []byte(v), Found: ok}, nil
}

func testConfig(mode string) config {
	return config{
		mode: mode, label: "test", duration: 900 * time.Millisecond, warmup: 100 * time.Millisecond,
		workers: 4, rate: 300, maxInflight: 4096, keys: 20, valueSize: 32, deadline: time.Second,
		checkTimeout: 30 * time.Second, seed: 1,
	}
}

func TestThroughputMode(t *testing.T) {
	res := run(context.Background(), testConfig(modeThroughput), newTestClient(t, &fakeKV{}))
	if res.OK < 100 || res.Failed != 0 || res.Ops != res.OK || res.Throughput <= 0 {
		t.Errorf("ops %d, ok %d, failed %d, throughput %v", res.Ops, res.OK, res.Failed, res.Throughput)
	}
	if res.Params.Workers != 4 || res.Params.Rate != 0 || res.Linearizable != "" || len(res.Timeline) != 1 {
		t.Errorf("params %+v, linearizable %q, timeline %d", res.Params, res.Linearizable, len(res.Timeline))
	}
}

func TestSteadyModeKeepsTheArrivalRate(t *testing.T) {
	cfg := testConfig(modeSteady)
	cfg.readRatio = 0.5
	res := run(context.Background(), cfg, newTestClient(t, &fakeKV{}))
	// 300 req/s for the 0.9s after the warmup.
	if res.Failed != 0 || res.Ops < 240 || res.Ops > 300 {
		t.Errorf("ops %d, failed %d; want about 270 and none failed", res.Ops, res.Failed)
	}
	if res.Params.Rate != 300 || res.Params.Workers != 0 {
		t.Errorf("params %+v", res.Params)
	}
}

func TestSteadyModeCountsOverloadAsFailed(t *testing.T) {
	cfg := testConfig(modeSteady)
	cfg.rate, cfg.maxInflight = 500, 2
	res := run(context.Background(), cfg, newTestClient(t, &fakeKV{delay: 20 * time.Millisecond}))
	if res.Errors.Overload == 0 || res.OK == 0 || res.Failed != res.Errors.Overload || res.Ops != res.OK+res.Failed {
		t.Errorf("ops %d, ok %d, failed %d, errors %+v", res.Ops, res.OK, res.Failed, res.Errors)
	}
}

func TestCallerCancellationStopsInflightRequests(t *testing.T) {
	cfg := testConfig(modeThroughput)
	cfg.duration, cfg.warmup, cfg.deadline = 10*time.Second, 0, 3*time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	res := run(ctx, cfg, newTestClient(t, &fakeKV{delay: time.Second}))
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("run returned after %v, want caller cancellation to stop in-flight RPCs promptly", elapsed)
	}
	if res.Failed == 0 {
		t.Fatalf("canceled run recorded no failed in-flight request: %+v", res)
	}
}

func TestCheckModeAcceptsALinearizableStore(t *testing.T) {
	cfg := testConfig(modeCheck)
	cfg.keys, cfg.readRatio = 4, 0.5
	res := run(context.Background(), cfg, newTestClient(t, &fakeKV{}))
	if res.Linearizable != "ok" || res.Failed != 0 {
		t.Errorf("linearizable %q, failed %d", res.Linearizable, res.Failed)
	}
}

func TestCheckModeDetectsStaleReads(t *testing.T) {
	cfg := testConfig(modeCheck)
	cfg.keys, cfg.readRatio, cfg.duration = 4, 0.5, 1500*time.Millisecond
	cfg.viz = filepath.Join(t.TempDir(), "viz", "history.html")
	res := run(context.Background(), cfg, newTestClient(t, &fakeKV{stale: 0.5}))
	if res.Linearizable != "illegal" {
		t.Errorf("linearizable = %q, want illegal", res.Linearizable)
	}
	if st, err := os.Stat(cfg.viz); err != nil || st.Size() == 0 {
		t.Errorf("visualization not written: %v", err)
	}
}

func TestExportHistoryFileAndStdout(t *testing.T) {
	ops := []porcupine.Operation{{ClientId: 1, Call: 2, Return: 3, Input: lincheck.Input{Op: lincheck.Put, Key: "k", Value: "v"}, Output: lincheck.Output{}}}
	path := filepath.Join(t.TempDir(), "history.json")
	if err := exportHistory(path, ops, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := lincheck.ReadHistory(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Input.(lincheck.Input) != ops[0].Input.(lincheck.Input) {
		t.Fatalf("file history = %#v", got)
	}

	var stdout bytes.Buffer
	if err := exportHistory("-", ops, &stdout); err != nil {
		t.Fatal(err)
	}
	got = decodeHistoryChunks(t, stdout.Bytes())
	if len(got) != 1 || got[0].Input.(lincheck.Input) != ops[0].Input.(lincheck.Input) {
		t.Fatalf("stdout history = %#v", got)
	}
}

type writeTrackingBuffer struct {
	bytes.Buffer
	maxWrite int
}

func (b *writeTrackingBuffer) Write(p []byte) (int, error) {
	if len(p) > b.maxWrite {
		b.maxWrite = len(p)
	}
	return b.Buffer.Write(p)
}

func decodeHistoryChunks(t *testing.T, output []byte) []porcupine.Operation {
	t.Helper()
	var encoded strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(string(output), "\n"), "\n") {
		if !strings.HasPrefix(line, "HISTORY ") || len(line)+1 >= 4096 {
			t.Fatalf("invalid or oversized HISTORY line (%d bytes): %.40q", len(line)+1, line)
		}
		encoded.WriteString(strings.TrimPrefix(line, "HISTORY "))
	}
	data, err := base64.StdEncoding.DecodeString(encoded.String())
	if err != nil {
		t.Fatalf("decode HISTORY chunks: %v", err)
	}
	got, err := lincheck.UnmarshalHistory(data)
	if err != nil {
		t.Fatalf("read HISTORY chunks: %v", err)
	}
	return got
}

func TestExportHistoryStdoutChunksStayBoundedForLargeHistory(t *testing.T) {
	value := strings.Repeat("payload", 400_000)
	ops := []porcupine.Operation{{ClientId: 1, Call: 2, Return: 3, Input: lincheck.Input{Op: lincheck.Put, Key: "large", Value: value}, Output: lincheck.Output{}}}
	var stdout writeTrackingBuffer
	if err := exportHistory("-", ops, &stdout); err != nil {
		t.Fatal(err)
	}
	if stdout.maxWrite >= 4096 {
		t.Fatalf("largest stdout write = %d bytes, want < 4096", stdout.maxWrite)
	}
	got := decodeHistoryChunks(t, stdout.Bytes())
	if len(got) != 1 || got[0].Input.(lincheck.Input).Value != value {
		t.Fatalf("decoded history did not preserve large payload (ops=%d)", len(got))
	}
}

func TestHistoryExportFailureDoesNotSkipCheck(t *testing.T) {
	rec := lincheck.NewRecorder()
	rec.End(rec.Begin(0, lincheck.Input{Op: lincheck.Put, Key: "k", Value: "v"}), lincheck.Output{}, nil)
	rec.End(rec.Begin(0, lincheck.Input{Op: lincheck.Get, Key: "k"}), lincheck.Output{Value: "v", Found: true}, nil)
	r := runner{cfg: config{history: t.TempDir(), checkTimeout: time.Second}, rec: rec}
	if got := r.check(); got != "ok" {
		t.Fatalf("history export error changed checker verdict to %q", got)
	}
}
