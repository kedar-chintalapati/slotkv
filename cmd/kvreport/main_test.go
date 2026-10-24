package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Fixtures follow the schema kvbench writes; fields kvreport ignores are kept
// so the tests also cover decoding the full document.
const (
	fixtureG1 = `{"label":"groups=1","mode":"throughput","params":{"workers":256,"rate":0,"keys":100000,"value_size":128,"read_ratio":0,"deadline_ms":0,"duration_s":30,"warmup_s":5},
"start_unix_ms":1700000000000,"end_unix_ms":1700000035000,"ops":30000,"ok":30000,"failed":0,"throughput_ops_s":1000,
"latency_ms":{"p50":10,"p90":20,"p99":25.5,"p999":40,"max":90},
"errors":{"deadline_exceeded":0,"unavailable":0,"not_leader":0,"overload":0,"other":0},"timeline":[{"t_s":0,"ok":1000,"failed":0,"p99_ms":25.5}],"linearizable":""}`

	fixtureG12 = `{"label":"groups=12","mode":"throughput","start_unix_ms":1,"end_unix_ms":2,"ops":82500,"ok":82500,"failed":0,"throughput_ops_s":2750,
"latency_ms":{"p50":4.25,"p90":8,"p99":12.5,"p999":20,"max":55},"timeline":[],"linearizable":""}`

	fixtureBaseline = `{"label":"baseline","mode":"steady","ops":300000,"ok":295500,"failed":4500,"throughput_ops_s":1970,
"latency_ms":{"p50":2,"p90":5,"p99":1000,"p999":1000,"max":1003.5},
"errors":{"deadline_exceeded":4000,"unavailable":500,"not_leader":0,"overload":0,"other":0},
"timeline":[{"t_s":19,"ok":2000,"failed":0,"p99_ms":3},{"t_s":20,"ok":0,"failed":900,"p99_ms":1000},{"t_s":21,"ok":0,"failed":1000,"p99_ms":1000},
{"t_s":22,"ok":10,"failed":1000,"p99_ms":1000},{"t_s":23,"ok":1500,"failed":100,"p99_ms":900},{"t_s":24,"ok":2000,"failed":0,"p99_ms":4},
{"t_s":30,"ok":1000,"failed":500,"p99_ms":1000},{"t_s":31,"ok":1900,"failed":1,"p99_ms":20}],"linearizable":""}`

	fixtureSlotkv = `{"label":"slotkv","mode":"steady","ops":300000,"ok":300000,"failed":0,"throughput_ops_s":2000,
"latency_ms":{"p50":2,"p90":4,"p99":15,"p999":120,"max":410},"timeline":[{"t_s":0,"ok":2000,"failed":0,"p99_ms":3}],"linearizable":""}`

	fixtureZone = `{"label":"zone","mode":"check","ops":36000,"ok":35100,"failed":900,"throughput_ops_s":300,
"latency_ms":{"p50":3,"p90":6,"p99":500,"p999":500,"max":501},"timeline":[],"linearizable":"ok"}`
)

func TestReports(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	g1, g12 := write("g1.json", fixtureG1), write("g12.json", fixtureG12)
	base, slot := write("base.json", fixtureBaseline), write("slotkv.json", fixtureSlotkv)
	zn := write("zone.json", fixtureZone)
	noLabel := write("no-label.json", `{"ops":1}`)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"throughput", []string{"throughput", g1, g12}, `| label | ops/s | p50 (ms) | p99 (ms) | p999 (ms) | speedup |
|---|---:|---:|---:|---:|---:|
| groups=1 | 1000 | 10.00 | 25.50 | 40.00 | 1.00x |
| groups=12 | 2750 | 4.25 | 12.50 | 20.00 | 2.75x |
`},
		{"rollout", []string{"rollout", base, slot}, `| label | failed | error % | p99 (ms) | p999 (ms) | max (ms) | longest failing run (s) |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 4500 | 1.500% | 1000.00 | 1000.00 | 1003.50 | 4 |
| slotkv | 0 | 0.000% | 15.00 | 120.00 | 410.00 | 0 |
`},
		{"zone", []string{"zone", zn}, `| label | ops | ok | failed | linearizable |
|---|---:|---:|---:|---:|
| zone | 36000 | 35100 | 900 | ok |
`},
		{"label falls back to file name", []string{"zone", noLabel}, `| label | ops | ok | failed | linearizable |
|---|---:|---:|---:|---:|
| no-label | 1 | 0 | 0 | - |
`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := run(tc.args, &buf); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tc.want)
			}
		})
	}
}

func TestRunErrors(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		nil,
		{"throughput"},
		{"bogus", bad},
		{"zone", bad},
		{"zone", filepath.Join(t.TempDir(), "missing.json")},
	} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Errorf("run(%q) succeeded, want an error", args)
		}
	}
}

func TestLongestFailRun(t *testing.T) {
	tests := []struct {
		name string
		tl   []point
		want int
	}{
		{"empty", nil, 0},
		{"no failures", []point{{0, 0}, {1, 0}}, 0},
		{"single", []point{{5, 1}}, 1},
		{"longest of several", []point{{1, 1}, {2, 1}, {3, 0}, {4, 2}, {5, 2}, {6, 2}, {7, 0}}, 3},
		{"missing second ends a run", []point{{1, 1}, {2, 1}, {4, 1}, {5, 1}}, 2},
		{"unsorted input", []point{{3, 1}, {1, 1}, {2, 1}}, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := longestFailRun(tc.tl); got != tc.want {
				t.Errorf("longestFailRun = %d, want %d", got, tc.want)
			}
		})
	}
}
