package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseFlagsBeforeHistoryFile(t *testing.T) {
	c, err := parse([]string{"--timeout=5s", "--viz=history.html", "history.json"})
	if err != nil {
		t.Fatal(err)
	}
	if c.path != "history.json" || c.timeout != 5*time.Second || c.viz != "history.html" {
		t.Fatalf("parsed config: %+v", c)
	}
	if _, err = parse([]string{"--timeout=-1s", "history.json"}); err == nil {
		t.Fatal("accepted negative timeout")
	}
	if _, err = parse([]string{"history.json", "--timeout=5s"}); err == nil {
		t.Fatal("accepted flags after positional history file")
	}
}

func TestEvaluateOfflineIllegalHistory(t *testing.T) {
	fixture := filepath.Join("..", "..", "internal", "lincheck", "testdata", "illegal.json")
	viz := filepath.Join(t.TempDir(), "illegal.html")
	out, err := evaluate(config{path: fixture, timeout: 5 * time.Second, viz: viz})
	if err != nil {
		t.Fatal(err)
	}
	if out.Verdict != "illegal" || out.Ops != 2 || out.Viz != viz {
		t.Fatalf("result: %+v", out)
	}
	if st, err := os.Stat(viz); err != nil || st.Size() == 0 {
		t.Fatalf("visualization missing or empty: %v", err)
	}
}

func TestEvaluateReportsHistoryReadErrors(t *testing.T) {
	_, err := evaluate(config{path: filepath.Join(t.TempDir(), "missing.json")})
	if err == nil {
		t.Fatal("missing history returned no error")
	}
}
