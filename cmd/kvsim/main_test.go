package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kedar-chintalapati/slotkv/internal/sim"
)

func TestParseVizFlag(t *testing.T) {
	c, err := parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.viz != "porcupine-kvsim-illegal.html" {
		t.Fatalf("default --viz = %q", c.viz)
	}
	c, err = parse([]string{"--viz=custom.html"})
	if err != nil {
		t.Fatal(err)
	}
	if c.viz != "custom.html" {
		t.Fatalf("--viz = %q", c.viz)
	}
	if _, err := parse([]string{"--viz="}); err == nil {
		t.Fatal("accepted empty --viz")
	}
}

func TestSummarizeVerdictsAndErrors(t *testing.T) {
	s := summarize([]sim.Result{
		{Verdict: "ok", Ops: 5},
		{Verdict: "illegal", Ops: 2},
		{Verdict: "unknown", FailedOps: 4, Err: "check failed"},
	})
	if s.Runs != 3 || s.OK != 1 || s.Illegal != 1 || s.Unknown != 1 || s.Ops != 7 || s.FailedOps != 4 || s.Errors != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	if s.exitCode() == 0 {
		t.Fatal("summary with a run error should fail")
	}
}

func TestWriteExclusiveDoesNotClobber(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "illegal.html")
	if err := os.WriteFile(want, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "run.html")
	if err := os.WriteFile(src, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := writeExclusive(src, want)
	if err != nil {
		t.Fatal(err)
	}
	if got == want {
		t.Fatal("writeExclusive reused an existing path")
	}
	old, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(old) != "existing" {
		t.Fatalf("existing artifact changed to %q", old)
	}
	newFile, err := os.ReadFile(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(newFile) != "new" {
		t.Fatalf("new artifact contains %q", newFile)
	}
}
