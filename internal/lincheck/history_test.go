package lincheck_test

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/kedar-chintalapati/slotkv/internal/lincheck"
)

func TestHistoryTypedRoundTrip(t *testing.T) {
	badBytes := string([]byte{0xff, 0x00, 0x81})
	ops := []porcupine.Operation{
		{ClientId: 3, Call: 10, Return: 20, Input: lincheck.Input{Op: lincheck.Put, Key: badBytes, Value: "one"}, Output: lincheck.Output{}},
		{ClientId: 4, Call: 15, Return: 25, Input: lincheck.Input{Op: lincheck.Delete, Key: "b"}, Output: lincheck.Output{Unknown: true}},
		{ClientId: 3, Call: 30, Return: 40, Input: lincheck.Input{Op: lincheck.Get, Key: "a"}, Output: lincheck.Output{Value: badBytes, Found: true}},
	}
	data, err := lincheck.MarshalHistory(ops)
	if err != nil {
		t.Fatal(err)
	}
	got, err := lincheck.ReadHistory(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ops) {
		t.Fatalf("round trip differs\n got: %#v\nwant: %#v", got, ops)
	}
}

func TestUnmarshalHistoryRejectsInvalidOperations(t *testing.T) {
	for _, data := range []string{
		`{"version":1,"operations":[{"input":{"op":3},"call":1,"return":2}]}`,
		`{"version":1,"operations":[{"input":{"op":0},"call":2,"return":2}]}`,
	} {
		if _, err := lincheck.UnmarshalHistory([]byte(strings.TrimSpace(data))); err == nil {
			t.Fatalf("accepted malformed history %s", data)
		}
	}
}

func TestOfflineIllegalHistoryFixture(t *testing.T) {
	f, err := os.Open("testdata/illegal.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ops, err := lincheck.ReadHistory(f)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := lincheck.Check(ops, 5*time.Second)
	if got != porcupine.Illegal {
		t.Fatalf("Check = %v, want Illegal", got)
	}
}
