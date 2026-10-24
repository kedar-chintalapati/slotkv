package topology

import (
	"fmt"
	"testing"
)

func nodes(n int) []Node {
	out := make([]Node, n)
	for i := range out {
		out[i] = Node{ID: fmt.Sprintf("n%d", i), GRPCAddr: fmt.Sprintf("h%d:7000", i), RaftAddr: fmt.Sprintf("h%d:7001", i)}
	}
	return out
}

func TestBuildPlacement(t *testing.T) {
	topo, err := Build(nodes(5), 10, 3, DefaultNumSlots)
	if err != nil {
		t.Fatal(err)
	}
	leaders := map[string]int{}
	for _, g := range topo.Groups {
		if len(g.Replicas) != 3 {
			t.Fatalf("group %d: %d replicas", g.ID, len(g.Replicas))
		}
		seen := map[string]bool{}
		for _, r := range g.Replicas {
			if seen[r] {
				t.Fatalf("group %d: replica %s placed twice", g.ID, r)
			}
			seen[r] = true
		}
		leaders[topo.PreferredLeader(g.ID)]++
	}
	for _, n := range topo.Nodes {
		if leaders[n.ID] != 2 {
			t.Fatalf("node %s is preferred leader of %d groups, want 2", n.ID, leaders[n.ID])
		}
	}
	owned := make([]int, len(topo.Groups))
	for _, g := range topo.SlotToGroup {
		owned[g]++
	}
	for g, n := range owned {
		if n == 0 {
			t.Fatalf("group %d owns no slots", g)
		}
	}
}

func TestEveryNodeHostsEveryGroupWithThreeNodes(t *testing.T) {
	topo, err := Build(nodes(3), 12, 3, DefaultNumSlots)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range topo.Nodes {
		if got := len(topo.GroupsOn(n.ID)); got != 12 {
			t.Fatalf("node %s hosts %d groups, want 12", n.ID, got)
		}
	}
}

func TestGroupForKeyIsStable(t *testing.T) {
	a, _ := Build(nodes(3), 6, 3, DefaultNumSlots)
	b, _ := Build(nodes(3), 6, 3, DefaultNumSlots)
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if a.GroupForKey(k) != b.GroupForKey(k) {
			t.Fatalf("key %s maps to different groups", k)
		}
	}
}

func TestBuildRejectsBadInput(t *testing.T) {
	if _, err := Build(nodes(2), 4, 3, DefaultNumSlots); err == nil {
		t.Fatal("replication larger than node count accepted")
	}
	if _, err := Build(nodes(3), 0, 3, DefaultNumSlots); err == nil {
		t.Fatal("zero groups accepted")
	}
	if _, err := Build(append(nodes(2), Node{ID: "n0"}), 1, 1, DefaultNumSlots); err == nil {
		t.Fatal("duplicate node ID accepted")
	}
}

func TestParsePeers(t *testing.T) {
	got, err := ParsePeers("kv-0=kv-0.kv:7000:7001, kv-1=10.0.0.2:7100:7101")
	if err != nil {
		t.Fatal(err)
	}
	want := []Node{
		{ID: "kv-0", GRPCAddr: "kv-0.kv:7000", RaftAddr: "kv-0.kv:7001"},
		{ID: "kv-1", GRPCAddr: "10.0.0.2:7100", RaftAddr: "10.0.0.2:7101"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d nodes", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("node %d: got %+v want %+v", i, got[i], want[i])
		}
	}
	for _, bad := range []string{"", "kv-0", "kv-0=host:7000", "=h:1:2"} {
		if _, err := ParsePeers(bad); err == nil {
			t.Fatalf("ParsePeers(%q) accepted", bad)
		}
	}
}
