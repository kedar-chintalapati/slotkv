// Package topology computes slotkv's static placement: which slot a key hashes
// to, which Raft group owns each slot, and which nodes replicate each group.
//
// Every node and client derives the same Topology from the same inputs, so
// placement needs no coordination.
package topology

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
)

// DefaultNumSlots is the number of hash slots keys are spread over.
const DefaultNumSlots = 1024

// Node is a server process. It hosts one replica of every group it belongs to.
type Node struct {
	ID       string
	GRPCAddr string // host:port of the KVService
	RaftAddr string // host:port of the Raft transport shared by all groups
}

// Group is one Raft group: a replicated state machine owning a set of slots.
type Group struct {
	ID       int
	Replicas []string // node IDs; Replicas[0] is the preferred leader
}

// Topology is the full placement.
type Topology struct {
	NumSlots    int
	Nodes       []Node
	Groups      []Group
	SlotToGroup []int // SlotToGroup[slot] is the owning group's ID

	nodes map[string]int
}

// Build places numGroups groups on nodes. Group g is replicated on nodes
// g, g+1, ..., g+replication-1 (mod len(nodes)), so with three nodes and a
// replication factor of three every node hosts every group. The preferred
// leader of group g is node g mod len(nodes), which spreads leaders evenly.
// Slots are assigned to groups round-robin.
func Build(nodes []Node, numGroups, replication, numSlots int) (*Topology, error) {
	switch {
	case len(nodes) == 0:
		return nil, errors.New("topology: no nodes")
	case replication < 1 || replication > len(nodes):
		return nil, fmt.Errorf("topology: replication %d must be between 1 and %d", replication, len(nodes))
	case numGroups < 1:
		return nil, fmt.Errorf("topology: need at least one group, got %d", numGroups)
	case numSlots < numGroups:
		return nil, fmt.Errorf("topology: %d slots cannot cover %d groups", numSlots, numGroups)
	}
	t := &Topology{
		NumSlots:    numSlots,
		Nodes:       append([]Node(nil), nodes...),
		Groups:      make([]Group, numGroups),
		SlotToGroup: make([]int, numSlots),
		nodes:       make(map[string]int, len(nodes)),
	}
	for i, n := range nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("topology: node %d has an empty ID", i)
		}
		if _, dup := t.nodes[n.ID]; dup {
			return nil, fmt.Errorf("topology: duplicate node ID %q", n.ID)
		}
		t.nodes[n.ID] = i
	}
	for g := range t.Groups {
		replicas := make([]string, replication)
		for r := range replicas {
			replicas[r] = nodes[(g+r)%len(nodes)].ID
		}
		t.Groups[g] = Group{ID: g, Replicas: replicas}
	}
	for s := range t.SlotToGroup {
		t.SlotToGroup[s] = s % numGroups
	}
	return t, nil
}

// SlotForKey returns the hash slot of key.
func (t *Topology) SlotForKey(key []byte) int {
	return int(crc32.ChecksumIEEE(key) % uint32(t.NumSlots))
}

// GroupForKey returns the ID of the group that owns key.
func (t *Topology) GroupForKey(key []byte) int {
	return t.SlotToGroup[t.SlotForKey(key)]
}

// Node returns the node with the given ID.
func (t *Topology) Node(id string) (Node, bool) {
	i, ok := t.nodes[id]
	if !ok {
		return Node{}, false
	}
	return t.Nodes[i], true
}

// PreferredLeader returns the node ID that should lead group g when healthy.
func (t *Topology) PreferredLeader(g int) string {
	return t.Groups[g].Replicas[0]
}

// GroupsOn returns the IDs of the groups that have a replica on node id.
func (t *Topology) GroupsOn(id string) []int {
	var out []int
	for _, g := range t.Groups {
		for _, r := range g.Replicas {
			if r == id {
				out = append(out, g.ID)
				break
			}
		}
	}
	return out
}

// ParsePeers parses a comma-separated list of "id=host:grpcPort:raftPort"
// entries, e.g. "kv-0=kv-0.kv:7000:7001,kv-1=kv-1.kv:7000:7001".
func ParsePeers(s string) ([]Node, error) {
	var nodes []Node
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, addr, ok := strings.Cut(entry, "=")
		if !ok || id == "" {
			return nil, fmt.Errorf("topology: peer %q: want id=host:grpcPort:raftPort", entry)
		}
		parts := strings.Split(addr, ":")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("topology: peer %q: want id=host:grpcPort:raftPort", entry)
		}
		nodes = append(nodes, Node{
			ID:       id,
			GRPCAddr: parts[0] + ":" + parts[1],
			RaftAddr: parts[0] + ":" + parts[2],
		})
	}
	if len(nodes) == 0 {
		return nil, errors.New("topology: no peers")
	}
	return nodes, nil
}
