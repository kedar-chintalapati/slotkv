package client

import (
	"testing"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

func validTopologyResponse() *slotkvv1.TopologyResponse {
	return &slotkvv1.TopologyResponse{
		NumSlots:    4,
		Nodes:       []*slotkvv1.Node{{Id: "n0", GrpcAddr: "n0:7000", RaftAddr: "n0:7001"}, {Id: "n1", GrpcAddr: "n1:7000", RaftAddr: "n1:7001"}, {Id: "n2", GrpcAddr: "n2:7000", RaftAddr: "n2:7001"}},
		Groups:      []*slotkvv1.Group{{Id: 0, Replicas: []string{"n0", "n1", "n2"}, LeaderId: "n0"}, {Id: 1, Replicas: []string{"n1", "n2", "n0"}}},
		SlotToGroup: []uint32{0, 1, 0, 1},
	}
}

func TestBuildTopologyRejectsMalformedResponse(t *testing.T) {
	tests := []struct {
		name string
		edit func(*slotkvv1.TopologyResponse) *slotkvv1.TopologyResponse
	}{
		{"nil response", func(*slotkvv1.TopologyResponse) *slotkvv1.TopologyResponse { return nil }},
		{"nil node", func(r *slotkvv1.TopologyResponse) *slotkvv1.TopologyResponse { r.Nodes[1] = nil; return r }},
		{"empty node address", func(r *slotkvv1.TopologyResponse) *slotkvv1.TopologyResponse { r.Nodes[1].GrpcAddr = ""; return r }},
		{"nil group", func(r *slotkvv1.TopologyResponse) *slotkvv1.TopologyResponse { r.Groups[1] = nil; return r }},
		{"unknown leader", func(r *slotkvv1.TopologyResponse) *slotkvv1.TopologyResponse { r.Groups[0].LeaderId = "n3"; return r }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildTopology(tc.edit(validTopologyResponse())); err == nil {
				t.Fatal("buildTopology succeeded, want malformed response error")
			}
		})
	}
}
