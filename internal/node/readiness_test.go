package node

import (
	"math"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

func TestReadReadyRequiresBarrierInCurrentLeadershipTerm(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       raft.RaftState
		term        uint64
		barrierTerm uint64
		want        bool
	}{
		{name: "current term barrier", state: raft.Leader, term: 7, barrierTerm: 7, want: true},
		{name: "new term before barrier", state: raft.Leader, term: 8, barrierTerm: 7},
		{name: "no barrier", state: raft.Leader, term: 8},
		{name: "lost leadership", state: raft.Follower, term: 7, barrierTerm: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := readReady(tc.state, tc.term, tc.barrierTerm); got != tc.want {
				t.Fatalf("readReady(%v, %d, %d) = %v, want %v", tc.state, tc.term, tc.barrierTerm, got, tc.want)
			}
		})
	}
}

func TestWithinLagAvoidsUnsignedOverflow(t *testing.T) {
	for _, tc := range []struct {
		applied, lag, commit uint64
		want                 bool
	}{
		{applied: 10, lag: 2, commit: 12, want: true},
		{applied: 10, lag: 1, commit: 12, want: false},
		{applied: 12, lag: 0, commit: 12, want: true},
		{applied: 0, lag: math.MaxUint64, commit: math.MaxUint64, want: true},
	} {
		if got := withinLag(tc.applied, tc.lag, tc.commit); got != tc.want {
			t.Errorf("withinLag(%d, %d, %d) = %v, want %v", tc.applied, tc.lag, tc.commit, got, tc.want)
		}
	}
}

func TestReadyRejectsCachedFormerLeaderStatus(t *testing.T) {
	c := newTestCluster(t, 1, nil)
	c.waitPreferred(15 * time.Second)

	follower := c.nodes[1]
	p := follower.peers["n0"]
	p.mu.Lock()
	p.st = &slotkvv1.StatusResponse{Groups: []*slotkvv1.GroupStatus{{
		GroupId: 0, State: "Follower", LeaderId: "n2", CommitIndex: 0,
	}}}
	p.err, p.at = nil, time.Now()
	p.mu.Unlock()

	if follower.Ready() {
		t.Fatal("follower accepted cached status from a former leader")
	}
}

func TestBalancerRefreshesTargetDrainingStatus(t *testing.T) {
	c := newTestCluster(t, 1, nil)
	c.waitPreferred(15 * time.Second)

	// n0 leads group 0 and has a recent cached response saying n1 is eligible.
	target := c.nodes[1]
	target.draining.Store(true)
	p := c.nodes[0].peers[target.id]
	p.mu.Lock()
	p.st = &slotkvv1.StatusResponse{Groups: []*slotkvv1.GroupStatus{{
		GroupId: 0, State: "Follower", LeaderId: "n0", AppliedIndex: ^uint64(0),
	}}}
	p.err, p.at = nil, time.Now()
	p.mu.Unlock()

	if caughtUp(p, 0, 0, "n0", c.nodes[0].Raft(0).CurrentTerm()) {
		t.Fatal("balancer accepted a cached status after the target began draining")
	}
}

func TestTransferTargetReadyRequiresCurrentFollower(t *testing.T) {
	for _, tc := range []struct {
		name string
		gs   *slotkvv1.GroupStatus
		want bool
	}{
		{name: "caught up follower", gs: &slotkvv1.GroupStatus{State: "Follower", LeaderId: "n0", Term: 7, AppliedIndex: 12}, want: true},
		{name: "candidate", gs: &slotkvv1.GroupStatus{State: "Candidate", LeaderId: "n0", Term: 7, AppliedIndex: 12}},
		{name: "leader", gs: &slotkvv1.GroupStatus{State: "Leader", LeaderId: "n0", Term: 7, AppliedIndex: 12}},
		{name: "stale leader", gs: &slotkvv1.GroupStatus{State: "Follower", LeaderId: "n2", Term: 7, AppliedIndex: 12}},
		{name: "wrong term", gs: &slotkvv1.GroupStatus{State: "Follower", LeaderId: "n0", Term: 6, AppliedIndex: 12}},
		{name: "below commit floor", gs: &slotkvv1.GroupStatus{State: "Follower", LeaderId: "n0", Term: 7, AppliedIndex: 11}},
		{name: "missing status", gs: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := transferTargetReady(tc.gs, "n0", 7, 12); got != tc.want {
				t.Fatalf("transferTargetReady(%v) = %v, want %v", tc.gs, got, tc.want)
			}
		})
	}
}

func TestBalancerAcceptsCaughtUpCurrentFollower(t *testing.T) {
	c := newTestCluster(t, 1, nil)
	c.waitPreferred(15 * time.Second)

	leader := c.nodes[0]
	p := leader.peers["n1"]
	term := leader.Raft(0).CurrentTerm()
	if !caughtUp(p, 0, 0, leader.id, term) {
		t.Fatalf("healthy follower n1 should be eligible in leader term %d", term)
	}
}
