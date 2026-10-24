package node

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

const (
	maxTransfersPerTick = 2
	transferTimeout     = 3 * time.Second
	failedTransferTicks = 10 // ticks to leave a group alone after a failed transfer
)

func (n *Node) balanceLoop() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.BalanceInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.balanceOnce()
		case <-n.stop:
			return
		}
	}
}

// balanceOnce moves leadership of groups this node leads, but does not
// prefer, to their preferred node. The target must answer Status, not be
// draining and be within ReadyMaxLag of this leader's commit index. At most
// maxTransfersPerTick transfers run per tick, and none while draining.
//
// That bound alone is too loose for a transfer. Raft backs off replication to a
// follower that was unreachable, for up to ten seconds, and a transfer to such
// a follower times out after blocking writes for an election timeout, after
// which its late TimeoutNow can depose the leader. Such a follower is missing
// entries committed long ago, so the target must also have applied everything
// this leader had committed one tick earlier.
func (n *Node) balanceOnce() {
	n.balanceMu.Lock()
	defer n.balanceMu.Unlock()
	moved := 0
	for _, id := range n.local {
		g := n.group(id)
		if g == nil {
			continue
		}
		commit := g.raft.CommitIndex()
		prev := g.prevCommit
		g.prevCommit = commit
		term := g.raft.CurrentTerm()
		pref := n.topo.PreferredLeader(id)
		if moved == maxTransfersPerTick || n.draining.Load() || pref == n.id ||
			g.raft.State() != raft.Leader || time.Now().Before(g.retryAt) {
			continue
		}
		p := n.peers[pref]
		if p == nil || !caughtUp(p, id, max(prev, commit-min(commit, n.cfg.ReadyMaxLag)), n.id, term) {
			continue
		}
		// Status is a fresh gRPC view, but its check and the Raft transfer are
		// not atomic. Recheck that this node is still the same leader and is not
		// draining before acting on the target's eligibility.
		_, leader := g.raft.LeaderWithID()
		if n.draining.Load() || g.raft.State() != raft.Leader || g.raft.CurrentTerm() != term || string(leader) != n.id {
			continue
		}
		target, _ := n.topo.Node(pref)
		moved++
		n.log.Info("balancer: transferring leadership", "group", id, "to", pref)
		f := g.raft.LeadershipTransferToServer(raft.ServerID(pref), raft.ServerAddress(target.RaftAddr))
		if err := await(context.Background(), f, transferTimeout); err != nil && g.raft.State() == raft.Leader {
			g.retryAt = time.Now().Add(failedTransferTicks * n.cfg.BalanceInterval)
			n.log.Warn("balancer: leadership transfer failed", "group", id, "to", pref, "err", err)
		}
	}
}

// caughtUp reports whether p answers a fresh Status, is not draining, and is a
// follower of leaderID in term with group id applied through floor. The term
// and leader checks prevent transferring to a restarted node that has restored
// old applied state but has not yet contacted the current leader over Raft.
func caughtUp(p *peer, id int, floor uint64, leaderID string, term uint64) bool {
	st, err := p.statusFresh()
	if err != nil || st.GetDraining() {
		return false
	}
	return transferTargetReady(groupStatus(st, id), leaderID, term, floor)
}

func transferTargetReady(gs *slotkvv1.GroupStatus, leaderID string, term, floor uint64) bool {
	return gs != nil && gs.GetState() == "Follower" && gs.GetLeaderId() == leaderID &&
		gs.GetTerm() == term && gs.GetAppliedIndex() >= floor
}
