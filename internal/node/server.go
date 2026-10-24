package node

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

var errTimeout = errors.New("timed out waiting for raft")

// server implements slotkvv1.KVServiceServer on top of a Node.
type server struct {
	slotkvv1.UnimplementedKVServiceServer
	n *Node
}

func (s *server) Get(ctx context.Context, req *slotkvv1.GetRequest) (*slotkvv1.GetResponse, error) {
	n := s.n
	g, gid := n.groupFor(req.GetKey())
	if g == nil {
		return nil, notLeader(gid, "", "")
	}
	if n.cfg.ReadMode == ReadVerify {
		if err := n.verifyLeader(ctx, g); err != nil {
			return nil, err
		}
	}
	v, found := g.fsm.get(req.GetKey())
	return &slotkvv1.GetResponse{Value: v, Found: found}, nil
}

func (s *server) Put(ctx context.Context, req *slotkvv1.PutRequest) (*slotkvv1.PutResponse, error) {
	if _, err := s.n.write(ctx, slotkvv1.Command_OP_PUT, req.GetKey(), req.GetValue(), req.GetSession()); err != nil {
		return nil, err
	}
	return &slotkvv1.PutResponse{}, nil
}

func (s *server) Delete(ctx context.Context, req *slotkvv1.DeleteRequest) (*slotkvv1.DeleteResponse, error) {
	res, err := s.n.write(ctx, slotkvv1.Command_OP_DELETE, req.GetKey(), nil, req.GetSession())
	if err != nil {
		return nil, err
	}
	return &slotkvv1.DeleteResponse{Existed: res.existed}, nil
}

func (s *server) Topology(context.Context, *slotkvv1.TopologyRequest) (*slotkvv1.TopologyResponse, error) {
	return s.n.topology(), nil
}

func (s *server) Status(context.Context, *slotkvv1.StatusRequest) (*slotkvv1.StatusResponse, error) {
	return s.n.Status(), nil
}

// groupFor returns the local replica of key's group (nil if not hosted) and
// the group's ID.
func (n *Node) groupFor(key []byte) (*group, int) {
	id := n.topo.GroupForKey(key)
	return n.group(id), id
}

// write replicates one put or delete through the key's group.
func (n *Node) write(ctx context.Context, op slotkvv1.Command_Op, key, value []byte, sess *slotkvv1.Session) (result, error) {
	if sess.GetClientId() == "" {
		return result{}, status.Error(codes.InvalidArgument, "session.client_id is required")
	}
	g, gid := n.groupFor(key)
	if g == nil {
		return result{}, notLeader(gid, "", "")
	}
	if g.raft.State() != raft.Leader {
		return result{}, n.notLeaderErr(g)
	}
	data, err := proto.Marshal(&slotkvv1.Command{Op: op, Key: key, Value: value, Session: sess})
	if err != nil {
		return result{}, status.Error(codes.Internal, err.Error())
	}
	f := g.raft.Apply(data, n.cfg.ApplyTimeout)
	if err := await(ctx, f, n.cfg.ApplyTimeout); err != nil {
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			return result{}, n.notLeaderErr(g)
		case errors.Is(err, raft.ErrLeadershipTransferInProgress):
			return result{}, notLeader(gid, "", "")
		}
		return result{}, unavailable(err)
	}
	switch r := f.Response().(type) {
	case result:
		return r, nil
	case error:
		return result{}, status.Error(codes.Internal, r.Error())
	}
	return result{}, status.Error(codes.Internal, "unexpected state machine response")
}

// verifyLeader gates a linearizable read: this node must lead g, have applied
// a barrier in its term, and still hold a quorum's confirmation.
func (n *Node) verifyLeader(ctx context.Context, g *group) error {
	if !g.ready() {
		if g.raft.State() != raft.Leader {
			return n.notLeaderErr(g)
		}
		return status.Error(codes.Unavailable, "leader has not applied a barrier in its term")
	}
	if err := await(ctx, g.raft.VerifyLeader(), n.cfg.ApplyTimeout); err != nil {
		if errors.Is(err, raft.ErrRaftShutdown) || errors.Is(err, errTimeout) || ctx.Err() != nil {
			return unavailable(err)
		}
		return n.notLeaderErr(g)
	}
	// Leadership may have changed while VerifyLeader was waiting. A successful
	// quorum check in the new term is not enough until that term's barrier has
	// applied, so recheck readiness after the future resolves.
	if !g.ready() {
		if g.raft.State() != raft.Leader {
			return n.notLeaderErr(g)
		}
		return status.Error(codes.Unavailable, "leader has not applied a barrier in its term")
	}
	return nil
}

// await waits for f, giving up once ctx ends or timeout passes. Raft always
// resolves a future eventually (commit, leadership loss or shutdown), so the
// goroutine it leaves behind on giving up does not leak.
func await(ctx context.Context, f raft.Future, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case err := <-done:
		return err
	case <-t.C:
		return errTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

// unavailable maps an error whose outcome is unknown to a retryable status.
func unavailable(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, err.Error())
}

// notLeader builds the FailedPrecondition error carrying a NotLeader detail.
func notLeader(group int, leaderID, leaderAddr string) error {
	st := status.New(codes.FailedPrecondition, "not leader")
	if d, err := st.WithDetails(&slotkvv1.NotLeader{GroupId: uint32(group), LeaderId: leaderID, LeaderAddr: leaderAddr}); err == nil {
		st = d
	}
	return st.Err()
}

// notLeaderErr is notLeader with the leader hint this node currently has.
func (n *Node) notLeaderErr(g *group) error {
	_, id := g.raft.LeaderWithID()
	if lid := string(id); lid != "" && lid != n.id {
		if nd, ok := n.topo.Node(lid); ok {
			return notLeader(g.id, lid, nd.GRPCAddr)
		}
	}
	return notLeader(g.id, "", "")
}

// topology builds the Topology response with this node's leader views.
func (n *Node) topology() *slotkvv1.TopologyResponse {
	t := n.topo
	resp := &slotkvv1.TopologyResponse{
		NumSlots:    uint32(t.NumSlots),
		SlotToGroup: make([]uint32, len(t.SlotToGroup)),
	}
	for _, nd := range t.Nodes {
		resp.Nodes = append(resp.Nodes, &slotkvv1.Node{Id: nd.ID, GrpcAddr: nd.GRPCAddr, RaftAddr: nd.RaftAddr})
	}
	for _, tg := range t.Groups {
		pg := &slotkvv1.Group{Id: uint32(tg.ID), Replicas: slices.Clone(tg.Replicas)}
		if g := n.group(tg.ID); g != nil {
			_, id := g.raft.LeaderWithID()
			pg.LeaderId = string(id)
		}
		resp.Groups = append(resp.Groups, pg)
	}
	for i, g := range t.SlotToGroup {
		resp.SlotToGroup[i] = uint32(g)
	}
	return resp
}
