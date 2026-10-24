package client

import (
	"context"

	slotkvv1 "github.com/kedar-chintalapati/slotkv/gen/slotkv/v1"
)

// Session issues requests on behalf of one logical client. Every Put or Delete
// takes the next sequence number, and the retries of that call reuse it, so the
// server applies the call at most once. A call that returns an error has an
// unknown outcome: it may or may not have taken effect.
//
// A Session is not safe for concurrent use: one request may be outstanding at a
// time. Use one Session per goroutine.
type Session struct {
	c   *Client
	id  string
	seq uint64
}

// ID returns the session's client ID.
func (s *Session) ID() string { return s.id }

// Put stores value under key.
func (s *Session) Put(ctx context.Context, key, value []byte) error {
	req := &slotkvv1.PutRequest{Session: s.begin(), Key: key, Value: value}
	return s.c.do(ctx, "put", s.c.topo.GroupForKey(key), false,
		func(ctx context.Context, kv slotkvv1.KVServiceClient) error {
			_, err := kv.Put(ctx, req)
			return err
		})
}

// Get returns the value stored under key. It carries no session: reads are
// idempotent.
func (s *Session) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	req := &slotkvv1.GetRequest{Key: key}
	var resp *slotkvv1.GetResponse
	err = s.c.do(ctx, "get", s.c.topo.GroupForKey(key), true,
		func(ctx context.Context, kv slotkvv1.KVServiceClient) (err error) {
			resp, err = kv.Get(ctx, req)
			return err
		})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// Delete removes key and reports whether it existed.
func (s *Session) Delete(ctx context.Context, key []byte) (existed bool, err error) {
	req := &slotkvv1.DeleteRequest{Session: s.begin(), Key: key}
	var resp *slotkvv1.DeleteResponse
	err = s.c.do(ctx, "delete", s.c.topo.GroupForKey(key), false,
		func(ctx context.Context, kv slotkvv1.KVServiceClient) (err error) {
			resp, err = kv.Delete(ctx, req)
			return err
		})
	if err != nil {
		return false, err
	}
	return resp.Existed, nil
}

// begin takes the next sequence number for a write call.
func (s *Session) begin() *slotkvv1.Session {
	s.seq++
	return &slotkvv1.Session{ClientId: s.id, Seq: s.seq}
}
