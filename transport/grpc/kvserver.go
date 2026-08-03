package grpctransport

import (
	"context"
	"errors"

	"github.com/krithik-sri/raft-kv/kvstore"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVServer struct {
	raftpb.UnimplementedKVServiceServer

	node      *raft.Raft
	store     *kvstore.Store
	addresses map[raft.NodeID]string
}

func NewKVServer(node *raft.Raft, store *kvstore.Store, peers []raft.Peer) *KVServer {
	addresses := make(map[raft.NodeID]string, len(peers))
	for _, peer := range peers {
		addresses[peer.ID] = peer.Address
	}

	return &KVServer{
		node:      node,
		store:     store,
		addresses: addresses,
	}
}

// addressFor prefers the live configuration over the list this process was
// started with, because the cluster can be reshaped since then and the startup
// list has no idea about anyone who joined afterwards.
func (s *KVServer) addressFor(id raft.NodeID) string {
	for _, m := range s.node.Configuration().Members {
		if m.ID == id && m.Address != "" {
			return m.Address
		}
	}

	return s.addresses[id]
}

func (s *KVServer) redirect() *raftpb.LeaderRedirect {
	id, isLeader := s.node.LeaderHint()
	if isLeader || id == "" {
		return &raftpb.LeaderRedirect{}
	}

	return &raftpb.LeaderRedirect{
		LeaderId:      string(id),
		LeaderAddress: s.addressFor(id),
	}
}

func (s *KVServer) apply(
	ctx context.Context,
	cmd kvstore.Command,
) (kvstore.Result, *raftpb.LeaderRedirect, error) {
	command, err := kvstore.EncodeCommand(cmd)
	if err != nil {
		return kvstore.Result{}, nil, status.Error(codes.Internal, err.Error())
	}

	raw, err := s.node.Propose(ctx, command)

	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLostLeadership) {
		return kvstore.Result{}, s.redirect(), nil
	}

	if err != nil {
		return kvstore.Result{}, nil, status.Error(codes.Unavailable, err.Error())
	}

	result, err := kvstore.DecodeResult(raw)
	if err != nil {
		return kvstore.Result{}, nil, status.Error(codes.Internal, err.Error())
	}

	return result, nil, nil
}

func (s *KVServer) Get(
	ctx context.Context,
	req *raftpb.GetRequest,
) (*raftpb.GetResponse, error) {
	// A stale read is answered by whichever node you happened to ask. It does
	// not have to be the leader and it may be behind. That is the entire point
	// of asking for one, and it means stale reads spread across every replica
	// instead of piling onto the leader.
	if req.AllowStale {
		value, found := s.store.Get(req.Key)

		return &raftpb.GetResponse{
			Value: value,
			Found: found,
		}, nil
	}

	if _, isLeader := s.node.LeaderHint(); !isLeader {
		return &raftpb.GetResponse{Redirect: s.redirect()}, nil
	}

	// Being leader is a claim, not proof. A node that has been cut off keeps
	// saying yes, so make it show a quorum before it answers.
	index, err := s.node.ReadIndex(ctx)

	if errors.Is(err, raft.ErrNotLeader) {
		return &raftpb.GetResponse{Redirect: s.redirect()}, nil
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	if err := s.node.WaitForApplied(ctx, index); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	value, found := s.store.Get(req.Key)

	return &raftpb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (s *KVServer) Put(
	ctx context.Context,
	req *raftpb.PutRequest,
) (*raftpb.PutResponse, error) {
	_, redirect, err := s.apply(ctx, kvstore.Command{
		Op:       kvstore.OpPut,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	if err != nil {
		return nil, err
	}

	return &raftpb.PutResponse{Redirect: redirect}, nil
}

func (s *KVServer) Delete(
	ctx context.Context,
	req *raftpb.DeleteRequest,
) (*raftpb.DeleteResponse, error) {
	_, redirect, err := s.apply(ctx, kvstore.Command{
		Op:       kvstore.OpDelete,
		Key:      req.Key,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	if err != nil {
		return nil, err
	}

	return &raftpb.DeleteResponse{Redirect: redirect}, nil
}

func (s *KVServer) CompareAndSwap(
	ctx context.Context,
	req *raftpb.CompareAndSwapRequest,
) (*raftpb.CompareAndSwapResponse, error) {
	result, redirect, err := s.apply(ctx, kvstore.Command{
		Op:       kvstore.OpCAS,
		Key:      req.Key,
		Expected: req.Expected,
		Value:    req.Value,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	if err != nil {
		return nil, err
	}

	return &raftpb.CompareAndSwapResponse{
		Redirect: redirect,
		Swapped:  result.Swapped,
	}, nil
}

func (s *KVServer) AddServer(
	ctx context.Context,
	req *raftpb.AddServerRequest,
) (*raftpb.AddServerResponse, error) {
	err := s.node.AddServer(ctx, raft.Peer{
		ID:      raft.NodeID(req.Id),
		Address: req.Address,
	})

	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLostLeadership) {
		return &raftpb.AddServerResponse{Redirect: s.redirect()}, nil
	}
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return &raftpb.AddServerResponse{}, nil
}

func (s *KVServer) RemoveServer(
	ctx context.Context,
	req *raftpb.RemoveServerRequest,
) (*raftpb.RemoveServerResponse, error) {
	err := s.node.RemoveServer(ctx, raft.NodeID(req.Id))

	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLostLeadership) {
		return &raftpb.RemoveServerResponse{Redirect: s.redirect()}, nil
	}
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return &raftpb.RemoveServerResponse{}, nil
}

// Members is answered by whichever node you asked. It reads that node's own
// view of the configuration, which can lag the leader by a moment during a
// change. Good enough for looking at, not something to make decisions from.
func (s *KVServer) Members(
	ctx context.Context,
	req *raftpb.MembersRequest,
) (*raftpb.MembersResponse, error) {
	config := s.node.Configuration()
	leaderID, _ := s.node.LeaderHint()

	members := make([]*raftpb.Member, 0, len(config.Members))
	for _, m := range config.Members {
		members = append(members, &raftpb.Member{
			Id:      string(m.ID),
			Address: m.Address,
			Voter:   m.Voter,
		})
	}

	return &raftpb.MembersResponse{
		Members:  members,
		LeaderId: string(leaderID),
	}, nil
}
