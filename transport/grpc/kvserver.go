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

func (s *KVServer) redirect() *raftpb.LeaderRedirect {
	id, isLeader := s.node.LeaderHint()
	if isLeader || id == "" {
		return &raftpb.LeaderRedirect{}
	}

	return &raftpb.LeaderRedirect{
		LeaderId:      string(id),
		LeaderAddress: s.addresses[id],
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
	if _, isLeader := s.node.LeaderHint(); !isLeader {
		return &raftpb.GetResponse{Redirect: s.redirect()}, nil
	}

	// A stale read trusts whatever this node happens to have. Being leader is
	// not proof of anything: a leader that has been cut off keeps saying yes.
	if !req.AllowStale {
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
