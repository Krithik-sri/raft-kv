package grpctransport

import (
	"context"
	"fmt"

	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
)

type Server struct {
	raftpb.UnimplementedRaftServiceServer

	node *raft.Raft
}

func (s *Server) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {

	raftReq := raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  raft.NodeID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	resp := s.node.HandleRequestVote(raftReq)

	fmt.Printf(
		"received RequestVote candidate=%s term=%d\n",
		req.CandidateId,
		req.Term,
	)
	return &raftpb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *Server) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {

	raftReq := raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     raft.NodeID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      fromProtoEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	}

	resp := s.node.HandleAppendEntries(raftReq)

	fmt.Printf(
		"received AppendEntries leader=%s term=%d\n",
		req.LeaderId,
		req.Term,
	)
	return &raftpb.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (s *Server) InstallSnapshot(
	ctx context.Context,
	req *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotResponse, error) {

	resp := s.node.HandleInstallSnapshot(raft.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          raft.NodeID(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})

	fmt.Printf(
		"received InstallSnapshot leader=%s index=%d\n",
		req.LeaderId,
		req.LastIncludedIndex,
	)

	return &raftpb.InstallSnapshotResponse{Term: resp.Term}, nil
}

func NewServer(node *raft.Raft) *Server {
	return &Server{
		node: node,
	}
}
