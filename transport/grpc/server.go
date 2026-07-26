package grpctransport

import (
	"context"
	"log/slog"

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
		PreVote:      req.PreVote,
	}

	resp := s.node.HandleRequestVote(raftReq)

	slog.Debug("received RequestVote", "candidate", req.CandidateId, "term", req.Term)
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

	slog.Debug("received AppendEntries", "leader", req.LeaderId, "term", req.Term)
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

	slog.Debug("received InstallSnapshot", "leader", req.LeaderId, "index", req.LastIncludedIndex)

	return &raftpb.InstallSnapshotResponse{Term: resp.Term}, nil
}

func NewServer(node *raft.Raft) *Server {
	return &Server{
		node: node,
	}
}
