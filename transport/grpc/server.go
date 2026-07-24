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

func NewServer(node *raft.Raft) *Server {
	return &Server{
		node: node,
	}
}
