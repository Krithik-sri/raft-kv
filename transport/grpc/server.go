package grpctransport

import (
	"context"
	"fmt"

	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
)

type Server struct {
	raftpb.UnimplementedRaftServiceServer

	id raft.NodeID
}

func (s *Server) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	fmt.Printf(
		"node=%s received RequestVote candidate=%s term=%d\n",
		s.id,
		req.CandidateId,
		req.Term,
	)
	return &raftpb.RequestVoteResponse{
		Term:        0,
		VoteGranted: false,
	}, nil
}

func NewServer(id raft.NodeID) *Server {
	return &Server{
		id: id,
	}
}
