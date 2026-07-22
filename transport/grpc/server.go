package grpctransport

import (
	"context"
	raftpb "github.com/krithik-sri/raft-kv/proto"
)

type Server struct {
	raftpb.UnimplementedRaftServiceServer
}

func (s *Server) RequestVote(
    ctx context.Context,
    req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
    return &raftpb.RequestVoteResponse{
        Term:        0,
        VoteGranted: false,
    }, nil
}