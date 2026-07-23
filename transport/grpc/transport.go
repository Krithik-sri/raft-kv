package grpctransport

import (
	"context"

	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
)

type Transport struct{}

var _ raft.Transport = (*Transport)(nil)

func (t *Transport) RequestVote(
	ctx context.Context,
	peer raft.Peer,
	req raft.RequestVoteRequest,
) (raft.RequestVoteResponse, error) {
	pbReq := &raftpb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  string(req.CandidateID),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	client, err := NewClient(peer.Address)

	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

	defer client.Close()

	pbResp, err := client.RequestVote(ctx, pbReq)

	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

	response := raft.RequestVoteResponse{
		Term:        pbResp.Term,
		VoteGranted: pbResp.VoteGranted,
	}

	return response, nil
}
