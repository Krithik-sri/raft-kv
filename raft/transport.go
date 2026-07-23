package raft

import "context"

type Transport interface {
	RequestVote(
		ctx context.Context,
		peer Peer,
		req RequestVoteRequest,
	) (RequestVoteResponse, error)
}
