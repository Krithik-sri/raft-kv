package raft

import "context"

type Transport interface {
	RequestVote(
		ctx context.Context,
		peer Peer,
		req RequestVoteRequest,
	) (RequestVoteResponse, error)

	AppendEntries(
		ctx context.Context,
		peer Peer,
		req AppendEntriesRequest,
	) (AppendEntriesResponse, error)
}
