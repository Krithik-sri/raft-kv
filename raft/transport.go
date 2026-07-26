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

	InstallSnapshot(
		ctx context.Context,
		peer Peer,
		req InstallSnapshotRequest,
	) (InstallSnapshotResponse, error)
}
