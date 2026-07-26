package grpctransport

import (
	"context"
	"sync"

	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

var _ raft.Transport = (*Transport)(nil)

func (t *Transport) clientFor(address string) (raftpb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.conns == nil {
		t.conns = make(map[string]*grpc.ClientConn)
	}

	conn, ok := t.conns[address]
	if !ok {
		var err error
		conn, err = grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		t.conns[address] = conn
	}

	return raftpb.NewRaftServiceClient(conn), nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var firstErr error
	for _, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.conns = nil

	return firstErr
}

func toProtoEntries(entries []raft.LogEntry) []*raftpb.LogEntry {
	if len(entries) == 0 {
		return nil
	}

	pbEntries := make([]*raftpb.LogEntry, len(entries))
	for i, entry := range entries {
		pbEntries[i] = &raftpb.LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		}
	}
	return pbEntries
}

func (t *Transport) InstallSnapshot(
	ctx context.Context,
	peer raft.Peer,
	req raft.InstallSnapshotRequest,
) (raft.InstallSnapshotResponse, error) {
	pbReq := &raftpb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          string(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	}

	client, err := t.clientFor(peer.Address)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}

	pbResp, err := client.InstallSnapshot(ctx, pbReq)

	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}

	return raft.InstallSnapshotResponse{Term: pbResp.Term}, nil
}

func fromProtoEntries(pbEntries []*raftpb.LogEntry) []raft.LogEntry {
	if len(pbEntries) == 0 {
		return nil
	}

	entries := make([]raft.LogEntry, len(pbEntries))
	for i, pbEntry := range pbEntries {
		entries[i] = raft.LogEntry{
			Term:    pbEntry.Term,
			Command: pbEntry.Command,
		}
	}
	return entries
}

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
		PreVote:      req.PreVote,
	}
	client, err := t.clientFor(peer.Address)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

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

func (t *Transport) AppendEntries(
	ctx context.Context,
	peer raft.Peer,
	req raft.AppendEntriesRequest,
) (raft.AppendEntriesResponse, error) {
	pbReq := &raftpb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     string(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      toProtoEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	}

	client, err := t.clientFor(peer.Address)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	pbResp, err := client.AppendEntries(ctx, pbReq)

	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}

	response := raft.AppendEntriesResponse{
		Term:    pbResp.Term,
		Success: pbResp.Success,
	}

	return response, nil
}
