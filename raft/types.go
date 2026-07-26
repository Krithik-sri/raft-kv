package raft

type NodeID string

type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Command []byte
}

type Peer struct {
	ID      NodeID
	Address string
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term uint64
}
