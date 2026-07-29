package raft

type NodeID string

type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

type EntryKind uint8

const (
	// EntryCommand is the zero value on purpose. Log entries written before
	// this field existed decode as ordinary commands, which is what they are.
	EntryCommand EntryKind = iota
	EntryConfig
)

type LogEntry struct {
	Term    uint64
	Kind    EntryKind
	Command []byte
}

type Peer struct {
	ID      NodeID
	Address string
}

// Member is one machine in the cluster. A member that is not a Voter still
// receives the log but is not counted towards any majority, which is how a new
// machine catches up without holding elections hostage while it does.
type Member struct {
	ID      NodeID
	Address string
	Voter   bool
}

type Configuration struct {
	Members []Member
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
