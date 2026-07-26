package raft

type PersistentState struct {
	CurrentTerm   uint64
	VotedFor      NodeID
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Log           []LogEntry
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Storage interface {
	Load() (PersistentState, error)
	SaveState(term uint64, votedFor NodeID) error
	AppendLog(entries []LogEntry) error
	TruncateLog(index uint64) error

	SaveSnapshot(
		snapshot Snapshot,
		term uint64,
		votedFor NodeID,
		retained []LogEntry,
	) error
	LoadSnapshot() ([]byte, bool, error)
}
