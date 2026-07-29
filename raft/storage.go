package raft

type PersistentState struct {
	CurrentTerm   uint64
	VotedFor      NodeID
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Log           []LogEntry

	// Config is the membership as of the snapshot. Nil means this machine has
	// never seen one and should fall back to the list it was started with.
	// Without this, a node that restores a snapshot forgets who its peers are.
	Config *Configuration
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
		config Configuration,
		retained []LogEntry,
	) error
	LoadSnapshot() ([]byte, bool, error)
}
