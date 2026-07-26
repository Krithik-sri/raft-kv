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

type nopStorage struct{}

var _ Storage = nopStorage{}

func (nopStorage) Load() (PersistentState, error) { return PersistentState{}, nil }
func (nopStorage) SaveState(uint64, NodeID) error { return nil }
func (nopStorage) AppendLog([]LogEntry) error     { return nil }
func (nopStorage) TruncateLog(index uint64) error { return nil }
func (nopStorage) LoadSnapshot() ([]byte, bool, error) {
	return nil, false, nil
}

func (nopStorage) SaveSnapshot(Snapshot, uint64, NodeID, []LogEntry) error {
	return nil
}
