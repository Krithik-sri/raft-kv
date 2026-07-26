package raft

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID
	Log         []LogEntry
}

type Storage interface {
	Load() (PersistentState, error)
	SaveState(term uint64, votedFor NodeID) error
	AppendLog(entries []LogEntry) error
	TruncateLog(length uint64) error
}

type nopStorage struct{}

var _ Storage = nopStorage{}

func (nopStorage) Load() (PersistentState, error)  { return PersistentState{}, nil }
func (nopStorage) SaveState(uint64, NodeID) error  { return nil }
func (nopStorage) AppendLog([]LogEntry) error      { return nil }
func (nopStorage) TruncateLog(length uint64) error { return nil }
