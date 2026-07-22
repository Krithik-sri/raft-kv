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
