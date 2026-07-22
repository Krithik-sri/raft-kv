package raft

import "sync"

type Raft struct {
	mu    sync.Mutex
	id    NodeID
	state State

	currentTerm uint64
	votedFor    NodeID
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
}
