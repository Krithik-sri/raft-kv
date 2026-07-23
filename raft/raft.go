package raft

import "sync"

type Raft struct {
	mu        sync.Mutex
	id        NodeID
	state     State
	peers     []Peer
	transport Transport

	currentTerm uint64
	votedFor    NodeID
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
}

func New(
	id NodeID,
	peers []Peer,
	transport Transport,
) *Raft {
	return &Raft{
		id:        id,
		state:     Follower,
		peers:     peers,
		transport: transport,

		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
	}
}
