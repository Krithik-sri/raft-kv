package raft

import (
	"context"
	"sync"
)

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

	electionResetCh chan struct{}
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

		electionResetCh: make(chan struct{}, 1),
	}
}

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

func (r *Raft) Start(ctx context.Context) {
	r.runElectionTimer(ctx)
}

func (r *Raft) becomeCandidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id

}

func (r *Raft) getStateAndTerm() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

func (r *Raft) makeRequestVoteRequest() RequestVoteRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	return RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
}
