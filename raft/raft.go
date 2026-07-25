package raft

import (
	"context"
	"fmt"
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

	applied [][]byte

	electionResetCh chan struct{}
	applyCh         chan struct{}
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

		log: []LogEntry{{}},

		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),

		electionResetCh: make(chan struct{}, 1),
		applyCh:         make(chan struct{}, 1),
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
	go r.runApplyLoop(ctx)
	r.runElectionTimer(ctx)
}

func (r *Raft) becomeCandidate() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Leader {
		return false
	}

	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	return true
}

func (r *Raft) getStateAndTerm() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

func (r *Raft) lastLogIndexAndTermLocked() (uint64, uint64) {
	lastIndex := uint64(len(r.log) - 1)
	return lastIndex, r.log[lastIndex].Term
}

func (r *Raft) makeRequestVoteRequest() RequestVoteRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTermLocked()

	return RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func (r *Raft) makeAppendEntriesRequestFor(peerID NodeID) AppendEntriesRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	nextIndex := r.nextIndex[peerID]
	if nextIndex < 1 {
		nextIndex = 1
	}

	prevLogIndex := nextIndex - 1

	entries := make([]LogEntry, uint64(len(r.log))-nextIndex)
	copy(entries, r.log[nextIndex:])

	return AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  r.log[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
}

func (r *Raft) Submit(command []byte) (uint64, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}

	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	index := uint64(len(r.log) - 1)

	fmt.Printf("node=%s submitted index=%d term=%d\n", r.id, index, r.currentTerm)

	return index, r.currentTerm, true
}

func (r *Raft) advancePeerProgress(peerID NodeID, matchIndex uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if matchIndex > r.matchIndex[peerID] {
		r.matchIndex[peerID] = matchIndex
	}
	r.nextIndex[peerID] = r.matchIndex[peerID] + 1

	r.advanceCommitIndexLocked()
}

func (r *Raft) retreatNextIndex(peerID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nextIndex[peerID] > 1 {
		r.nextIndex[peerID]--
	}
}

func (r *Raft) advanceCommitIndexLocked() {
	majority := (len(r.peers)+1)/2 + 1
	lastLogIndex := uint64(len(r.log) - 1)

	for index := lastLogIndex; index > r.commitIndex; index-- {
		if r.log[index].Term != r.currentTerm {
			continue
		}

		count := 1
		for _, peer := range r.peers {
			if r.matchIndex[peer.ID] >= index {
				count++
			}
		}

		if count >= majority {
			r.commitIndex = index
			r.signalApply()

			fmt.Printf(
				"node=%s commitIndex=%d term=%d\n",
				r.id,
				index,
				r.currentTerm,
			)
			return
		}
	}
}

func (r *Raft) isLeaderForTerm(term uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state == Leader && r.currentTerm == term
}

func (r *Raft) isCandidateTerm(term uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state == Candidate && r.currentTerm == term
}

func (r *Raft) becomeFollower(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}

	r.state = Follower
}

func (r *Raft) resetElectionTimer() {
	select {
	case r.electionResetCh <- struct{}{}:
	default:
	}
}

func (r *Raft) becomeLeader(term uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Candidate || r.currentTerm != term {
		return false
	}

	r.state = Leader

	lastLogIndex, _ := r.lastLogIndexAndTermLocked()
	for _, peer := range r.peers {
		r.nextIndex[peer.ID] = lastLogIndex + 1
		r.matchIndex[peer.ID] = 0
	}

	return true
}
