package raft

import (
	"context"
	"fmt"
	"sync"
)

type Raft struct {
	mu           sync.Mutex
	applyMu      sync.Mutex
	id           NodeID
	state        State
	peers        []Peer
	transport    Transport
	stateMachine StateMachine
	storage      Storage

	currentTerm uint64
	votedFor    NodeID
	leaderID    NodeID
	log         []LogEntry

	snapshotIndex     uint64
	snapshotTerm      uint64
	snapshotThreshold uint64

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	waiters map[uint64]chan applyOutcome

	electionResetCh chan struct{}
	applyCh         chan struct{}
}

func New(
	id NodeID,
	peers []Peer,
	transport Transport,
	stateMachine StateMachine,
	storage Storage,
) (*Raft, error) {
	if storage == nil {
		storage = nopStorage{}
	}

	r := &Raft{
		id:           id,
		state:        Follower,
		peers:        peers,
		transport:    transport,
		stateMachine: stateMachine,
		storage:      storage,

		log:               []LogEntry{{}},
		snapshotThreshold: defaultSnapshotThreshold,

		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
		waiters:    make(map[uint64]chan applyOutcome),

		electionResetCh: make(chan struct{}, 1),
		applyCh:         make(chan struct{}, 1),
	}

	state, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("load persistent state: %w", err)
	}

	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	r.snapshotIndex = state.SnapshotIndex
	r.snapshotTerm = state.SnapshotTerm
	r.commitIndex = state.SnapshotIndex
	r.lastApplied = state.SnapshotIndex

	r.log = []LogEntry{{Term: state.SnapshotTerm}}
	r.log = append(r.log, state.Log...)

	data, ok, err := storage.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	if ok {
		if err := stateMachine.Restore(data); err != nil {
			return nil, fmt.Errorf("restore snapshot: %w", err)
		}
	}

	if state.SnapshotIndex > 0 || len(state.Log) > 0 {
		fmt.Printf(
			"node=%s recovered term=%d votedFor=%q snapshot=%d entries=%d\n",
			id,
			r.currentTerm,
			r.votedFor,
			r.snapshotIndex,
			len(state.Log),
		)
	}

	return r, nil
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

	term := r.currentTerm + 1

	if err := r.storage.SaveState(term, r.id); err != nil {
		fmt.Printf("node=%s failed persisting candidacy: %v\n", r.id, err)
		return false
	}

	r.state = Candidate
	r.currentTerm = term
	r.votedFor = r.id
	r.leaderID = ""
	return true
}

func (r *Raft) getStateAndTerm() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

func (r *Raft) lastLogIndexLocked() uint64 {
	return r.snapshotIndex + uint64(len(r.log)) - 1
}

func (r *Raft) offsetLocked(index uint64) (int, bool) {
	if index < r.snapshotIndex {
		return 0, false
	}

	offset := int(index - r.snapshotIndex)
	if offset >= len(r.log) {
		return 0, false
	}

	return offset, true
}

func (r *Raft) termAtLocked(index uint64) (uint64, bool) {
	offset, ok := r.offsetLocked(index)
	if !ok {
		return 0, false
	}

	return r.log[offset].Term, true
}

func (r *Raft) lastLogIndexAndTermLocked() (uint64, uint64) {
	return r.lastLogIndexLocked(), r.log[len(r.log)-1].Term
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

	if nextIndex <= r.snapshotIndex {
		nextIndex = r.snapshotIndex + 1
	}
	if last := r.lastLogIndexLocked(); nextIndex > last+1 {
		nextIndex = last + 1
	}

	start := int(nextIndex - r.snapshotIndex)

	entries := make([]LogEntry, len(r.log)-start)
	copy(entries, r.log[start:])

	return AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  r.log[start-1].Term,
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

	index, term, err := r.appendCommandLocked(command)
	if err != nil {
		fmt.Printf("node=%s failed persisting command: %v\n", r.id, err)
		return 0, 0, false
	}

	fmt.Printf("node=%s submitted index=%d term=%d\n", r.id, index, term)

	return index, term, true
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

	for index := r.lastLogIndexLocked(); index > r.commitIndex; index-- {
		if term, ok := r.termAtLocked(index); !ok || term != r.currentTerm {
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
		if err := r.storage.SaveState(term, ""); err != nil {
			fmt.Printf("node=%s failed persisting term=%d: %v\n", r.id, term, err)
		} else {
			r.currentTerm = term
			r.votedFor = ""
		}
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

	noop := LogEntry{Term: r.currentTerm}

	if err := r.storage.AppendLog([]LogEntry{noop}); err != nil {
		fmt.Printf("node=%s failed persisting leader no-op: %v\n", r.id, err)
		return false
	}

	r.state = Leader
	r.leaderID = r.id

	lastLogIndex, _ := r.lastLogIndexAndTermLocked()
	for _, peer := range r.peers {
		r.nextIndex[peer.ID] = lastLogIndex + 1
		r.matchIndex[peer.ID] = 0
	}

	r.log = append(r.log, noop)

	return true
}
