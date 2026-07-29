package raft

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type Raft struct {
	mu           sync.Mutex
	applyMu      sync.Mutex
	id           NodeID
	address      string
	state        State
	transport    Transport
	stateMachine StateMachine
	storage      Storage
	logger       *slog.Logger

	currentTerm uint64
	votedFor    NodeID
	leaderID    NodeID
	lastHeard   time.Time
	log         []LogEntry

	snapshotIndex     uint64
	snapshotTerm      uint64
	snapshotThreshold uint64

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	waiters     map[uint64]chan applyOutcome
	replicating map[NodeID]bool

	// readBarrier is bumped by every ReadIndex. ackedBarrier records the
	// highest barrier each peer has answered, which is how we prove we still
	// hold a quorum without sending extra messages.
	readBarrier  uint64
	ackedBarrier map[NodeID]uint64

	// lastAck is when each peer last answered us. A leader that cannot reach a
	// majority any more uses this to work out that it is finished.
	lastAck map[NodeID]time.Time

	// config is who is in the cluster right now, and configIndex is the log
	// entry it came from. Both change at runtime, so both are guarded by r.mu.
	config      Configuration
	configIndex uint64

	// baseConfig is the membership as of the snapshot, i.e. the starting point
	// the log is replayed on top of. Kept so the config can be recomputed when
	// a truncation takes a configuration entry back out again.
	baseConfig Configuration

	// persistedIndex is the highest log index known to be on disk. The log in
	// memory runs ahead of it, which is the entire point, and is also why the
	// leader may not count itself towards a majority above it.
	persistedIndex uint64

	electionResetCh chan struct{}
	applyCh         chan struct{}
	replicateCh     chan struct{}
	persistCh       chan struct{}
	progressCh      chan struct{}
}

func New(
	self Peer,
	peers []Peer,
	transport Transport,
	stateMachine StateMachine,
	storage Storage,
) (*Raft, error) {
	r := &Raft{
		id:           self.ID,
		address:      self.Address,
		logger:       slog.Default().With("node", string(self.ID)),
		state:        Follower,
		transport:    transport,
		stateMachine: stateMachine,
		storage:      storage,

		log:               []LogEntry{{}},
		snapshotThreshold: defaultSnapshotThreshold,

		nextIndex:    make(map[NodeID]uint64),
		matchIndex:   make(map[NodeID]uint64),
		waiters:      make(map[uint64]chan applyOutcome),
		replicating:  make(map[NodeID]bool),
		ackedBarrier: make(map[NodeID]uint64),
		lastAck:      make(map[NodeID]time.Time),

		electionResetCh: make(chan struct{}, 1),
		applyCh:         make(chan struct{}, 1),
		replicateCh:     make(chan struct{}, 1),
		persistCh:       make(chan struct{}, 1),
		progressCh:      make(chan struct{}),
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
	r.persistedUpToLocked()

	// Where we start from: whatever the snapshot remembered, or the list we
	// were handed if this is a brand new machine. The log gets the last word.
	base := state.Config
	if base == nil {
		bootstrap := configurationFromPeers(self.ID, self.Address, peers)
		base = &bootstrap
	}

	r.baseConfig = *base
	r.refreshConfigLocked(r.baseConfig, state.SnapshotIndex)

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
		r.logger.Info("recovered",
			"term", r.currentTerm,
			"votedFor", r.votedFor,
			"snapshot", r.snapshotIndex,
			"entries", len(state.Log),
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
	go r.runPersistLoop(ctx)
	r.runElectionTimer(ctx)
}

func (r *Raft) becomeCandidate(expectedTerm uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Leader || r.currentTerm != expectedTerm {
		return false
	}

	term := r.currentTerm + 1

	if err := r.storage.SaveState(term, r.id); err != nil {
		r.logger.Error("failed persisting candidacy", "err", err)
		return false
	}

	r.state = Candidate
	r.currentTerm = term
	r.votedFor = r.id
	r.leaderID = ""
	r.notifyProgressLocked()
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

func (r *Raft) makeAppendEntriesRequestFor(
	peerID NodeID,
	term uint64,
) (AppendEntriesRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader || r.currentTerm != term {
		return AppendEntriesRequest{}, false
	}

	nextIndex := r.nextIndex[peerID]

	nextIndex = min(max(nextIndex, r.snapshotIndex+1), r.lastLogIndexLocked()+1)

	start := int(nextIndex - r.snapshotIndex)

	entries := slices.Clone(r.log[start:])

	return AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  r.log[start-1].Term,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}, true
}

func (r *Raft) submit(command []byte) (uint64, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}

	index, term, err := r.appendCommandLocked(command)
	if err != nil {
		r.logger.Error("failed persisting command", "err", err)
		return 0, 0, false
	}

	r.logger.Debug("submitted", "index", index, "term", term)

	return index, term, true
}

func (r *Raft) advancePeerProgress(peerID NodeID, matchIndex, term, barrier uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader || r.currentTerm != term {
		return
	}

	if matchIndex > r.matchIndex[peerID] {
		r.matchIndex[peerID] = matchIndex
	}
	r.nextIndex[peerID] = r.matchIndex[peerID] + 1

	if barrier > r.ackedBarrier[peerID] {
		r.ackedBarrier[peerID] = barrier
	}
	r.lastAck[peerID] = time.Now()

	r.advanceCommitIndexLocked()
	r.notifyProgressLocked()
}

// currentBarrier reads the read barrier before a message is built.
//
// It has to be captured before the send, never after. An ack only proves the
// peer answered a message sent at or after the barrier it is credited with.
// Capturing late could credit an ack to a barrier raised after the message
// left, which would let a read through without a real quorum check. Capturing
// early is safe: the worst case is one extra round trip.
func (r *Raft) currentBarrier() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.readBarrier
}

func (r *Raft) retreatNextIndex(peerID NodeID, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader || r.currentTerm != term {
		return
	}

	if r.nextIndex[peerID] > 1 {
		r.nextIndex[peerID]--
	}
}

func (r *Raft) advanceCommitIndexLocked() {
	majority := r.majorityLocked()

	for index := r.lastLogIndexLocked(); index > r.commitIndex; index-- {
		if term, ok := r.termAtLocked(index); !ok || term != r.currentTerm {
			continue
		}

		// Our own copy only counts once it is durable. Counting it earlier
		// lets a crash turn the majority that committed this entry into a
		// minority, which loses acknowledged writes.
		count := 0
		if r.config.isVoter(r.id) && r.persistedIndex >= index {
			count = 1
		}

		for _, member := range r.config.Members {
			if member.ID == r.id || !member.Voter {
				continue
			}
			if r.matchIndex[member.ID] >= index {
				count++
			}
		}

		if count >= majority {
			r.commitIndex = index
			r.signalApply()

			r.logger.Debug("commit advanced", "index", index, "term", r.currentTerm)
			return
		}
	}
}

// hasQuorumRecently reports whether a majority has answered us lately.
//
// This is CheckQuorum. Being leader is a claim, not a fact: a node that has
// been cut off keeps believing it until somebody tells it otherwise, and
// nobody can, because it is cut off. So it has to notice by itself.
//
// The window is a full election timeout. By then any follower that could hear
// us would have answered, and any follower that couldn't is off holding its
// own election.
func (r *Raft) hasQuorumRecently() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return true
	}

	cutoff := time.Now().Add(-maxElectionTimeout)

	reachable := 0
	if r.config.isVoter(r.id) {
		reachable = 1
	}

	for _, member := range r.config.Members {
		if member.ID == r.id || !member.Voter {
			continue
		}
		if r.lastAck[member.ID].After(cutoff) {
			reachable++
		}
	}

	return reachable >= r.majorityLocked()
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
			r.logger.Error("failed persisting term", "term", term, "err", err)
		} else {
			r.currentTerm = term
			r.votedFor = ""
		}
	}

	r.state = Follower
	r.notifyProgressLocked()
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

	r.state = Leader
	r.leaderID = r.id

	// Nobody has answered this term yet. Without this, the quorum check below
	// would depose the leader the instant it was elected.
	now := time.Now()
	peers := r.peersLocked()

	for _, peer := range peers {
		r.lastAck[peer.ID] = now
	}

	lastLogIndex, _ := r.lastLogIndexAndTermLocked()
	for _, peer := range peers {
		r.nextIndex[peer.ID] = lastLogIndex + 1
		r.matchIndex[peer.ID] = 0
	}

	r.log = append(r.log, noop)
	r.signalPersist()

	return true
}
