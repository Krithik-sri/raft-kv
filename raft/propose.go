package raft

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNotLeader      = errors.New("not leader")
	ErrLostLeadership = errors.New("lost leadership before commit")
)

type applyOutcome struct {
	term   uint64
	result []byte
	err    error
}

type Status struct {
	ID            NodeID
	State         State
	Term          uint64
	VotedFor      NodeID
	LeaderID      NodeID
	LogLength     int
	LastLogIndex  uint64
	SnapshotIndex uint64
	CommitIndex   uint64
	LastApplied   uint64
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	return Status{
		ID:            r.id,
		State:         r.state,
		Term:          r.currentTerm,
		VotedFor:      r.votedFor,
		LeaderID:      r.leaderID,
		LogLength:     len(r.log),
		LastLogIndex:  r.lastLogIndexLocked(),
		SnapshotIndex: r.snapshotIndex,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
	}
}

func (r *Raft) LeaderHint() (NodeID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderID, r.state == Leader
}

func (r *Raft) appendCommandLocked(command []byte) (uint64, uint64, error) {
	entry := LogEntry{Term: r.currentTerm, Command: command}

	if err := r.storage.AppendLog([]LogEntry{entry}); err != nil {
		return 0, 0, err
	}

	r.log = append(r.log, entry)
	return r.lastLogIndexLocked(), r.currentTerm, nil
}

func (r *Raft) clearWaiter(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.waiters, index)
}

func (r *Raft) notifyWaiter(index uint64, outcome applyOutcome) {
	r.mu.Lock()
	waiter, ok := r.waiters[index]
	if ok {
		delete(r.waiters, index)
	}
	r.mu.Unlock()

	if ok {
		waiter <- outcome
	}
}

func (r *Raft) Propose(ctx context.Context, command []byte) ([]byte, error) {
	r.mu.Lock()

	if r.state != Leader {
		r.mu.Unlock()
		return nil, ErrNotLeader
	}

	index, term, err := r.appendCommandLocked(command)
	if err != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("persist command: %w", err)
	}

	waiter := make(chan applyOutcome, 1)
	r.waiters[index] = waiter

	r.mu.Unlock()

	defer r.clearWaiter(index)

	select {
	case outcome := <-waiter:
		if outcome.term != term {
			return nil, ErrLostLeadership
		}
		return outcome.result, outcome.err

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
