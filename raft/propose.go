package raft

import (
	"context"
	"errors"
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

func (r *Raft) LeaderHint() (NodeID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderID, r.state == Leader
}

func (r *Raft) appendCommandLocked(command []byte) (uint64, uint64) {
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	return uint64(len(r.log) - 1), r.currentTerm
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

	index, term := r.appendCommandLocked(command)

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
