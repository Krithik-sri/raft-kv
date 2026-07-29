package raft

import (
	"context"
	"slices"
)

func (r *Raft) signalApply() {
	select {
	case r.applyCh <- struct{}{}:
	default:
	}
}

func (r *Raft) applyCommitted() {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	r.mu.Lock()

	end := r.commitIndex
	if last := r.lastLogIndexLocked(); end > last {
		end = last
	}

	if r.lastApplied >= end {
		r.mu.Unlock()
		return
	}

	first := r.lastApplied + 1

	start, ok := r.offsetLocked(first)
	if !ok {
		r.mu.Unlock()
		return
	}

	count := min(int(end-r.lastApplied), len(r.log)-start)

	pending := slices.Clone(r.log[start : start+count])

	r.mu.Unlock()

	for offset, entry := range pending {
		index := first + uint64(offset)
		
		if entry.Kind != EntryCommand || len(entry.Command) == 0 {
			continue
		}

		result, err := r.stateMachine.Apply(entry.Command)

		r.notifyWaiter(index, applyOutcome{
			term:   entry.Term,
			result: result,
			err:    err,
		})

		if err != nil {
			r.logger.Error("apply failed", "index", index, "err", err)
			continue
		}

		r.logger.Debug("applied", "index", index, "term", entry.Term)
	}

	r.mu.Lock()
	r.lastApplied = first + uint64(len(pending)) - 1
	r.notifyProgressLocked()
	r.mu.Unlock()
}

func (r *Raft) maybeSnapshot() {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	r.mu.Lock()

	index := r.lastApplied
	if index-r.snapshotIndex < r.snapshotThreshold {
		r.mu.Unlock()
		return
	}

	if _, ok := r.termAtLocked(index); !ok {
		r.mu.Unlock()
		return
	}

	r.mu.Unlock()

	data, err := r.stateMachine.Snapshot()
	if err != nil {
		r.logger.Error("snapshot failed", "err", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.snapshotIndex {
		return
	}

	offset, ok := r.offsetLocked(index)
	if !ok {
		return
	}

	term, ok := r.termAtLocked(index)
	if !ok {
		return
	}

	retained := slices.Clone(r.log[offset+1:])

	err = r.storage.SaveSnapshot(
		Snapshot{Index: index, Term: term, Data: data},
		r.currentTerm,
		r.votedFor,
		r.config,
		retained,
	)
	if err != nil {
		r.logger.Error("persisting snapshot failed", "err", err)
		return
	}

	r.log = append([]LogEntry{{Term: term}}, retained...)
	r.snapshotIndex = index
	r.snapshotTerm = term
	r.persistedUpToLocked()
	r.baseConfig = r.config

	r.logger.Info("snapshot taken", "index", index, "term", term, "retained", len(retained))
}

func (r *Raft) runApplyLoop(ctx context.Context) {
	for {
		select {
		case <-r.applyCh:
			r.applyCommitted()
			r.maybeSnapshot()
		case <-ctx.Done():
			return
		}
	}
}
