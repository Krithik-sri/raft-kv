package raft

import (
	"context"
	"fmt"
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

	count := int(end - r.lastApplied)
	if start+count > len(r.log) {
		count = len(r.log) - start
	}

	pending := make([]LogEntry, count)
	copy(pending, r.log[start:start+count])
	r.lastApplied = first + uint64(count) - 1

	r.mu.Unlock()

	for offset, entry := range pending {
		index := first + uint64(offset)

		if len(entry.Command) == 0 {
			continue
		}

		result, err := r.stateMachine.Apply(entry.Command)

		r.notifyWaiter(index, applyOutcome{
			term:   entry.Term,
			result: result,
			err:    err,
		})

		if err != nil {
			fmt.Printf(
				"node=%s apply failed index=%d: %v\n",
				r.id,
				index,
				err,
			)
			continue
		}

		fmt.Printf(
			"node=%s applied index=%d term=%d\n",
			r.id,
			index,
			entry.Term,
		)
	}
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

	term, ok := r.termAtLocked(index)
	if !ok {
		r.mu.Unlock()
		return
	}

	r.mu.Unlock()

	data, err := r.stateMachine.Snapshot()
	if err != nil {
		fmt.Printf("node=%s snapshot failed: %v\n", r.id, err)
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

	term, ok = r.termAtLocked(index)
	if !ok {
		return
	}

	retained := make([]LogEntry, len(r.log)-offset-1)
	copy(retained, r.log[offset+1:])

	err = r.storage.SaveSnapshot(
		Snapshot{Index: index, Term: term, Data: data},
		r.currentTerm,
		r.votedFor,
		retained,
	)
	if err != nil {
		fmt.Printf("node=%s persisting snapshot failed: %v\n", r.id, err)
		return
	}

	r.log = append([]LogEntry{{Term: term}}, retained...)
	r.snapshotIndex = index
	r.snapshotTerm = term

	fmt.Printf(
		"node=%s snapshot index=%d term=%d retained=%d\n",
		r.id,
		index,
		term,
		len(retained),
	)
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
