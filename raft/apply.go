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
	r.mu.Lock()

	if r.lastApplied >= r.commitIndex {
		r.mu.Unlock()
		return
	}

	first := r.lastApplied + 1

	start, ok := r.offsetLocked(first)
	if !ok {
		r.mu.Unlock()
		return
	}

	pending := make([]LogEntry, r.commitIndex-r.lastApplied)
	copy(pending, r.log[start:start+len(pending)])
	r.lastApplied = r.commitIndex

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

func (r *Raft) runApplyLoop(ctx context.Context) {
	for {
		select {
		case <-r.applyCh:
			r.applyCommitted()
		case <-ctx.Done():
			return
		}
	}
}
