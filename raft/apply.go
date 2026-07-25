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
	defer r.mu.Unlock()

	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := r.log[r.lastApplied]
		r.applied = append(r.applied, entry.Command)

		fmt.Printf(
			"node=%s applied index=%d term=%d\n",
			r.id,
			r.lastApplied,
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
