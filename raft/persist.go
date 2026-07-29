package raft

import "context"

// The leader used to flush every proposal to disk on its own, one at a time,
// which made the disk the whole cost of a write. Now proposals land in memory
// and a loop flushes whatever has piled up since last time. Eight concurrent
// writes cost one flush instead of eight.
//
// The catch is that an entry now exists in memory before it exists on disk, so
// the leader must not count itself towards a majority until its own copy is
// durable. See advanceCommitIndexLocked. Get that wrong and you commit data
// that is on nobody's disk.
//
// The flush happens with r.mu held, which sounds like it would ruin the whole
// idea. It doesn't. Proposals that arrive during a flush queue on the mutex,
// append the moment it is released, and get picked up by the next flush. That
// is the batching. Releasing the lock would let a follower truncate the log
// underneath us and leave the stale tail sitting after the new entries on disk.

func (r *Raft) signalPersist() {
	select {
	case r.persistCh <- struct{}{}:
	default:
	}
}

// persistedUpToLocked marks everything currently in the log as durable. Callers
// that wrote synchronously use this. Call with r.mu held.
func (r *Raft) persistedUpToLocked() {
	r.persistedIndex = r.lastLogIndexLocked()
}

func (r *Raft) persistPending() {
	r.mu.Lock()
	defer r.mu.Unlock()

	last := r.lastLogIndexLocked()
	if r.persistedIndex >= last {
		return
	}

	first := max(r.persistedIndex+1, r.snapshotIndex+1)

	start, ok := r.offsetLocked(first)
	if !ok {
		// Everything we were going to flush has been compacted away, which
		// means it is already on disk inside the snapshot.
		r.persistedIndex = max(r.persistedIndex, r.snapshotIndex)
		return
	}

	if err := r.storage.AppendLog(r.log[start:]); err != nil {
		r.logger.Error("persisting log failed", "err", err)
		return
	}

	r.persistedIndex = last
	r.notifyProgressLocked()

	// Our own copy just became durable, which may be the last thing a commit
	// was waiting on.
	r.signalReplicate()
}

func (r *Raft) runPersistLoop(ctx context.Context) {
	for {
		select {
		case <-r.persistCh:
			r.persistPending()
		case <-ctx.Done():
			return
		}
	}
}
