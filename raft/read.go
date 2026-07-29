package raft

import "context"

// waitFor blocks until cond says it is satisfied, or ctx runs out.
//
// cond is called with r.mu held, so it must not lock anything itself. It
// returns (satisfied, fatal). A fatal error stops the wait immediately;
// otherwise we sleep until something advances and check again.
func (r *Raft) waitFor(ctx context.Context, cond func() (bool, error)) error {
	for {
		r.mu.Lock()
		done, err := cond()
		progress := r.progressCh
		r.mu.Unlock()

		if err != nil {
			return err
		}
		if done {
			return nil
		}

		select {
		case <-progress:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// notifyProgressLocked wakes everyone waiting in waitFor. Call with r.mu held.
//
// Closing the channel and swapping in a fresh one is the usual Go way to do a
// broadcast. Waiters grab the channel under the lock, so they either see the
// new state or they are already parked on the old channel, which is about to
// close.
func (r *Raft) notifyProgressLocked() {
	close(r.progressCh)
	r.progressCh = make(chan struct{})
}

// quorumAckedLocked reports whether a majority has answered us since the read
// barrier was raised. We count ourselves.
func (r *Raft) quorumAckedLocked(barrier uint64) bool {
	acked := 1

	for _, member := range r.config.Members {
		if member.ID == r.id || !member.Voter {
			continue
		}
		if r.ackedBarrier[member.ID] >= barrier {
			acked++
		}
	}

	return acked >= r.majorityLocked()
}

// ReadIndex returns a log index that is safe to read at, once it has proved we
// are still the leader. This is the §6.4 read barrier.
//
// Being leader is not enough on its own. A leader that has been cut off from
// the network still thinks it is in charge, and its local state machine can be
// arbitrarily far behind. So we raise a counter, wait for a majority to answer
// a message sent after that point, and only then hand back an index.
//
// Read the index, wait for it to be applied, and the value you get is
// linearizable.
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	r.mu.Lock()

	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}

	term := r.currentTerm

	r.readBarrier++
	barrier := r.readBarrier

	r.mu.Unlock()

	// Don't sit around for the next heartbeat tick.
	r.signalReplicate()

	var index uint64

	err := r.waitFor(ctx, func() (bool, error) {
		if r.state != Leader || r.currentTerm != term {
			return false, ErrNotLeader
		}

		// A brand new leader has not committed anything of its own yet, and
		// §5.4.2 forbids committing an inherited entry by counting copies. Until
		// the no-op from becomeLeader lands, commitIndex can sit below writes we
		// inherited, and reading here would miss them. It commits within a
		// heartbeat, so wait.
		if entryTerm, ok := r.termAtLocked(r.commitIndex); !ok || entryTerm != term {
			return false, nil
		}

		if !r.quorumAckedLocked(barrier) {
			return false, nil
		}

		index = r.commitIndex
		return true, nil
	})

	return index, err
}

// WaitForApplied blocks until the state machine has caught up to index.
func (r *Raft) WaitForApplied(ctx context.Context, index uint64) error {
	return r.waitFor(ctx, func() (bool, error) {
		return r.lastApplied >= index, nil
	})
}
