package raft

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

const (
	defaultSnapshotThreshold = 100
	replicationTimeout       = 500 * time.Millisecond
	snapshotSendTimeout      = 5 * time.Second
)

func randomElectionTimeout() time.Duration {
	spread := maxElectionTimeout - minElectionTimeout

	return minElectionTimeout + rand.N(spread+1)
}

func (r *Raft) runElectionTimer(ctx context.Context) {
	for {
		timeout := randomElectionTimeout()
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			state, term := r.getStateAndTerm()
			r.logger.Debug("election timeout", "after", timeout, "state", state, "term", term)

			go r.campaign(ctx)

		case <-r.electionResetCh:
			timer.Stop()
			r.logger.Debug("election timer reset")
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
