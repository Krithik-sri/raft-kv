package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	randomNumber := 150 + rand.IntN(151)

	return time.Duration(randomNumber) * time.Millisecond
}

func (r *Raft) runElectionTimer(ctx context.Context) {
	for {
		timeout := randomElectionTimeout()
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:

			if !r.becomeCandidate() {
				continue
			}
			state, term := r.getStateAndTerm()
			fmt.Printf("node=%s election timeout after=%s state=%s term=%d\n", r.id, timeout, state, term)
			go r.RequestVotes(ctx)

		case <-r.electionResetCh:
			timer.Stop()
			fmt.Printf("node=%s election timer reset\n", r.id)
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
