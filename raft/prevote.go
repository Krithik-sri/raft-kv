package raft

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (r *Raft) handlePreVote(req RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	deny := RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}

	if r.state == Leader {
		return deny
	}

	if !r.lastHeard.IsZero() && time.Since(r.lastHeard) < minElectionTimeout {
		return deny
	}

	if req.Term < r.currentTerm {
		return deny
	}

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTermLocked()

	upToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	return RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: upToDate,
	}
}

func (r *Raft) winPreVote(ctx context.Context, req RequestVoteRequest) bool {
	if len(r.peers) == 0 {
		return true
	}

	majority := (len(r.peers)+1)/2 + 1

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		votes = 1
		ahead uint64
	)

	wg.Add(len(r.peers))

	for _, peer := range r.peers {
		go func(peer Peer) {
			defer wg.Done()

			resp, err := r.transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if resp.Term > ahead {
				ahead = resp.Term
			}
			if resp.VoteGranted {
				votes++
			}
		}(peer)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if ahead > req.Term {
		r.becomeFollower(ahead)
		return false
	}

	return votes >= majority
}

func (r *Raft) campaign(ctx context.Context) {
	r.mu.Lock()

	if r.state == Leader {
		r.mu.Unlock()
		return
	}

	term := r.currentTerm
	lastLogIndex, lastLogTerm := r.lastLogIndexAndTermLocked()

	r.mu.Unlock()

	preVote := RequestVoteRequest{
		Term:         term + 1,
		CandidateID:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PreVote:      true,
	}

	if !r.winPreVote(ctx, preVote) {
		fmt.Printf("node=%s pre-vote failed term=%d\n", r.id, term+1)
		return
	}

	if !r.becomeCandidate(term) {
		return
	}

	fmt.Printf("node=%s starting election term=%d\n", r.id, term+1)

	r.RequestVotes(ctx)
}
