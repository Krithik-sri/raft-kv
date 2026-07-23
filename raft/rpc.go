package raft

import (
	"context"
	"fmt"
	"sync"
)

func (r *Raft) RequestVotes(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(len(r.peers))

	for _, peer := range r.peers {
		go func(peer Peer) {
			defer wg.Done()
			req := RequestVoteRequest{
				Term:         1,
				CandidateID:  r.id,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}
			resp, err := r.transport.RequestVote(ctx, peer, req)
			if err != nil {
				fmt.Printf("failed requesting vote from %s: %v\n", peer.ID, err)
				return
			}
			fmt.Printf(
				"peer=%s term=%d granted=%t\n",
				peer.ID,
				resp.Term,
				resp.VoteGranted,
			)
		}(peer)
	}
	wg.Wait()
}
