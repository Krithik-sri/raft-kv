package raft

import (
	"context"
	"fmt"
	"sync"
)

func (r *Raft) RequestVotes(ctx context.Context) {
	var wg sync.WaitGroup
	var votesMu sync.Mutex

	votes := 1

	clusterSize := len(r.peers) + 1
	majority := clusterSize/2 + 1

	req := r.makeRequestVoteRequest()

	wg.Add(len(r.peers))

	for _, peer := range r.peers {
		go func(peer Peer) {
			defer wg.Done()

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

			if resp.Term > req.Term {
				r.becomeFollower(resp.Term)
				return
			}

			if !r.isCandidateTerm(req.Term) {
				return
			}

			if resp.VoteGranted {
				votesMu.Lock()
				votes++
				currentVotes := votes
				votesMu.Unlock()

				fmt.Printf(
					"node=%s election term=%d votes=%d majority=%d\n",
					r.id,
					req.Term,
					currentVotes,
					majority,
				)

				if currentVotes >= majority {
					if r.becomeLeader(req.Term) {
						fmt.Printf("node=%s became leader term=%d\n", r.id, req.Term)
					}
				}
			}

		}(peer)
	}
	wg.Wait()
}

func (r *Raft) HandleRequestVote(
	req RequestVoteRequest,
) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.currentTerm {
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.state = Follower
		r.votedFor = ""
	}

	if r.votedFor == "" || r.votedFor == req.CandidateID {
		r.votedFor = req.CandidateID
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: true,
		}
	}
	return RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}
}
