package raft

import (
	"context"
	"fmt"
	"sync"
	"time"
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
						go r.runReplication(ctx, req.Term)
					}
				}
			}

		}(peer)
	}
	wg.Wait()
}

func (r *Raft) replicateTo(ctx context.Context, peer Peer, term uint64) {
	req := r.makeAppendEntriesRequestFor(peer.ID)

	resp, err := r.transport.AppendEntries(ctx, peer, req)

	if err != nil {
		fmt.Printf("failed replicating to %s: %v\n", peer.ID, err)
		return
	}

	if resp.Term > term {
		fmt.Printf(
			"node=%s stepping down peer=%s term=%d\n",
			r.id,
			peer.ID,
			resp.Term,
		)
		r.becomeFollower(resp.Term)
		return
	}

	if !r.isLeaderForTerm(term) {
		return
	}

	if resp.Success {
		r.advancePeerProgress(peer.ID, req.PrevLogIndex+uint64(len(req.Entries)))
		return
	}

	r.retreatNextIndex(peer.ID)
}

func (r *Raft) replicateToAll(ctx context.Context, term uint64) {
	for _, peer := range r.peers {
		go r.replicateTo(ctx, peer, term)
	}
}

func (r *Raft) runReplication(ctx context.Context, term uint64) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		if !r.isLeaderForTerm(term) {
			fmt.Printf("node=%s stopping replication term=%d\n", r.id, term)
			return
		}

		r.replicateToAll(ctx, term)

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (r *Raft) HandleRequestVote(
	req RequestVoteRequest,
) RequestVoteResponse {
	r.mu.Lock()

	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()

		return RequestVoteResponse{
			Term:        term,
			VoteGranted: false,
		}
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.state = Follower
		r.votedFor = ""
	}

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTermLocked()

	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if logUpToDate && (r.votedFor == "" || r.votedFor == req.CandidateID) {
		r.votedFor = req.CandidateID
		term := r.currentTerm
		r.mu.Unlock()

		r.resetElectionTimer()

		return RequestVoteResponse{
			Term:        term,
			VoteGranted: true,
		}
	}

	term := r.currentTerm
	r.mu.Unlock()

	return RequestVoteResponse{
		Term:        term,
		VoteGranted: false,
	}
}

func (r *Raft) HandleAppendEntries(
	req AppendEntriesRequest,
) AppendEntriesResponse {
	r.mu.Lock()

	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()

		return AppendEntriesResponse{
			Term:    term,
			Success: false,
		}
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = ""
	}

	r.state = Follower
	r.leaderID = req.LeaderID

	if req.PrevLogIndex >= uint64(len(r.log)) ||
		r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		term := r.currentTerm
		r.mu.Unlock()

		r.resetElectionTimer()

		return AppendEntriesResponse{
			Term:    term,
			Success: false,
		}
	}

	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + uint64(i)

		if index >= uint64(len(r.log)) {
			r.log = append(r.log, req.Entries[i:]...)
			break
		}

		if r.log[index].Term != entry.Term {
			r.log = append(r.log[:index], req.Entries[i:]...)
			break
		}
	}

	if req.LeaderCommit > r.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))

		if req.LeaderCommit < lastNewIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}

		r.signalApply()
	}

	term := r.currentTerm
	r.mu.Unlock()

	r.resetElectionTimer()

	return AppendEntriesResponse{
		Term:    term,
		Success: true,
	}
}
