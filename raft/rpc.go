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
	if r.needsSnapshot(peer.ID) {
		r.sendSnapshot(ctx, peer, term)
		return
	}

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
	if req.PreVote {
		return r.handlePreVote(req)
	}

	r.mu.Lock()

	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()

		return RequestVoteResponse{
			Term:        term,
			VoteGranted: false,
		}
	}

	term := r.currentTerm
	votedFor := r.votedFor
	stepDown := false

	if req.Term > r.currentTerm {
		term = req.Term
		votedFor = ""
		stepDown = true
	}

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTermLocked()

	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	granted := logUpToDate && (votedFor == "" || votedFor == req.CandidateID)
	if granted {
		votedFor = req.CandidateID
	}

	if term != r.currentTerm || votedFor != r.votedFor {
		if err := r.storage.SaveState(term, votedFor); err != nil {
			fmt.Printf("node=%s failed persisting vote: %v\n", r.id, err)

			current := r.currentTerm
			r.mu.Unlock()

			return RequestVoteResponse{
				Term:        current,
				VoteGranted: false,
			}
		}
	}

	r.currentTerm = term
	r.votedFor = votedFor
	if stepDown {
		r.state = Follower
	}
	if granted {
		r.lastHeard = time.Now()
	}

	r.mu.Unlock()

	if granted {
		r.resetElectionTimer()
	}

	return RequestVoteResponse{
		Term:        term,
		VoteGranted: granted,
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
		if err := r.storage.SaveState(req.Term, ""); err != nil {
			fmt.Printf("node=%s failed persisting term: %v\n", r.id, err)

			current := r.currentTerm
			r.mu.Unlock()

			return AppendEntriesResponse{
				Term:    current,
				Success: false,
			}
		}

		r.currentTerm = req.Term
		r.votedFor = ""
	}

	r.state = Follower
	r.leaderID = req.LeaderID
	r.lastHeard = time.Now()

	if prevTerm, ok := r.termAtLocked(req.PrevLogIndex); !ok || prevTerm != req.PrevLogTerm {
		term := r.currentTerm
		r.mu.Unlock()

		r.resetElectionTimer()

		return AppendEntriesResponse{
			Term:    term,
			Success: false,
		}
	}

	appendFrom := -1
	truncateAt := uint64(0)

	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + uint64(i)

		existing, ok := r.termAtLocked(index)
		if !ok {
			appendFrom = i
			break
		}

		if existing != entry.Term {
			appendFrom = i
			truncateAt = index
			break
		}
	}

	if appendFrom >= 0 {
		fresh := req.Entries[appendFrom:]

		if truncateAt > 0 {
			if err := r.storage.TruncateLog(truncateAt); err != nil {
				fmt.Printf("node=%s failed persisting truncation: %v\n", r.id, err)

				current := r.currentTerm
				r.mu.Unlock()

				r.resetElectionTimer()

				return AppendEntriesResponse{
					Term:    current,
					Success: false,
				}
			}
		}

		if err := r.storage.AppendLog(fresh); err != nil {
			fmt.Printf("node=%s failed persisting entries: %v\n", r.id, err)

			current := r.currentTerm
			r.mu.Unlock()

			r.resetElectionTimer()

			return AppendEntriesResponse{
				Term:    current,
				Success: false,
			}
		}

		if truncateAt > 0 {
			r.log = append(r.log[:truncateAt-r.snapshotIndex], fresh...)
		} else {
			r.log = append(r.log, fresh...)
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
