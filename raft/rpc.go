package raft

import (
	"context"
	"sync"
	"time"
)

func (r *Raft) RequestVotes(ctx context.Context) {
	var wg sync.WaitGroup
	var votesMu sync.Mutex

	votes := 1

	majority := r.majority()

	req := r.makeRequestVoteRequest()

	wg.Add(len(r.peers))

	for _, peer := range r.peers {
		go func(peer Peer) {
			defer wg.Done()

			resp, err := r.transport.RequestVote(ctx, peer, req)

			if err != nil {
				r.logger.Debug("vote request failed", "peer", peer.ID, "err", err)
				return
			}

			r.logger.Debug("vote reply", "peer", peer.ID, "term", resp.Term, "granted", resp.VoteGranted)

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

				r.logger.Debug("election progress", "term", req.Term, "votes", currentVotes, "majority", majority)

				if currentVotes >= majority {
					if r.becomeLeader(req.Term) {
						r.logger.Info("became leader", "term", req.Term)
						go r.runReplication(ctx, req.Term)
					}
				}
			}

		}(peer)
	}
	wg.Wait()
}

func (r *Raft) beginReplication(peerID NodeID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.replicating[peerID] {
		return false
	}

	r.replicating[peerID] = true
	return true
}

func (r *Raft) endReplication(peerID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.replicating, peerID)
}

func (r *Raft) replicateTo(ctx context.Context, peer Peer, term uint64) {
	if !r.beginReplication(peer.ID) {
		return
	}
	defer r.endReplication(peer.ID)

	if r.needsSnapshot(peer.ID) {
		r.sendSnapshot(ctx, peer, term)
		return
	}

	req, ok := r.makeAppendEntriesRequestFor(peer.ID, term)
	if !ok {
		return
	}

	resp, err := r.transport.AppendEntries(ctx, peer, req)

	if err != nil {
		r.logger.Debug("replication failed", "peer", peer.ID, "err", err)
		return
	}

	if resp.Term > term {
		r.logger.Info("stepping down", "peer", peer.ID, "term", resp.Term)
		r.becomeFollower(resp.Term)
		return
	}

	if !r.isLeaderForTerm(term) {
		return
	}

	if resp.Success {
		r.advancePeerProgress(peer.ID, req.PrevLogIndex+uint64(len(req.Entries)), term)
		return
	}

	r.retreatNextIndex(peer.ID, term)
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
			r.logger.Info("stopping replication", "term", term)
			return
		}

		r.replicateToAll(ctx, term)

		select {
		case <-ticker.C:
		case <-r.replicateCh:
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
			r.logger.Error("failed persisting vote", "err", err)

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
			r.logger.Error("failed persisting term", "err", err)

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

		if truncateAt > 0 && truncateAt <= r.lastApplied {
			r.logger.Warn("SAFETY: truncation would discard applied entries",
				"truncateAt", truncateAt,
				"lastApplied", r.lastApplied,
				"commitIndex", r.commitIndex,
			)
		}

		if truncateAt > 0 {
			if err := r.storage.TruncateLog(truncateAt); err != nil {
				r.logger.Error("failed persisting truncation", "err", err)

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
			r.logger.Error("failed persisting entries", "err", err)

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

		r.commitIndex = min(req.LeaderCommit, lastNewIndex)

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
