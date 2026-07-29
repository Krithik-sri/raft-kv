package raft

import (
	"context"
	"slices"
	"time"
)

func (r *Raft) needsSnapshot(peerID NodeID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.snapshotIndex > 0 && r.nextIndex[peerID] <= r.snapshotIndex
}

func (r *Raft) sendSnapshot(ctx context.Context, peer Peer, term uint64) {
	barrier := r.currentBarrier()

	r.mu.Lock()

	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}

	data, ok, err := r.storage.LoadSnapshot()
	if err != nil {
		r.mu.Unlock()
		r.logger.Error("reading snapshot failed", "err", err)
		return
	}
	if !ok {
		r.mu.Unlock()
		r.logger.Warn("peer needs a snapshot but none is stored", "peer", peer.ID)
		return
	}

	req := InstallSnapshotRequest{
		Term:              r.currentTerm,
		LeaderID:          r.id,
		LastIncludedIndex: r.snapshotIndex,
		LastIncludedTerm:  r.snapshotTerm,
		Data:              data,
	}

	r.mu.Unlock()

	r.logger.Debug("sending snapshot", "peer", peer.ID, "index", req.LastIncludedIndex)

	callCtx, cancel := context.WithTimeout(ctx, snapshotSendTimeout)
	resp, err := r.transport.InstallSnapshot(callCtx, peer, req)
	cancel()
	if err != nil {
		r.logger.Debug("snapshot install failed", "peer", peer.ID, "err", err)
		return
	}

	if resp.Term > term {
		r.becomeFollower(resp.Term)
		return
	}

	if !r.isLeaderForTerm(term) {
		return
	}

	r.advancePeerProgress(peer.ID, req.LastIncludedIndex, term, barrier)
}

func (r *Raft) HandleInstallSnapshot(
	req InstallSnapshotRequest,
) InstallSnapshotResponse {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	r.mu.Lock()

	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()

		return InstallSnapshotResponse{Term: term}
	}

	if req.Term > r.currentTerm {
		if err := r.storage.SaveState(req.Term, ""); err != nil {
			r.logger.Error("failed persisting term", "err", err)

			term := r.currentTerm
			r.mu.Unlock()

			return InstallSnapshotResponse{Term: term}
		}

		r.currentTerm = req.Term
		r.votedFor = ""
	}

	r.state = Follower
	r.leaderID = req.LeaderID
	r.lastHeard = time.Now()

	term := r.currentTerm

	if req.LastIncludedIndex <= r.snapshotIndex {
		r.mu.Unlock()
		r.resetElectionTimer()

		return InstallSnapshotResponse{Term: term}
	}

	defer r.mu.Unlock()

	var retained []LogEntry
	if existing, ok := r.termAtLocked(req.LastIncludedIndex); ok &&
		existing == req.LastIncludedTerm {
		offset, _ := r.offsetLocked(req.LastIncludedIndex)
		retained = slices.Clone(r.log[offset+1:])
	}

	if err := r.stateMachine.Restore(req.Data); err != nil {
		r.logger.Error("restoring snapshot failed", "err", err)
		return InstallSnapshotResponse{Term: term}
	}

	err := r.storage.SaveSnapshot(
		Snapshot{
			Index: req.LastIncludedIndex,
			Term:  req.LastIncludedTerm,
			Data:  req.Data,
		},
		r.currentTerm,
		r.votedFor,
		retained,
	)
	if err != nil {
		r.logger.Error("persisting snapshot failed", "err", err)
		return InstallSnapshotResponse{Term: term}
	}

	r.log = append([]LogEntry{{Term: req.LastIncludedTerm}}, retained...)
	r.snapshotIndex = req.LastIncludedIndex
	r.snapshotTerm = req.LastIncludedTerm

	r.commitIndex = max(r.commitIndex, req.LastIncludedIndex)
	r.lastApplied = max(r.lastApplied, req.LastIncludedIndex)

	if r.commitIndex > r.lastApplied {
		r.signalApply()
	}

	r.logger.Info("installed snapshot",
		"index", req.LastIncludedIndex,
		"term", req.LastIncludedTerm,
		"retained", len(retained),
	)

	r.resetElectionTimer()

	return InstallSnapshotResponse{Term: term}
}
