package raft

import (
	"context"
	"fmt"
	"time"
)

func (r *Raft) needsSnapshot(peerID NodeID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.snapshotIndex > 0 && r.nextIndex[peerID] <= r.snapshotIndex
}

func (r *Raft) sendSnapshot(ctx context.Context, peer Peer, term uint64) {
	r.mu.Lock()

	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}

	data, ok, err := r.storage.LoadSnapshot()
	if err != nil {
		r.mu.Unlock()
		fmt.Printf("node=%s reading snapshot failed: %v\n", r.id, err)
		return
	}
	if !ok {
		r.mu.Unlock()
		fmt.Printf(
			"node=%s peer=%s needs a snapshot but none is stored\n",
			r.id,
			peer.ID,
		)
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

	fmt.Printf(
		"node=%s sending snapshot peer=%s index=%d\n",
		r.id,
		peer.ID,
		req.LastIncludedIndex,
	)

	callCtx, cancel := context.WithTimeout(ctx, snapshotSendTimeout)
	resp, err := r.transport.InstallSnapshot(callCtx, peer, req)
	cancel()
	if err != nil {
		fmt.Printf("failed installing snapshot on %s: %v\n", peer.ID, err)
		return
	}

	if resp.Term > term {
		r.becomeFollower(resp.Term)
		return
	}

	if !r.isLeaderForTerm(term) {
		return
	}

	r.advancePeerProgress(peer.ID, req.LastIncludedIndex, term)
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
			fmt.Printf("node=%s failed persisting term: %v\n", r.id, err)

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
		retained = make([]LogEntry, len(r.log)-offset-1)
		copy(retained, r.log[offset+1:])
	}

	if err := r.stateMachine.Restore(req.Data); err != nil {
		fmt.Printf("node=%s restoring snapshot failed: %v\n", r.id, err)
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
		fmt.Printf("node=%s persisting snapshot failed: %v\n", r.id, err)
		return InstallSnapshotResponse{Term: term}
	}

	r.log = append([]LogEntry{{Term: req.LastIncludedTerm}}, retained...)
	r.snapshotIndex = req.LastIncludedIndex
	r.snapshotTerm = req.LastIncludedTerm

	if r.commitIndex < req.LastIncludedIndex {
		r.commitIndex = req.LastIncludedIndex
	}
	if r.lastApplied < req.LastIncludedIndex {
		r.lastApplied = req.LastIncludedIndex
	}

	if r.commitIndex > r.lastApplied {
		r.signalApply()
	}

	fmt.Printf(
		"node=%s installed snapshot index=%d term=%d retained=%d\n",
		r.id,
		req.LastIncludedIndex,
		req.LastIncludedTerm,
		len(retained),
	)

	r.resetElectionTimer()

	return InstallSnapshotResponse{Term: term}
}
