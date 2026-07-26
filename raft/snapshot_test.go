package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSnapshotTrimsLogAndKeepsBoundary(t *testing.T) {
	nodes, machines, _, cancel := startClusterWithThreshold(t, 3, 10)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	const commands = 40
	for i := 0; i < commands; i++ {
		if _, _, ok := leader.Submit([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("submit %d rejected", i)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, node := range nodes {
			if node.Status().SnapshotIndex == 0 {
				done = false
				break
			}
		}
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i, node := range nodes {
		status := node.Status()

		if status.SnapshotIndex == 0 {
			t.Errorf("node=%s never snapshotted", status.ID)
			continue
		}

		if status.LogLength > 20 {
			t.Errorf("node=%s log still holds %d entries, want it trimmed",
				status.ID, status.LogLength)
		}

		if uint64(status.LogLength)+status.SnapshotIndex-1 != status.LastLogIndex {
			t.Errorf("node=%s log length %d and snapshot %d disagree with lastLogIndex %d",
				status.ID, status.LogLength, status.SnapshotIndex, status.LastLogIndex)
		}

		if got := machines[i].count(); got != commands {
			t.Errorf("node=%s applied %d commands, want %d", status.ID, got, commands)
		}
	}
}

func TestReplicationContinuesAfterSnapshot(t *testing.T) {
	nodes, machines, _, cancel := startClusterWithThreshold(t, 3, 10)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	for i := 0; i < 30; i++ {
		if _, _, ok := leader.Submit([]byte(fmt.Sprintf("first-%d", i))); !ok {
			t.Fatalf("submit %d rejected", i)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Status().SnapshotIndex > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader.Status().SnapshotIndex == 0 {
		t.Fatal("leader never snapshotted")
	}

	ctx, timeout := context.WithTimeout(context.Background(), 10*time.Second)
	defer timeout()

	if _, err := leader.Propose(ctx, []byte("after-snapshot")); err != nil {
		t.Fatalf("Propose after snapshot: %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, machine := range machines {
			if machine.count() != 31 {
				done = false
				break
			}
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i, machine := range machines {
		t.Errorf("node=%s applied %d, want 31", nodes[i].id, machine.count())
	}
}

func TestSnapshotBoundaryTermIsPreserved(t *testing.T) {
	r := newTrimmedRaft(0, 0, []uint64{1, 1, 1, 1, 1})
	r.snapshotThreshold = 3
	r.currentTerm = 1
	r.commitIndex = 5
	r.lastApplied = 5

	r.maybeSnapshot()

	if r.snapshotIndex != 5 {
		t.Fatalf("snapshotIndex = %d, want 5", r.snapshotIndex)
	}
	if r.snapshotTerm != 1 {
		t.Errorf("snapshotTerm = %d, want 1", r.snapshotTerm)
	}
	if len(r.log) != 1 {
		t.Fatalf("log length = %d, want 1 (boundary entry only)", len(r.log))
	}
	if r.log[0].Term != 1 {
		t.Errorf("boundary entry term = %d, want 1", r.log[0].Term)
	}
	if got := r.lastLogIndexLocked(); got != 5 {
		t.Errorf("lastLogIndex = %d, want 5", got)
	}

	term, ok := r.termAtLocked(5)
	if !ok || term != 1 {
		t.Errorf("termAt(5) = (%d,%v), want (1,true)", term, ok)
	}

	if _, ok := r.termAtLocked(4); ok {
		t.Error("termAt(4) should be unavailable after compaction")
	}
}

func TestSnapshotRetainsUncommittedTail(t *testing.T) {
	r := newTrimmedRaft(0, 0, []uint64{1, 1, 1, 1, 1})
	r.snapshotThreshold = 2
	r.currentTerm = 1
	r.commitIndex = 3
	r.lastApplied = 3

	r.maybeSnapshot()

	if r.snapshotIndex != 3 {
		t.Fatalf("snapshotIndex = %d, want 3", r.snapshotIndex)
	}
	if len(r.log) != 3 {
		t.Fatalf("log length = %d, want 3 (boundary + entries 4 and 5)", len(r.log))
	}
	if got := r.lastLogIndexLocked(); got != 5 {
		t.Errorf("lastLogIndex = %d, want 5", got)
	}
}

func TestLaggingFollowerReceivesSnapshot(t *testing.T) {
	nodes, machines, network, cancel := startClusterWithThreshold(t, 3, 10)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	var (
		follower      *Raft
		followerIndex int
	)
	for i, node := range nodes {
		if node != leader {
			follower, followerIndex = node, i
			break
		}
	}

	network.block(follower.id)

	const commands = 40
	for i := 0; i < commands; i++ {
		if _, _, ok := leader.Submit([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("submit %d rejected", i)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Status().SnapshotIndex > follower.Status().LastLogIndex {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader.Status().SnapshotIndex <= follower.Status().LastLogIndex {
		t.Fatalf("leader snapshot %d never passed the follower's log end %d",
			leader.Status().SnapshotIndex, follower.Status().LastLogIndex)
	}

	before := network.installCount()
	network.unblock(follower.id)

	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if network.installCount() > before && machines[followerIndex].count() == commands {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Errorf("installs %d (was %d), follower applied %d of %d, lastLogIndex %d (leader %d)",
		network.installCount(), before,
		machines[followerIndex].count(), commands,
		follower.Status().LastLogIndex, leader.Status().LastLogIndex)
}

func TestNeedsSnapshotOnlyBelowBoundary(t *testing.T) {
	r := newTrimmedRaft(50, 3, []uint64{3, 3})

	tests := []struct {
		nextIndex uint64
		want      bool
	}{
		{nextIndex: 1, want: true},
		{nextIndex: 50, want: true},
		{nextIndex: 51, want: false},
		{nextIndex: 52, want: false},
	}

	for _, tt := range tests {
		r.nextIndex["n2"] = tt.nextIndex

		if got := r.needsSnapshot("n2"); got != tt.want {
			t.Errorf("needsSnapshot(nextIndex=%d) = %v, want %v",
				tt.nextIndex, got, tt.want)
		}
	}
}
