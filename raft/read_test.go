package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadIndexOnFollowerIsRejected(t *testing.T) {
	nodes, _, _, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	var follower *Raft
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}

	ctx, timeout := context.WithTimeout(context.Background(), 2*time.Second)
	defer timeout()

	if _, err := follower.ReadIndex(ctx); !errors.Is(err, ErrNotLeader) {
		t.Errorf("ReadIndex on a follower = %v, want ErrNotLeader", err)
	}
}

func TestReadIndexCoversCommittedWrites(t *testing.T) {
	nodes, machines, _, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, timeout := context.WithTimeout(context.Background(), 15*time.Second)
	defer timeout()

	if _, err := leader.Propose(ctx, []byte("written")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	index, err := leader.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if err := leader.WaitForApplied(ctx, index); err != nil {
		t.Fatalf("WaitForApplied: %v", err)
	}

	// Whatever index the barrier handed back, the write that already returned
	// must be visible in the state machine by now.
	var applied *recordingStateMachine
	for i, node := range nodes {
		if node == leader {
			applied = machines[i]
		}
	}

	found := false
	for _, command := range applied.snapshot() {
		if command == "written" {
			found = true
		}
	}

	if !found {
		t.Errorf("read index %d was applied but the committed write is missing", index)
	}
}

// This is the whole point of ReadIndex. A leader that has been cut off still
// believes it is in charge. Before, it would answer reads from its own stale
// map. Now it cannot produce a read index at all.
func TestReadIndexFailsOnPartitionedLeader(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, timeout := context.WithTimeout(context.Background(), 15*time.Second)
	defer timeout()

	if _, err := leader.Propose(ctx, []byte("before")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	network.block(leader.id)

	// It still thinks it is the leader, which is exactly the dangerous case.
	if state, _ := leader.getStateAndTerm(); state != Leader {
		t.Skip("leader stepped down before we could test the cut-off case")
	}

	readCtx, readTimeout := context.WithTimeout(context.Background(), 2*time.Second)
	defer readTimeout()

	if _, err := leader.ReadIndex(readCtx); err == nil {
		t.Fatal("a cut-off leader handed out a read index")
	}
}

func TestReadIndexRecoversAfterHealing(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	network.block(leader.id)

	readCtx, readTimeout := context.WithTimeout(context.Background(), 1*time.Second)
	if _, err := leader.ReadIndex(readCtx); err == nil {
		readTimeout()
		t.Fatal("a cut-off leader handed out a read index")
	}
	readTimeout()

	network.heal()

	// Somebody is leading again, maybe the same node, maybe not. Reads work.
	settled := waitForLeader(t, nodes, 15*time.Second)

	ctx, timeout := context.WithTimeout(context.Background(), 15*time.Second)
	defer timeout()

	if _, err := settled.ReadIndex(ctx); err != nil {
		t.Errorf("ReadIndex after healing: %v", err)
	}
}

func TestWaitForAppliedReturnsWhenCaughtUp(t *testing.T) {
	nodes, _, _, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, timeout := context.WithTimeout(context.Background(), 15*time.Second)
	defer timeout()

	for i := 0; i < 10; i++ {
		if _, err := leader.Propose(ctx, []byte("x")); err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	index, err := leader.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if err := leader.WaitForApplied(ctx, index); err != nil {
		t.Fatalf("WaitForApplied: %v", err)
	}

	if got := leader.Status().LastApplied; got < index {
		t.Errorf("lastApplied = %d after waiting for %d", got, index)
	}
}
