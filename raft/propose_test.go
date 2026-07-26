package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProposeReturnsApplyResult(t *testing.T) {
	nodes, _, cancel := startCluster(t, 3)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()

	result, err := leader.Propose(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	if string(result) != "ok:hello" {
		t.Errorf("result = %q, want %q", result, "ok:hello")
	}
}

func TestProposeWaitsForCommit(t *testing.T) {
	nodes, machines, cancel := startCluster(t, 3)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	var leaderMachine *recordingStateMachine
	for i, node := range nodes {
		if node == leader {
			leaderMachine = machines[i]
		}
	}

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()

	if _, err := leader.Propose(ctx, []byte("committed")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	applied := leaderMachine.snapshot()
	if len(applied) != 1 || applied[0] != "committed" {
		t.Errorf("state machine saw %v, want [committed] by the time Propose returned", applied)
	}
}

func TestProposeOnFollowerIsRejected(t *testing.T) {
	nodes, _, cancel := startCluster(t, 3)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	var follower *Raft
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()

	if _, err := follower.Propose(ctx, []byte("nope")); !errors.Is(err, ErrNotLeader) {
		t.Errorf("Propose on follower = %v, want ErrNotLeader", err)
	}
}

func TestProposeHonoursContextCancellation(t *testing.T) {
	machine := &recordingStateMachine{}
	r, err := New("n1", []Peer{{ID: "n2"}, {ID: "n3"}}, nil, machine, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r.state = Leader
	r.currentTerm = 1

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := r.Propose(ctx, []byte("stuck")); !errors.Is(err, context.Canceled) {
		t.Errorf("Propose with cancelled context = %v, want context.Canceled", err)
	}

	r.mu.Lock()
	remaining := len(r.waiters)
	r.mu.Unlock()

	if remaining != 0 {
		t.Errorf("%d waiters leaked after cancellation, want 0", remaining)
	}
}

func TestLeaderHint(t *testing.T) {
	nodes, _, cancel := startCluster(t, 3)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	id, isLeader := leader.LeaderHint()
	if !isLeader || id != leader.id {
		t.Errorf("leader reports (%q,%v), want (%q,true)", id, isLeader, leader.id)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allPoint := true
		for _, node := range nodes {
			hint, self := node.LeaderHint()
			if self {
				continue
			}
			if hint != leader.id {
				allPoint = false
				break
			}
		}
		if allPoint {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, node := range nodes {
		hint, self := node.LeaderHint()
		t.Errorf("node=%s hint=%q isLeader=%v, want hint=%q", node.id, hint, self, leader.id)
	}
}
