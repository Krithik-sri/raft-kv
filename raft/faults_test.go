package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func submitN(t *testing.T, leader *Raft, prefix string, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		if _, _, ok := leader.Submit([]byte(fmt.Sprintf("%s-%d", prefix, i))); !ok {
			t.Fatalf("submit %s-%d rejected", prefix, i)
		}
	}
}

func waitForApplied(
	t *testing.T,
	machines []*recordingStateMachine,
	want int,
	timeout time.Duration,
) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := true
		for _, machine := range machines {
			if machine.count() != want {
				done = false
				break
			}
		}
		if done {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestClusterToleratesDroppedMessages(t *testing.T) {
	nodes, machines, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	network.setFaults(0.3, 0, 0)

	leader := waitForLeader(t, nodes, 15*time.Second)
	submitN(t, leader, "drop", 20)

	if !waitForApplied(t, machines, 20, 20*time.Second) {
		t.Fatalf("cluster did not converge under 30%% drops: %+v", network.snapshotStats())
	}
}

func TestFaultInjectorDropsEverything(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	network.setFaults(1.0, 0, 0)

	time.Sleep(2 * time.Second)

	if stats := network.snapshotStats(); stats.dropped == 0 {
		t.Fatal("a drop rate of 1.0 dropped nothing; the injector is not wired up")
	}

	for _, node := range nodes {
		if state, _ := node.getStateAndTerm(); state == Leader {
			t.Errorf("node=%s won an election with every message dropped", node.id)
		}
	}
}

func TestFaultInjectorDuplicatesEverything(t *testing.T) {
	nodes, machines, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)
	network.setFaults(0, 1.0, 0)

	submitN(t, leader, "dup", 20)

	if !waitForApplied(t, machines, 20, 20*time.Second) {
		t.Fatalf("cluster did not converge with every message duplicated: %+v",
			network.snapshotStats())
	}

	if stats := network.snapshotStats(); stats.duplicated == 0 {
		t.Fatal("a duplicate rate of 1.0 duplicated nothing")
	}
}

func TestClusterToleratesDelays(t *testing.T) {
	nodes, machines, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	network.setFaults(0, 0, 20*time.Millisecond)

	leader := waitForLeader(t, nodes, 15*time.Second)
	submitN(t, leader, "slow", 20)

	if !waitForApplied(t, machines, 20, 20*time.Second) {
		t.Fatalf("cluster did not converge under delays: %+v", network.snapshotStats())
	}
}

func TestClusterToleratesDuplicatedMessages(t *testing.T) {
	nodes, machines, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	network.setFaults(0, 0.5, 0)

	leader := waitForLeader(t, nodes, 15*time.Second)
	submitN(t, leader, "dup", 20)

	if !waitForApplied(t, machines, 20, 20*time.Second) {
		t.Fatalf("cluster did not converge under duplication: %+v", network.snapshotStats())
	}

	for i, machine := range machines {
		if got := machine.count(); got != 20 {
			t.Errorf("node=%s applied %d entries, want 20; duplicates were applied twice",
				nodes[i].id, got)
		}
	}
}

func TestPartitionedLeaderCannotCommit(t *testing.T) {
	nodes, machines, network, cancel := startClusterWithThreshold(t, 5, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)
	submitN(t, leader, "before", 5)

	if !waitForApplied(t, machines, 5, 15*time.Second) {
		t.Fatal("cluster did not converge before the partition")
	}

	minority := []NodeID{leader.id}
	var majority []NodeID

	for _, node := range nodes {
		if node == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, node.id)
			continue
		}
		majority = append(majority, node.id)
	}

	if len(majority) != 3 || len(minority) != 2 {
		t.Fatalf("bad split: majority=%v minority=%v", majority, minority)
	}

	network.partitionInto(majority, minority)

	ctx, timeout := context.WithTimeout(context.Background(), 2*time.Second)
	defer timeout()

	if _, err := leader.Propose(ctx, []byte("stranded")); err == nil {
		t.Error("a leader stranded in the minority committed a write")
	}

	survivor := waitForLeaderAmong(t, nodes, majority, 15*time.Second)
	submitN(t, survivor, "after", 5)

	if !waitForAppliedOn(t, machines, nodes, majority, 10, 15*time.Second) {
		t.Fatal("majority side could not make progress during the partition")
	}

	network.heal()

	if !waitForApplied(t, machines, 10, 20*time.Second) {
		t.Fatalf("cluster did not reconverge after healing: %+v", network.snapshotStats())
	}

	for i, machine := range machines {
		for _, command := range machine.snapshot() {
			if command == "stranded" {
				t.Errorf("node=%s applied the stranded write", nodes[i].id)
			}
		}
	}
}

func waitForAppliedOn(
	t *testing.T,
	machines []*recordingStateMachine,
	nodes []*Raft,
	ids []NodeID,
	want int,
	timeout time.Duration,
) bool {
	t.Helper()

	allowed := make(map[NodeID]bool, len(ids))
	for _, id := range ids {
		allowed[id] = true
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := true
		for i, node := range nodes {
			if !allowed[node.id] {
				continue
			}
			if machines[i].count() != want {
				done = false
				break
			}
		}
		if done {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func waitForLeaderAmong(
	t *testing.T,
	nodes []*Raft,
	ids []NodeID,
	timeout time.Duration,
) *Raft {
	t.Helper()

	allowed := make(map[NodeID]bool, len(ids))
	for _, id := range ids {
		allowed[id] = true
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if !allowed[node.id] {
				continue
			}
			if state, _ := node.getStateAndTerm(); state == Leader {
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("no leader emerged among the requested nodes")
	return nil
}
