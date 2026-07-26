package raft

import (
	"testing"
	"time"
)

func TestIsolatedNodeDoesNotDisruptOnRejoin(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)
	termBefore := leader.Status().Term

	var victim *Raft
	for _, node := range nodes {
		if node != leader {
			victim = node
			break
		}
	}

	network.block(victim.id)
	time.Sleep(2 * time.Second)

	isolatedTerm := victim.Status().Term
	if isolatedTerm != termBefore {
		t.Errorf("isolated node inflated its term from %d to %d; pre-vote should keep it pinned",
			termBefore, isolatedTerm)
	}

	network.unblock(victim.id)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if victim.Status().LeaderID == leader.id && leader.Status().State == Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader.Status().State != Leader {
		t.Fatalf("leader was deposed by the rejoining node, term is now %d",
			leader.Status().Term)
	}

	if got := leader.Status().Term; got != termBefore {
		t.Errorf("term rose from %d to %d after a rejoin; no election should have happened",
			termBefore, got)
	}

	if _, _, ok := leader.Submit([]byte("still-writable")); !ok {
		t.Error("leader could not accept a write after the rejoin")
	}
}

func TestPreVoteDeniedWhileLeaderIsHealthy(t *testing.T) {
	r := newTrimmedRaft(0, 0, []uint64{1})
	r.lastHeard = time.Now()

	resp := r.handlePreVote(RequestVoteRequest{
		Term:         99,
		CandidateID:  "n2",
		LastLogIndex: 99,
		LastLogTerm:  99,
		PreVote:      true,
	})

	if resp.VoteGranted {
		t.Error("pre-vote granted despite having just heard from a leader")
	}
}

func TestPreVoteGrantedWhenLeaderIsSilent(t *testing.T) {
	r := newTrimmedRaft(0, 0, []uint64{1})
	r.lastHeard = time.Now().Add(-time.Second)

	resp := r.handlePreVote(RequestVoteRequest{
		Term:         2,
		CandidateID:  "n2",
		LastLogIndex: 1,
		LastLogTerm:  1,
		PreVote:      true,
	})

	if !resp.VoteGranted {
		t.Error("pre-vote denied despite a silent leader and an up-to-date log")
	}
}

func TestPreVoteDoesNotMutateState(t *testing.T) {
	r := newTrimmedRaft(0, 0, []uint64{1})
	r.currentTerm = 5
	r.votedFor = "n3"
	r.lastHeard = time.Now().Add(-time.Second)

	r.handlePreVote(RequestVoteRequest{
		Term:         500,
		CandidateID:  "n2",
		LastLogIndex: 500,
		LastLogTerm:  500,
		PreVote:      true,
	})

	if r.currentTerm != 5 {
		t.Errorf("currentTerm = %d, want 5; pre-vote must not adopt the proposed term", r.currentTerm)
	}
	if r.votedFor != "n3" {
		t.Errorf("votedFor = %q, want \"n3\"; pre-vote must not record a vote", r.votedFor)
	}
}

func TestPreVoteDeniedForStaleLog(t *testing.T) {
	r := newTrimmedRaft(0, 0, []uint64{1, 1, 1})
	r.lastHeard = time.Now().Add(-time.Second)

	resp := r.handlePreVote(RequestVoteRequest{
		Term:         2,
		CandidateID:  "n2",
		LastLogIndex: 1,
		LastLogTerm:  1,
		PreVote:      true,
	})

	if resp.VoteGranted {
		t.Error("pre-vote granted to a candidate with a shorter log")
	}
}

func TestClusterStillElectsAfterLeaderLoss(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)
	network.block(leader.id)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, node := range nodes {
			if node == leader {
				continue
			}
			if state, _ := node.getStateAndTerm(); state == Leader {
				count++
			}
		}
		if count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, node := range nodes {
		t.Logf("node=%s %+v", node.id, node.Status())
	}
	t.Fatal("pre-vote blocked a legitimate election after the leader went away")
}
