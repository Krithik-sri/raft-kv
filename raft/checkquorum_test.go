package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitForState(t *testing.T, node *Raft, want State, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if state, _ := node.getStateAndTerm(); state == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// Before CheckQuorum, a cut-off leader sat there claiming the job forever.
// Nothing unsafe came of it once the read barrier landed, but every read sent
// its way had to wait for a timeout before failing.
func TestLeaderStepsDownWithoutQuorum(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)
	network.block(leader.id)

	if !waitForState(t, leader, Follower, 3*time.Second) {
		t.Fatal("a leader that could not reach anyone kept calling itself leader")
	}
}

// Stepping down must not invent a new term. There isn't one. This node simply
// isn't in charge any more, and bumping the term here would disrupt whatever
// the reachable side is doing.
func TestSteppingDownKeepsTheTerm(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)
	_, termBefore := leader.getStateAndTerm()

	network.block(leader.id)

	if !waitForState(t, leader, Follower, 3*time.Second) {
		t.Fatal("leader never stepped down")
	}

	if _, termAfter := leader.getStateAndTerm(); termAfter != termBefore {
		t.Errorf("term went from %d to %d while stepping down", termBefore, termAfter)
	}
}

// The payoff. A read aimed at a cut-off leader used to hang until the caller
// gave up. Now the node knows it is not the leader and says so.
func TestReadIndexFailsFastOnceLeaderStepsDown(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)
	network.block(leader.id)

	if !waitForState(t, leader, Follower, 3*time.Second) {
		t.Fatal("leader never stepped down")
	}

	// Ten seconds of rope. If ReadIndex needed anywhere near that, it is
	// waiting rather than answering.
	ctx, timeout := context.WithTimeout(context.Background(), 10*time.Second)
	defer timeout()

	began := time.Now()
	_, err := leader.ReadIndex(ctx)
	elapsed := time.Since(began)

	if !errors.Is(err, ErrNotLeader) {
		t.Errorf("ReadIndex = %v, want ErrNotLeader", err)
	}

	if elapsed > time.Second {
		t.Errorf("ReadIndex took %s to give up; it should know immediately", elapsed)
	}
}

// The other half of the story: a leader that can hear everyone must be left
// alone. A quorum check that is too eager is just a slower way of having no
// leader at all.
func TestHealthyLeaderIsLeftAlone(t *testing.T) {
	nodes, _, _, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)
	_, termBefore := leader.getStateAndTerm()

	// Several election timeouts of nothing going wrong.
	time.Sleep(2 * time.Second)

	state, termAfter := leader.getStateAndTerm()
	if state != Leader {
		t.Errorf("a healthy leader stepped down on its own")
	}
	if termAfter != termBefore {
		t.Errorf("term climbed from %d to %d with no faults at all", termBefore, termAfter)
	}
}

// A minority that loses contact should also stand down, but a leader on the
// majority side of a split has no reason to.
func TestLeaderOnTheMajoritySideStays(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 5, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	majority := []NodeID{leader.id}
	var minority []NodeID

	for _, node := range nodes {
		if node == leader {
			continue
		}
		if len(majority) < 3 {
			majority = append(majority, node.id)
			continue
		}
		minority = append(minority, node.id)
	}

	network.partitionInto(majority, minority)

	_, termBefore := leader.getStateAndTerm()
	time.Sleep(2 * time.Second)

	state, termAfter := leader.getStateAndTerm()
	if state != Leader {
		t.Error("a leader with three of five nodes stepped down anyway")
	}
	if termAfter != termBefore {
		t.Errorf("term climbed from %d to %d on the healthy side", termBefore, termAfter)
	}
}
