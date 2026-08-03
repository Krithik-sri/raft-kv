package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func joinNode(
	t *testing.T,
	ctx context.Context,
	network *memNetwork,
	id NodeID,
	existing []NodeID,
	threshold uint64,
) (*Raft, *recordingStateMachine) {
	t.Helper()

	var peers []Peer
	for _, other := range existing {
		peers = append(peers, Peer{ID: other, Address: string(other)})
	}

	machine := &recordingStateMachine{}

	node, err := New(
		Peer{ID: id, Address: string(id)},
		peers,
		&memTransport{self: id, net: network},
		machine,
		&memStorage{},
	)
	if err != nil {
		t.Fatalf("New %s: %v", id, err)
	}

	node.snapshotThreshold = threshold
	network.register(id, node)

	go node.Start(ctx)

	return node, machine
}

func waitForMembers(node *Raft, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(node.Configuration().Members) == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func waitForCount(machine *recordingStateMachine, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if machine.count() >= want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestAddServerGrowsTheCluster(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()

	_, joinerMachine := joinNode(t, ctx, network, "n4", []NodeID{"n1", "n2", "n3"}, 1<<40)

	if err := leader.AddServer(ctx, Peer{ID: "n4", Address: "n4"}); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	config := leader.Configuration()
	if len(config.Members) != 4 {
		t.Fatalf("config has %d members, want 4", len(config.Members))
	}
	if !config.isVoter("n4") {
		t.Error("n4 was added but never promoted to a voter")
	}

	if _, err := leader.Propose(ctx, []byte("written after n4 joined")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	if !waitForCount(joinerMachine, 1, 10*time.Second) {
		t.Error("the new machine never applied anything")
	}
}

// The point of the learner step. A machine that cannot be reached must not be
// handed a vote, and the cluster it is joining must carry on working while it
// fails to catch up.
func TestUnreachableJoinerStaysALearner(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()

	joinNode(t, ctx, network, "n4", []NodeID{"n1", "n2", "n3"}, 1<<40)
	network.block("n4")

	addCtx, cancelAdd := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelAdd()

	err := leader.AddServer(addCtx, Peer{ID: "n4", Address: "n4"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AddServer = %v, want it to give up waiting for the catch-up", err)
	}

	config := leader.Configuration()
	if !config.contains("n4") {
		t.Fatal("n4 was never added as a learner")
	}
	if config.isVoter("n4") {
		t.Error("a machine that never answered a single message was given a vote")
	}
	if got := config.voters(); got != 3 {
		t.Errorf("voters = %d, want 3: the learner changed the majority", got)
	}

	// The cluster has to still work. This is the whole reason for the learner
	// step rather than adding a voter straight away.
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWrite()

	if _, err := leader.Propose(writeCtx, []byte("still working")); err != nil {
		t.Errorf("cluster stopped accepting writes while a learner lagged: %v", err)
	}
}

func TestRemoveServerShrinksTheCluster(t *testing.T) {
	nodes, _, _, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	var victim NodeID
	for _, node := range nodes {
		if node != leader {
			victim = node.id
			break
		}
	}

	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()

	if err := leader.RemoveServer(ctx, victim); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	config := leader.Configuration()
	if len(config.Members) != 2 {
		t.Fatalf("config has %d members, want 2", len(config.Members))
	}
	if config.contains(victim) {
		t.Errorf("%s is still in the configuration", victim)
	}

	if _, err := leader.Propose(ctx, []byte("after shrinking")); err != nil {
		t.Errorf("cluster stopped accepting writes after a removal: %v", err)
	}
}

// A leader is allowed to remove itself. It runs the cluster until the new
// configuration commits and then stands down, because at that point it is not a
// member of anything.
func TestLeaderRemovingItselfStepsDown(t *testing.T) {
	nodes, _, _, cancel := startClusterWithThreshold(t, 3, 1<<40)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()

	if err := leader.RemoveServer(ctx, leader.id); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	if !waitForState(t, leader, Follower, 5*time.Second) {
		t.Fatal("a leader that removed itself carried on being the leader")
	}

	var remaining []*Raft
	for _, node := range nodes {
		if node != leader {
			remaining = append(remaining, node)
		}
	}

	next := waitForLeader(t, remaining, 15*time.Second)
	if next.id == leader.id {
		t.Fatal("the removed machine came back as leader")
	}
}

// Two changes in flight at once and you cannot say which majority you need,
// because you do not yet know which configuration is real.
func TestOnlyOneConfigChangeAtATime(t *testing.T) {
	r := newTestLeader([]uint64{0, 1}, 1)
	r.commitIndex = 1
	r.configIndex = 5

	_, _, err := r.beginConfigChange(func(c Configuration) (Configuration, error) {
		return c, nil
	})

	if !errors.Is(err, ErrConfigChangeInFlight) {
		t.Errorf("err = %v, want ErrConfigChangeInFlight", err)
	}
}

// The rule the original one-at-a-time write-up left out. A leader that has not
// committed anything of its own does not reliably know where the commit point
// is, and must not start reshaping the cluster from there.
func TestConfigChangeNeedsAnEntryFromTheLeadersTerm(t *testing.T) {
	r := newTestLeader([]uint64{0, 1}, 2)
	r.commitIndex = 1

	_, _, err := r.beginConfigChange(func(c Configuration) (Configuration, error) {
		return c, nil
	})

	if !errors.Is(err, ErrLeaderNotCaughtUp) {
		t.Errorf("err = %v, want ErrLeaderNotCaughtUp", err)
	}
}

func TestCannotRemoveTheLastVoter(t *testing.T) {
	r := newTestLeader([]uint64{0, 1}, 1)
	r.commitIndex = 1
	r.adoptConfigLocked(Configuration{Members: []Member{{ID: "n1", Voter: true}}}, 0)

	err := r.RemoveServer(context.Background(), "n1")
	if !errors.Is(err, ErrLastVoter) {
		t.Errorf("err = %v, want ErrLastVoter", err)
	}
}

func TestConfigChangeOnAFollowerIsRefused(t *testing.T) {
	r, err := New(Peer{ID: "n1"}, []Peer{{ID: "n2"}}, nil, &recordingStateMachine{}, &memStorage{})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.AddServer(context.Background(), Peer{ID: "n3"}); !errors.Is(err, ErrNotLeader) {
		t.Errorf("AddServer on a follower = %v, want ErrNotLeader", err)
	}
	if err := r.RemoveServer(context.Background(), "n2"); !errors.Is(err, ErrNotLeader) {
		t.Errorf("RemoveServer on a follower = %v, want ErrNotLeader", err)
	}
}

// A learner has no vote, so it has no business starting an election. Same goes
// for a machine that has been removed and has not noticed yet.
func TestNonVotersDoNotCampaign(t *testing.T) {
	r, err := New(Peer{ID: "n1"}, []Peer{{ID: "n2"}}, nil, &recordingStateMachine{}, &memStorage{})
	if err != nil {
		t.Fatal(err)
	}

	r.adoptConfigLocked(Configuration{Members: []Member{
		{ID: "n1", Voter: false},
		{ID: "n2", Voter: true},
	}}, 1)

	before := r.currentTerm

	// The transport is nil. Getting through this without a panic is itself the
	// assertion that no election was attempted.
	r.campaign(context.Background())

	if r.currentTerm != before {
		t.Errorf("term moved from %d to %d: a non-voter held an election", before, r.currentTerm)
	}
	if r.state != Follower {
		t.Errorf("state = %v, want Follower", r.state)
	}
}

// The one that ties the whole repo together: join a cluster that has already
// thrown away the log the new machine needs, so the only way in is a snapshot.
func TestNewMemberCatchesUpThroughSnapshot(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 5)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()

	const writes = 25
	for i := range writes {
		if _, err := leader.Propose(ctx, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	leader.mu.Lock()
	compactedAt := leader.snapshotIndex
	leader.mu.Unlock()

	if compactedAt == 0 {
		t.Fatal("the log never compacted, so this test proves nothing")
	}

	before := network.installCount()

	// Point it at one machine and nothing else, the way you would in real life.
	// The rest of the membership it learns from the configuration entries that
	// admit it, which land after the snapshot and arrive the ordinary way.
	joiner, joinerMachine := joinNode(t, ctx, network, "n4", []NodeID{leader.id}, 5)

	if err := leader.AddServer(ctx, Peer{ID: "n4", Address: "n4"}); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	if network.installCount() <= before {
		t.Error("the new machine caught up without a snapshot transfer")
	}

	if !waitForCount(joinerMachine, writes, 20*time.Second) {
		t.Errorf("new machine applied %d of %d writes", joinerMachine.count(), writes)
	}

	if !leader.Configuration().isVoter("n4") {
		t.Error("n4 never became a voter")
	}

	if !waitForMembers(joiner, 4, 20*time.Second) {
		t.Errorf(
			"joiner sees %d members, want 4: it never learned who the cluster is",
			len(joiner.Configuration().Members),
		)
	}
}

// The case the snapshot has to carry the membership for.
//
// A machine is admitted, the cluster writes enough to compact past the entry
// that admitted it, and then that machine is replaced by a fresh one with empty
// storage. The only record of who the cluster is now lives inside the snapshot.
// Replay the log all you like: the entry is gone.
func TestReplacedMachineLearnsMembershipFromSnapshot(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 5)
	defer cancel()

	leader := waitForLeader(t, nodes, 15*time.Second)

	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()

	firstCtx, stopFirst := context.WithCancel(ctx)
	joinNode(t, firstCtx, network, "n4", []NodeID{"n1", "n2", "n3"}, 5)

	if err := leader.AddServer(ctx, Peer{ID: "n4", Address: "n4"}); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	leader.mu.Lock()
	admittedAt := leader.configIndex
	leader.mu.Unlock()

	for i := range 40 {
		if _, err := leader.Propose(ctx, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	leader.mu.Lock()
	compactedAt := leader.snapshotIndex
	leader.mu.Unlock()

	if compactedAt < admittedAt {
		t.Fatalf(
			"snapshot at %d never buried the configuration entry at %d, so this test proves nothing",
			compactedAt, admittedAt,
		)
	}

	// Same machine, fresh disk, and it only knows one address to call.
	stopFirst()
	replacement, _ := joinNode(t, ctx, network, "n4", []NodeID{leader.id}, 5)

	if !waitForMembers(replacement, 4, 20*time.Second) {
		t.Errorf(
			"replacement sees %d members, want 4: the snapshot never told it who the cluster is",
			len(replacement.Configuration().Members),
		)
	}
}

// Membership churn while the cluster is busy. Everything else in this file
// changes the configuration on a quiet cluster, which is the easy case: nothing
// is reading the membership at the moment it changes.
//
// This is also the only test that puts a writer and a configuration change in
// the same instant, which is what the race detector needs in order to have an
// opinion about any of this.
func TestMembershipChurnUnderLoad(t *testing.T) {
	nodes, _, network, cancel := startClusterWithThreshold(t, 3, 20)
	defer cancel()

	waitForLeader(t, nodes, 15*time.Second)

	ctx, stop := context.WithTimeout(context.Background(), 90*time.Second)
	defer stop()

	joinNode(t, ctx, network, "n4", []NodeID{"n1", "n2", "n3"}, 20)

	writes := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := 0; ; i++ {
			select {
			case <-writes:
				return
			default:
			}

			leader := currentLeader(nodes)
			if leader == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			callCtx, cancelCall := context.WithTimeout(ctx, 2*time.Second)
			// A write can legitimately fail mid-handover. We are looking for
			// races and corruption here, not for a perfect success rate.
			_, _ = leader.Propose(callCtx, []byte(fmt.Sprintf("churn-%d", i)))
			cancelCall()
		}
	}()

	for round := range 3 {
		leader := waitForLeader(t, nodes, 20*time.Second)

		if err := leader.AddServer(ctx, Peer{ID: "n4", Address: "n4"}); err != nil {
			close(writes)
			<-done
			t.Fatalf("round %d AddServer: %v", round, err)
		}

		leader = waitForLeader(t, nodes, 20*time.Second)

		if err := leader.RemoveServer(ctx, "n4"); err != nil {
			close(writes)
			<-done
			t.Fatalf("round %d RemoveServer: %v", round, err)
		}
	}

	close(writes)
	<-done

	leader := waitForLeader(t, nodes, 20*time.Second)

	config := leader.Configuration()
	if len(config.Members) != 3 {
		t.Errorf("ended with %d members, want 3: %+v", len(config.Members), config.Members)
	}
	if config.contains("n4") {
		t.Error("n4 is still a member after being removed on the last round")
	}
}
