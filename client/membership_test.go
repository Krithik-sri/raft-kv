package client

import (
	"context"
	"testing"
	"time"
)

func memberByID(members []Member, id string) (Member, bool) {
	for _, m := range members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// The whole feature, over real gRPC, through the same client anyone else would
// use: grow a running cluster, check the new machine has the data, then shrink
// it again.
func TestGrowAndShrinkARunningCluster(t *testing.T) {
	c := startCluster(t, 3)
	kv := newTestClient(t, c.Addresses)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := kv.Put(ctx, "before", "joining"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	joined, err := c.Join("n4", t.TempDir())
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	// A machine that has not been admitted yet must not be counted or start
	// campaigning. It sits there as a learner until somebody asks for it.
	for _, m := range joined.Raft.Configuration().Members {
		if m.ID == "n4" && m.Voter {
			t.Error("a joining machine started up believing it had a vote")
		}
	}

	if err := kv.AddServer(ctx, "n4", joined.Address); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	members, leaderID, err := kv.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(members) != 4 {
		t.Fatalf("cluster reports %d members, want 4: %+v", len(members), members)
	}
	if leaderID == "" {
		t.Error("nobody reported a leader")
	}

	n4, ok := memberByID(members, "n4")
	if !ok {
		t.Fatal("n4 is missing from the membership list")
	}
	if !n4.Voter {
		t.Error("n4 joined but was never promoted to a voter")
	}

	// The new machine has to have the write that happened before it existed.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if value, found := joined.Store.Get("before"); found && value == "joining" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if value, found := joined.Store.Get("before"); !found || value != "joining" {
		t.Errorf("new machine has before=%q found=%v, want \"joining\" true", value, found)
	}

	// And the cluster still works at its new size.
	if err := kv.Put(ctx, "after", "growing"); err != nil {
		t.Fatalf("Put after growing: %v", err)
	}

	if err := kv.RemoveServer(ctx, "n4"); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	members, _, err = kv.Members(ctx)
	if err != nil {
		t.Fatalf("Members after removal: %v", err)
	}
	if _, still := memberByID(members, "n4"); still {
		t.Errorf("n4 is still listed after being removed: %+v", members)
	}

	if err := kv.Put(ctx, "after", "shrinking"); err != nil {
		t.Fatalf("Put after shrinking: %v", err)
	}
}

// Membership commands go to the leader like any other write, so sending one to
// a follower has to redirect rather than fail.
func TestMembershipCommandsFindTheLeader(t *testing.T) {
	c := startCluster(t, 3)

	leader := c.WaitForStableLeader(15 * time.Second)
	if leader == nil {
		t.Fatal("no stable leader")
	}

	var follower string
	for _, node := range c.Nodes {
		if node != leader {
			follower = node.Address
			break
		}
	}

	// A client that only knows one follower, and nothing else.
	kv := newTestClient(t, []string{follower})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	joined, err := c.Join("n4", t.TempDir())
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	if err := kv.AddServer(ctx, "n4", joined.Address); err != nil {
		t.Fatalf("AddServer through a follower: %v", err)
	}

	members, _, err := kv.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(members) != 4 {
		t.Errorf("cluster reports %d members, want 4", len(members))
	}
}
