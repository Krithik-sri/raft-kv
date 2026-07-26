package grpctransport_test

import (
	"context"
	"testing"
	"time"

	"github.com/krithik-sri/raft-kv/internal/cluster"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testNode struct {
	*cluster.Node

	client raftpb.KVServiceClient
}

func startTestCluster(t *testing.T, size int) ([]*testNode, *cluster.Cluster) {
	t.Helper()

	c, err := cluster.Start(size, t.TempDir())
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	t.Cleanup(c.Stop)

	nodes := make([]*testNode, len(c.Nodes))
	for i, node := range c.Nodes {
		conn, err := grpc.NewClient(
			node.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial %s: %v", node.Address, err)
		}
		t.Cleanup(func() { conn.Close() })

		nodes[i] = &testNode{Node: node, client: raftpb.NewKVServiceClient(conn)}
	}

	return nodes, c
}

// findLeader waits for the cluster to settle on one agreed leader, so callers
// can safely treat every other node as a follower that will redirect.
func findLeader(
	t *testing.T,
	nodes []*testNode,
	c *cluster.Cluster,
	timeout time.Duration,
) *testNode {
	t.Helper()

	leader := c.WaitForStableLeader(timeout)
	if leader == nil {
		t.Fatal("cluster did not settle on a single agreed leader")
	}

	for _, node := range nodes {
		if node.Node == leader {
			return node
		}
	}

	t.Fatal("leader is not in the node list")
	return nil
}

func TestKVServicePutGetDelete(t *testing.T) {
	nodes, c := startTestCluster(t, 3)
	leader := findLeader(t, nodes, c, 15*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	put, err := leader.client.Put(ctx, &raftpb.PutRequest{Key: "colour", Value: "blue"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Redirect != nil {
		t.Fatalf("leader redirected the write to %q", put.Redirect.LeaderAddress)
	}

	got, err := leader.client.Get(ctx, &raftpb.GetRequest{Key: "colour"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Found || got.Value != "blue" {
		t.Errorf("Get = %q found=%v, want \"blue\" found=true", got.Value, got.Found)
	}

	if _, err := leader.client.Delete(ctx, &raftpb.DeleteRequest{Key: "colour"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err = leader.client.Get(ctx, &raftpb.GetRequest{Key: "colour"})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.Found {
		t.Errorf("Get after delete found=%v, want false", got.Found)
	}
}

func TestKVServiceCompareAndSwap(t *testing.T) {
	nodes, c := startTestCluster(t, 3)
	leader := findLeader(t, nodes, c, 15*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	put, err := leader.client.Put(ctx, &raftpb.PutRequest{Key: "n", Value: "1"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Redirect != nil {
		t.Fatalf("leader redirected the write to %q", put.Redirect.LeaderAddress)
	}

	swap, err := leader.client.CompareAndSwap(ctx, &raftpb.CompareAndSwapRequest{
		Key: "n", Expected: "1", Value: "2",
	})
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if !swap.Swapped {
		t.Error("expected swap to succeed when the expected value matches")
	}

	swap, err = leader.client.CompareAndSwap(ctx, &raftpb.CompareAndSwapRequest{
		Key: "n", Expected: "1", Value: "3",
	})
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if swap.Swapped {
		t.Error("expected swap to fail when the expected value is stale")
	}

	got, err := leader.client.Get(ctx, &raftpb.GetRequest{Key: "n"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Value != "2" {
		t.Errorf("value = %q, want \"2\"", got.Value)
	}
}

func TestKVServiceRedirectsFromFollower(t *testing.T) {
	nodes, c := startTestCluster(t, 3)
	leader := findLeader(t, nodes, c, 15*time.Second)

	var follower *testNode
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := follower.client.Put(ctx, &raftpb.PutRequest{Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("Put on follower: %v", err)
	}

	if resp.Redirect == nil {
		t.Fatal("follower accepted a write instead of redirecting")
	}

	if resp.Redirect.LeaderAddress != leader.Address {
		t.Errorf("redirect address = %q, want %q", resp.Redirect.LeaderAddress, leader.Address)
	}

	if resp.Redirect.LeaderId != string(leader.ID) {
		t.Errorf("redirect id = %q, want %q", resp.Redirect.LeaderId, leader.ID)
	}
}
