package grpctransport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/krithik-sri/raft-kv/kvstore"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testNode struct {
	id      raft.NodeID
	address string
	client  raftpb.KVServiceClient
}

func startTestCluster(t *testing.T, size int) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, size)
	ids := make([]raft.NodeID, size)
	addresses := make([]string, size)

	for i := 0; i < size; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = listener
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
		addresses[i] = listener.Addr().String()
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	nodes := make([]*testNode, size)

	for i := 0; i < size; i++ {
		var peers []raft.Peer
		for j := 0; j < size; j++ {
			if j != i {
				peers = append(peers, raft.Peer{ID: ids[j], Address: addresses[j]})
			}
		}

		store := kvstore.New()
		node := raft.New(ids[i], peers, &Transport{}, store)

		server := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(server, NewServer(node))
		raftpb.RegisterKVServiceServer(server, NewKVServer(node, store, peers))

		listener := listeners[i]
		go server.Serve(listener)
		t.Cleanup(server.Stop)

		go node.Start(ctx)

		conn, err := grpc.NewClient(
			addresses[i],
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial %s: %v", addresses[i], err)
		}
		t.Cleanup(func() { conn.Close() })

		nodes[i] = &testNode{
			id:      ids[i],
			address: addresses[i],
			client:  raftpb.NewKVServiceClient(conn),
		}
	}

	return nodes
}

func findLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := node.client.Put(ctx, &raftpb.PutRequest{
				Key:   "__probe",
				Value: "1",
			})
			cancel()

			if err == nil && resp.Redirect == nil {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("no leader answered a Put within the timeout")
	return nil
}

func TestKVServicePutGetDelete(t *testing.T) {
	nodes := startTestCluster(t, 3)
	leader := findLeader(t, nodes, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := leader.client.Put(ctx, &raftpb.PutRequest{Key: "colour", Value: "blue"}); err != nil {
		t.Fatalf("Put: %v", err)
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
	nodes := startTestCluster(t, 3)
	leader := findLeader(t, nodes, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := leader.client.Put(ctx, &raftpb.PutRequest{Key: "n", Value: "1"}); err != nil {
		t.Fatalf("Put: %v", err)
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
	nodes := startTestCluster(t, 3)
	leader := findLeader(t, nodes, 10*time.Second)

	var follower *testNode
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := follower.client.Put(ctx, &raftpb.PutRequest{Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("Put on follower: %v", err)
	}

	if resp.Redirect == nil {
		t.Fatal("follower accepted a write instead of redirecting")
	}

	if resp.Redirect.LeaderAddress != leader.address {
		t.Errorf("redirect address = %q, want %q", resp.Redirect.LeaderAddress, leader.address)
	}

	if resp.Redirect.LeaderId != string(leader.id) {
		t.Errorf("redirect id = %q, want %q", resp.Redirect.LeaderId, leader.id)
	}
}
