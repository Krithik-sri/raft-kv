package client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/krithik-sri/raft-kv/kvstore"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	grpctransport "github.com/krithik-sri/raft-kv/transport/grpc"
	"google.golang.org/grpc"
)

type clusterNode struct {
	address string
	node    *raft.Raft
	server  *grpc.Server
	cancel  context.CancelFunc
	stopped bool
}

func (n *clusterNode) stop() {
	if n.stopped {
		return
	}
	n.stopped = true
	n.cancel()
	n.server.Stop()
}

func startCluster(t *testing.T, size int) ([]*clusterNode, []string) {
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

	nodes := make([]*clusterNode, size)

	for i := 0; i < size; i++ {
		var peers []raft.Peer
		for j := 0; j < size; j++ {
			if j != i {
				peers = append(peers, raft.Peer{ID: ids[j], Address: addresses[j]})
			}
		}

		store := kvstore.New()
		node, err := raft.New(ids[i], peers, &grpctransport.Transport{}, store, nil)
		if err != nil {
			t.Fatalf("New %s: %v", ids[i], err)
		}

		server := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(server, grpctransport.NewServer(node))
		raftpb.RegisterKVServiceServer(server, grpctransport.NewKVServer(node, store, peers))

		ctx, cancel := context.WithCancel(context.Background())

		go server.Serve(listeners[i])
		go node.Start(ctx)

		nodes[i] = &clusterNode{
			address: addresses[i],
			node:    node,
			server:  server,
			cancel:  cancel,
		}
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop()
		}
	})

	return nodes, addresses
}

func waitForLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.stopped {
				continue
			}
			if _, isLeader := n.node.LeaderHint(); isLeader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("no leader emerged")
	return nil
}

func newTestClient(t *testing.T, addresses []string) *Client {
	t.Helper()

	kv, err := New(addresses)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { kv.Close() })

	return kv
}

func TestClientPutGetDelete(t *testing.T) {
	nodes, addresses := startCluster(t, 3)
	waitForLeader(t, nodes, 10*time.Second)

	kv := newTestClient(t, addresses)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := kv.Put(ctx, "foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	value, found, err := kv.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || value != "bar" {
		t.Errorf("Get = %q found=%v, want \"bar\" true", value, found)
	}

	if err := kv.Delete(ctx, "foo"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, found, err = kv.Get(ctx, "foo"); err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if found {
		t.Error("key still present after delete")
	}
}

func TestClientCompareAndSwap(t *testing.T) {
	nodes, addresses := startCluster(t, 3)
	waitForLeader(t, nodes, 10*time.Second)

	kv := newTestClient(t, addresses)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := kv.Put(ctx, "n", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	swapped, err := kv.CompareAndSwap(ctx, "n", "1", "2")
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if !swapped {
		t.Error("expected the swap to succeed")
	}

	swapped, err = kv.CompareAndSwap(ctx, "n", "1", "3")
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if swapped {
		t.Error("expected the stale swap to be rejected")
	}
}

func TestClientSurvivesLeaderFailure(t *testing.T) {
	nodes, addresses := startCluster(t, 3)
	waitForLeader(t, nodes, 10*time.Second)

	kv := newTestClient(t, addresses)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := kv.Put(ctx, "foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	leader := waitForLeader(t, nodes, 10*time.Second)
	leader.stop()

	value, found, err := kv.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get after leader failure: %v", err)
	}
	if !found || value != "bar" {
		t.Errorf("Get after leader failure = %q found=%v, want \"bar\" true", value, found)
	}

	if err := kv.Put(ctx, "foo", "baz"); err != nil {
		t.Fatalf("Put after leader failure: %v", err)
	}

	value, found, err = kv.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get after failover write: %v", err)
	}
	if !found || value != "baz" {
		t.Errorf("Get after failover write = %q found=%v, want \"baz\" true", value, found)
	}
}
