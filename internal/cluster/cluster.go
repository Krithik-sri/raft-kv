// Package cluster starts a real multi-node raft-kv cluster inside one process:
// real gRPC over loopback, real on-disk storage. Shared by the benchmark tool
// and the integration tests so there is one definition of "a running cluster".
package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/krithik-sri/raft-kv/kvstore"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	"github.com/krithik-sri/raft-kv/storage"
	grpctransport "github.com/krithik-sri/raft-kv/transport/grpc"
	"google.golang.org/grpc"
)

type Node struct {
	ID      raft.NodeID
	Address string
	Raft    *raft.Raft
	Store   *kvstore.Store

	server  *grpc.Server
	storage *storage.File
	cancel  context.CancelFunc
	stopped bool
}

// Stop kills the node the way a crash would: the gRPC server goes away and the
// raft goroutines are cancelled. Safe to call more than once.
func (n *Node) Stop() {
	if n.stopped {
		return
	}

	n.stopped = true
	n.cancel()
	n.server.Stop()
	n.storage.Close()
}

func (n *Node) Stopped() bool {
	return n.stopped
}

type Cluster struct {
	Nodes     []*Node
	Addresses []string
}

// Start brings up size nodes on loopback, each persisting into dataDir. The
// caller owns dataDir; Stop does not remove it.
func Start(size int, dataDir string) (*Cluster, error) {
	listeners := make([]net.Listener, size)
	ids := make([]raft.NodeID, size)
	addresses := make([]string, size)

	for i := range size {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("listen: %w", err)
		}

		listeners[i] = listener
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
		addresses[i] = listener.Addr().String()
	}

	c := &Cluster{Addresses: addresses}

	for i := range size {
		var peers []raft.Peer
		for j := range size {
			if j != i {
				peers = append(peers, raft.Peer{ID: ids[j], Address: addresses[j]})
			}
		}

		persist, err := storage.Open(filepath.Join(dataDir, string(ids[i])+".log"))
		if err != nil {
			c.Stop()
			return nil, err
		}

		store := kvstore.New()

		self := raft.Peer{ID: ids[i], Address: addresses[i]}

		node, err := raft.New(self, peers, &grpctransport.Transport{}, store, persist)
		if err != nil {
			c.Stop()
			return nil, fmt.Errorf("new node %s: %w", ids[i], err)
		}

		server := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(server, grpctransport.NewServer(node))
		raftpb.RegisterKVServiceServer(server, grpctransport.NewKVServer(node, store, peers))

		ctx, cancel := context.WithCancel(context.Background())

		go server.Serve(listeners[i])
		go node.Start(ctx)

		c.Nodes = append(c.Nodes, &Node{
			ID:      ids[i],
			Address: addresses[i],
			Raft:    node,
			Store:   store,
			server:  server,
			storage: persist,
			cancel:  cancel,
		})
	}

	return c, nil
}

func (c *Cluster) Stop() {
	for _, node := range c.Nodes {
		node.Stop()
	}
}

// Leader returns the current leader, or nil. Several nodes can claim
// leadership at once during a handover -- a deposed leader keeps saying yes
// until it hears a higher term -- so the highest term wins.
func (c *Cluster) Leader() *Node {
	var (
		leader *Node
		term   uint64
	)

	for _, node := range c.Nodes {
		if node.stopped {
			continue
		}

		status := node.Raft.Status()
		if status.State != raft.Leader {
			continue
		}

		if leader == nil || status.Term > term {
			leader, term = node, status.Term
		}
	}

	return leader
}

// WaitForStableLeader waits until exactly one node claims leadership and every
// other live node agrees on who it is. WaitForLeader is happy mid-handover;
// this is not. Tests need the stronger version: a follower that has not yet
// heard from the new leader redirects to nowhere, and a leader that is about to
// be deposed silently turns writes into redirects.
func (c *Cluster) WaitForStableLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leader := c.stableLeader(); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (c *Cluster) stableLeader() *Node {
	var leaders []*Node

	for _, node := range c.Nodes {
		if node.stopped {
			continue
		}
		if status := node.Raft.Status(); status.State == raft.Leader {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) != 1 {
		return nil
	}

	leader := leaders[0]

	for _, node := range c.Nodes {
		if node.stopped || node == leader {
			continue
		}
		if hint, _ := node.Raft.LeaderHint(); hint != leader.ID {
			return nil
		}
	}

	return leader
}

func (c *Cluster) WaitForLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leader := c.Leader(); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}
