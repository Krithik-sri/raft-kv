package client

import (
	"context"
	"testing"
	"time"

	"github.com/krithik-sri/raft-kv/internal/cluster"
)

func startCluster(t *testing.T, size int) *cluster.Cluster {
	t.Helper()

	c, err := cluster.Start(size, t.TempDir())
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	t.Cleanup(c.Stop)

	if c.WaitForLeader(15*time.Second) == nil {
		t.Fatal("no leader emerged")
	}

	return c
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
	c := startCluster(t, 3)
	kv := newTestClient(t, c.Addresses)

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
	c := startCluster(t, 3)
	kv := newTestClient(t, c.Addresses)

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
	c := startCluster(t, 3)
	kv := newTestClient(t, c.Addresses)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := kv.Put(ctx, "foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	c.WaitForLeader(10 * time.Second).Stop()

	// Reads are linearizable, so the value has to be there on the first try.
	// The client retries transport errors internally while it finds the new
	// leader, but it never hands back a stale answer.
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

// The point of the read barrier, end to end. Cut the leader off from everyone
// else and it still thinks it is in charge. A linearizable read has to refuse.
// A stale read is allowed to answer, and will answer with old data, which is
// exactly what you asked for by passing WithStale.
func TestIsolatedLeaderRefusesLinearizableRead(t *testing.T) {
	c := startCluster(t, 3)

	kv := newTestClient(t, c.Addresses)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := kv.Put(ctx, "foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	leader := c.WaitForStableLeader(15 * time.Second)
	if leader == nil {
		t.Fatal("no stable leader")
	}

	for _, node := range c.Nodes {
		if node != leader {
			node.Stop()
		}
	}

	// Talk only to the marooned leader.
	alone, err := New([]string{leader.Address})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer alone.Close()

	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()

	if value, found, err := alone.Get(readCtx, "foo"); err == nil {
		t.Errorf("isolated leader served a linearizable read: %q found=%v", value, found)
	}

	staleCtx, staleCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer staleCancel()

	value, found, err := alone.Get(staleCtx, "foo", WithStale())
	if err != nil {
		t.Fatalf("stale Get on the isolated leader: %v", err)
	}
	if !found || value != "bar" {
		t.Errorf("stale Get = %q found=%v, want \"bar\" true", value, found)
	}
}
