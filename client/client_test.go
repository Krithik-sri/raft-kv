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

// getEventually retries because reads are served from the leader's local state
// machine with no quorum check: a leader that has just been deposed but has not
// noticed will answer with stale data. Committed writes are never lost, so the
// value must show up, but not necessarily on the first attempt.
func getEventually(t *testing.T, kv *Client, key, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last string

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		value, found, err := kv.Get(ctx, key)
		cancel()

		if err == nil && found && value == want {
			return
		}
		last = value

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("Get(%q) = %q, want %q within %s", key, last, want, timeout)
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

	getEventually(t, kv, "foo", "bar", 15*time.Second)

	if err := kv.Put(ctx, "foo", "baz"); err != nil {
		t.Fatalf("Put after leader failure: %v", err)
	}

	getEventually(t, kv, "foo", "baz", 15*time.Second)
}
