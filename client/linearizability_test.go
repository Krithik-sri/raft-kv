package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/krithik-sri/raft-kv/internal/linearizability"
)

func randomOp(rng *rand.Rand, keys []string) linearizability.Input {
	key := keys[rng.IntN(len(keys))]

	switch rng.IntN(10) {
	case 0, 1, 2, 3, 4:
		return linearizability.Input{Op: "get", Key: key}
	case 5, 6, 7:
		return linearizability.Input{
			Op:    "put",
			Key:   key,
			Value: fmt.Sprintf("v%d", rng.IntN(5)),
		}
	case 8:
		return linearizability.Input{Op: "delete", Key: key}
	default:
		return linearizability.Input{
			Op:       "cas",
			Key:      key,
			Expected: fmt.Sprintf("v%d", rng.IntN(5)),
			Value:    fmt.Sprintf("v%d", rng.IntN(5)),
		}
	}
}

func applyOp(ctx context.Context, kv *Client, in linearizability.Input) linearizability.Output {
	switch in.Op {
	case "get":
		value, found, err := kv.Get(ctx, in.Key)
		if err != nil {
			return linearizability.Output{Unknown: true}
		}
		return linearizability.Output{Value: value, Found: found}

	case "put":
		if err := kv.Put(ctx, in.Key, in.Value); err != nil {
			return linearizability.Output{Unknown: true}
		}
		return linearizability.Output{}

	case "delete":
		if err := kv.Delete(ctx, in.Key); err != nil {
			return linearizability.Output{Unknown: true}
		}
		return linearizability.Output{}

	default:
		swapped, err := kv.CompareAndSwap(ctx, in.Key, in.Expected, in.Value)
		if err != nil {
			return linearizability.Output{Unknown: true}
		}
		return linearizability.Output{Swapped: swapped}
	}
}

// TestLinearizability drives the whole stack: real gRPC, the real client with
// its redirects and retries, real disks. Several clients at once, and the
// leader gets killed twice along the way.
//
// It cannot cut a leader off and leave it running, because these are real
// servers on real ports. That case, which is the one the read barrier exists
// for, is covered by TestLinearizabilityUnderPartitions in the raft package.
// This test is here to catch anything the layers above raft might get wrong:
// redirects, retries, duplicate detection.
func TestLinearizability(t *testing.T) {
	const (
		clients  = 4
		duration = 6 * time.Second
	)

	keys := []string{"a", "b", "c"}

	c := startCluster(t, 5)

	log := &linearizability.Recorder{}
	ctx, stop := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	for id := 0; id < clients; id++ {
		kv := newTestClient(t, c.Addresses)
		rng := rand.New(rand.NewPCG(uint64(id)+1, 99))

		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for ctx.Err() == nil {
				in := randomOp(rng, keys)

				callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				call := time.Now()
				out := applyOp(callCtx, kv, in)
				ret := time.Now()
				cancel()

				// A read that failed saw nothing, so it constrains nothing.
				// Leaving it out is always safe. Leaving out a write is not.
				if out.Unknown && in.Op == "get" {
					continue
				}

				log.Record(id, in, out, call, ret)
			}
		}(id)
	}

	// Take the leader away twice. Five nodes, so three survive and the cluster
	// keeps working.
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 2; i++ {
			select {
			case <-time.After(duration / 3):
			case <-ctx.Done():
				return
			}

			if leader := c.WaitForLeader(5 * time.Second); leader != nil {
				t.Logf("killing leader %s", leader.ID)
				leader.Stop()
			}
		}
	}()

	time.Sleep(duration)
	stop()
	wg.Wait()

	history := log.History()
	if len(history) < 50 {
		t.Fatalf("only recorded %d operations, not enough to prove anything", len(history))
	}

	result, info := porcupine.CheckOperationsVerbose(linearizability.Model, history, 30*time.Second)

	switch result {
	case porcupine.Ok:
		t.Logf("linearizable: %d operations across %d clients", len(history), clients)

	case porcupine.Illegal:
		path := t.TempDir() + "/linearizability.html"
		if err := porcupine.VisualizePath(linearizability.Model, info, path); err == nil {
			t.Logf("visualisation written to %s", path)
		}
		t.Fatalf("history is NOT linearizable (%d operations)", len(history))

	default:
		t.Fatalf("checker gave up after 30s on %d operations", len(history))
	}
}
