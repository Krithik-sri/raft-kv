package raft

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/krithik-sri/raft-kv/internal/linearizability"
	"github.com/krithik-sri/raft-kv/kvstore"
)

var staleReads = flag.Bool(
	"linearizability.stale",
	false,
	"skip the read barrier, to check the checker actually catches it",
)

// startKVCluster is startClusterWithThreshold with a real key-value store
// behind it, because the linearizability model needs get and put to mean
// something.
func startKVCluster(t *testing.T, size int) ([]*Raft, []*kvstore.Store, *memNetwork, context.CancelFunc) {
	t.Helper()

	network := newMemNetwork()

	ids := make([]NodeID, size)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("n%d", i+1))
	}

	nodes := make([]*Raft, size)
	stores := make([]*kvstore.Store, size)

	for i, id := range ids {
		var peers []Peer
		for _, other := range ids {
			if other != id {
				peers = append(peers, Peer{ID: other, Address: string(other)})
			}
		}

		stores[i] = kvstore.New()

		node, err := New(id, peers, &memTransport{self: id, net: network}, stores[i], &memStorage{})
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}

		node.snapshotThreshold = 1 << 40

		nodes[i] = node
		network.register(id, node)
	}

	ctx, cancel := context.WithCancel(context.Background())
	for _, node := range nodes {
		go node.Start(ctx)
	}

	return nodes, stores, network, cancel
}

func randomInput(rng *rand.Rand, keys []string) linearizability.Input {
	key := keys[rng.IntN(len(keys))]

	switch rng.IntN(10) {
	case 0, 1, 2, 3, 4:
		return linearizability.Input{Op: "get", Key: key}
	case 5, 6, 7:
		return linearizability.Input{
			Op:    "put",
			Key:   key,
			Value: fmt.Sprintf("v%d", rng.IntN(4)),
		}
	case 8:
		return linearizability.Input{Op: "delete", Key: key}
	default:
		return linearizability.Input{
			Op:       "cas",
			Key:      key,
			Expected: fmt.Sprintf("v%d", rng.IntN(4)),
			Value:    fmt.Sprintf("v%d", rng.IntN(4)),
		}
	}
}

// apply runs one operation against whichever node currently believes it leads.
// Reads go through ReadIndex unless -linearizability.stale says otherwise.
func apply(
	ctx context.Context,
	rng *rand.Rand,
	nodes []*Raft,
	stores []*kvstore.Store,
	in linearizability.Input,
) linearizability.Output {
	// Pick at random among everyone claiming to lead, which is what a real
	// client effectively does: it talks to whichever node told it "I am the
	// leader". During a handover two nodes claim it at once, and always
	// choosing the same one hides the interesting case entirely.
	var candidates []int
	for i, node := range nodes {
		if state, _ := node.getStateAndTerm(); state == Leader {
			candidates = append(candidates, i)
		}
	}

	if len(candidates) == 0 {
		return linearizability.Output{Unknown: true}
	}

	chosen := candidates[rng.IntN(len(candidates))]
	leader, store := nodes[chosen], stores[chosen]

	if in.Op == "get" {
		if !*staleReads {
			index, err := leader.ReadIndex(ctx)
			if err != nil {
				return linearizability.Output{Unknown: true}
			}
			if err := leader.WaitForApplied(ctx, index); err != nil {
				return linearizability.Output{Unknown: true}
			}
		}

		value, found := store.Get(in.Key)
		return linearizability.Output{Value: value, Found: found}
	}

	command := kvstore.Command{Key: in.Key, Value: in.Value, Expected: in.Expected}
	switch in.Op {
	case "put":
		command.Op = kvstore.OpPut
	case "delete":
		command.Op = kvstore.OpDelete
	default:
		command.Op = kvstore.OpCAS
	}

	encoded, err := kvstore.EncodeCommand(command)
	if err != nil {
		return linearizability.Output{Unknown: true}
	}

	raw, err := leader.Propose(ctx, encoded)
	if err != nil {
		return linearizability.Output{Unknown: true}
	}

	result, err := kvstore.DecodeResult(raw)
	if err != nil {
		return linearizability.Output{Unknown: true}
	}

	return linearizability.Output{Swapped: result.Swapped}
}

// TestLinearizabilityUnderPartitions is the one that proves something.
//
// The client-level check kills leaders, and a dead leader misleads nobody. The
// dangerous case is a leader that is alive, cut off, and still convinced it is
// in charge. Only the in-memory network can produce that, so this test lives
// here rather than next to the gRPC one.
//
// -linearizability.stale skips the read barrier. It usually produces a
// violation and sometimes doesn't, because it depends on two callers happening
// to pick different self-declared leaders at the wrong moment. So it is a
// diagnostic, not an assertion. The proof that the model catches violations at
// all is deterministic and lives in internal/linearizability.
func TestLinearizabilityUnderPartitions(t *testing.T) {
	const (
		callers  = 4
		duration = 6 * time.Second
	)

	keys := []string{"a", "b", "c"}

	nodes, stores, network, cancel := startKVCluster(t, 5)
	defer cancel()

	waitForLeader(t, nodes, 15*time.Second)

	log := &linearizability.Recorder{}
	ctx, stop := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	for id := 0; id < callers; id++ {
		rng := rand.New(rand.NewPCG(uint64(id)+1, 7))

		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for ctx.Err() == nil {
				in := randomInput(rng, keys)

				callCtx, cancelCall := context.WithTimeout(ctx, 2*time.Second)
				call := time.Now()
				out := apply(callCtx, rng, nodes, stores, in)
				ret := time.Now()
				cancelCall()

				// A read that failed saw nothing, so it constrains nothing.
				// Leaving it out is safe. Leaving out a write is not.
				if out.Unknown && in.Op == "get" {
					continue
				}

				log.Record(id, in, out, call, ret)

				// The in-memory cluster is quick enough to record six figures
				// of operations in a few seconds, and the checker is
				// exponential in the worst case. Slow down so the history stays
				// checkable.
				time.Sleep(3 * time.Millisecond)
			}
		}(id)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		rng := rand.New(rand.NewPCG(99, 100))
		ids := make([]NodeID, len(nodes))
		for i, node := range nodes {
			ids[i] = node.id
		}

		for ctx.Err() == nil {
			// Strand the leader on the minority side, which is exactly the
			// shape that used to hand out stale reads.
			var minority, majority []NodeID
			for _, node := range nodes {
				if state, _ := node.getStateAndTerm(); state == Leader && len(minority) < 2 {
					minority = append(minority, node.id)
				}
			}
			for _, id := range ids {
				if len(minority) < 2 && !contains(minority, id) {
					minority = append(minority, id)
					continue
				}
				if !contains(minority, id) {
					majority = append(majority, id)
				}
			}

			network.partitionInto(majority, minority)
			sleepFor(ctx, time.Duration(300+rng.IntN(400))*time.Millisecond)

			network.heal()
			sleepFor(ctx, time.Duration(200+rng.IntN(300))*time.Millisecond)
		}
	}()

	time.Sleep(duration)
	stop()
	wg.Wait()

	network.heal()

	history := log.History()
	if len(history) < 50 {
		t.Fatalf("only recorded %d operations, not enough to prove anything", len(history))
	}

	result, info := porcupine.CheckOperationsVerbose(linearizability.Model, history, 60*time.Second)

	if *staleReads {
		t.Logf("stale mode: checker returned %v over %d operations", result, len(history))
		t.Log("a diagnostic, not a verdict. See internal/linearizability for the real proof.")
		return
	}

	switch result {
	case porcupine.Ok:
		t.Logf("linearizable: %d operations, %d partitions injected",
			len(history), network.snapshotStats().partitioned)

	case porcupine.Illegal:
		path := t.TempDir() + "/linearizability.html"
		if err := porcupine.VisualizePath(linearizability.Model, info, path); err == nil {
			t.Logf("visualisation written to %s", path)
		}
		t.Fatalf("history is NOT linearizable (%d operations)", len(history))

	default:
		t.Fatalf("checker gave up on %d operations", len(history))
	}
}

func contains(ids []NodeID, id NodeID) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}
