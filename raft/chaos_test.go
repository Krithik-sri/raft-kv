package raft

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

var chaosDuration = flag.Duration(
	"chaos.duration",
	3*time.Second,
	"how long the chaos test injects faults",
)

var chaosSeed = flag.Uint64(
	"chaos.seed",
	7,
	"seed for the chaos fault scheduler",
)

func currentLeader(nodes []*Raft) *Raft {
	for _, node := range nodes {
		if state, _ := node.getStateAndTerm(); state == Leader {
			return node
		}
	}
	return nil
}

type invariantChecker struct {
	mu       sync.Mutex
	nodes    []*Raft
	machines []*recordingStateMachine

	leaderByTerm map[uint64]NodeID
	lastCommit   map[NodeID]uint64
	canonical    []string
	failures     []string
	checks       int
}

func newInvariantChecker(nodes []*Raft, machines []*recordingStateMachine) *invariantChecker {
	return &invariantChecker{
		nodes:        nodes,
		machines:     machines,
		leaderByTerm: make(map[uint64]NodeID),
		lastCommit:   make(map[NodeID]uint64),
	}
}

func (c *invariantChecker) fail(format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	for _, existing := range c.failures {
		if existing == message {
			return
		}
	}

	c.failures = append(c.failures, message)
}

func (c *invariantChecker) check() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checks++

	for _, node := range c.nodes {
		status := node.Status()

		if status.State == Leader {
			if existing, ok := c.leaderByTerm[status.Term]; ok && existing != status.ID {
				c.fail("two leaders in term %d: %s and %s",
					status.Term, existing, status.ID)
			}
			c.leaderByTerm[status.Term] = status.ID
		}

		if previous, ok := c.lastCommit[status.ID]; ok && status.CommitIndex < previous {
			c.fail("node=%s commitIndex moved backward: %d then %d",
				status.ID, previous, status.CommitIndex)
		}
		c.lastCommit[status.ID] = status.CommitIndex
	}

	for i, machine := range c.machines {
		applied := machine.snapshot()

		shared := len(applied)
		if len(c.canonical) < shared {
			shared = len(c.canonical)
		}

		for j := 0; j < shared; j++ {
			if applied[j] != c.canonical[j] {
				c.fail("node=%s applied %q at position %d, others applied %q",
					c.nodes[i].id, applied[j], j, c.canonical[j])
				return
			}
		}

		if len(applied) > len(c.canonical) {
			c.canonical = applied
		}
	}
}

func (c *invariantChecker) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.check()
		case <-ctx.Done():
			c.check()
			return
		}
	}
}

func (c *invariantChecker) report() ([]string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]string, len(c.failures))
	copy(out, c.failures)
	return out, c.checks
}

type workload struct {
	mu       sync.Mutex
	acked    []string
	attempts int
	rejected int
}

func (w *workload) record(command string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.attempts++
	if err != nil {
		w.rejected++
		return
	}
	w.acked = append(w.acked, command)
}

func (w *workload) acknowledged() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]string, len(w.acked))
	copy(out, w.acked)
	return out
}

func (w *workload) counts() (int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.attempts, w.rejected
}

func (w *workload) writer(ctx context.Context, nodes []*Raft, id int) {
	for seq := 1; ctx.Err() == nil; seq++ {
		leader := currentLeader(nodes)
		if leader == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		command := fmt.Sprintf("w%d-%d", id, seq)

		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, err := leader.Propose(callCtx, []byte(command))
		cancel()

		w.record(command, err)

		time.Sleep(5 * time.Millisecond)
	}
}

func injectFaults(ctx context.Context, network *memNetwork, nodes []*Raft, rng *rand.Rand) {
	ids := make([]NodeID, len(nodes))
	for i, node := range nodes {
		ids[i] = node.id
	}

	for ctx.Err() == nil {
		switch rng.IntN(4) {
		case 0:
			leader := currentLeader(nodes)
			if leader != nil {
				network.block(leader.id)
			}

		case 1:
			var left, right []NodeID
			for _, id := range ids {
				if len(left) < len(ids)/2 {
					left = append(left, id)
					continue
				}
				right = append(right, id)
			}
			network.partitionInto(left, right)

		case 2:
			network.block(ids[rng.IntN(len(ids))])

		case 3:
			network.setFaults(0.2, 0.2, 5*time.Millisecond)
		}

		sleepFor(ctx, time.Duration(100+rng.IntN(300))*time.Millisecond)

		network.heal()
		network.setFaults(0, 0, 0)

		sleepFor(ctx, time.Duration(100+rng.IntN(200))*time.Millisecond)
	}
}

func sleepFor(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func containsAll(applied, wanted []string) []string {
	present := make(map[string]bool, len(applied))
	for _, command := range applied {
		present[command] = true
	}

	var missing []string
	for _, command := range wanted {
		if !present[command] {
			missing = append(missing, command)
		}
	}
	return missing
}

func TestChaos(t *testing.T) {
	const writers = 4

	nodes, machines, network, cancel := startClusterWithThreshold(t, 5, 30)
	defer cancel()

	waitForLeader(t, nodes, 15*time.Second)

	checker := newInvariantChecker(nodes, machines)

	ctx, stop := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		checker.run(ctx, 5*time.Millisecond)
	}()

	load := &workload{}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			load.writer(ctx, nodes, id)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		injectFaults(ctx, network, nodes, rand.New(rand.NewPCG(*chaosSeed, *chaosSeed+11)))
	}()

	time.Sleep(*chaosDuration)

	stop()
	wg.Wait()

	network.heal()
	network.setFaults(0, 0, 0)

	acked := load.acknowledged()
	attempts, rejected := load.counts()

	converged := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		reference := machines[0].snapshot()

		same := true
		for _, machine := range machines[1:] {
			if !equalStrings(reference, machine.snapshot()) {
				same = false
				break
			}
		}

		if same && len(containsAll(reference, acked)) == 0 {
			converged = true
			break
		}

		checker.check()
		time.Sleep(20 * time.Millisecond)
	}

	checker.check()
	failures, checks := checker.report()

	stats := network.snapshotStats()
	t.Logf("duration=%s writes attempted=%d acked=%d rejected=%d",
		*chaosDuration, attempts, len(acked), rejected)
	t.Logf("network delivered=%d dropped=%d duplicated=%d partitioned=%d",
		stats.delivered, stats.dropped, stats.duplicated, stats.partitioned)
	t.Logf("invariant checks=%d violations=%d", checks, len(failures))

	for i, failure := range failures {
		if i == 10 {
			t.Errorf("... and %d more violations", len(failures)-10)
			break
		}
		t.Errorf("invariant violation: %s", failure)
	}

	if len(acked) == 0 {
		t.Fatal("no writes were acknowledged; the workload did nothing")
	}

	if !converged {
		reference := machines[0].snapshot()
		for i, machine := range machines {
			t.Logf("node=%s applied=%d", nodes[i].id, machine.count())
		}
		if missing := containsAll(reference, acked); len(missing) > 0 {
			t.Errorf("%d acknowledged writes are missing after healing, first=%q",
				len(missing), missing[0])
		}
		t.Fatal("cluster did not reconverge after faults stopped")
	}
}
