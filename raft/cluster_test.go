package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

type memStorage struct {
	mu       sync.Mutex
	snapshot []byte
	present  bool
}

var _ Storage = (*memStorage)(nil)

func (s *memStorage) Load() (PersistentState, error) { return PersistentState{}, nil }
func (s *memStorage) SaveState(uint64, NodeID) error { return nil }
func (s *memStorage) AppendLog([]LogEntry) error     { return nil }
func (s *memStorage) TruncateLog(uint64) error       { return nil }

func (s *memStorage) SaveSnapshot(
	snapshot Snapshot,
	term uint64,
	votedFor NodeID,
	config Configuration,
	retained []LogEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot = snapshot.Data
	s.present = true
	return nil
}

func (s *memStorage) LoadSnapshot() ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshot, s.present, nil
}

type netStats struct {
	delivered   int
	dropped     int
	duplicated  int
	partitioned int
}

type memNetwork struct {
	mu       sync.Mutex
	nodes    map[NodeID]*Raft
	group    map[NodeID]int
	nextIsle int
	installs int

	dropRate      float64
	duplicateRate float64
	maxDelay      time.Duration

	rng   *rand.Rand
	stats netStats
}

func newMemNetwork() *memNetwork {
	return &memNetwork{
		nodes:    make(map[NodeID]*Raft),
		group:    make(map[NodeID]int),
		nextIsle: 1,
		rng:      rand.New(rand.NewPCG(1, 2)),
	}
}

func (n *memNetwork) register(id NodeID, node *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

func (n *memNetwork) block(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.group[id] = n.nextIsle
	n.nextIsle++
}

func (n *memNetwork) unblock(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group[id] = 0
}

func (n *memNetwork) partitionInto(groups ...[]NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for i, group := range groups {
		for _, id := range group {
			n.group[id] = i + 1
		}
	}
	n.nextIsle = len(groups) + 1
}

func (n *memNetwork) heal() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for id := range n.group {
		n.group[id] = 0
	}
	n.nextIsle = 1
}

func (n *memNetwork) setFaults(dropRate, duplicateRate float64, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.dropRate = dropRate
	n.duplicateRate = duplicateRate
	n.maxDelay = maxDelay
}

func (n *memNetwork) snapshotStats() netStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

func (n *memNetwork) installCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.installs
}

func (n *memNetwork) countInstall() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.installs++
}

func (n *memNetwork) shouldDuplicate() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.duplicateRate <= 0 || n.rng.Float64() >= n.duplicateRate {
		return false
	}

	n.stats.duplicated++
	return true
}

func (n *memNetwork) route(from, to NodeID) (*Raft, error) {
	n.mu.Lock()

	if n.group[from] != n.group[to] {
		n.stats.partitioned++
		n.mu.Unlock()
		return nil, fmt.Errorf("%s is partitioned from %s", from, to)
	}

	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		n.stats.dropped++
		n.mu.Unlock()
		return nil, fmt.Errorf("message from %s to %s dropped", from, to)
	}

	var delay time.Duration
	if n.maxDelay > 0 {
		delay = time.Duration(n.rng.Int64N(int64(n.maxDelay)))
	}

	node, ok := n.nodes[to]
	if !ok {
		n.mu.Unlock()
		return nil, fmt.Errorf("unknown peer %s", to)
	}

	n.stats.delivered++
	n.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	return node, nil
}

type memTransport struct {
	self NodeID
	net  *memNetwork
}

var _ Transport = (*memTransport)(nil)

func (t *memTransport) RequestVote(
	ctx context.Context,
	peer Peer,
	req RequestVoteRequest,
) (RequestVoteResponse, error) {
	node, err := t.net.route(t.self, peer.ID)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	return node.HandleRequestVote(req), nil
}

func (t *memTransport) AppendEntries(
	ctx context.Context,
	peer Peer,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	node, err := t.net.route(t.self, peer.ID)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	if t.net.shouldDuplicate() {
		node.HandleAppendEntries(req)
	}

	return node.HandleAppendEntries(req), nil
}

func (t *memTransport) InstallSnapshot(
	ctx context.Context,
	peer Peer,
	req InstallSnapshotRequest,
) (InstallSnapshotResponse, error) {
	node, err := t.net.route(t.self, peer.ID)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	t.net.countInstall()

	return node.HandleInstallSnapshot(req), nil
}

func startCluster(t *testing.T, size int) ([]*Raft, []*recordingStateMachine, context.CancelFunc) {
	t.Helper()

	nodes, machines, _, cancel := startClusterWithThreshold(t, size, 1<<40)
	return nodes, machines, cancel
}

func startClusterWithThreshold(
	t *testing.T,
	size int,
	threshold uint64,
) ([]*Raft, []*recordingStateMachine, *memNetwork, context.CancelFunc) {
	t.Helper()

	network := newMemNetwork()

	ids := make([]NodeID, size)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("n%d", i+1))
	}

	nodes := make([]*Raft, size)
	machines := make([]*recordingStateMachine, size)
	for i, id := range ids {
		var peers []Peer
		for _, other := range ids {
			if other != id {
				peers = append(peers, Peer{ID: other, Address: string(other)})
			}
		}
		machines[i] = &recordingStateMachine{}

		node, err := New(Peer{ID: id}, peers, &memTransport{self: id, net: network}, machines[i], &memStorage{})
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}

		node.snapshotThreshold = threshold

		nodes[i] = node
		network.register(id, node)
	}

	ctx, cancel := context.WithCancel(context.Background())
	for _, node := range nodes {
		go node.Start(ctx)
	}

	return nodes, machines, network, cancel
}

func snapshotLog(r *Raft) []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]LogEntry, len(r.log))
	copy(out, r.log)
	return out
}

func waitForLeader(t *testing.T, nodes []*Raft, timeout time.Duration) *Raft {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Raft
		for _, node := range nodes {
			if state, _ := node.getStateAndTerm(); state == Leader {
				leaders = append(leaders, node)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("no single leader emerged")
	return nil
}

func logsMatch(a, b []LogEntry) bool {
	return slices.EqualFunc(a, b, func(x, y LogEntry) bool {
		return x.Term == y.Term && string(x.Command) == string(y.Command)
	})
}

func waitForLogConvergence(t *testing.T, nodes []*Raft, wantLen int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		reference := snapshotLog(nodes[0])

		converged := len(reference) == wantLen
		for _, node := range nodes[1:] {
			if !logsMatch(reference, snapshotLog(node)) {
				converged = false
				break
			}
		}

		if converged {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, node := range nodes {
		t.Logf("node=%s log=%v", node.id, logTerms(snapshotLog(node)))
	}
	t.Fatalf("logs did not converge to %d entries", wantLen)
}

func TestClusterReplicatesAndApplies(t *testing.T) {
	nodes, machines, cancel := startCluster(t, 3)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	const commands = 100
	for i := 0; i < commands; i++ {
		if _, _, ok := leader.submit([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("submit %d rejected by the leader", i)
		}
	}

	waitForLogConvergence(t, nodes, commands+2, 10*time.Second)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, machine := range machines {
			if machine.count() != commands {
				done = false
				break
			}
		}
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	want := make([]string, commands)
	for i := range want {
		want[i] = fmt.Sprintf("cmd-%d", i)
	}

	for i, machine := range machines {
		if got := machine.snapshot(); !slices.Equal(got, want) {
			t.Errorf("node=%s applied %d commands, want %d in order",
				nodes[i].id, len(got), commands)
		}
	}
}

func TestClusterRepairsDivergentFollower(t *testing.T) {
	nodes, _, cancel := startCluster(t, 3)
	defer cancel()

	leader := waitForLeader(t, nodes, 5*time.Second)

	const commands = 20
	for i := 0; i < commands; i++ {
		if _, _, ok := leader.submit([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("submit %d rejected by the leader", i)
		}
	}

	waitForLogConvergence(t, nodes, commands+2, 10*time.Second)

	var follower *Raft
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}

	follower.mu.Lock()
	follower.log[len(follower.log)-1] = LogEntry{Term: 99, Command: []byte("bogus")}
	follower.mu.Unlock()

	if logsMatch(snapshotLog(leader), snapshotLog(follower)) {
		t.Fatal("follower log was not corrupted, test proves nothing")
	}

	waitForLogConvergence(t, nodes, commands+2, 10*time.Second)
}
