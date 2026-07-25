package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type memTransport struct {
	mu    sync.Mutex
	nodes map[NodeID]*Raft
}

var _ Transport = (*memTransport)(nil)

func newMemTransport() *memTransport {
	return &memTransport{nodes: make(map[NodeID]*Raft)}
}

func (t *memTransport) register(id NodeID, node *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

func (t *memTransport) lookup(id NodeID) (*Raft, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	node, ok := t.nodes[id]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", id)
	}
	return node, nil
}

func (t *memTransport) RequestVote(
	ctx context.Context,
	peer Peer,
	req RequestVoteRequest,
) (RequestVoteResponse, error) {
	node, err := t.lookup(peer.ID)
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
	node, err := t.lookup(peer.ID)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	return node.HandleAppendEntries(req), nil
}

func startCluster(t *testing.T, size int) ([]*Raft, []*recordingStateMachine, context.CancelFunc) {
	t.Helper()

	transport := newMemTransport()

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
		nodes[i] = New(id, peers, transport, machines[i])
		transport.register(id, nodes[i])
	}

	ctx, cancel := context.WithCancel(context.Background())
	for _, node := range nodes {
		go node.Start(ctx)
	}

	return nodes, machines, cancel
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
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || string(a[i].Command) != string(b[i].Command) {
			return false
		}
	}
	return true
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
		if _, _, ok := leader.Submit([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("submit %d rejected by the leader", i)
		}
	}

	waitForLogConvergence(t, nodes, commands+1, 10*time.Second)

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
		if got := machine.snapshot(); !equalStrings(got, want) {
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
		if _, _, ok := leader.Submit([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("submit %d rejected by the leader", i)
		}
	}

	waitForLogConvergence(t, nodes, commands+1, 10*time.Second)

	var follower *Raft
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}

	follower.mu.Lock()
	follower.log[commands] = LogEntry{Term: 99, Command: []byte("bogus")}
	follower.mu.Unlock()

	if logsMatch(snapshotLog(leader), snapshotLog(follower)) {
		t.Fatal("follower log was not corrupted, test proves nothing")
	}

	waitForLogConvergence(t, nodes, commands+1, 10*time.Second)
}
