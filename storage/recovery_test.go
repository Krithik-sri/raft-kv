package storage

import (
	"path/filepath"
	"testing"

	"github.com/krithik-sri/raft-kv/kvstore"
	"github.com/krithik-sri/raft-kv/raft"
)

func newNode(t *testing.T, path string) (*raft.Raft, *File) {
	t.Helper()

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	node, err := raft.New("n1", nil, nil, kvstore.New(), store)
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}

	return node, store
}

func TestRaftRecoversTermAndVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	node, store := newNode(t, path)

	resp := node.HandleRequestVote(raft.RequestVoteRequest{
		Term:        5,
		CandidateID: "n2",
	})
	if !resp.VoteGranted {
		t.Fatalf("vote was not granted: %+v", resp)
	}
	store.Close()

	recovered, store := newNode(t, path)
	defer store.Close()

	status := recovered.Status()
	if status.Term != 5 {
		t.Errorf("Term = %d, want 5", status.Term)
	}
	if status.VotedFor != "n2" {
		t.Errorf("VotedFor = %q, want \"n2\"", status.VotedFor)
	}
}

func TestRaftRecoversLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	node, store := newNode(t, path)

	resp := node.HandleAppendEntries(raft.AppendEntriesRequest{
		Term:         3,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []raft.LogEntry{
			{Term: 3, Command: []byte("a")},
			{Term: 3, Command: []byte("b")},
		},
	})
	if !resp.Success {
		t.Fatalf("AppendEntries rejected: %+v", resp)
	}
	store.Close()

	recovered, store := newNode(t, path)
	defer store.Close()

	status := recovered.Status()
	if status.Term != 3 {
		t.Errorf("Term = %d, want 3", status.Term)
	}
	if status.LogLength != 3 {
		t.Errorf("LogLength = %d, want 3 (sentinel + 2 entries)", status.LogLength)
	}
}

func TestRaftRecoversTruncatedLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	node, store := newNode(t, path)

	node.HandleAppendEntries(raft.AppendEntriesRequest{
		Term: 3, LeaderID: "n2", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []raft.LogEntry{
			{Term: 3, Command: []byte("a")},
			{Term: 3, Command: []byte("b")},
			{Term: 3, Command: []byte("c")},
		},
	})

	resp := node.HandleAppendEntries(raft.AppendEntriesRequest{
		Term: 4, LeaderID: "n3", PrevLogIndex: 1, PrevLogTerm: 3,
		Entries: []raft.LogEntry{{Term: 4, Command: []byte("z")}},
	})
	if !resp.Success {
		t.Fatalf("conflicting AppendEntries rejected: %+v", resp)
	}
	store.Close()

	recovered, store := newNode(t, path)
	defer store.Close()

	status := recovered.Status()
	if status.LogLength != 3 {
		t.Errorf("LogLength = %d, want 3 (sentinel + a + z)", status.LogLength)
	}
	if status.Term != 4 {
		t.Errorf("Term = %d, want 4", status.Term)
	}
}

func TestRaftStartsCleanWithoutPriorState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	node, store := newNode(t, path)
	defer store.Close()

	status := node.Status()
	if status.Term != 0 || status.VotedFor != "" || status.LogLength != 1 {
		t.Errorf("fresh node status = %+v, want term 0, no vote, sentinel only", status)
	}
}
