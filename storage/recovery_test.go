package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/krithik-sri/raft-kv/kvstore"
	"github.com/krithik-sri/raft-kv/raft"
)

func newNode(t *testing.T, path string) (*raft.Raft, *File) {
	t.Helper()

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	node, err := raft.New(raft.Peer{ID: "n1"}, nil, nil, kvstore.New(), store)
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

func TestSnapshotSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	machine := kvstore.New()
	node, err := raft.New(raft.Peer{ID: "n1"}, nil, nil, machine, store)
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}

	entries := make([]raft.LogEntry, 0, 120)
	for i := 0; i < 120; i++ {
		command, err := kvstore.EncodeCommand(kvstore.Command{
			Op: kvstore.OpPut, Key: "k", Value: "v",
			ClientID: "c1", Seq: uint64(i + 1),
		})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		entries = append(entries, raft.LogEntry{Term: 1, Command: command})
	}

	resp := node.HandleAppendEntries(raft.AppendEntriesRequest{
		Term: 1, LeaderID: "n2", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: entries, LeaderCommit: 120,
	})
	if !resp.Success {
		t.Fatalf("AppendEntries rejected: %+v", resp)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go node.Start(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if node.Status().SnapshotIndex > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	before := node.Status()
	cancel()

	if before.SnapshotIndex == 0 {
		store.Close()
		t.Fatal("node never snapshotted")
	}
	store.Close()

	recovered, store := newNode(t, path)
	defer store.Close()

	after := recovered.Status()
	if after.SnapshotIndex != before.SnapshotIndex {
		t.Errorf("SnapshotIndex = %d, want %d", after.SnapshotIndex, before.SnapshotIndex)
	}
	if after.LastLogIndex != before.LastLogIndex {
		t.Errorf("LastLogIndex = %d, want %d", after.LastLogIndex, before.LastLogIndex)
	}
	if after.LastApplied != before.SnapshotIndex {
		t.Errorf("LastApplied = %d, want %d", after.LastApplied, before.SnapshotIndex)
	}
}

func TestSnapshotRestoresDedupSessions(t *testing.T) {
	machine := kvstore.New()

	for i := 1; i <= 3; i++ {
		command, err := kvstore.EncodeCommand(kvstore.Command{
			Op: kvstore.OpPut, Key: "k", Value: fmt.Sprintf("v%d", i),
			ClientID: "c1", Seq: uint64(i),
		})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := machine.Apply(command); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	image, err := machine.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := kvstore.New()
	if err := restored.Restore(image); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if value, _ := restored.Get("k"); value != "v3" {
		t.Errorf("restored value = %q, want \"v3\"", value)
	}

	replay, err := kvstore.EncodeCommand(kvstore.Command{
		Op: kvstore.OpPut, Key: "k", Value: "REPLAYED",
		ClientID: "c1", Seq: 2,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := restored.Apply(replay); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if value, _ := restored.Get("k"); value != "v3" {
		t.Errorf("value = %q after replaying seq 2; snapshot lost the dedup sessions", value)
	}
}
