package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/krithik-sri/raft-kv/raft"
)

func openTemp(t *testing.T) (*File, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "raft.log")

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	return store, path
}

func reopen(t *testing.T, path string) *File {
	t.Helper()

	store, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	return store
}

func mustLoad(t *testing.T, store *File) raft.PersistentState {
	t.Helper()

	state, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return state
}

func logTerms(entries []raft.LogEntry) []uint64 {
	terms := make([]uint64, len(entries))
	for i, e := range entries {
		terms[i] = e.Term
	}
	return terms
}

func equalTerms(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFreshFileLoadsEmptyState(t *testing.T) {
	store, _ := openTemp(t)

	state := mustLoad(t, store)

	if state.CurrentTerm != 0 || state.VotedFor != "" || len(state.Log) != 0 {
		t.Errorf("fresh state = %+v, want zero values", state)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	store, path := openTemp(t)

	if err := store.SaveState(7, "n2"); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	store.Close()

	state := mustLoad(t, reopen(t, path))

	if state.CurrentTerm != 7 {
		t.Errorf("CurrentTerm = %d, want 7", state.CurrentTerm)
	}
	if state.VotedFor != "n2" {
		t.Errorf("VotedFor = %q, want \"n2\"", state.VotedFor)
	}
}

func TestLatestStateWins(t *testing.T) {
	store, path := openTemp(t)

	for term := uint64(1); term <= 5; term++ {
		if err := store.SaveState(term, "n1"); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
	}
	if err := store.SaveState(6, ""); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	store.Close()

	state := mustLoad(t, reopen(t, path))

	if state.CurrentTerm != 6 {
		t.Errorf("CurrentTerm = %d, want 6", state.CurrentTerm)
	}
	if state.VotedFor != "" {
		t.Errorf("VotedFor = %q, want empty", state.VotedFor)
	}
}

func TestLogSurvivesReopen(t *testing.T) {
	store, path := openTemp(t)

	if err := store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
	}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	if err := store.AppendLog([]raft.LogEntry{{Term: 2, Command: []byte("c")}}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	store.Close()

	state := mustLoad(t, reopen(t, path))

	if got := logTerms(state.Log); !equalTerms(got, []uint64{1, 1, 2}) {
		t.Fatalf("log terms = %v, want [1 1 2]", got)
	}
	if string(state.Log[2].Command) != "c" {
		t.Errorf("last command = %q, want \"c\"", state.Log[2].Command)
	}
}

func TestTruncateSurvivesReopen(t *testing.T) {
	store, path := openTemp(t)

	if err := store.AppendLog([]raft.LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 1, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}

	if err := store.TruncateLog(2); err != nil {
		t.Fatalf("TruncateLog: %v", err)
	}

	if err := store.AppendLog([]raft.LogEntry{{Term: 3, Command: []byte("z")}}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	store.Close()

	state := mustLoad(t, reopen(t, path))

	if got := logTerms(state.Log); !equalTerms(got, []uint64{1, 3}) {
		t.Fatalf("log terms = %v, want [1 3]", got)
	}
	if string(state.Log[1].Command) != "z" {
		t.Errorf("last command = %q, want \"z\"", state.Log[1].Command)
	}
}

func TestTornFinalRecordIsIgnored(t *testing.T) {
	store, path := openTemp(t)

	if err := store.SaveState(3, "n1"); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := store.AppendLog([]raft.LogEntry{{Term: 3, Command: []byte("good")}}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	store.Close()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen for corruption: %v", err)
	}
	if _, err := file.Write([]byte{0x00, 0x00, 0x01, 0x2c, 'h', 'a', 'l', 'f'}); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	file.Close()

	state := mustLoad(t, reopen(t, path))

	if state.CurrentTerm != 3 {
		t.Errorf("CurrentTerm = %d, want 3", state.CurrentTerm)
	}
	if got := logTerms(state.Log); !equalTerms(got, []uint64{3}) {
		t.Errorf("log terms = %v, want [3]", got)
	}
}

func TestWritesAppendAfterLoad(t *testing.T) {
	store, path := openTemp(t)

	if err := store.AppendLog([]raft.LogEntry{{Term: 1, Command: []byte("a")}}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	store.Close()

	reopened := reopen(t, path)
	mustLoad(t, reopened)

	if err := reopened.AppendLog([]raft.LogEntry{{Term: 2, Command: []byte("b")}}); err != nil {
		t.Fatalf("AppendLog after load: %v", err)
	}

	if got := logTerms(mustLoad(t, reopened).Log); !equalTerms(got, []uint64{1, 2}) {
		t.Errorf("log terms = %v, want [1 2]", got)
	}
}
