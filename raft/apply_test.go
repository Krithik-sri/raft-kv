package raft

import (
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
)

type recordingStateMachine struct {
	mu       sync.Mutex
	commands []string
	failOn   string
}

var _ StateMachine = (*recordingStateMachine)(nil)

func (m *recordingStateMachine) Apply(command []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failOn != "" && string(command) == m.failOn {
		return nil, errors.New("apply rejected")
	}

	m.commands = append(m.commands, string(command))
	return []byte("ok:" + string(command)), nil
}

func (m *recordingStateMachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return json.Marshal(m.commands)
}

func (m *recordingStateMachine) Restore(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.commands = nil

	return json.Unmarshal(data, &m.commands)
}

func (m *recordingStateMachine) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]string, len(m.commands))
	copy(out, m.commands)
	return out
}

func (m *recordingStateMachine) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.commands)
}

func newTestApplier(commands []string, commitIndex uint64) (*Raft, *recordingStateMachine) {
	machine := &recordingStateMachine{}
	r, err := New("n1", nil, nil, machine, &memStorage{})
	if err != nil {
		panic(err)
	}
	r.currentTerm = 1

	entries := []LogEntry{{}}
	for _, command := range commands {
		entries = append(entries, LogEntry{Term: 1, Command: []byte(command)})
	}
	r.log = entries
	r.commitIndex = commitIndex

	return r, machine
}

func TestApplyCommitted(t *testing.T) {
	tests := []struct {
		name        string
		commands    []string
		commitIndex uint64
		wantApplied []string
	}{
		{
			name:        "nothing committed applies nothing",
			commands:    []string{"a", "b", "c"},
			commitIndex: 0,
			wantApplied: []string{},
		},
		{
			name:        "applies up to commit index in order",
			commands:    []string{"a", "b", "c"},
			commitIndex: 2,
			wantApplied: []string{"a", "b"},
		},
		{
			name:        "applies the whole committed log",
			commands:    []string{"a", "b", "c"},
			commitIndex: 3,
			wantApplied: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, machine := newTestApplier(tt.commands, tt.commitIndex)

			r.applyCommitted()

			if got := machine.snapshot(); !slices.Equal(got, tt.wantApplied) {
				t.Errorf("applied = %v, want %v", got, tt.wantApplied)
			}

			if r.lastApplied != tt.commitIndex {
				t.Errorf("lastApplied = %d, want %d", r.lastApplied, tt.commitIndex)
			}
		})
	}
}

func TestApplyCommittedIsIdempotent(t *testing.T) {
	r, machine := newTestApplier([]string{"a", "b"}, 2)

	r.applyCommitted()
	r.applyCommitted()

	if machine.count() != 2 {
		t.Fatalf("applied %d commands after two passes, want 2", machine.count())
	}
}

func TestApplyCommittedResumesAfterCommitAdvances(t *testing.T) {
	r, machine := newTestApplier([]string{"a", "b", "c"}, 1)

	r.applyCommitted()
	r.commitIndex = 3
	r.applyCommitted()

	want := []string{"a", "b", "c"}
	if got := machine.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("applied = %v, want %v", got, want)
	}
}

func TestApplyCommittedSkipsFailedEntry(t *testing.T) {
	r, machine := newTestApplier([]string{"a", "bad", "c"}, 3)
	machine.failOn = "bad"

	r.applyCommitted()

	want := []string{"a", "c"}
	if got := machine.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("applied = %v, want %v", got, want)
	}

	if r.lastApplied != 3 {
		t.Errorf("lastApplied = %d, want 3", r.lastApplied)
	}
}
