package raft

import (
	"bytes"
	"testing"
)

func newTestApplier(commands []string, commitIndex uint64) *Raft {
	r := New("n1", nil, nil)
	r.currentTerm = 1

	entries := []LogEntry{{}}
	for _, command := range commands {
		entries = append(entries, LogEntry{Term: 1, Command: []byte(command)})
	}
	r.log = entries
	r.commitIndex = commitIndex

	return r
}

func appliedStrings(applied [][]byte) []string {
	out := make([]string, len(applied))
	for i, command := range applied {
		out[i] = string(command)
	}
	return out
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
			r := newTestApplier(tt.commands, tt.commitIndex)

			r.applyCommitted()

			got := appliedStrings(r.applied)
			if len(got) != len(tt.wantApplied) {
				t.Fatalf("applied = %v, want %v", got, tt.wantApplied)
			}
			for i := range got {
				if got[i] != tt.wantApplied[i] {
					t.Fatalf("applied = %v, want %v", got, tt.wantApplied)
				}
			}

			if r.lastApplied != tt.commitIndex {
				t.Errorf("lastApplied = %d, want %d", r.lastApplied, tt.commitIndex)
			}
		})
	}
}

func TestApplyCommittedIsIdempotent(t *testing.T) {
	r := newTestApplier([]string{"a", "b"}, 2)

	r.applyCommitted()
	r.applyCommitted()

	if len(r.applied) != 2 {
		t.Fatalf("applied %d entries after two passes, want 2", len(r.applied))
	}
}

func TestApplyCommittedResumesAfterCommitAdvances(t *testing.T) {
	r := newTestApplier([]string{"a", "b", "c"}, 1)

	r.applyCommitted()
	r.commitIndex = 3
	r.applyCommitted()

	want := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	if len(r.applied) != len(want) {
		t.Fatalf("applied = %v, want %v", appliedStrings(r.applied), appliedStrings(want))
	}
	for i := range want {
		if !bytes.Equal(r.applied[i], want[i]) {
			t.Fatalf("applied = %v, want %v", appliedStrings(r.applied), appliedStrings(want))
		}
	}
}
