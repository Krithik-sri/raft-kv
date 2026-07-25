package raft

import "testing"

func newTestRaft() *Raft {
	r := New("n1", nil, nil)
	r.currentTerm = 1
	r.log = []LogEntry{
		{},
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 1, Command: []byte("c")},
	}
	return r
}

func logTerms(entries []LogEntry) []uint64 {
	terms := make([]uint64, len(entries))
	for i, entry := range entries {
		terms[i] = entry.Term
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

func TestHandleAppendEntries(t *testing.T) {
	tests := []struct {
		name        string
		req         AppendEntriesRequest
		wantSuccess bool
		wantTerms   []uint64
		wantCommit  uint64
	}{
		{
			name:        "stale term is rejected",
			req:         AppendEntriesRequest{Term: 0, PrevLogIndex: 3, PrevLogTerm: 1},
			wantSuccess: false,
			wantTerms:   []uint64{0, 1, 1, 1},
		},
		{
			name:        "prev index past end of log is rejected",
			req:         AppendEntriesRequest{Term: 1, PrevLogIndex: 9, PrevLogTerm: 1},
			wantSuccess: false,
			wantTerms:   []uint64{0, 1, 1, 1},
		},
		{
			name:        "prev term mismatch is rejected",
			req:         AppendEntriesRequest{Term: 1, PrevLogIndex: 3, PrevLogTerm: 9},
			wantSuccess: false,
			wantTerms:   []uint64{0, 1, 1, 1},
		},
		{
			name:        "heartbeat leaves log untouched",
			req:         AppendEntriesRequest{Term: 1, PrevLogIndex: 3, PrevLogTerm: 1},
			wantSuccess: true,
			wantTerms:   []uint64{0, 1, 1, 1},
		},
		{
			name: "new entry is appended",
			req: AppendEntriesRequest{
				Term: 1, PrevLogIndex: 3, PrevLogTerm: 1,
				Entries: []LogEntry{{Term: 1, Command: []byte("d")}},
			},
			wantSuccess: true,
			wantTerms:   []uint64{0, 1, 1, 1, 1},
		},
		{
			name: "duplicate delivery does not truncate later entries",
			req: AppendEntriesRequest{
				Term: 1, PrevLogIndex: 1, PrevLogTerm: 1,
				Entries: []LogEntry{{Term: 1, Command: []byte("b")}},
			},
			wantSuccess: true,
			wantTerms:   []uint64{0, 1, 1, 1},
		},
		{
			name: "conflicting entry truncates the suffix",
			req: AppendEntriesRequest{
				Term: 1, PrevLogIndex: 1, PrevLogTerm: 1,
				Entries: []LogEntry{{Term: 2, Command: []byte("x")}},
			},
			wantSuccess: true,
			wantTerms:   []uint64{0, 1, 2},
		},
		{
			name: "leader commit is clamped to last new entry",
			req: AppendEntriesRequest{
				Term: 1, PrevLogIndex: 3, PrevLogTerm: 1,
				Entries:      []LogEntry{{Term: 1, Command: []byte("d")}},
				LeaderCommit: 99,
			},
			wantSuccess: true,
			wantTerms:   []uint64{0, 1, 1, 1, 1},
			wantCommit:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRaft()

			resp := r.HandleAppendEntries(tt.req)

			if resp.Success != tt.wantSuccess {
				t.Errorf("Success = %v, want %v", resp.Success, tt.wantSuccess)
			}

			got := logTerms(r.log)
			if !equalTerms(got, tt.wantTerms) {
				t.Errorf("log terms = %v, want %v", got, tt.wantTerms)
			}

			if r.commitIndex != tt.wantCommit {
				t.Errorf("commitIndex = %d, want %d", r.commitIndex, tt.wantCommit)
			}
		})
	}
}
