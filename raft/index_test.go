package raft

import "testing"

func newTrimmedRaft(snapshotIndex, snapshotTerm uint64, terms []uint64) *Raft {
	r, err := New("n1", []Peer{{ID: "n2"}}, nil, &recordingStateMachine{}, nil)
	if err != nil {
		panic(err)
	}

	r.snapshotIndex = snapshotIndex
	r.snapshotTerm = snapshotTerm
	r.currentTerm = snapshotTerm
	r.commitIndex = snapshotIndex
	r.lastApplied = snapshotIndex

	r.log = []LogEntry{{Term: snapshotTerm}}
	for _, term := range terms {
		r.log = append(r.log, LogEntry{Term: term})
	}

	return r
}

func TestLastLogIndexAccountsForSnapshot(t *testing.T) {
	r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})

	if got := r.lastLogIndexLocked(); got != 103 {
		t.Errorf("lastLogIndex = %d, want 103", got)
	}

	index, term := r.lastLogIndexAndTermLocked()
	if index != 103 || term != 6 {
		t.Errorf("lastLogIndexAndTerm = (%d,%d), want (103,6)", index, term)
	}
}

func TestTermAtRespectsSnapshotBoundary(t *testing.T) {
	r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})

	tests := []struct {
		index    uint64
		wantTerm uint64
		wantOK   bool
	}{
		{index: 99, wantOK: false},
		{index: 100, wantTerm: 4, wantOK: true},
		{index: 101, wantTerm: 5, wantOK: true},
		{index: 103, wantTerm: 6, wantOK: true},
		{index: 104, wantOK: false},
		{index: 5000, wantOK: false},
	}

	for _, tt := range tests {
		term, ok := r.termAtLocked(tt.index)
		if ok != tt.wantOK || (ok && term != tt.wantTerm) {
			t.Errorf("termAt(%d) = (%d,%v), want (%d,%v)",
				tt.index, term, ok, tt.wantTerm, tt.wantOK)
		}
	}
}

func TestAppendEntriesRequestSlicesFromSnapshotOffset(t *testing.T) {
	tests := []struct {
		name         string
		nextIndex    uint64
		wantPrevIdx  uint64
		wantPrevTerm uint64
		wantEntries  int
	}{
		{
			name:         "caught up peer gets a heartbeat",
			nextIndex:    104,
			wantPrevIdx:  103,
			wantPrevTerm: 6,
			wantEntries:  0,
		},
		{
			name:         "lagging peer gets the suffix",
			nextIndex:    102,
			wantPrevIdx:  101,
			wantPrevTerm: 5,
			wantEntries:  2,
		},
		{
			name:         "peer at the snapshot boundary gets everything retained",
			nextIndex:    101,
			wantPrevIdx:  100,
			wantPrevTerm: 4,
			wantEntries:  3,
		},
		{
			name:         "peer behind the snapshot is clamped to the boundary",
			nextIndex:    7,
			wantPrevIdx:  100,
			wantPrevTerm: 4,
			wantEntries:  3,
		},
		{
			name:         "peer ahead of the log is clamped to the end",
			nextIndex:    900,
			wantPrevIdx:  103,
			wantPrevTerm: 6,
			wantEntries:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})
			r.state = Leader
			r.nextIndex["n2"] = tt.nextIndex

			req, ok := r.makeAppendEntriesRequestFor("n2", 4)
			if !ok {
				t.Fatal("leader refused to build a request for its own term")
			}

			if req.PrevLogIndex != tt.wantPrevIdx {
				t.Errorf("PrevLogIndex = %d, want %d", req.PrevLogIndex, tt.wantPrevIdx)
			}
			if req.PrevLogTerm != tt.wantPrevTerm {
				t.Errorf("PrevLogTerm = %d, want %d", req.PrevLogTerm, tt.wantPrevTerm)
			}
			if len(req.Entries) != tt.wantEntries {
				t.Errorf("len(Entries) = %d, want %d", len(req.Entries), tt.wantEntries)
			}
		})
	}
}

func TestHandleAppendEntriesAgainstTrimmedLog(t *testing.T) {
	t.Run("matching at the snapshot boundary is accepted", func(t *testing.T) {
		r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})

		resp := r.HandleAppendEntries(AppendEntriesRequest{
			Term:         6,
			LeaderID:     "n2",
			PrevLogIndex: 100,
			PrevLogTerm:  4,
			Entries:      []LogEntry{{Term: 7}},
		})

		if !resp.Success {
			t.Fatal("append at the snapshot boundary was rejected")
		}
		if got := r.lastLogIndexLocked(); got != 101 {
			t.Errorf("lastLogIndex = %d, want 101 (conflicting suffix replaced)", got)
		}
	})

	t.Run("entries below the snapshot are rejected", func(t *testing.T) {
		r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})

		resp := r.HandleAppendEntries(AppendEntriesRequest{
			Term:         6,
			LeaderID:     "n2",
			PrevLogIndex: 50,
			PrevLogTerm:  2,
		})

		if resp.Success {
			t.Error("append below the snapshot boundary was accepted")
		}
	})

	t.Run("appending past the end extends the log", func(t *testing.T) {
		r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})

		resp := r.HandleAppendEntries(AppendEntriesRequest{
			Term:         6,
			LeaderID:     "n2",
			PrevLogIndex: 103,
			PrevLogTerm:  6,
			Entries:      []LogEntry{{Term: 6}, {Term: 6}},
		})

		if !resp.Success {
			t.Fatal("append past the end was rejected")
		}
		if got := r.lastLogIndexLocked(); got != 105 {
			t.Errorf("lastLogIndex = %d, want 105", got)
		}
	})
}

func TestAdvanceCommitIndexWithSnapshot(t *testing.T) {
	r := newTrimmedRaft(100, 4, []uint64{5, 5, 6})
	r.currentTerm = 6
	r.state = Leader

	r.advancePeerProgress("n2", 103, 6)

	if r.commitIndex != 103 {
		t.Errorf("commitIndex = %d, want 103", r.commitIndex)
	}
}
