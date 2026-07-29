package raft

import "testing"

func newTestLeader(terms []uint64, currentTerm uint64) *Raft {
	peers := []Peer{{ID: "n2"}, {ID: "n3"}}
	r, err := New("n1", peers, nil, &recordingStateMachine{}, &memStorage{})
	if err != nil {
		panic(err)
	}
	r.currentTerm = currentTerm
	r.state = Leader

	entries := make([]LogEntry, len(terms))
	for i, term := range terms {
		entries[i] = LogEntry{Term: term}
	}
	r.log = entries

	for _, peer := range peers {
		r.nextIndex[peer.ID] = uint64(len(entries))
		r.matchIndex[peer.ID] = 0
	}
	return r
}

func TestAdvanceCommitIndex(t *testing.T) {
	tests := []struct {
		name       string
		terms      []uint64
		term       uint64
		peerMatch  uint64
		wantCommit uint64
	}{
		{
			name:       "previous term entry is not committed by replica count",
			terms:      []uint64{0, 1},
			term:       2,
			peerMatch:  1,
			wantCommit: 0,
		},
		{
			name:       "current term entry commits with a majority",
			terms:      []uint64{0, 1, 2},
			term:       2,
			peerMatch:  2,
			wantCommit: 2,
		},
		{
			name:       "committing a current term entry commits earlier terms",
			terms:      []uint64{0, 1, 1, 2},
			term:       2,
			peerMatch:  3,
			wantCommit: 3,
		},
		{
			name:       "no majority means no commit",
			terms:      []uint64{0, 1, 2},
			term:       2,
			peerMatch:  0,
			wantCommit: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestLeader(tt.terms, tt.term)

			r.advancePeerProgress("n2", tt.peerMatch, tt.term, 0)

			if r.commitIndex != tt.wantCommit {
				t.Errorf("commitIndex = %d, want %d", r.commitIndex, tt.wantCommit)
			}
		})
	}
}

func TestMakeAppendEntriesRequestFor(t *testing.T) {
	tests := []struct {
		name         string
		nextIndex    uint64
		setNext      bool
		wantPrevIdx  uint64
		wantPrevTerm uint64
		wantEntries  int
	}{
		{
			name:         "caught up peer gets a heartbeat",
			nextIndex:    4,
			setNext:      true,
			wantPrevIdx:  3,
			wantPrevTerm: 2,
			wantEntries:  0,
		},
		{
			name:         "lagging peer gets the missing suffix",
			nextIndex:    2,
			setNext:      true,
			wantPrevIdx:  1,
			wantPrevTerm: 1,
			wantEntries:  2,
		},
		{
			name:         "peer rewound to start gets the whole log",
			nextIndex:    1,
			setNext:      true,
			wantPrevIdx:  0,
			wantPrevTerm: 0,
			wantEntries:  3,
		},
		{
			name:         "unknown peer does not underflow prev index",
			setNext:      false,
			wantPrevIdx:  0,
			wantPrevTerm: 0,
			wantEntries:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestLeader([]uint64{0, 1, 1, 2}, 2)
			if tt.setNext {
				r.nextIndex["n2"] = tt.nextIndex
			} else {
				delete(r.nextIndex, "n2")
			}

			req, ok := r.makeAppendEntriesRequestFor("n2", 2)
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

func TestMakeAppendEntriesRequestForCopiesEntries(t *testing.T) {
	r := newTestLeader([]uint64{0, 1, 1, 2}, 2)
	r.nextIndex["n2"] = 1

	req, ok := r.makeAppendEntriesRequestFor("n2", 2)
	if !ok {
		t.Fatal("leader refused to build a request for its own term")
	}
	r.log[1].Term = 99

	if req.Entries[0].Term != 1 {
		t.Errorf("request aliases the leader log: entry term became %d", req.Entries[0].Term)
	}
}
