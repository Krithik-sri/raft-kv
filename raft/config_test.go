package raft

import "testing"

func TestConfigurationRoundTrips(t *testing.T) {
	want := Configuration{Members: []Member{
		{ID: "n1", Address: "localhost:1", Voter: true},
		{ID: "n2", Address: "localhost:2", Voter: false},
	}}

	encoded, err := encodeConfiguration(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := decodeConfiguration(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Members) != len(want.Members) {
		t.Fatalf("got %d members, want %d", len(got.Members), len(want.Members))
	}
	for i, m := range got.Members {
		if m != want.Members[i] {
			t.Errorf("member %d = %+v, want %+v", i, m, want.Members[i])
		}
	}
}

// The whole reason learners exist. A machine that is still catching up must not
// make the majority harder to reach while it does.
func TestMajorityIgnoresLearners(t *testing.T) {
	config := Configuration{Members: []Member{
		{ID: "n1", Voter: true},
		{ID: "n2", Voter: true},
		{ID: "n3", Voter: true},
		{ID: "n4", Voter: false},
		{ID: "n5", Voter: false},
	}}

	if voters := config.voters(); voters != 3 {
		t.Errorf("voters = %d, want 3", voters)
	}

	r := &Raft{config: config}
	if got := r.majorityLocked(); got != 2 {
		t.Errorf("majority = %d, want 2: two learners made the cluster harder to run", got)
	}

	if config.isVoter("n4") {
		t.Error("a learner reported itself as a voter")
	}
	if !config.isVoter("n1") {
		t.Error("a voter reported itself as a learner")
	}
}

// Learners still get the log shipped to them, so they belong in the peer list
// even though they don't vote.
func TestPeersExcludeSelfButKeepLearners(t *testing.T) {
	config := Configuration{Members: []Member{
		{ID: "n1", Address: "a", Voter: true},
		{ID: "n2", Address: "b", Voter: true},
		{ID: "n3", Address: "c", Voter: false},
	}}

	peers := config.peers("n1")
	if len(peers) != 2 {
		t.Fatalf("got %d peers, want 2", len(peers))
	}
	for _, p := range peers {
		if p.ID == "n1" {
			t.Error("we are in our own peer list")
		}
	}
	if peers[1].ID != "n3" || peers[1].Address != "c" {
		t.Errorf("learner missing from the peer list: %+v", peers)
	}
}

// A configuration entry in the log beats the list this node was started with,
// which is what lets membership survive a restart.
func TestLogConfigurationWinsOverBootstrap(t *testing.T) {
	r, err := New(Peer{ID: "n1"}, []Peer{{ID: "n2"}}, nil, &recordingStateMachine{}, &memStorage{})
	if err != nil {
		t.Fatal(err)
	}

	if got := len(r.config.Members); got != 2 {
		t.Fatalf("bootstrap gave %d members, want 2", got)
	}

	added := Configuration{Members: []Member{
		{ID: "n1", Voter: true},
		{ID: "n2", Voter: true},
		{ID: "n3", Voter: true},
	}}

	encoded, err := encodeConfiguration(added)
	if err != nil {
		t.Fatal(err)
	}

	r.log = append(r.log, LogEntry{Term: 1, Kind: EntryConfig, Command: encoded})
	r.refreshConfigLocked(configurationFromPeers("n1", "", []Peer{{ID: "n2"}}), 0)

	if got := len(r.config.Members); got != 3 {
		t.Errorf("config has %d members, want 3: the log entry was ignored", got)
	}
	if r.configIndex != 1 {
		t.Errorf("configIndex = %d, want 1", r.configIndex)
	}
}

// A machine that leaves should not leave its bookkeeping behind, or it arrives
// back with stale progress that could be counted towards a majority.
func TestAdoptingConfigForgetsDepartedMembers(t *testing.T) {
	r, err := New(Peer{ID: "n1"}, []Peer{{ID: "n2"}, {ID: "n3"}}, nil, &recordingStateMachine{}, &memStorage{})
	if err != nil {
		t.Fatal(err)
	}

	r.matchIndex["n3"] = 9
	r.nextIndex["n3"] = 10
	r.ackedBarrier["n3"] = 4

	r.adoptConfigLocked(Configuration{Members: []Member{
		{ID: "n1", Voter: true},
		{ID: "n2", Voter: true},
	}}, 7)

	if _, ok := r.matchIndex["n3"]; ok {
		t.Error("matchIndex kept a machine that left the cluster")
	}
	if _, ok := r.ackedBarrier["n3"]; ok {
		t.Error("ackedBarrier kept a machine that left the cluster")
	}
	if r.configIndex != 7 {
		t.Errorf("configIndex = %d, want 7", r.configIndex)
	}
}

// A configuration entry has a payload, so the old "skip empty commands" check
// would have handed it to the store, which has never heard of it.
func TestConfigEntriesNeverReachTheStateMachine(t *testing.T) {
	machine := &recordingStateMachine{}

	r, err := New(Peer{ID: "n1"}, nil, nil, machine, &memStorage{})
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := encodeConfiguration(Configuration{Members: []Member{{ID: "n1", Voter: true}}})
	if err != nil {
		t.Fatal(err)
	}

	r.log = append(r.log,
		LogEntry{Term: 1, Kind: EntryCommand, Command: []byte("real command")},
		LogEntry{Term: 1, Kind: EntryConfig, Command: encoded},
		LogEntry{Term: 1, Kind: EntryCommand, Command: []byte("another one")},
	)
	r.commitIndex = 3

	r.applyCommitted()

	applied := machine.snapshot()
	if len(applied) != 2 {
		t.Fatalf("applied %d entries, want 2: %v", len(applied), applied)
	}
	for _, cmd := range applied {
		if cmd == string(encoded) {
			t.Error("a configuration entry was applied to the store")
		}
	}

	if r.lastApplied != 3 {
		t.Errorf("lastApplied = %d, want 3: skipping an entry must not stall the log", r.lastApplied)
	}
}
