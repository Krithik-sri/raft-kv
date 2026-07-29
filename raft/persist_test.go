package raft

import "testing"

// The rule that makes batched flushing safe. An entry sitting in memory is not
// an entry the leader is allowed to count for itself, because a crash would
// turn the majority that committed it into a minority.
func TestUnpersistedEntryDoesNotCommit(t *testing.T) {
	r := newTestLeader([]uint64{0, 1, 2}, 2)

	// Index 2 is in the log but has not reached the disk yet.
	r.persistedIndex = 1

	// Both followers have it. Without the leader that is one of three, and a
	// majority here is two.
	r.advancePeerProgress("n2", 2, 2, 0)

	if r.commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0: the leader counted a copy that is not on disk", r.commitIndex)
	}

	// Same cluster, same acks, but now our own copy is durable.
	r.persistedUpToLocked()
	r.advancePeerProgress("n2", 2, 2, 0)

	if r.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 once the entry is durable", r.commitIndex)
	}
}

// A follower answering is worth counting on its own, because followers still
// write synchronously before they reply.
func TestQuorumOfFollowersCommitsWithoutTheLeader(t *testing.T) {
	r := newTestLeader([]uint64{0, 1, 2}, 2)
	r.persistedIndex = 1

	r.advancePeerProgress("n2", 2, 2, 0)
	r.advancePeerProgress("n3", 2, 2, 0)

	if r.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2: two of three followers is a majority", r.commitIndex)
	}
}
