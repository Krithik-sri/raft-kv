package raft

import (
	"context"
	"errors"
	"time"
)

var (
	ErrConfigChangeInFlight = errors.New("a configuration change is already in progress")
	ErrLeaderNotCaughtUp    = errors.New("leader has not committed an entry in its own term yet")
	ErrAlreadyMember        = errors.New("already a member of the cluster")
	ErrNotMember            = errors.New("not a member of the cluster")
	ErrLastVoter            = errors.New("cannot remove the last voter")
)

// Membership changes, one machine at a time, from §4.1 of Ongaro's thesis
// rather than the joint consensus in §6 of the paper. Adding or removing a
// single machine can never split the old majority from the new one, so none of
// the two-phase Cold,new machinery is needed. It is what etcd does and there is
// a great deal less to get wrong.
//
// Two rules do the load bearing here, and both are easy to leave out:
//
// Only one change may be uncommitted at a time. With two in flight you cannot
// say which majority you need, because you do not yet know which of them is
// real.
//
// The leader must have committed an entry from its own term first. The original
// write-up of one-at-a-time missed this and it is genuinely unsafe without it: a
// leader that has committed nothing of its own does not reliably know where the
// commit point is, and can shrink the cluster out from under an entry that was
// already acknowledged.

// AddServer brings a machine in as a learner, waits for it to catch up, and only
// then gives it a vote.
//
// The catch-up matters more than it looks. Going straight to a voter makes the
// majority larger the instant the entry is appended, while the new machine is
// still empty and useless. On a cluster that is already one node down, that is
// the difference between a brief pause and an outage that lasts as long as a
// full snapshot transfer.
func (r *Raft) AddServer(ctx context.Context, member Peer) error {
	addIndex, term, err := r.beginConfigChange(func(c Configuration) (Configuration, error) {
		if c.contains(member.ID) {
			return Configuration{}, ErrAlreadyMember
		}

		next := c.clone()
		next.Members = append(next.Members, Member{
			ID:      member.ID,
			Address: member.Address,
			Voter:   false,
		})

		return next, nil
	})
	if err != nil {
		return err
	}

	r.logger.Info("added learner", "member", member.ID, "index", addIndex)

	if err := r.waitForCommit(ctx, addIndex, term); err != nil {
		return err
	}

	if err := r.waitForCatchUp(ctx, member.ID, addIndex, term); err != nil {
		return err
	}

	promoteIndex, term, err := r.beginConfigChange(func(c Configuration) (Configuration, error) {
		if !c.contains(member.ID) {
			return Configuration{}, ErrNotMember
		}

		next := c.clone()
		for i := range next.Members {
			if next.Members[i].ID == member.ID {
				next.Members[i].Voter = true
			}
		}

		return next, nil
	})
	if err != nil {
		return err
	}

	r.logger.Info("promoted learner to voter", "member", member.ID, "index", promoteIndex)

	return r.waitForCommit(ctx, promoteIndex, term)
}

// RemoveServer takes a machine out of the cluster. Removing the leader is
// allowed; it keeps running the cluster until the new configuration commits and
// then stands down, because by then it is not a member of anything.
func (r *Raft) RemoveServer(ctx context.Context, id NodeID) error {
	index, term, err := r.beginConfigChange(func(c Configuration) (Configuration, error) {
		if !c.contains(id) {
			return Configuration{}, ErrNotMember
		}

		next := Configuration{}
		for _, m := range c.Members {
			if m.ID != id {
				next.Members = append(next.Members, m)
			}
		}

		if next.voters() == 0 {
			return Configuration{}, ErrLastVoter
		}

		return next, nil
	})
	if err != nil {
		return err
	}

	r.logger.Info("removing member", "member", id, "index", index)

	if err := r.waitForCommit(ctx, index, term); err != nil {
		return err
	}

	r.mu.Lock()
	steppingDown := r.state == Leader && !r.config.contains(r.id)
	current := r.currentTerm
	r.mu.Unlock()

	if steppingDown {
		r.logger.Info("removed self from the cluster, stepping down", "term", current)
		r.becomeFollower(current)
	}

	return nil
}

// Configuration returns the membership this node is currently using.
func (r *Raft) Configuration() Configuration {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.config.clone()
}

func (r *Raft) beginConfigChange(
	mutate func(Configuration) (Configuration, error),
) (uint64, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}

	if r.configIndex > r.commitIndex {
		return 0, 0, ErrConfigChangeInFlight
	}

	if term, ok := r.termAtLocked(r.commitIndex); !ok || term != r.currentTerm {
		return 0, 0, ErrLeaderNotCaughtUp
	}

	next, err := mutate(r.config)
	if err != nil {
		return 0, 0, err
	}

	index, err := r.appendConfigLocked(next)
	if err != nil {
		return 0, 0, err
	}

	return index, r.currentTerm, nil
}

// appendConfigLocked writes a configuration into the log and starts using it
// straight away, before it commits. That is the rule from §4.1 and it is the
// part that reads like a mistake.
func (r *Raft) appendConfigLocked(config Configuration) (uint64, error) {
	encoded, err := EncodeConfiguration(config)
	if err != nil {
		return 0, err
	}

	r.log = append(r.log, LogEntry{
		Term:    r.currentTerm,
		Kind:    EntryConfig,
		Command: encoded,
	})

	index := r.lastLogIndexLocked()

	r.adoptConfigLocked(config, index)
	r.trackNewMembersLocked()

	r.signalPersist()
	r.signalReplicate()

	return index, nil
}

func (r *Raft) trackNewMembersLocked() {
	lastLogIndex := r.lastLogIndexLocked()
	now := time.Now()

	for _, m := range r.config.Members {
		if m.ID == r.id {
			continue
		}
		if _, known := r.nextIndex[m.ID]; known {
			continue
		}

		r.nextIndex[m.ID] = lastLogIndex + 1
		r.matchIndex[m.ID] = 0
		r.lastAck[m.ID] = now
	}
}

func (r *Raft) waitForCommit(ctx context.Context, index, term uint64) error {
	return r.waitFor(ctx, func() (bool, error) {
		if r.state != Leader || r.currentTerm != term {
			return false, ErrLostLeadership
		}

		return r.commitIndex >= index, nil
	})
}

// waitForCatchUp blocks until the new machine has the log up to the point it
// joined at. Anything written after that it will keep streaming on its own.
func (r *Raft) waitForCatchUp(ctx context.Context, id NodeID, target, term uint64) error {
	return r.waitFor(ctx, func() (bool, error) {
		if r.state != Leader || r.currentTerm != term {
			return false, ErrLostLeadership
		}

		return r.matchIndex[id] >= target, nil
	})
}
