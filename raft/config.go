package raft

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Who is in the cluster is not a startup flag any more. It lives in the log as
// an ordinary entry, which is what lets it change while the cluster is running.
//
// The rule that catches people out: a node follows the newest configuration in
// its log whether or not that entry is committed. It sounds reckless. It is
// what makes the whole thing work. A configuration that is waiting to commit is
// waiting on a majority, and you cannot ask the right majority unless you are
// already using the new membership.
//
// Everything here is guarded by r.mu. r.config used to be an immutable slice
// that anything could read at any time, and three call sites did exactly that.
// Now that it changes, those reads have to take the lock or copy first.

func (c Configuration) clone() Configuration {
	return Configuration{Members: slices.Clone(c.Members)}
}

func (c Configuration) contains(id NodeID) bool {
	return slices.ContainsFunc(c.Members, func(m Member) bool { return m.ID == id })
}

func (c Configuration) isVoter(id NodeID) bool {
	for _, m := range c.Members {
		if m.ID == id {
			return m.Voter
		}
	}
	return false
}

func (c Configuration) voters() int {
	count := 0
	for _, m := range c.Members {
		if m.Voter {
			count++
		}
	}
	return count
}

// peers is everyone except us, voters and learners alike. Learners are in here
// because they still need the log shipped to them.
func (c Configuration) peers(self NodeID) []Peer {
	peers := make([]Peer, 0, len(c.Members))
	for _, m := range c.Members {
		if m.ID == self {
			continue
		}
		peers = append(peers, Peer{ID: m.ID, Address: m.Address})
	}
	return peers
}

func configurationFromPeers(self NodeID, address string, peers []Peer) Configuration {
	members := make([]Member, 0, len(peers)+1)
	members = append(members, Member{ID: self, Address: address, Voter: true})

	for _, p := range peers {
		members = append(members, Member{ID: p.ID, Address: p.Address, Voter: true})
	}

	return Configuration{Members: members}
}

// configurationForJoiner is the starting point for a machine being added to a
// cluster that already exists. It lists itself as a learner, which keeps it from
// campaigning or counting itself before the cluster has actually admitted it.
// The real membership arrives soon after, in the log or in a snapshot.
func configurationForJoiner(self NodeID, address string, peers []Peer) Configuration {
	members := make([]Member, 0, len(peers)+1)
	members = append(members, Member{ID: self, Address: address, Voter: false})

	for _, p := range peers {
		members = append(members, Member{ID: p.ID, Address: p.Address, Voter: true})
	}

	return Configuration{Members: members}
}

func EncodeConfiguration(c Configuration) ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encode configuration: %w", err)
	}
	return data, nil
}

func DecodeConfiguration(data []byte) (Configuration, error) {
	var c Configuration
	if err := json.Unmarshal(data, &c); err != nil {
		return Configuration{}, fmt.Errorf("decode configuration: %w", err)
	}
	return c, nil
}

// currentPeers copies the peer list out from under the lock. Callers that fan
// out over the network use this, because holding r.mu across an RPC is how this
// codebase earned most of its scar tissue.
func (r *Raft) currentPeers() []Peer {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.peersLocked()
}

func (r *Raft) peersLocked() []Peer {
	return r.config.peers(r.id)
}

func (r *Raft) majorityLocked() int {
	return r.config.voters()/2 + 1
}

// adoptConfigLocked switches to a configuration found at the given log index.
// Peers that are no longer members lose their progress tracking, so a machine
// that leaves and comes back does not arrive with stale bookkeeping.
func (r *Raft) adoptConfigLocked(config Configuration, index uint64) {
	r.config = config.clone()
	r.configIndex = index

	for id := range r.nextIndex {
		if !config.contains(id) {
			delete(r.nextIndex, id)
			delete(r.matchIndex, id)
			delete(r.ackedBarrier, id)
			delete(r.lastAck, id)
		}
	}
}

// configAsOfLocked is the membership as it stood at the given index. That is
// not the same as the current one: a configuration change that has been
// appended but not committed lives above the commit point, and a snapshot must
// not claim it.
func (r *Raft) configAsOfLocked(index uint64) Configuration {
	config := r.baseConfig

	for offset, entry := range r.log {
		if offset == 0 || entry.Kind != EntryConfig {
			continue
		}
		if r.snapshotIndex+uint64(offset) > index {
			break
		}

		decoded, err := DecodeConfiguration(entry.Command)
		if err != nil {
			continue
		}

		config = decoded
	}

	return config
}

// refreshConfigLocked walks the log for the newest configuration entry and
// adopts it. Used on startup and after the log is repaired by a truncation,
// which can take a configuration back out again.
func (r *Raft) refreshConfigLocked(base Configuration, baseIndex uint64) {
	config, index := base, baseIndex

	for offset, entry := range r.log {
		if offset == 0 || entry.Kind != EntryConfig {
			continue
		}

		decoded, err := DecodeConfiguration(entry.Command)
		if err != nil {
			r.logger.Error("ignoring unreadable configuration entry", "err", err)
			continue
		}

		config, index = decoded, r.snapshotIndex+uint64(offset)
	}

	r.adoptConfigLocked(config, index)
}
