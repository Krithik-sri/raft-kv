package storage

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/krithik-sri/raft-kv/raft"
)

// Log files written before configurations existed have no "kind" on entries and
// no "config" on the compaction record. They still have to load, and every
// entry in them has to come back as an ordinary command rather than as
// something the cluster might mistake for a membership change.
func TestLoadsLogWrittenBeforeConfigurations(t *testing.T) {
	old := []map[string]any{
		{"kind": "state", "term": 3, "voted_for": "n1"},
		{"kind": "compact", "snapshot_index": 2, "snapshot_term": 1},
		{"kind": "append", "entries": []map[string]any{
			{"term": 3, "command": []byte("set a 1")},
			{"term": 3, "command": []byte("set b 2")},
		}},
	}

	var buf []byte
	for _, rec := range old {
		payload, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}

		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(payload)))
		buf = append(buf, header...)
		buf = append(buf, payload...)
	}

	path := filepath.Join(t.TempDir(), "old.log")
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	file, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	state, err := file.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if state.CurrentTerm != 3 || state.VotedFor != "n1" {
		t.Errorf("term/vote = %d/%s, want 3/n1", state.CurrentTerm, state.VotedFor)
	}
	if state.SnapshotIndex != 2 || state.SnapshotTerm != 1 {
		t.Errorf("snapshot = %d/%d, want 2/1", state.SnapshotIndex, state.SnapshotTerm)
	}
	if state.Config != nil {
		t.Error("an old file produced a configuration out of nowhere")
	}
	if len(state.Log) != 2 {
		t.Fatalf("got %d entries, want 2", len(state.Log))
	}
	for i, entry := range state.Log {
		if entry.Kind != raft.EntryCommand {
			t.Errorf("entry %d decoded as kind %d, want a plain command", i, entry.Kind)
		}
	}
}
