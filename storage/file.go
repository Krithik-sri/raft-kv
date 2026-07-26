package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/krithik-sri/raft-kv/raft"
)

type kind string

const (
	kindState    kind = "state"
	kindAppend   kind = "append"
	kindTruncate kind = "truncate"
	kindCompact  kind = "compact"
)

type entry struct {
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

type record struct {
	Kind          kind    `json:"kind"`
	Term          uint64  `json:"term,omitempty"`
	VotedFor      string  `json:"voted_for,omitempty"`
	Entries       []entry `json:"entries,omitempty"`
	Length        uint64  `json:"length,omitempty"`
	SnapshotIndex uint64  `json:"snapshot_index,omitempty"`
	SnapshotTerm  uint64  `json:"snapshot_term,omitempty"`
}

type File struct {
	mu   sync.Mutex
	path string
	file *os.File
}

var _ raft.Storage = (*File)(nil)

func Open(path string) (*File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	return &File{path: path, file: file}, nil
}

func (s *File) snapshotPath() string {
	return s.path + ".snapshot"
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"

	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

func (s *File) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.file.Close()
}

func (s *File) writeRecord(rec record) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}

	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	if _, err := s.file.Write(frame); err != nil {
		return fmt.Errorf("write record: %w", err)
	}

	return s.file.Sync()
}

func (s *File) SaveState(term uint64, votedFor raft.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeRecord(record{
		Kind:     kindState,
		Term:     term,
		VotedFor: string(votedFor),
	})
}

func (s *File) AppendLog(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeRecord(record{
		Kind:    kindAppend,
		Entries: encodeEntries(entries),
	})
}

func (s *File) TruncateLog(length uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeRecord(record{
		Kind:   kindTruncate,
		Length: length,
	})
}

func encodeEntries(entries []raft.LogEntry) []entry {
	encoded := make([]entry, len(entries))
	for i, e := range entries {
		encoded[i] = entry{Term: e.Term, Command: e.Command}
	}
	return encoded
}

func (s *File) LoadSnapshot() ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.snapshotPath())
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read snapshot: %w", err)
	}

	return data, true, nil
}

func (s *File) SaveSnapshot(
	snapshot raft.Snapshot,
	term uint64,
	votedFor raft.NodeID,
	retained []raft.LogEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := writeFileAtomic(s.snapshotPath(), snapshot.Data); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}

	compacted := []record{
		{
			Kind:          kindCompact,
			SnapshotIndex: snapshot.Index,
			SnapshotTerm:  snapshot.Term,
		},
		{
			Kind:     kindState,
			Term:     term,
			VotedFor: string(votedFor),
		},
	}

	if len(retained) > 0 {
		compacted = append(compacted, record{
			Kind:    kindAppend,
			Entries: encodeEntries(retained),
		})
	}

	var buf []byte
	for _, rec := range compacted {
		payload, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("encode record: %w", err)
		}

		frame := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
		copy(frame[4:], payload)

		buf = append(buf, frame...)
	}

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close log before compaction: %w", err)
	}

	if err := writeFileAtomic(s.path, buf); err != nil {
		return fmt.Errorf("rewrite log: %w", err)
	}

	file, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen log: %w", err)
	}
	s.file = file

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end: %w", err)
	}

	return nil
}

func (s *File) Load() (raft.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return raft.PersistentState{}, fmt.Errorf("rewind: %w", err)
	}

	var (
		state   raft.PersistentState
		entries []raft.LogEntry
		reader  = bufio.NewReader(s.file)
	)

	for {
		var header [4]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			break
		}

		payload := make([]byte, binary.BigEndian.Uint32(header[:]))
		if _, err := io.ReadFull(reader, payload); err != nil {
			break
		}

		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			break
		}

		switch rec.Kind {
		case kindState:
			state.CurrentTerm = rec.Term
			state.VotedFor = raft.NodeID(rec.VotedFor)

		case kindAppend:
			for _, e := range rec.Entries {
				entries = append(entries, raft.LogEntry{Term: e.Term, Command: e.Command})
			}

		case kindTruncate:
			keep := rec.Length - state.SnapshotIndex - 1
			if rec.Length <= state.SnapshotIndex {
				entries = nil
			} else if keep < uint64(len(entries)) {
				entries = entries[:keep]
			}

		case kindCompact:
			state.SnapshotIndex = rec.SnapshotIndex
			state.SnapshotTerm = rec.SnapshotTerm
			entries = nil
		}
	}

	state.Log = entries

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return raft.PersistentState{}, fmt.Errorf("seek to end: %w", err)
	}

	return state, nil
}
