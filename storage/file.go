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
)

type entry struct {
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

type record struct {
	Kind     kind    `json:"kind"`
	Term     uint64  `json:"term,omitempty"`
	VotedFor string  `json:"voted_for,omitempty"`
	Entries  []entry `json:"entries,omitempty"`
	Length   uint64  `json:"length,omitempty"`
}

type File struct {
	mu   sync.Mutex
	file *os.File
}

var _ raft.Storage = (*File)(nil)

func Open(path string) (*File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	return &File{file: file}, nil
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

	encoded := make([]entry, len(entries))
	for i, e := range entries {
		encoded[i] = entry{Term: e.Term, Command: e.Command}
	}

	return s.writeRecord(record{
		Kind:    kindAppend,
		Entries: encoded,
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
			if rec.Length == 0 {
				entries = nil
			} else if rec.Length-1 < uint64(len(entries)) {
				entries = entries[:rec.Length-1]
			}
		}
	}

	state.Log = entries

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return raft.PersistentState{}, fmt.Errorf("seek to end: %w", err)
	}

	return state, nil
}
