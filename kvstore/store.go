package kvstore

import (
	"encoding/json"
	"fmt"
	"sync"
)

type OpKind string

const (
	OpPut    OpKind = "put"
	OpDelete OpKind = "delete"
	OpCAS    OpKind = "cas"
)

type Command struct {
	Op       OpKind `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

type Result struct {
	Swapped bool `json:"swapped,omitempty"`
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeResult(data []byte) (Result, error) {
	var result Result
	if len(data) == 0 {
		return result, nil
	}

	err := json.Unmarshal(data, &result)
	return result, err
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

type snapshot struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session
}

func New() *Store {
	return &Store{
		data:     make(map[string]string),
		sessions: make(map[string]session),
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Apply(command []byte) ([]byte, error) {
	var cmd Command
	if err := json.Unmarshal(command, &cmd); err != nil {
		return nil, fmt.Errorf("decode command: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if cmd.ClientID != "" {
		if previous, ok := s.sessions[cmd.ClientID]; ok && cmd.Seq <= previous.Seq {
			return json.Marshal(previous.Result)
		}
	}

	var result Result

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value

	case OpDelete:
		delete(s.data, cmd.Key)

	case OpCAS:
		current, ok := s.data[cmd.Key]
		if (ok && current == cmd.Expected) || (!ok && cmd.Expected == "") {
			s.data[cmd.Key] = cmd.Value
			result.Swapped = true
		}

	default:
		return nil, fmt.Errorf("unknown op %q", cmd.Op)
	}

	if cmd.ClientID != "" {
		s.sessions[cmd.ClientID] = session{Seq: cmd.Seq, Result: result}
	}

	return json.Marshal(result)
}

func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	image := snapshot{
		Data:     make(map[string]string, len(s.data)),
		Sessions: make(map[string]session, len(s.sessions)),
	}

	for key, value := range s.data {
		image.Data[key] = value
	}
	for client, tracked := range s.sessions {
		image.Sessions[client] = tracked
	}

	return json.Marshal(image)
}

func (s *Store) Restore(data []byte) error {
	var image snapshot
	if err := json.Unmarshal(data, &image); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = image.Data
	if s.data == nil {
		s.data = make(map[string]string)
	}

	s.sessions = image.Sessions
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}

	return nil
}
