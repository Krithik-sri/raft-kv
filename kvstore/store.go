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

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
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

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		return json.Marshal(Result{})

	case OpDelete:
		delete(s.data, cmd.Key)
		return json.Marshal(Result{})

	case OpCAS:
		current, ok := s.data[cmd.Key]
		if (ok && current == cmd.Expected) || (!ok && cmd.Expected == "") {
			s.data[cmd.Key] = cmd.Value
			return json.Marshal(Result{Swapped: true})
		}
		return json.Marshal(Result{})

	default:
		return nil, fmt.Errorf("unknown op %q", cmd.Op)
	}
}
