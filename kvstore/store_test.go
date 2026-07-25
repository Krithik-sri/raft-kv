package kvstore

import "testing"

func mustEncode(t *testing.T, cmd Command) []byte {
	t.Helper()

	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("encode %v: %v", cmd, err)
	}
	return data
}

func applyCommand(t *testing.T, s *Store, cmd Command) Result {
	t.Helper()

	raw, err := s.Apply(mustEncode(t, cmd))
	if err != nil {
		t.Fatalf("apply %v: %v", cmd, err)
	}

	result, err := DecodeResult(raw)
	if err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return result
}

func TestPutGetDelete(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Error("Get on empty store reported a hit")
	}

	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "v"})
	if value, ok := s.Get("k"); !ok || value != "v" {
		t.Errorf("Get after put = %q,%v want \"v\",true", value, ok)
	}

	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "v2"})
	if value, _ := s.Get("k"); value != "v2" {
		t.Errorf("Get after overwrite = %q, want \"v2\"", value)
	}

	applyCommand(t, s, Command{Op: OpDelete, Key: "k"})
	if _, ok := s.Get("k"); ok {
		t.Error("Get after delete reported a hit")
	}
}

func TestDeleteMissingKeyIsNotAnError(t *testing.T) {
	s := New()

	applyCommand(t, s, Command{Op: OpDelete, Key: "nope"})

	if s.Len() != 0 {
		t.Errorf("Len = %d, want 0", s.Len())
	}
}

func TestCompareAndSwap(t *testing.T) {
	tests := []struct {
		name        string
		seed        bool
		seedValue   string
		expected    string
		newValue    string
		wantSwapped bool
		wantValue   string
	}{
		{
			name:        "swaps when the expected value matches",
			seed:        true,
			seedValue:   "old",
			expected:    "old",
			newValue:    "new",
			wantSwapped: true,
			wantValue:   "new",
		},
		{
			name:        "refuses when the expected value differs",
			seed:        true,
			seedValue:   "old",
			expected:    "wrong",
			newValue:    "new",
			wantSwapped: false,
			wantValue:   "old",
		},
		{
			name:        "creates an absent key when expecting empty",
			seed:        false,
			expected:    "",
			newValue:    "new",
			wantSwapped: true,
			wantValue:   "new",
		},
		{
			name:        "refuses an absent key when expecting a value",
			seed:        false,
			expected:    "something",
			newValue:    "new",
			wantSwapped: false,
			wantValue:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.seed {
				applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: tt.seedValue})
			}

			result := applyCommand(t, s, Command{
				Op:       OpCAS,
				Key:      "k",
				Expected: tt.expected,
				Value:    tt.newValue,
			})

			if result.Swapped != tt.wantSwapped {
				t.Errorf("Swapped = %v, want %v", result.Swapped, tt.wantSwapped)
			}

			value, _ := s.Get("k")
			if value != tt.wantValue {
				t.Errorf("value = %q, want %q", value, tt.wantValue)
			}
		})
	}
}

func TestApplyRejectsBadInput(t *testing.T) {
	s := New()

	if _, err := s.Apply([]byte("not json")); err == nil {
		t.Error("Apply accepted malformed json")
	}

	if _, err := s.Apply(mustEncode(t, Command{Op: "frobnicate", Key: "k"})); err == nil {
		t.Error("Apply accepted an unknown op")
	}
}
