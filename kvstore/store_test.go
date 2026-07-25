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

func TestDuplicateRequestIsNotReapplied(t *testing.T) {
	s := New()

	cmd := Command{Op: OpPut, Key: "n", Value: "first", ClientID: "c1", Seq: 1}
	applyCommand(t, s, cmd)

	applyCommand(t, s, Command{Op: OpPut, Key: "n", Value: "second", ClientID: "c1", Seq: 1})

	if value, _ := s.Get("n"); value != "first" {
		t.Errorf("value = %q, want \"first\" (retry must not reapply)", value)
	}
}

func TestDuplicateCompareAndSwapReturnsOriginalResult(t *testing.T) {
	s := New()

	applyCommand(t, s, Command{Op: OpPut, Key: "n", Value: "1"})

	swap := Command{Op: OpCAS, Key: "n", Expected: "1", Value: "2", ClientID: "c1", Seq: 7}

	first := applyCommand(t, s, swap)
	if !first.Swapped {
		t.Fatal("first compare-and-swap should have succeeded")
	}

	retry := applyCommand(t, s, swap)
	if !retry.Swapped {
		t.Error("retry reported Swapped=false; the client would wrongly believe it lost the race")
	}

	if value, _ := s.Get("n"); value != "2" {
		t.Errorf("value = %q, want \"2\"", value)
	}
}

func TestSequenceNumbersAreTrackedPerClient(t *testing.T) {
	s := New()

	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "a", ClientID: "c1", Seq: 1})
	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "b", ClientID: "c2", Seq: 1})

	if value, _ := s.Get("k"); value != "b" {
		t.Errorf("value = %q, want \"b\" (different clients must not dedup each other)", value)
	}

	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "c", ClientID: "c1", Seq: 2})

	if value, _ := s.Get("k"); value != "c" {
		t.Errorf("value = %q, want \"c\" (a higher seq must apply)", value)
	}
}

func TestCommandsWithoutClientIDAreNeverDeduped(t *testing.T) {
	s := New()

	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "a"})
	applyCommand(t, s, Command{Op: OpPut, Key: "k", Value: "b"})

	if value, _ := s.Get("k"); value != "b" {
		t.Errorf("value = %q, want \"b\"", value)
	}
}
