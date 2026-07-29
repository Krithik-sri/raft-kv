// Package linearizability holds the model and history recorder used to check
// that the store behaves like a single machine, no matter how badly the network
// is behaving underneath.
//
// The idea: record every operation with the time it was sent and the time it
// came back, then ask whether there is some single-machine ordering of those
// operations that explains every answer. If there isn't, the cluster told two
// callers contradictory things.
package linearizability

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

type Input struct {
	Op       string // get, put, delete, cas
	Key      string
	Value    string
	Expected string // cas only
}

type Output struct {
	Value   string
	Found   bool
	Swapped bool

	// Unknown means the call timed out or errored, so we never learned whether
	// it happened. Dropping those would be unsound for writes: if the write did
	// land, a later read sees a value nothing accounts for and the checker
	// reports a violation that isn't real.
	Unknown bool
}

type state struct {
	present bool
	value   string
}

// Model is a single key that you can put, delete and compare-and-swap.
var Model = porcupine.Model{
	// One key at a time. Checking is exponential in the worst case, and
	// splitting by key keeps each piece small.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			key := op.Input.(Input).Key
			byKey[key] = append(byKey[key], op)
		}

		partitions := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			partitions = append(partitions, ops)
		}
		return partitions
	},

	Init: func() any { return state{} },

	Step: func(current, input, output any) (bool, any) {
		now := current.(state)
		in := input.(Input)
		out := output.(Output)

		switch in.Op {
		case "get":
			if out.Unknown {
				return true, now
			}
			return out.Found == now.present && out.Value == now.value, now

		case "put":
			return true, state{present: true, value: in.Value}

		case "delete":
			return true, state{}

		case "cas":
			matches := (now.present && now.value == in.Expected) ||
				(!now.present && in.Expected == "")

			// An unknown compare-and-swap may or may not have gone through.
			// Let the rest of the history decide.
			if !out.Unknown && out.Swapped != matches {
				return false, now
			}

			if matches {
				return true, state{present: true, value: in.Value}
			}
			return true, now
		}

		return false, now
	},

	Equal: func(a, b any) bool { return a.(state) == b.(state) },

	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		out := output.(Output)

		if out.Unknown {
			return fmt.Sprintf("%s(%s) -> ???", in.Op, in.Key)
		}

		switch in.Op {
		case "get":
			if !out.Found {
				return fmt.Sprintf("get(%s) -> missing", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
		case "put":
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		case "delete":
			return fmt.Sprintf("delete(%s)", in.Key)
		default:
			return fmt.Sprintf("cas(%s, %q -> %q) -> swapped=%v",
				in.Key, in.Expected, in.Value, out.Swapped)
		}
	},
}

// Recorder collects a history from several callers at once.
type Recorder struct {
	mu      sync.Mutex
	history []porcupine.Operation
}

func (r *Recorder) Record(id int, in Input, out Output, call, ret time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	returned := ret.UnixNano()
	if out.Unknown {
		// Still in flight as far as we know. Letting the checker place it
		// anywhere after the call, including right at the end where it affects
		// nothing, is what makes an unknown outcome sound.
		returned = math.MaxInt64
	}

	r.history = append(r.history, porcupine.Operation{
		ClientId: id,
		Input:    in,
		Output:   out,
		Call:     call.UnixNano(),
		Return:   returned,
	})
}

func (r *Recorder) History() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]porcupine.Operation(nil), r.history...)
}
