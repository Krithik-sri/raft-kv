package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// A checker that never rejects anything is worse than no checker, because it
// looks like evidence. These feed it histories with known answers.

func op(id int, in Input, out Output, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: id,
		Input:    in,
		Output:   out,
		Call:     call,
		Return:   ret,
	}
}

func check(t *testing.T, history []porcupine.Operation) porcupine.CheckResult {
	t.Helper()

	result, _ := porcupine.CheckOperationsVerbose(Model, history, 10*time.Second)
	return result
}

func TestModelAcceptsASensibleHistory(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{}, 10, 20),
		op(0, Input{Op: "get", Key: "a"}, Output{Value: "v1", Found: true}, 30, 40),
		op(0, Input{Op: "delete", Key: "a"}, Output{}, 50, 60),
		op(0, Input{Op: "get", Key: "a"}, Output{}, 70, 80),
	}

	if got := check(t, history); got != porcupine.Ok {
		t.Errorf("a perfectly ordinary history was rejected: %v", got)
	}
}

// The exact bug the read barrier fixes. The write finished at t=20. The read
// starts at t=30, after it, and still sees the old value. No ordering of these
// two operations explains that.
func TestModelCatchesAStaleRead(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v0"}, Output{}, 0, 5),
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{}, 10, 20),
		op(1, Input{Op: "get", Key: "a"}, Output{Value: "v0", Found: true}, 30, 40),
	}

	if got := check(t, history); got != porcupine.Illegal {
		t.Errorf("a stale read slipped past the checker: %v", got)
	}
}

func TestModelCatchesALostWrite(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{}, 0, 10),
		op(1, Input{Op: "get", Key: "a"}, Output{}, 20, 30), // reports missing
	}

	if got := check(t, history); got != porcupine.Illegal {
		t.Errorf("a vanished write slipped past the checker: %v", got)
	}
}

func TestModelCatchesAWrongCompareAndSwap(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{}, 0, 10),
		// The value is v1, so swapping on "nope" cannot have succeeded.
		op(0, Input{Op: "cas", Key: "a", Expected: "nope", Value: "v2"},
			Output{Swapped: true}, 20, 30),
	}

	if got := check(t, history); got != porcupine.Illegal {
		t.Errorf("an impossible compare-and-swap slipped past the checker: %v", got)
	}
}

// Operations that overlap in time can be ordered either way, so this one is
// fine even though it looks alarming: the read and the write were in flight at
// the same moment.
func TestModelAllowsConcurrentOperationsToBeOrderedEitherWay(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{}, 0, 100),
		op(1, Input{Op: "get", Key: "a"}, Output{}, 10, 90),
	}

	if got := check(t, history); got != porcupine.Ok {
		t.Errorf("overlapping operations were rejected: %v", got)
	}
}

// A write that timed out may or may not have landed. The checker has to allow
// both, or every unlucky client would look like a bug.
func TestModelAllowsAnUnknownWriteToHaveHappened(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{Unknown: true}, 0, 1<<62),
		op(1, Input{Op: "get", Key: "a"}, Output{Value: "v1", Found: true}, 10, 20),
	}

	if got := check(t, history); got != porcupine.Ok {
		t.Errorf("an unknown write that clearly landed was rejected: %v", got)
	}
}

func TestModelAllowsAnUnknownWriteToHaveBeenLost(t *testing.T) {
	history := []porcupine.Operation{
		op(0, Input{Op: "put", Key: "a", Value: "v1"}, Output{Unknown: true}, 0, 1<<62),
		op(1, Input{Op: "get", Key: "a"}, Output{}, 10, 20), // missing, also fine
	}

	if got := check(t, history); got != porcupine.Ok {
		t.Errorf("an unknown write that never landed was rejected: %v", got)
	}
}
