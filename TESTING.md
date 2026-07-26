# Testing, and the six bugs it caught

Back to the [README](README.md).

```bash
go test ./...                                          # 66 tests
powershell -File scripts/crash-test.ps1                # kill -9, repeatedly
powershell -File scripts/chaos-sweep.ps1 -Seeds 20     # invariant sweep
go test ./raft/ -run TestChaos -chaos.duration=30m     # soak
```

Four layers, each catching what the one below it can't.

**Unit tests** cover the stuff that is easy to get quietly wrong: the commit rule, truncation
versus duplicate delivery, snapshot boundary arithmetic, `uint64` subtraction that wraps to
eighteen quintillion if you look at it funny.

**In-process cluster tests** run actual multi-node Raft over an in-memory transport, no sockets
involved. A hundred commands converging across five nodes, a follower whose log I deliberately
vandalise repairing itself, a follower left behind long enough that it needs a whole snapshot.

**Crash testing** (`scripts/crash-test.ps1`) starts real processes and `kill -9`s a random one
mid-workload, restarts it, and checks every acknowledged write is still readable. Twenty cycles.
Graceful shutdown testing is a comfort blanket; it proves nothing, because the bugs live in the
gap between "wrote to the page cache" and "actually on the disk".

**Chaos testing** (`TestChaos`) is where the value is. Four writers hammering five nodes while a
scheduler partitions them, isolates the leader, and layers on message loss, duplication and
delay. Three invariants checked every 5ms for the whole run:

- at most one leader per term
- `commitIndex` never goes backwards
- every node's applied sequence is a prefix of every other node's

The third one quietly covers "no acknowledged write ever vanished", which is the one that would
end your week.

Fifteen minutes of it:

```
writes attempted=48473  acked=45939  rejected=2534
delivered=61401  dropped=1879  duplicated=1502  partitioned=26675
invariant checks=179748  violations=0
```

A third of all attempted RPCs hit an active partition, and 5% of writes were correctly refused.
If those numbers were near zero I'd have written a very expensive way of doing nothing.

Run the sweep rather than a single seed. The seed controls fault *scheduling*, not goroutine
timing, so a seed that passes once can fail on the third run. I learned that the tedious way.

---

## Six bugs, ranked by how bad I felt

Every one of these passed the full unit and integration suite at the time. Every one needs a
partition, a snapshot and an election happening simultaneously, which is exactly the scenario
you are not picturing while writing unit tests. None of them return an error. They just corrupt
data and let you carry on.

**1. `sendSnapshot` read the snapshot blob and the snapshot index in two separate lock
acquisitions.** Take a new snapshot in the gap and you ship old data labelled with a new index.
The follower installs it, sets `lastApplied` to an index whose contents it never received, and
everything in between is gone. Permanently. This is the one that made acknowledged writes
disappear, and it is why I stopped trusting myself around mutexes.

**2. `makeAppendEntriesRequestFor` stamped requests with the current term instead of the term
the replication loop was running for.** So a node that had already stepped down carried on
sending `AppendEntries` under a term it no longer owned, and followers dutifully truncated their
committed entries to match a node that was not the leader. Four nodes threw away a committed
entry in one run.

**3. `applyCommitted` sliced `log[start:start+n]`.** Go permits slicing past `len` as long as
you're within `cap`. After a truncation the discarded entries are still sitting in the backing
array, so this read them and applied them. No panic. No error. The node simply started living in
a slightly different universe.

**4. `advancePeerProgress` checked leadership outside the lock it then mutated under.** A reply
from a long-dead term could land after re-election and repopulate `matchIndex`, which
`becomeLeader` had just zeroed. Instant fabricated majority.

**5. `HandleInstallSnapshot` computed which log entries to keep, dropped the lock to restore the
state machine, then wrote that now-stale list back.** Anything appended during the restore was
silently erased.

**6. `maybeSnapshot` serialised the state machine without holding the apply lock.** Contents
from one index, label from another. Same joke as #1, different function.

Bug 2 got caught by an assertion that is still in the code: if a truncation would ever discard
an already-applied entry, the node logs a `SAFETY:` line. It should never appear. If it does,
committed data is already gone, and I would rather find out immediately than reverse-engineer a
divergence three hours later from `applied=115` versus `applied=116`.

Five of the six are the same mistake in different costumes: drop a lock partway through an
operation, then keep using a value you read before the gap. If you take one thing from this
repo, take that.

---

## Three flakes worth knowing about

Not bugs in the store, but tests asserting things the system never promised. Worth writing down
because each one looked like a real failure first.

**Reads after a leader kill.** A test killed the leader and immediately read the value back. It
passed for weeks, then started flaking once the code got fast enough to hit the startup election
race. The read was landing on a *different* stale leader, which answered `found=false` with no
error. That is the documented non-linearizable read, working exactly as designed. The test now
asserts eventual readability, which is what the system actually guarantees. Pretending otherwise
was the bug.

**"Some messages were dropped."** A tolerance test set a 30% drop rate and then asserted that
drops had occurred. With few enough RPCs, 30% can legitimately drop nothing. Replaced with a
rate of 1.0 and a deterministic assertion.

**Picking a follower.** A redirect test grabbed the first node that wasn't the leader, except
during a handover two nodes can both be claiming leadership. The harness now waits for exactly
one leader *and* every other node agreeing on who it is before any test makes that assumption.
