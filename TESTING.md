# How i test it, and the six bugs that i found

Back to the [README](README.md).

```bash
go test ./...                                       # 66 tests
./scripts/crash-test.sh                             # kills nodes with -9
./scripts/chaos-sweep.sh --seeds 20                 # breaks the network
go test ./raft/ -run TestChaos -chaos.duration=30m  # the long one
```

Every script has a `.ps1` twin if you're on Windows
(`scripts/crash-test.ps1 -Iterations 20`).

There are four layers. Each one catches things the layer below it can't.

## Unit tests

The small stuff that's easy to get quietly wrong. Which entries are safe to commit. What happens
when a duplicate message arrives late. The exact boundary where a snapshot ends and the log
begins. Subtracting from a `uint64` and wrapping round to eighteen quintillion.

## Cluster tests, no network

Five real Raft nodes wired straight into each other's function calls. No sockets. A hundred
writes converging across all five. A follower whose log I deliberately corrupt, fixing itself. A
follower left behind so long it needs a whole snapshot.

These run in milliseconds, which is what makes the chaos tests possible.

## Crash tests

`scripts/crash-test.sh` starts real processes, then `kill -9`s a random one in the middle of a
write workload. Restarts it. Checks every write it previously confirmed is still there. Twenty
times.

Shutting things down politely proves nothing. The bugs live in the gap between "I wrote it" and
"the disk actually has it". You only see that gap if you kill the process without warning.

## Chaos tests

This is the good one. `TestChaos` runs four writers hammering five nodes while something else
keeps breaking the network on purpose: splitting it in half, cutting off the leader, dropping
messages, duplicating messages, adding delays.

While that happens, three things get checked every 5 milliseconds:

- there's never more than one leader in the same term
- no machine ever moves its commit point backwards
- every machine's list of applied writes is a prefix of every other machine's

That last one is doing the heavy lifting. If two machines ever disagree about what happened, or
if a confirmed write goes missing, that check catches it.

Fifteen minutes of this:

```
writes attempted=48473  acked=45939  rejected=2534
delivered=61401  dropped=1879  duplicated=1502  partitioned=26675
invariant checks=179748  violations=0
```

A third of all attempted messages hit a network split. About 5% of writes were correctly
refused, because the leader couldn't reach a majority. If those numbers were near zero I'd have
built a very slow way of doing nothing.

Run the sweep, not one seed. The seed controls the order faults happen in, but it doesn't
control goroutine timing. A seed that passes once can fail on the third run. I learned that
the slow way.

---

# The six bugs

Every one of these passed all the tests I had at the time. Every one needed a network split, a
snapshot, and an election happening at the same moment. None of them crashed. None returned an
error. They corrupted data and let me carry on.

I've ordered them by how bad I felt.

### 1. Reading the snapshot in two steps

`sendSnapshot` grabbed the snapshot data, let go of the lock, then grabbed the snapshot's index
number separately.

If a new snapshot got taken in that gap, I'd send old data labelled with a new index. The
follower installs it and marks itself up to date at an index whose contents it never received.
Everything in between is gone. Not delayed. Gone.

This is the one that made confirmed writes disappear. It's also why I stopped trusting myself
around mutexes.

### 2. Sending messages under a term I no longer owned

`makeAppendEntriesRequestFor` stamped each message with the node's *current* term, not the term
its replication loop was started for.

So a node that had already stepped down kept sending messages that looked like they came from
the leader. Followers believed them and deleted their own committed entries to match. In one run
four nodes threw away committed data because a demoted node told them to.

### 3. Reading past the end of a slice

`applyCommitted` did `log[start:start+n]`.

Go lets you slice past the end of a slice as long as you're still inside its capacity. So after
the log was truncated, the deleted entries were still sitting in memory, and this read them and
applied them.

No panic. No error. The machine just quietly started living in a different universe from
everyone else.

### 4. Checking leadership outside the lock

`advancePeerProgress` checked "am I still the leader?" and then took the lock separately.

A reply from an old, dead term could land in that gap and refill the progress tracking that
`becomeLeader` had just cleared. Instant fake majority.

### 5. Using a stale list after letting go of the lock

`HandleInstallSnapshot` worked out which log entries to keep, released the lock to restore the
state machine, then wrote that list back.

Anything that arrived during the restore got silently erased.

### 6. Taking a snapshot without the apply lock

`maybeSnapshot` serialised the state machine while something else could be replacing it.
Contents from one moment, label from another. Same joke as bug 1, different function.

---

Bug 2 got caught by an assertion I left in the code. If a truncation would ever throw away an
entry that was already applied, the node logs a line starting with `SAFETY:`.

It should never appear. If it does, committed data is already gone, and I'd rather find out
right then than reverse-engineer it three hours later from `applied=115` on one machine and
`applied=116` on another.

Five of the six are the same mistake wearing different clothes: let go of a lock partway
through, then keep using something you read before you let go. If you take one thing away from
this repo, take that.

---

# Three flakes that weren't bugs

These looked like failures. They were tests asserting things the system never promised. Worth
writing down, because each one fooled me for a bit.

**Reading right after killing the leader.** A test killed the leader and immediately read the
value back. It passed for weeks. Then I made the code faster and it started failing.

Turned out the read was landing on a *different* stale leader, which answered "not found" with
total confidence. Which is exactly the stale-read problem I documented in
[DESIGN.md](DESIGN.md), working as designed.

The test now waits for the value to show up instead of demanding it instantly. That's what the
system actually guarantees. Asserting more than that was the real bug.

**"Some messages got dropped."** A test set a 30% drop rate and then checked that drops had
happened. With few enough messages, 30% can legitimately drop nothing. I replaced it with a
100% drop rate and a check that can't be unlucky.

**Picking a follower.** A test grabbed the first node that wasn't the leader. But during a
handover two nodes can both think they're the leader, so sometimes it grabbed one of those.

The test helper now waits until exactly one node claims leadership *and* every other node agrees
who it is, before any test assumes anything.
