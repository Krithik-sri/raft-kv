# raft-kv

A distributed key-value store built on Raft.

It works now. It also worked six separate times when it didn't, and the only reason I know
that is a test harness that spends its life partitioning the cluster and asking rude
questions.

```bash
powershell -File scripts/start-cluster.ps1

go run ./cmd/kvctl --peers localhost:5001,localhost:5002,localhost:5003 put foo bar
go run ./cmd/kvctl --peers localhost:5001,localhost:5002,localhost:5003 get foo
```

Close whichever terminal is the leader. Run `get` again. The client sorts itself out.

Yes, the scripts are PowerShell. I develop on Windows. We all have our burdens. (If i could switch to linux i would trust me.) 
[I'll add the scripts for bash later]

---

Leader election with pre-vote, log replication, snapshots and `InstallSnapshot`, crash-safe
persistence, request dedup, and a client that follows leader changes without being told. Five
nodes, gRPC between them, nothing exotic in the dependency list.

| | |
|---|---|
| [DESIGN.md](DESIGN.md) | how it fits together, how much thought went into each decision, and everything it can't do |
| [TESTING.md](TESTING.md) | the chaos harness, and the six safety bugs it found |
| [BENCHMARKS.md](BENCHMARKS.md) | numbers, including the one where I made it 2.5x faster and broke it |

## The numbers, briefly

```
3 nodes   245 writes/s   p50 30ms   failover 269ms
5 nodes   235 writes/s   p50 32ms   failover 240ms
7 nodes   228 writes/s   p50 32ms   failover 214ms
```

Adding nodes barely costs anything. The p50 is two serial `fsync`s and not much else.

## The bugs, briefly

Six safety bugs got past the entire unit and integration suite. Every one needed a partition,
a snapshot and an election happening at the same time. None of them returned an error. They
corrupted things quietly and let you carry on.

Five were the same mistake wearing different hats: drop a lock partway through an operation,
then keep using a value you read before the gap.

## The caveats, briefly

Reads come from the leader's local map with no quorum check, so a leader that got partitioned
away and hasn't noticed will hand you stale data with total confidence. Writes are fine, it
can't commit without a majority. Snapshots go in one gRPC message, so a large state machine
won't transfer at all. The peer set is fixed at startup. Full list with reasons in
[DESIGN.md](DESIGN.md).

## Running it

Go 1.26+.

```bash
go build ./...
go test ./...                                          # 66 tests
powershell -File scripts/crash-test.ps1                # kill -9, repeatedly
powershell -File scripts/chaos-sweep.ps1 -Seeds 20     # invariant sweep
```

Five nodes locally, each persisting to `data/<id>.log`:

```bash
powershell -File scripts/start-cluster.ps1
```

One node by hand:

```bash
go run ./cmd/server --id node1 --addr localhost:5001 --peers node2=localhost:5002,node3=localhost:5003 --data-dir data
```

`kvctl` does `get`, `put`, `delete` and `cas`. `get` exits non-zero on a missing key so it
composes in scripts.

Logging is `log/slog` on stderr at info. `--log-level debug` if you want to watch replication
happen, though it is a firehose.

## Reference

Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*. The sections I kept
going back to: §5.4.2 for the commit rule, §5.3 for log matching, §6.4 for read-only queries,
§9.6 for pre-vote.

The paper is genuinely well written. Most of my bugs were in the parts it doesn't cover, which
is to say the parts where I had to make my own decisions.
