# raft-kv

A key-value store that runs across five machines and keeps your data when one of them dies.

I built this to learn Raft. Raft is the algorithm that makes those five machines agree on
things. It turns out to be much harder than it looks. I got it wrong six times, and the only
reason I know that is a test harness that spends all day breaking the cluster on purpose.

```bash
./scripts/start-cluster.sh        # or: powershell -File scripts/start-cluster.ps1

go run ./cmd/kvctl --peers localhost:5001,localhost:5002,localhost:5003 put foo bar
go run ./cmd/kvctl --peers localhost:5001,localhost:5002,localhost:5003 get foo
```

Now close whichever terminal is the leader. Run `get` again. It still works. The client finds
the new leader on its own.

Every script comes twice, `.sh` and `.ps1`, so it runs on Linux, macOS, or Windows. I develop
on Windows. We all have our burdens. (If i could switch to linux i would trust me.)

---

## What it does

Picks a leader. Copies every write to a majority of machines before saying "done". Survives
crashes, because everything hits the disk before anyone gets an answer. Trims its own log so it
doesn't grow forever. Retries on your behalf when the leader changes.

Five nodes, gRPC between them, no external raft libraries (this is not a wrapper). gRPC is the
only third-party thing that ships in the binary. Tests also use Porcupine to check
linearizability, but that never leaves the test build.

| | |
|---|---|
| [DESIGN.md](DESIGN.md) | how it works, why I made each choice, and everything it still can't do |
| [TESTING.md](TESTING.md) | how I test it, the six bugs that found, and the linearizability checker |
| [BENCHMARKS.md](BENCHMARKS.md) | how fast it is, plus the time I made it faster and broke it |

## How fast

```
            writes/s   p50      reads/s   p50      failover
3 nodes     422        17.6ms   14092     524us    211ms
5 nodes     402        18.3ms   13139     530us    264ms
7 nodes     395        18.3ms   12488     562us    209ms
```

Adding machines barely slows it down. The 17ms on writes is almost entirely waiting for disks.

Reads are much cheaper than writes because nothing has to be written down. They still cost
about half a millisecond, because a read checks that this node really is still the leader
before answering. Pass `--stale` and you skip that check: about 18,000 reads/s and no waiting,
but the answer might be out of date.

"Failover" is how long you wait after the leader dies before writes work again. About a fifth
of a second.

## The six bugs

All six passed every test I had written at the time. All six needed three things to go wrong at
once: a network split, a snapshot, and an election. None of them crashed or returned an error.
They just quietly corrupted data and let me carry on.

Five of the six were the same mistake in a different outfit. I'd unlock a mutex halfway through
doing something, then keep using a value I'd read before letting go.

Full write-ups in [TESTING.md](TESTING.md). They're the best part of this repo.

## What it can't do

Snapshots go over the wire in one message, so a big state machine won't transfer at all.

Every write flushes to disk on its own. Batching them together is the obvious next speedup and
I haven't done it.

You can't add or remove machines while it's running. The list is fixed at startup.

Longer list, with reasons, in [DESIGN.md](DESIGN.md).

## Running it

You need Go 1.26 or newer.

```bash
go build ./...
go test ./...                              # 86 tests
./scripts/crash-test.sh                    # kills nodes with -9, over and over
./scripts/chaos-sweep.sh --seeds 20        # breaks the network, checks nothing broke
./scripts/start-cluster.sh                 # five nodes, Ctrl-C to stop
```

Same thing on Windows:

```powershell
powershell -File scripts/crash-test.ps1 -Iterations 20
powershell -File scripts/chaos-sweep.ps1 -Seeds 20
powershell -File scripts/start-cluster.ps1
```

The bash scripts stick to bash 3.2 and skip `shuf`, GNU `timeout` and associative arrays. macOS
still ships a bash from 2007 and I'd rather not learn that from a bug report.

One node by hand, if you want to see the moving parts:

```bash
go run ./cmd/server --id node1 --addr localhost:5001 --peers node2=localhost:5002,node3=localhost:5003 --data-dir data
```

`kvctl` does `get`, `put`, `delete` and `cas`. `get` exits non-zero when the key is missing, so
you can use it in scripts. `get --stale` skips the leader check if you want speed over
freshness.

Logs go to stderr. Default is `info`, which is quiet. Add `--log-level debug` to watch every
message fly past, but be ready for a lot of scrolling.

## The paper

Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*. It's short and it's
good. I kept coming back to four bits: §5.4.2 for the rule about what you're allowed to commit,
§5.3 for how logs get repaired, §6.4 for reads, §9.6 for pre-vote.

Most of my bugs were in the parts the paper doesn't cover. Which is fair. Those are the parts
where I had to decide things myself.
