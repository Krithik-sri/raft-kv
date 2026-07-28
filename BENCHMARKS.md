# How fast it is

Back to the [README](README.md).

Five nodes in one process, real gRPC over localhost, real disk flushes. Eight writers going at
once, 15 seconds per cluster size. This is a laptop, not a data centre, so pay attention to the
shape rather than the absolute numbers.

```bash
go run ./cmd/bench --sizes 3,5,7 --writers 8 --duration 15s
```

```
nodes   writers     writes/s          p50          p95          p99   failed
3       8              245.5     29.706ms     48.593ms     65.033ms        0
5       8              235.2     31.537ms      52.97ms      64.22ms        0
7       8              228.4     32.153ms     53.271ms     69.612ms        0

failover: leader dies, how long until the next write works
nodes   trials            mean          min          max
3       1                269ms        269ms        269ms
5       2                240ms        211ms        269ms
7       3                214ms        211ms        218ms
```

**More machines barely costs anything.** 245 down to 228 going from three to seven. That
surprised me at first, but it makes sense. A majority of seven is four. The leader sends to
everyone at once and only waits for the fastest four. The extra machines are just... there.

**Failover is about a quarter of a second.** That's the election timeout doing its job, nothing
clever. Each node waits a random 150-300ms before deciding the leader is gone, so somebody
notices quickly and the rest fall in line.

**The 30ms is disk.** A write gets flushed on the leader, then flushed on each follower, and the
leader can't call it done until a majority have finished. Two flushes back to back on a normal
SSD is basically the whole number.

Batching several pending writes into one flush is the obvious next win. Haven't done it.

---

## The time I made it 2.5x faster and broke it

My first benchmark run was embarrassing.

159 writes per second. Identical at three, five and seven nodes. p50 of 49.98ms.

That last number is not a coincidence. My heartbeat interval was 50ms. Writes were landing in
the log and then just sitting there until the next heartbeat came round to send them. Every
write waited an average of 25ms for no reason at all. Cluster size didn't matter because the
cluster was never the problem. I was.

The fix was ten lines. A little channel that `Propose` pokes so replication wakes up straight
away instead of waiting for the timer. 159 writes/s went to 406, and the numbers finally started
changing when I changed the cluster size.

I would never have found that by reading my own code. I found it because 49.98 is suspiciously
close to 50.

**Then the chaos tests started failing.** Two seeds out of twelve. Machines disagreeing about
what they'd applied.

My first thought was that I'd just made things fast enough to expose an old bug. So I reverted
my change and ran it again with the same number of writes. Clean. Three times in a row.

I hadn't exposed anything. I'd broken it.

Here's what I'd missed. That 50ms timer had been quietly guaranteeing something I never noticed:
only one message in flight per follower at a time. Once replication woke up on every single
write, several messages to the same follower could be in the air at once, and they raced.

The fix is a flag per follower. A write still wakes replication immediately, but if that
follower already has a message in flight, it waits its turn. I also added timeouts, so a
follower that stops responding gives up its slot instead of blocking itself forever.

That costs me the pipelining. 406 back down to about 245. Still much better than the 159 I
started with, and it survives 20 chaos seeds including the two that used to fail every time.

I'll take 245 that's correct over 406 that isn't.

Whoever wrote that 50ms timer did me a favour by accident. That was me. Three weeks earlier.
Which is a slightly humbling thing to discover about yourself.

## The time I optimised something and nothing happened

The Raft transport used to open a fresh gRPC connection for every single message and close it
afterwards. At 50ms heartbeats across four peers that's roughly eighty connections a second,
all doing nothing useful.

Obvious win, right? So I cached one connection per peer, and this time I measured before
announcing anything.

- without caching: 231 / 241 / 331 writes per second
- with caching: 250 / 255 / 257

The averages are basically the same. The spread got much tighter.

So it's not a throughput win. It's a consistency win, and saying otherwise would be making
something up. I kept it, because eighty connections a second to accomplish nothing is still a
bad look.
