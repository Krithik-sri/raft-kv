# How fast it is

Back to the [README](README.md).

Five nodes in one process, real gRPC over localhost, real disk flushes. Eight writers going at
once, 15 seconds per cluster size. This is a laptop, not a data centre, so pay attention to the
shape rather than the absolute numbers.

```bash
go run ./cmd/bench --sizes 3,5,7 --writers 8 --duration 15s
```

```
writes
nodes   writers     writes/s          p50          p95          p99   failed
3       8              422.2      17.62ms     30.229ms     37.069ms        0
5       8              402.3      18.28ms     30.823ms     37.379ms        0
7       8              395.5     18.334ms     30.649ms     38.161ms        0

reads
nodes   mode              reads/s          p50          p95          p99   failed
3       linearizable      14092.0        524us      1.112ms      2.002ms        0
3       stale             18337.1           0s        613us     12.095ms        0
5       linearizable      13139.4        530us      1.505ms      2.023ms        0
5       stale             20138.9           0s        599us      11.37ms        0
7       linearizable      12488.0        562us      1.507ms      2.013ms        0
7       stale             20021.7           0s        607us     11.443ms        0

failover: leader dies, how long until the next write works
nodes   trials            mean          min          max
3       1                211ms        211ms        211ms
5       2                264ms        213ms        315ms
7       3                209ms        207ms        211ms
```

**More machines barely costs anything.** 422 down to 395 going from three to seven. That
surprised me at first, but it makes sense. A majority of seven is four. The leader sends to
everyone at once and only waits for the fastest four. The extra machines are just... there.

**Failover is about a fifth of a second.** That's the election timeout doing its job, nothing
clever. Each node waits a random 150-300ms before deciding the leader is gone, so somebody
notices quickly and the rest fall in line.

**Writes are 17ms and that's disk.** A write gets flushed on the leader, then flushed on each
follower, and the leader can't call it done until a majority have finished. Two flushes back to
back on a normal SSD is basically the whole number.

Batching several pending writes into one flush is the obvious next win. Haven't done it.

**Reads cost about half a millisecond.** Which is a lot cheaper than I expected.

A linearizable read has to prove the leader still holds a quorum before it answers. I assumed
that meant paying a full heartbeat interval, 50ms, on every single read. It doesn't, for two
reasons. A read pokes the replication loop instead of waiting for the next tick, so it costs
one round trip and not one timer. And reads that arrive together all clear on the same round,
so eight concurrent readers pay for roughly one exchange between them rather than eight.

`--stale` skips the check entirely. It reads the local map and returns, so p50 rounds to zero
and you get about 40% more throughput. Its p99 is *worse* than the linearizable one, which
looks backwards until you realise stale reads have no waiting in them to smooth things out, so
they feel every GC pause directly.

## A correction

Earlier versions of this file said 245 writes/s at a p50 of 30ms. Those numbers were wrong.

I measured them while a 15 minute chaos soak was running in the background on the same laptop.
Which is a stupid thing to do, and exactly the sort of thing I would spot immediately in
somebody else's work.

I only caught it because the ReadIndex numbers came back at 422 writes/s and I assumed I had
broken something. So I built the previous commit from scratch and benchmarked that too. Also
417. Nothing had changed. The old number was just taken on a busy machine.

Writing it down instead of quietly fixing it. If a number improves and you can't explain why,
that isn't good news. That's a bug you haven't found yet.

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

I assumed this would cost me the speed I'd just gained. It doesn't. Same code, same machine,
the only difference being whether the flag is there:

- with the flag: 422 writes/s, p50 17.6ms
- without it: 252 writes/s, p50 29.4ms

The flag makes it *faster*. Which took me a minute to accept.

Without it, every write kicks off another round to every follower, so you get a pile of
overlapping messages carrying overlapping entries. The followers handle them one at a time
anyway, so most of that work is thrown away. With the flag, a follower that's busy just doesn't
get a second message, and the next one carries everything that piled up in the meantime. It
batches by accident.

So the fix for the safety bug was also the fix for a performance bug I didn't know I had. I
would love to say I saw that coming.

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

Fair warning: those two rows come from the same stretch of bad measuring I owned up to above, so
don't read much into the absolute values. The conclusion I drew was that pooling didn't move
throughput, and I haven't re-run it since.

So it's not a throughput win. It's a consistency win, and saying otherwise would be making
something up. I kept it, because eighty connections a second to accomplish nothing is still a
bad look.
