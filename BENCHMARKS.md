# How fast it is

Back to the [README](README.md).

Five nodes in one process, real gRPC over localhost, real disk flushes. Eight writers going at
once, 15 seconds per cluster size. This is a laptop, not a data centre, so pay attention to the
shape rather than the absolute numbers.

```bash
go run ./cmd/bench --sizes 3 --writers 8 --duration 15s
```

One cluster size per run. See [how I measure](#how-i-measure) for why that matters more than
you'd think.

```
writes
nodes   writers     writes/s          p50          p95          p99   failed
3       8              393.1     18.078ms     39.691ms     53.601ms        0
5       8              357.1     20.471ms     42.628ms     54.411ms        0
7       8              346.7      21.26ms     40.984ms     57.854ms        0

reads
nodes   mode              reads/s          p50          p95          p99   failed
3       linearizable      13510.2        522µs      1.156ms      2.508ms        0
3       stale             21116.3           0s        799µs      6.524ms        0
5       linearizable      12996.5        528µs      1.504ms      2.192ms        0
5       stale             20951.5           0s        996µs       7.36ms        0
7       linearizable      10820.9        554µs      1.996ms      3.137ms        0
7       stale             20953.9           0s        731µs      7.519ms        0

failover: leader dies, how long until the next write works
nodes   trials            mean          min          max
3       1                229ms        229ms        229ms
5       2                225ms        223ms        226ms
7       3                240ms        215ms        277ms
```

Those write numbers are lower than the ones this file used to show, and the code got roughly
twice as fast in between. Both statements are true. Read [how I measure](#how-i-measure).

**More machines barely costs anything.** 393 down to 347 going from three to seven. That
surprised me at first, but it makes sense. A majority of seven is four. The leader sends to
everyone at once and only waits for the fastest four. The extra machines are just... there.

**Failover is about a fifth of a second.** That's the election timeout doing its job, nothing
clever. Each node waits a random 150-300ms before deciding the leader is gone, so somebody
notices quickly and the rest fall in line.

**Writes are 18ms and that's still disk.** A write gets flushed on the leader, then flushed on
each follower, and the leader can't call it done until a majority have finished. Two flushes
back to back on a normal SSD is basically the whole number.

It used to be two flushes *per write*. Now the leader's half is shared. See below.

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

## Batching the flushes

The old code flushed the leader's log to disk once per write. Eight writers meant eight flushes,
one after another, each waiting its turn.

Before changing anything I measured the ceiling. I ripped the flush out entirely, which is wildly
unsafe and completely fine for ten minutes, and ran the benchmark again.

- a flush per write: 395 writes/s, p50 18.4ms
- no flush at all: 2688 writes/s, p50 550µs

So the disk was about 85% of a write. Worth fixing.

My plan was to batch inside the storage layer, where `AppendLog` could collect a few callers'
bytes and flush them together. Raft wouldn't have to know anything about it.

That plan was nonsense, and I only found out because I checked before writing it. Raft holds its
lock across every storage call, so storage never sees two callers at once. I added a counter to
be sure: peak concurrent callers on a single file was 1, over about four thousand appends. A
batching layer down there would have spent its life batching one thing at a time.

So it had to move up into raft. Entries go into the log in memory, and a loop flushes whatever
has piled up since the last one. Writes that arrive mid-flush queue behind it and go out
together in the next. Eight writers, one flush.

The part that needs care is the commit rule. An entry now exists in memory before it exists on
disk, so the leader is not allowed to count its own copy towards a majority until the flush
covering it has finished. Skip that and a crash turns the majority that committed a write into a
minority, and the write is gone. One line, unusually bad failure mode, its own test.

Alternating runs, same sitting, three nodes:

- before: 229, 224, 229 writes/s, p50 around 32ms
- after: 406, 408, 392 writes/s, p50 around 18ms

Call it 1.8x. Not the 6.4x the ceiling promised, because the followers still flush once per batch
and somebody still has to send the messages. The disk just isn't the whole story any more.

## How i measure

This laptop is not a reliable instrument. Same binary, same benchmark, same everything: 229
writes/s one minute and 355 the next. Nothing changed except the machine's mood.

Two habits came out of that.

**One cluster size per process.** Running `--sizes 3,5,7` in one go used to produce clean
evidence that seven nodes are faster than three, which is nonsense. Whatever runs later in a
process runs faster. Each size gets its own run now.

**Compare two builds by alternating them.** Old, new, old, new, same sitting. The absolute
numbers still wander but the ratio between two builds measured seconds apart holds still. The
1.8x above is four rounds of that, and every round landed between 1.79 and 1.90.

If I'd measured the old build on Monday and the new one on Tuesday, I could have proven whatever
I felt like. In either direction.

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
