# Numbers

Back to the [README](README.md).

Five nodes in one process, real gRPC over loopback, real `fsync` to a real SSD. Eight
concurrent writers, 15 seconds per cluster size. This is a laptop, not a datacentre, so read
the shape rather than the absolute values.

```bash
go run ./cmd/bench --sizes 3,5,7 --writers 8 --duration 15s
```

```
nodes   writers     writes/s          p50          p95          p99   failed
3       8              245.5     29.706ms     48.593ms     65.033ms        0
5       8              235.2     31.537ms      52.97ms      64.22ms        0
7       8              228.4     32.153ms     53.271ms     69.612ms        0

failover: leader death to the next successful write
nodes   trials            mean          min          max
3       1                269ms        269ms        269ms
5       2                240ms        211ms        269ms
7       3                214ms        211ms        218ms
```

**Growing the cluster costs almost nothing.** 245 to 228 writes/s going from three nodes to
seven. A majority of seven is four, the extra peers are replicated to in parallel, and the
leader waits on the fastest majority either way.

**Failover lands around 250ms**, which is just the randomised election timeout (150-300ms)
doing its job. Nothing clever, and it doesn't need to be.

**p50 of 30ms is mostly disk.** A write is `fsync` on the leader, then `fsync` on each follower
before it acknowledges, and the leader can't commit until a majority have done that. Two serial
flushes on consumer hardware is basically the whole number. Batching concurrent proposals into
one flush is the obvious next win and is not implemented.

---

## The one where I made it 2.5x faster and broke it

**The first benchmark run was embarrassing.** 159 writes/s, identical at every cluster size, p50
of 49.98ms. That is not a coincidence: the heartbeat interval is 50ms. Writes were being
appended to the log and then sitting there doing nothing until the replication ticker happened
to come round, so every write paid an average of 25ms of pure waiting. Cluster size was
irrelevant because the cluster was never the bottleneck.

The fix was a buffered channel that `Propose` pokes so replication wakes immediately rather than
on the tick. Ten lines. 159 to 406 writes/s, and cluster size finally started showing a real
cost curve instead of a flat line.

I would not have found that by reading the code. It showed up because the number was
suspiciously round.

**Then the chaos tests started failing.** Two seeds out of twelve, applied logs diverging between
nodes. I assumed I had merely exposed something old, since throughput had gone up 2.5x and each
run now did far more writes. So I reverted the change and re-ran at matched write volume: clean,
three for three. I had broken it.

The 50ms ticker had been quietly enforcing something I never noticed: at most one
`AppendEntries` in flight per follower at a time. Waking replication on every proposal removed
that, and concurrent requests to the same follower started racing.

The fix is a per-peer in-flight flag, so a proposal still wakes replication instantly but a peer
that is already mid-request doesn't get a second one. Plus per-RPC timeouts, so a peer that
stops answering releases its slot instead of blocking replication to itself forever.

That costs the pipelining: 406 back down to about 245. Still half again the ticker-bound
baseline, and it passes 20 chaos seeds including the two that reliably failed. I would rather
have 245 that is correct.

Whoever wrote that 50ms ticker did me a favour by accident, which is a slightly humbling thing
to discover about yourself.

## The one that turned out not to matter

Connection pooling in the Raft transport looked like an obvious win. It was dialling and closing
a gRPC connection per RPC, roughly eighty a second at 50ms heartbeats across four peers. So I
A/B'd it rather than assuming:

- without caching: 231 / 241 / 331 writes/s
- with caching: 250 / 255 / 257

The mean is inside the noise band. The variance collapsed. So it's a latency-consistency win,
not a throughput win, and saying otherwise would be inventing a result. Kept it anyway, because
eighty connections a second to achieve nothing is still a bad look.
