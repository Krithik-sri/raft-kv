package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/krithik-sri/raft-kv/client"
	"github.com/krithik-sri/raft-kv/internal/cluster"
)

func newCluster(size int) (*cluster.Cluster, string, error) {
	dataDir, err := os.MkdirTemp("", "raft-bench-")
	if err != nil {
		return nil, "", err
	}

	c, err := cluster.Start(size, dataDir)
	if err != nil {
		os.RemoveAll(dataDir)
		return nil, "", err
	}

	return c, dataDir, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func runThroughput(size, writers int, duration time.Duration) error {
	c, dataDir, err := newCluster(size)
	if err != nil {
		return err
	}
	defer c.Stop()
	defer os.RemoveAll(dataDir)

	if c.WaitForLeader(20*time.Second) == nil {
		return fmt.Errorf("no leader emerged for a %d node cluster", size)
	}

	kv, err := client.New(c.Addresses)
	if err != nil {
		return err
	}
	defer kv.Close()

	ctx, stop := context.WithCancel(context.Background())

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		latencies []time.Duration
		failed    int
	)

	start := time.Now()

	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			local := make([]time.Duration, 0, 1024)
			localFailed := 0

			for seq := 0; ctx.Err() == nil; seq++ {
				key := fmt.Sprintf("w%d-k%d", id, seq)

				callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				began := time.Now()
				err := kv.Put(callCtx, key, "v")
				elapsed := time.Since(began)
				cancel()

				if err != nil {
					if ctx.Err() == nil {
						localFailed++
					}
					continue
				}
				local = append(local, elapsed)
			}

			mu.Lock()
			latencies = append(latencies, local...)
			failed += localFailed
			mu.Unlock()
		}(w)
	}

	time.Sleep(duration)
	stop()
	wg.Wait()

	elapsed := time.Since(start)

	slices.Sort(latencies)

	throughput := float64(len(latencies)) / elapsed.Seconds()

	fmt.Printf(
		"%-7d %-9d %10.1f %12s %12s %12s %8d\n",
		size,
		writers,
		throughput,
		percentile(latencies, 0.50).Round(time.Microsecond),
		percentile(latencies, 0.95).Round(time.Microsecond),
		percentile(latencies, 0.99).Round(time.Microsecond),
		failed,
	)

	return nil
}

func runReads(size, readers int, duration time.Duration, stale bool) error {
	c, dataDir, err := newCluster(size)
	if err != nil {
		return err
	}
	defer c.Stop()
	defer os.RemoveAll(dataDir)

	if c.WaitForLeader(20*time.Second) == nil {
		return fmt.Errorf("no leader emerged for a %d node cluster", size)
	}

	kv, err := client.New(c.Addresses)
	if err != nil {
		return err
	}
	defer kv.Close()

	seed, cancelSeed := context.WithTimeout(context.Background(), 20*time.Second)
	for i := 0; i < 50; i++ {
		if err := kv.Put(seed, fmt.Sprintf("k%d", i), "v"); err != nil {
			cancelSeed()
			return fmt.Errorf("seeding: %w", err)
		}
	}
	cancelSeed()

	var opts []client.GetOption
	if stale {
		opts = append(opts, client.WithStale())
	}

	ctx, stop := context.WithCancel(context.Background())

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		latencies []time.Duration
		failed    int
	)

	start := time.Now()

	for w := 0; w < readers; w++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			local := make([]time.Duration, 0, 1024)
			localFailed := 0

			for seq := 0; ctx.Err() == nil; seq++ {
				key := fmt.Sprintf("k%d", seq%50)

				callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				began := time.Now()
				_, _, err := kv.Get(callCtx, key, opts...)
				elapsed := time.Since(began)
				cancel()

				if err != nil {
					if ctx.Err() == nil {
						localFailed++
					}
					continue
				}
				local = append(local, elapsed)
			}

			mu.Lock()
			latencies = append(latencies, local...)
			failed += localFailed
			mu.Unlock()
		}(w)
	}

	time.Sleep(duration)
	stop()
	wg.Wait()

	elapsed := time.Since(start)
	slices.Sort(latencies)

	mode := "linearizable"
	if stale {
		mode = "stale"
	}

	fmt.Printf(
		"%-7d %-14s %10.1f %12s %12s %12s %8d\n",
		size,
		mode,
		float64(len(latencies))/elapsed.Seconds(),
		percentile(latencies, 0.50).Round(time.Microsecond),
		percentile(latencies, 0.95).Round(time.Microsecond),
		percentile(latencies, 0.99).Round(time.Microsecond),
		failed,
	)

	return nil
}

func runFailover(size, trials int) error {
	c, dataDir, err := newCluster(size)
	if err != nil {
		return err
	}
	defer c.Stop()
	defer os.RemoveAll(dataDir)

	kv, err := client.New(c.Addresses)
	if err != nil {
		return err
	}
	defer kv.Close()

	var samples []time.Duration

	for trial := 0; trial < trials; trial++ {
		if size-trial-1 < size/2+1 {
			break
		}

		leader := c.WaitForLeader(20 * time.Second)
		if leader == nil {
			return fmt.Errorf("no leader emerged before trial %d", trial)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := kv.Put(ctx, fmt.Sprintf("warm-%d", trial), "v"); err != nil {
			cancel()
			return fmt.Errorf("warmup write failed: %w", err)
		}
		cancel()

		leader.Stop()
		began := time.Now()

		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		err := kv.Put(ctx, fmt.Sprintf("after-%d", trial), "v")
		cancel()

		if err != nil {
			return fmt.Errorf("no write succeeded after killing the leader: %w", err)
		}

		samples = append(samples, time.Since(began))
	}

	if len(samples) == 0 {
		return fmt.Errorf("no failover trials could run for %d nodes", size)
	}

	slices.Sort(samples)

	var total time.Duration
	for _, sample := range samples {
		total += sample
	}

	fmt.Printf(
		"%-7d %-9d %12s %12s %12s\n",
		size,
		len(samples),
		(total / time.Duration(len(samples))).Round(time.Millisecond),
		samples[0].Round(time.Millisecond),
		samples[len(samples)-1].Round(time.Millisecond),
	)

	return nil
}

func main() {
	sizes := flag.String("sizes", "3,5,7", "comma separated cluster sizes")
	writers := flag.Int("writers", 8, "concurrent writers")
	duration := flag.Duration("duration", 10*time.Second, "throughput run length per size")
	trials := flag.Int("failover-trials", 3, "leader kills per cluster size")
	verbose := flag.Bool("verbose", false, "let the cluster log at info level")
	flag.Parse()

	level := slog.LevelError
	if *verbose {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))

	var parsed []int
	for _, field := range strings.Split(*sizes, ",") {
		size, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			log.Fatalf("bad size %q: %v", field, err)
		}
		parsed = append(parsed, size)
	}

	fmt.Printf("writes: %d concurrent writers, %s per cluster size\n\n", *writers, *duration)
	fmt.Printf("%-7s %-9s %10s %12s %12s %12s %8s\n",
		"nodes", "writers", "writes/s", "p50", "p95", "p99", "failed")

	for _, size := range parsed {
		if err := runThroughput(size, *writers, *duration); err != nil {
			log.Fatalf("throughput %d nodes: %v", size, err)
		}
	}

	fmt.Printf("\nreads: %d concurrent readers, %s per mode\n\n", *writers, *duration)
	fmt.Printf("%-7s %-14s %10s %12s %12s %12s %8s\n",
		"nodes", "mode", "reads/s", "p50", "p95", "p99", "failed")

	for _, size := range parsed {
		for _, stale := range []bool{false, true} {
			if err := runReads(size, *writers, *duration, stale); err != nil {
				log.Fatalf("reads %d nodes: %v", size, err)
			}
		}
	}

	fmt.Printf("\nfailover: time from leader death to the next successful write\n\n")
	fmt.Printf("%-7s %-9s %12s %12s %12s\n", "nodes", "trials", "mean", "min", "max")

	for _, size := range parsed {
		if err := runFailover(size, *trials); err != nil {
			log.Fatalf("failover %d nodes: %v", size, err)
		}
	}
}
