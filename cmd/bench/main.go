package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/krithik-sri/raft-kv/client"
	"github.com/krithik-sri/raft-kv/kvstore"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	"github.com/krithik-sri/raft-kv/storage"
	grpctransport "github.com/krithik-sri/raft-kv/transport/grpc"
	"google.golang.org/grpc"
)

type node struct {
	id      raft.NodeID
	address string
	raft    *raft.Raft
	server  *grpc.Server
	store   *storage.File
	cancel  context.CancelFunc
	stopped bool
}

func (n *node) stop() {
	if n.stopped {
		return
	}
	n.stopped = true
	n.cancel()
	n.server.Stop()
	n.store.Close()
}

type cluster struct {
	nodes     []*node
	addresses []string
	dataDir   string
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.stop()
	}
	os.RemoveAll(c.dataDir)
}

func (c *cluster) leader() *node {
	for _, n := range c.nodes {
		if n.stopped {
			continue
		}
		if _, isLeader := n.raft.LeaderHint(); isLeader {
			return n
		}
	}
	return nil
}

func (c *cluster) waitForLeader(timeout time.Duration) *node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := c.leader(); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func startCluster(size int) (*cluster, error) {
	dataDir, err := os.MkdirTemp("", "raft-bench-")
	if err != nil {
		return nil, err
	}

	listeners := make([]net.Listener, size)
	ids := make([]raft.NodeID, size)
	addresses := make([]string, size)

	for i := 0; i < size; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		listeners[i] = listener
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
		addresses[i] = listener.Addr().String()
	}

	c := &cluster{addresses: addresses, dataDir: dataDir}

	for i := 0; i < size; i++ {
		var peers []raft.Peer
		for j := 0; j < size; j++ {
			if j != i {
				peers = append(peers, raft.Peer{ID: ids[j], Address: addresses[j]})
			}
		}

		store, err := storage.Open(filepath.Join(dataDir, string(ids[i])+".log"))
		if err != nil {
			return nil, err
		}

		machine := kvstore.New()
		node2, err := raft.New(ids[i], peers, &grpctransport.Transport{}, machine, store)
		if err != nil {
			return nil, err
		}

		server := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(server, grpctransport.NewServer(node2))
		raftpb.RegisterKVServiceServer(server, grpctransport.NewKVServer(node2, machine, peers))

		ctx, cancel := context.WithCancel(context.Background())

		go server.Serve(listeners[i])
		go node2.Start(ctx)

		c.nodes = append(c.nodes, &node{
			id:      ids[i],
			address: addresses[i],
			raft:    node2,
			server:  server,
			store:   store,
			cancel:  cancel,
		})
	}

	return c, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func runThroughput(size, writers int, duration time.Duration) error {
	c, err := startCluster(size)
	if err != nil {
		return err
	}
	defer c.stop()

	if c.waitForLeader(20*time.Second) == nil {
		return fmt.Errorf("no leader emerged for a %d node cluster", size)
	}

	kv, err := client.New(c.addresses)
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

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

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

func runFailover(size, trials int) error {
	c, err := startCluster(size)
	if err != nil {
		return err
	}
	defer c.stop()

	kv, err := client.New(c.addresses)
	if err != nil {
		return err
	}
	defer kv.Close()

	var samples []time.Duration

	for trial := 0; trial < trials; trial++ {
		if size-trial-1 < size/2+1 {
			break
		}

		leader := c.waitForLeader(20 * time.Second)
		if leader == nil {
			return fmt.Errorf("no leader emerged before trial %d", trial)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := kv.Put(ctx, fmt.Sprintf("warm-%d", trial), "v"); err != nil {
			cancel()
			return fmt.Errorf("warmup write failed: %w", err)
		}
		cancel()

		leader.stop()
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

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

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
	flag.Parse()

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

	fmt.Printf("\nfailover: time from leader death to the next successful write\n\n")
	fmt.Printf("%-7s %-9s %12s %12s %12s\n", "nodes", "trials", "mean", "min", "max")

	for _, size := range parsed {
		if err := runFailover(size, *trials); err != nil {
			log.Fatalf("failover %d nodes: %v", size, err)
		}
	}
}
