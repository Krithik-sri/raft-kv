package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/krithik-sri/raft-kv/kvstore"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
	"github.com/krithik-sri/raft-kv/storage"
	grpctransport "github.com/krithik-sri/raft-kv/transport/grpc"
	"google.golang.org/grpc"
)

func parsePeers(value string) ([]raft.Peer, error) {
	if value == "" {
		return nil, nil
	}

	var peers []raft.Peer
	peerStrings := strings.Split(value, ",")
	for _, p := range peerStrings {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed peer configuration: %q must be in id=address format", p)
		}

		peers = append(peers, raft.Peer{
			ID:      raft.NodeID(parts[0]),
			Address: parts[1],
		})
	}
	return peers, nil
}

func main() {
	id := flag.String("id", "", "node ID")
	addr := flag.String("addr", "", "address this node listens on")
	peersFlag := flag.String("peers", "", "comma separated peers in id=address format")
	dataDir := flag.String("data-dir", "data", "directory for persistent raft state")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	join := flag.Bool("join", false, "joining a cluster that already exists, rather than forming a new one")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		log.Fatalf("bad --log-level %q: %v", *logLevel, err)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("Failed to parse peers: %v\n", err)
	}
	fmt.Printf("Starting Raft node with ID: %s, listening on address: %s\n", *id, *addr)
	fmt.Printf("Parsed Peers: %+v\n", peers)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	persistence, err := storage.Open(filepath.Join(*dataDir, *id+".log"))
	if err != nil {
		log.Fatalf("failed to open storage: %v", err)
	}
	defer persistence.Close()

	raftTransport := &grpctransport.Transport{}
	store := kvstore.New()
	var opts []raft.Option
	if *join {
		opts = append(opts, raft.Joining())
	}

	raftNode, err := raft.New(
		raft.Peer{ID: raft.NodeID(*id), Address: *addr},
		peers,
		raftTransport,
		store,
		persistence,
		opts...,
	)
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := grpctransport.NewServer(raftNode)
	kvServer := grpctransport.NewKVServer(raftNode, store, peers)

	raftpb.RegisterRaftServiceServer(grpcServer, server)
	raftpb.RegisterKVServiceServer(grpcServer, kvServer)

	ctx := context.Background()
	go raftNode.Start(ctx)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
