package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	raftpb "github.com/krithik-sri/raft-kv/proto"
	"github.com/krithik-sri/raft-kv/raft"
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
	flag.Parse()

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

	raftTransport := &grpctransport.Transport{}
	raftNode := raft.New(
		raft.NodeID(*id),
		peers,
		raftTransport,
	)

	grpcServer := grpc.NewServer()
	server := grpctransport.NewServer(raftNode)

	raftpb.RegisterRaftServiceServer(grpcServer, server)

	ctx := context.Background()
	go raftNode.Start(ctx)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
