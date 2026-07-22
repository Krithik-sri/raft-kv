package main

import (
	"flag"
	"log"
	"net"
	"fmt"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	grpctransport "github.com/krithik-sri/raft-kv/transport/grpc"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "", "node ID")
	addr := flag.String("addr", "", "address this node listens on")
	flag.Parse()

	fmt.Printf("Starting Raft node with ID: %s, listening on address: %s\n", *id, *addr)
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := &grpctransport.Server{}

	raftpb.RegisterRaftServiceServer(grpcServer, server)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}