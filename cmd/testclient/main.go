package main

import (
	"context"
	"fmt"
	"time"
	raftpb "github.com/krithik-sri/raft-kv/proto"
	grpctransport "github.com/krithik-sri/raft-kv/transport/grpc"
)

func main() {
	client, err := grpctransport.NewClient("localhost:5002")

	if err != nil {
		defer client.Close()
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	request := &raftpb.RequestVoteRequest{
		Term: 1,
		CandidateId: "node1",
		LastLogIndex: 0,
		LastLogTerm:0,
	}

	response, err := client.RequestVote(ctx, request)

	if err != nil {
		fmt.Printf("Error while requesting vote: %v\n", err)
		return
	}
	fmt.Printf("Response: %+v\n", response)
}