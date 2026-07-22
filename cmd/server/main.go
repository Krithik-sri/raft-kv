package main

import (
	"flag"
	"fmt"
)

func main() {
	id := flag.String("id", "", "node ID")
	addr := flag.String("addr", "", "address this node listens on")
	flag.Parse()

	fmt.Printf("starting node id=%s addr=%s\n", *id, *addr)
}
