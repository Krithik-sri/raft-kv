package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/krithik-sri/raft-kv/client"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: kvctl [flags] <command> [args]

commands:
  get <key>
  put <key> <value>
  delete <key>
  cas <key> <expected> <value>
  members
  add <id> <address>
  remove <id>

flags:
`)
	flag.PrintDefaults()
}

func run() error {
	peers := flag.String("peers", "", "comma separated cluster addresses")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout (add needs more, it waits for the catch-up)")
	stale := flag.Bool("stale", false, "allow a possibly out-of-date read (get only)")
	flag.Usage = usage
	flag.Parse()

	if *peers == "" {
		return fmt.Errorf("--peers is required")
	}

	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("a command is required")
	}

	addresses := strings.Split(*peers, ",")
	kv, err := client.New(addresses)
	if err != nil {
		return err
	}
	defer kv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "get":
		if len(args) != 2 {
			return fmt.Errorf("get requires exactly one key")
		}

		var opts []client.GetOption
		if *stale {
			opts = append(opts, client.WithStale())
		}

		value, found, err := kv.Get(ctx, args[1], opts...)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("key %q not found", args[1])
		}
		fmt.Println(value)

	case "put":
		if len(args) != 3 {
			return fmt.Errorf("put requires a key and a value")
		}
		if err := kv.Put(ctx, args[1], args[2]); err != nil {
			return err
		}
		fmt.Println("ok")

	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("delete requires exactly one key")
		}
		if err := kv.Delete(ctx, args[1]); err != nil {
			return err
		}
		fmt.Println("ok")

	case "cas":
		if len(args) != 4 {
			return fmt.Errorf("cas requires a key, an expected value and a new value")
		}

		swapped, err := kv.CompareAndSwap(ctx, args[1], args[2], args[3])
		if err != nil {
			return err
		}
		if !swapped {
			return fmt.Errorf("compare-and-swap rejected: %q does not hold %q", args[1], args[2])
		}
		fmt.Println("ok")

	case "members":
		members, leaderID, err := kv.Members(ctx)
		if err != nil {
			return err
		}

		for _, m := range members {
			role := "voter"
			if !m.Voter {
				role = "learner"
			}
			marker := " "
			if m.ID == leaderID {
				marker = "*"
			}
			fmt.Printf("%s %-8s %-22s %s\n", marker, m.ID, m.Address, role)
		}

	case "add":
		if len(args) != 3 {
			return fmt.Errorf("add requires an id and an address")
		}
		if err := kv.AddServer(ctx, args[1], args[2]); err != nil {
			return err
		}
		fmt.Println("ok")

	case "remove":
		if len(args) != 2 {
			return fmt.Errorf("remove requires exactly one id")
		}
		if err := kv.RemoveServer(ctx, args[1]); err != nil {
			return err
		}
		fmt.Println("ok")

	default:
		return fmt.Errorf("unknown command %q", args[0])
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kvctl: %v\n", err)
		os.Exit(1)
	}
}
