package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	raftpb "github.com/krithik-sri/raft-kv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxAttempts  = 30
	retryBackoff = 50 * time.Millisecond
)

var ErrNoLeader = errors.New("no leader reachable")

type Client struct {
	addresses []string
	id        string
	seq       atomic.Uint64

	mu     sync.Mutex
	leader string
	conns  map[string]*grpc.ClientConn
}

func newClientID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func New(addresses []string) (*Client, error) {
	if len(addresses) == 0 {
		return nil, errors.New("at least one cluster address is required")
	}

	id, err := newClientID()
	if err != nil {
		return nil, fmt.Errorf("generate client id: %w", err)
	}

	return &Client{
		addresses: addresses,
		id:        id,
		conns:     make(map[string]*grpc.ClientConn),
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)

	return firstErr
}

func (c *Client) serviceFor(address string) (raftpb.KVServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, ok := c.conns[address]
	if !ok {
		var err error
		conn, err = grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		c.conns[address] = conn
	}

	return raftpb.NewKVServiceClient(conn), nil
}

func (c *Client) currentTarget() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leader != "" {
		return c.leader
	}
	return c.addresses[0]
}

func (c *Client) setLeader(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leader = address
}

func (c *Client) nextTarget(current string) string {
	for i, address := range c.addresses {
		if address == current {
			return c.addresses[(i+1)%len(c.addresses)]
		}
	}
	return c.addresses[0]
}

func (c *Client) pause(ctx context.Context) error {
	select {
	case <-time.After(retryBackoff):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type call func(raftpb.KVServiceClient) (*raftpb.LeaderRedirect, error)

func (c *Client) do(ctx context.Context, fn call) error {
	target := c.currentTarget()

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		service, err := c.serviceFor(target)
		if err != nil {
			return fmt.Errorf("connect to %s: %w", target, err)
		}

		redirect, err := fn(service)

		if err != nil {
			c.setLeader("")
			target = c.nextTarget(target)

			if err := c.pause(ctx); err != nil {
				return err
			}
			continue
		}

		if redirect == nil {
			c.setLeader(target)
			return nil
		}

		if redirect.LeaderAddress != "" && redirect.LeaderAddress != target {
			target = redirect.LeaderAddress
			continue
		}

		c.setLeader("")
		target = c.nextTarget(target)

		if err := c.pause(ctx); err != nil {
			return err
		}
	}

	return ErrNoLeader
}

func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	var (
		value string
		found bool
	)

	err := c.do(ctx, func(service raftpb.KVServiceClient) (*raftpb.LeaderRedirect, error) {
		resp, err := service.Get(ctx, &raftpb.GetRequest{Key: key})
		if err != nil {
			return nil, err
		}
		if resp.Redirect != nil {
			return resp.Redirect, nil
		}

		value, found = resp.Value, resp.Found
		return nil, nil
	})

	return value, found, err
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	seq := c.seq.Add(1)

	return c.do(ctx, func(service raftpb.KVServiceClient) (*raftpb.LeaderRedirect, error) {
		resp, err := service.Put(ctx, &raftpb.PutRequest{
			Key:      key,
			Value:    value,
			ClientId: c.id,
			Seq:      seq,
		})
		if err != nil {
			return nil, err
		}
		return resp.Redirect, nil
	})
}

func (c *Client) Delete(ctx context.Context, key string) error {
	seq := c.seq.Add(1)

	return c.do(ctx, func(service raftpb.KVServiceClient) (*raftpb.LeaderRedirect, error) {
		resp, err := service.Delete(ctx, &raftpb.DeleteRequest{
			Key:      key,
			ClientId: c.id,
			Seq:      seq,
		})
		if err != nil {
			return nil, err
		}
		return resp.Redirect, nil
	})
}

func (c *Client) CompareAndSwap(
	ctx context.Context,
	key, expected, value string,
) (bool, error) {
	var swapped bool
	seq := c.seq.Add(1)

	err := c.do(ctx, func(service raftpb.KVServiceClient) (*raftpb.LeaderRedirect, error) {
		resp, err := service.CompareAndSwap(ctx, &raftpb.CompareAndSwapRequest{
			Key:      key,
			Expected: expected,
			Value:    value,
			ClientId: c.id,
			Seq:      seq,
		})
		if err != nil {
			return nil, err
		}
		if resp.Redirect != nil {
			return resp.Redirect, nil
		}

		swapped = resp.Swapped
		return nil, nil
	})

	return swapped, err
}
