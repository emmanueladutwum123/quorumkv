// Command quorumkvctl is the operator and client CLI for a quorumkv cluster.
//
// It handles the one thing every client of a Raft-backed store has to handle:
// only the leader accepts writes, and which node that is changes. Rather than
// requiring the caller to know, the CLI takes a list of endpoints and follows the
// redirect a follower returns.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
)

const usage = `quorumkvctl - operator CLI for a quorumkv cluster

Usage:
  quorumkvctl [flags] <command> [args]

Commands:
  put <key> <value>                 store a value
  get <key>                         read a value (linearizable by default)
  del <key>                         delete a key
  cas <key> <expected> <new>        replace a value only if it matches
  create <key> <value>              store a value only if the key is absent
  status                            report cluster state
  health <admin-addr>               probe liveness: the process is up
  ready <admin-addr>                probe readiness: the node recognises a leader
  member add-learner <id> <addr>    add a non-voting member (safe way to grow)
  member promote <id>               promote a caught-up learner to a voter
  member add <id> <addr>            add a voter directly (raises quorum at once)
  member remove <id>                remove a member

Flags:
  -endpoints   comma-separated node addresses (default 127.0.0.1:9000)
  -stale       serve reads from the local replica without coordination
  -timeout     per-request timeout (default 5s)
  -client-id   client identity for exactly-once retries (default: random)

Reads are linearizable unless -stale is given: they observe every write that
completed before the read began, at the cost of one round trip to a quorum.
`

func main() {
	var (
		endpoints = flag.String("endpoints", "127.0.0.1:9000", "comma-separated node addresses")
		stale     = flag.Bool("stale", false, "serve reads locally without coordination")
		timeout   = flag.Duration("timeout", 5*time.Second, "per-request timeout")
		clientID  = flag.Uint64("client-id", 0, "client identity for exactly-once retries")
	)
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	addrs := splitEndpoints(*endpoints)
	if len(addrs) == 0 {
		fail("no endpoints given")
	}

	// A client identity is what makes a retry safe: the state machine recognises a
	// repeated (client, sequence) pair and returns the original result instead of
	// applying the command twice. A random id per invocation is right for a CLI,
	// where each run is a fresh client.
	id := *clientID
	if id == 0 {
		id = rand.Uint64() | 1
	}

	c := &client{addrs: addrs, timeout: *timeout, clientID: id, sequence: 1}
	defer c.close()

	if err := c.run(args, *stale); err != nil {
		fail(err.Error())
	}
}

type client struct {
	addrs    []string
	timeout  time.Duration
	clientID uint64
	sequence uint64

	conns map[string]*grpc.ClientConn
}

func (c *client) run(args []string, stale bool) error {
	cmd, rest := args[0], args[1:]

	switch cmd {
	case "put":
		if len(rest) != 2 {
			return errors.New("usage: put <key> <value>")
		}
		return c.put(rest[0], rest[1])
	case "get":
		if len(rest) != 1 {
			return errors.New("usage: get <key>")
		}
		return c.get(rest[0], stale)
	case "del", "delete":
		if len(rest) != 1 {
			return errors.New("usage: del <key>")
		}
		return c.del(rest[0])
	case "cas":
		if len(rest) != 3 {
			return errors.New("usage: cas <key> <expected> <new>")
		}
		return c.cas(rest[0], rest[1], rest[2], false)
	case "create":
		if len(rest) != 2 {
			return errors.New("usage: create <key> <value>")
		}
		return c.cas(rest[0], "", rest[1], true)
	case "status":
		return c.status()
	case "health":
		if len(rest) != 1 {
			return errors.New("usage: health <admin-addr>")
		}
		return probe(rest[0], "/healthz", c.timeout)
	case "ready":
		if len(rest) != 1 {
			return errors.New("usage: ready <admin-addr>")
		}
		return probe(rest[0], "/readyz", c.timeout)
	case "member":
		return c.member(rest)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// probe requests one of the node's admin endpoints and reports the outcome as an
// exit status.
//
// It exists so that a container healthcheck has something to run. The runtime
// image has no shell and no curl by design, and shipping either one to make a
// probe work would widen the attack surface of a consensus node for the sake of
// a GET request.
//
// Which endpoint to point at is the caller's decision and it matters. Liveness
// (`health`) says the process is up; a container runtime should act on that
// alone, because a node that cannot see a leader is not broken and restarting it
// will not help -- a restart policy wired to readiness takes down every node in a
// cluster at once during a partition. Readiness (`ready`) says the node
// recognises a leader, which is what a load balancer and a startup script want:
// a fresh cluster answers liveness immediately but cannot serve anything until an
// election completes.
func probe(addr, path string, timeout time.Duration) error {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(addr, "/")+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", addr, err)
	}
	defer func() { _ = res.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s: %s", strings.TrimPrefix(path, "/"), res.Status, strings.TrimSpace(string(body)))
	}
	fmt.Println(strings.TrimSpace(string(body)))
	return nil
}

// member runs a membership subcommand.
func (c *client) member(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: member <add-learner|promote|add|remove> ...")
	}
	sub, rest := args[0], args[1:]

	parseID := func(s string) (uint64, error) {
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil || id == 0 {
			return 0, fmt.Errorf("invalid node id %q", s)
		}
		return id, nil
	}

	var (
		ccType kvv1.ConfChangeType
		id     uint64
		addr   string
		err    error
	)

	switch sub {
	case "add-learner":
		if len(rest) != 2 {
			return errors.New("usage: member add-learner <id> <address>")
		}
		ccType = kvv1.ConfChangeType_CONF_CHANGE_TYPE_ADD_LEARNER
		if id, err = parseID(rest[0]); err != nil {
			return err
		}
		addr = rest[1]

	case "promote":
		if len(rest) != 1 {
			return errors.New("usage: member promote <id>")
		}
		ccType = kvv1.ConfChangeType_CONF_CHANGE_TYPE_PROMOTE_LEARNER
		if id, err = parseID(rest[0]); err != nil {
			return err
		}

	case "add":
		if len(rest) != 2 {
			return errors.New("usage: member add <id> <address>")
		}
		ccType = kvv1.ConfChangeType_CONF_CHANGE_TYPE_ADD_VOTER
		if id, err = parseID(rest[0]); err != nil {
			return err
		}
		addr = rest[1]
		// Adding a voter directly raises the quorum requirement immediately, while
		// the new member may still be transferring a snapshot and unable to help
		// meet it. The two-step path avoids that window entirely.
		fmt.Fprintln(os.Stderr,
			"warning: adding a voter directly raises the quorum requirement before the new\n"+
				"         node has caught up. Prefer: member add-learner, wait, member promote.")

	case "remove":
		if len(rest) != 1 {
			return errors.New("usage: member remove <id>")
		}
		ccType = kvv1.ConfChangeType_CONF_CHANGE_TYPE_REMOVE_NODE
		if id, err = parseID(rest[0]); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown member subcommand %q", sub)
	}

	resp, err := call(c, func(ctx context.Context, kv kvv1.KVServiceClient) (*kvv1.ChangeMembershipResponse, error) {
		return kv.ChangeMembership(ctx, &kvv1.ChangeMembershipRequest{
			Type: ccType, NodeId: id, Address: addr,
		})
	})
	if err != nil {
		return err
	}
	fmt.Printf("membership change committed at index %d\n", resp.GetCommitIndex())
	return nil
}

func (c *client) header() *kvv1.RequestHeader {
	h := &kvv1.RequestHeader{ClientId: c.clientID, Sequence: c.sequence}
	c.sequence++
	return h
}

func (c *client) put(key, value string) error {
	hdr := c.header()
	resp, err := call(c, func(ctx context.Context, kv kvv1.KVServiceClient) (*kvv1.PutResponse, error) {
		return kv.Put(ctx, &kvv1.PutRequest{Header: hdr, Key: []byte(key), Value: []byte(value)})
	})
	if err != nil {
		return err
	}
	fmt.Printf("OK  committed at index %d\n", resp.GetCommitIndex())
	return nil
}

func (c *client) get(key string, stale bool) error {
	level := kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE
	if stale {
		level = kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_STALE
	}
	resp, err := call(c, func(ctx context.Context, kv kvv1.KVServiceClient) (*kvv1.GetResponse, error) {
		return kv.Get(ctx, &kvv1.GetRequest{Key: []byte(key), Consistency: level})
	})
	if err != nil {
		return err
	}
	if !resp.GetFound() {
		fmt.Printf("(not found)  read index %d\n", resp.GetReadIndex())
		return nil
	}
	fmt.Printf("%s\n", resp.GetValue())
	return nil
}

func (c *client) del(key string) error {
	hdr := c.header()
	resp, err := call(c, func(ctx context.Context, kv kvv1.KVServiceClient) (*kvv1.DeleteResponse, error) {
		return kv.Delete(ctx, &kvv1.DeleteRequest{Header: hdr, Key: []byte(key)})
	})
	if err != nil {
		return err
	}
	if resp.GetExisted() {
		fmt.Printf("deleted  committed at index %d\n", resp.GetCommitIndex())
	} else {
		fmt.Printf("(key did not exist)  committed at index %d\n", resp.GetCommitIndex())
	}
	return nil
}

func (c *client) cas(key, expected, next string, expectAbsent bool) error {
	hdr := c.header()
	resp, err := call(c, func(ctx context.Context, kv kvv1.KVServiceClient) (*kvv1.CompareAndSwapResponse, error) {
		return kv.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{
			Header:        hdr,
			Key:           []byte(key),
			ExpectedValue: []byte(expected),
			ExpectAbsent:  expectAbsent,
			NewValue:      []byte(next),
		})
	})
	if err != nil {
		return err
	}
	if resp.GetSwapped() {
		fmt.Printf("swapped  committed at index %d\n", resp.GetCommitIndex())
		return nil
	}
	if resp.GetFound() {
		fmt.Printf("not swapped: key holds %q\n", resp.GetCurrentValue())
	} else {
		fmt.Println("not swapped: key does not exist")
	}
	// A failed conditional write is a legitimate outcome, but scripts need to be
	// able to tell it from success, so it exits non-zero.
	os.Exit(1)
	return nil
}

func (c *client) status() error {
	// Status is answered by any node, so this reports every endpoint rather than
	// only the leader: seeing which replicas lag is the point of the command.
	for _, addr := range c.addrs {
		conn, err := c.dial(addr)
		if err != nil {
			fmt.Printf("%s  UNREACHABLE (%v)\n", addr, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		resp, err := kvv1.NewKVServiceClient(conn).Status(ctx, &kvv1.StatusRequest{})
		cancel()
		if err != nil {
			fmt.Printf("%s  UNREACHABLE (%v)\n", addr, rpcMessage(err))
			continue
		}
		fmt.Printf("%s  node=%d role=%-9s term=%d leader=%d commit=%d applied=%d last=%d snapshot=%d\n",
			addr, resp.GetNodeId(), resp.GetRole(), resp.GetTerm(), resp.GetLeaderId(),
			resp.GetCommitIndex(), resp.GetAppliedIndex(), resp.GetLastLogIndex(), resp.GetSnapshotIndex())
		for _, p := range resp.GetPeers() {
			kind := "voter"
			if p.GetIsLearner() {
				kind = "learner"
			}
			fmt.Printf("    peer %d  %-7s match=%-8d %-8s %s\n",
				p.GetNodeId(), kind, p.GetMatchIndex(), p.GetReachability(), p.GetAddress())
		}
	}
	return nil
}

// call runs an operation against the cluster, following leader redirects.
//
// A follower that refuses a write names the leader in its error, so the retry
// goes straight there. The endpoints are otherwise tried in order, which matters
// on a cold start when no node knows who leads yet.
func call[T any](c *client, op func(context.Context, kvv1.KVServiceClient) (T, error)) (T, error) {
	var zero T
	var lastErr error

	tried := make(map[string]bool)
	queue := append([]string(nil), c.addrs...)

	for attempt := 0; attempt < 2*len(c.addrs)+4 && len(queue) > 0; attempt++ {
		addr := queue[0]
		queue = queue[1:]
		if tried[addr] && len(queue) > 0 {
			continue
		}
		tried[addr] = true

		conn, err := c.dial(addr)
		if err != nil {
			lastErr = err
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		resp, err := op(ctx, kvv1.NewKVServiceClient(conn))
		cancel()
		if err == nil {
			return resp, nil
		}
		lastErr = fmt.Errorf("%s: %s", addr, rpcMessage(err))

		// A redirect names the leader's address. Jump straight to it rather than
		// working through the remaining endpoints.
		if leader := leaderFromError(err); leader != "" && !tried[leader] {
			queue = append([]string{leader}, queue...)
			continue
		}
		// Aborted means a proposal was lost to a leader change; the retry is safe
		// because it carries the same sequence number.
		if status.Code(err) == codes.Aborted || status.Code(err) == codes.FailedPrecondition {
			continue
		}
		return zero, lastErr
	}

	if lastErr == nil {
		lastErr = errors.New("no endpoint accepted the request")
	}
	return zero, lastErr
}

// leaderFromError extracts the address a redirect points at.
func leaderFromError(err error) string {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return ""
	}
	// The server formats redirects as "... leader is node N at host:port".
	const marker = " at "
	msg := st.Message()
	i := strings.LastIndex(msg, marker)
	if i < 0 {
		return ""
	}
	addr := strings.TrimSpace(msg[i+len(marker):])
	if addr == "" || strings.ContainsAny(addr, " \t") {
		return ""
	}
	return addr
}

func rpcMessage(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

func (c *client) dial(addr string) (*grpc.ClientConn, error) {
	if c.conns == nil {
		c.conns = make(map[string]*grpc.ClientConn)
	}
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *client) close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func splitEndpoints(spec string) []string {
	var out []string
	for _, part := range strings.Split(spec, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "error: "+msg)
	os.Exit(1)
}
