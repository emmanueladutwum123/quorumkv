// Command quorumkvd runs one node of a quorumkv cluster.
//
// A single listener serves both the peer consensus protocol and the client API.
// They are separate gRPC services on one port, which keeps deployment simple; a
// production setup would more likely split them so peer traffic can be secured
// and firewalled independently of client traffic.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
	"github.com/emmanueladutwum123/quorumkv/internal/server"
	"github.com/emmanueladutwum123/quorumkv/internal/transport"
)

func main() {
	var (
		id        = flag.Uint64("id", 0, "this node's id (required, non-zero, stable across restarts)")
		addr      = flag.String("addr", "127.0.0.1:9000", "listen address for the peer and client APIs")
		dataDir   = flag.String("data-dir", "", "directory for the write-ahead log and snapshots (required)")
		peerList  = flag.String("peers", "", "initial cluster as id=host:port,... including this node")
		learner   = flag.Bool("learner", false, "join as a non-voting learner")
		tick      = flag.Duration("tick", 100*time.Millisecond, "wall-clock period of one logical tick")
		election  = flag.Int("election-ticks", 10, "election timeout in ticks")
		heartbeat = flag.Int("heartbeat-ticks", 1, "heartbeat interval in ticks")
		snapEvery = flag.Uint64("snapshot-threshold", 10000, "entries past the compaction boundary before snapshotting")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
	)
	flag.Parse()

	logger := newLogger(*logLevel)

	if *id == 0 {
		fatal(logger, "-id is required and must be non-zero")
	}
	if *dataDir == "" {
		fatal(logger, "-data-dir is required")
	}

	peers, err := parsePeers(*peerList)
	if err != nil {
		fatal(logger, "invalid -peers: "+err.Error())
	}
	if len(peers) == 0 {
		// A single-node cluster is a legitimate configuration and the simplest way
		// to try the store out, so it is allowed rather than rejected.
		peers = map[raft.NodeID]string{raft.NodeID(*id): *addr}
		logger.Info("no peers given, starting as a single-node cluster")
	}
	if _, ok := peers[raft.NodeID(*id)]; !ok {
		fatal(logger, fmt.Sprintf("-peers does not include this node (id %d)", *id))
	}

	srv, err := server.New(server.Config{
		NodeID:                raft.NodeID(*id),
		Addr:                  *addr,
		DataDir:               *dataDir,
		Peers:                 peers,
		TickInterval:          *tick,
		ElectionTimeoutTicks:  *election,
		HeartbeatTimeoutTicks: *heartbeat,
		SnapshotThreshold:     *snapEvery,
		Learner:               *learner,
	})
	if err != nil {
		fatal(logger, "start node: "+err.Error())
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fatal(logger, "listen: "+err.Error())
	}

	grpcServer := grpc.NewServer()
	transport.NewPeerServer(raft.NodeID(*id), srv.StepAndWait).Register(grpcServer)
	server.NewKVService(srv).Register(grpcServer)

	// The consensus loop runs in its own goroutine and owns the core; the gRPC
	// server runs in another and only ever hands work to it.
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run() }()

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()

	logger.Info("node started",
		"id", *id, "addr", *addr, "data_dir", *dataDir,
		"peers", len(peers), "learner", *learner)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signals:
		logger.Info("shutting down", "signal", sig.String())
	case err := <-runErr:
		if err != nil {
			// The consensus loop only stops on a durability failure, which cannot be
			// recovered in place: continuing would mean acknowledging writes the node
			// cannot honour.
			logger.Error("consensus loop failed", "error", err)
			grpcServer.Stop()
			srv.Stop()
			os.Exit(1)
		}
	case err := <-serveErr:
		if err != nil {
			logger.Error("grpc server failed", "error", err)
			srv.Stop()
			os.Exit(1)
		}
	}

	// Stop accepting new work before shutting the node down, so no request is
	// admitted that cannot be completed.
	grpcServer.GracefulStop()
	if err := srv.Stop(); err != nil {
		logger.Error("shutdown", "error", err)
		os.Exit(1)
	}
	logger.Info("stopped cleanly")
}

// parsePeers reads an "id=host:port,..." cluster description.
func parsePeers(spec string) (map[raft.NodeID]string, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	out := make(map[raft.NodeID]string)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// SplitN with 2, because the address itself contains a colon and may
		// contain an equals sign in exotic forms.
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("peer %q is not in id=address form", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peer %q has a non-numeric id", part)
		}
		if id == 0 {
			return nil, fmt.Errorf("peer id 0 is reserved")
		}
		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			return nil, fmt.Errorf("peer %q has an empty address", part)
		}
		if prev, dup := out[raft.NodeID(id)]; dup {
			return nil, fmt.Errorf("peer id %d appears twice (%s and %s)", id, prev, addr)
		}
		out[raft.NodeID(id)] = addr
	}
	return out, nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func fatal(logger *slog.Logger, msg string) {
	logger.Error(msg)
	os.Exit(2)
}
