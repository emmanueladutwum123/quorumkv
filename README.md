# quorumkv

A distributed, strongly-consistent key-value store built on a from-scratch
implementation of the Raft consensus algorithm.

quorumkv replicates every write to a majority of nodes before acknowledging it,
survives the loss of any minority of the cluster without downtime or data loss,
and serves reads that are linearizable — a read always observes every write that
completed before it started, even immediately after a leader failover.

This is a study of the hard parts of distributed systems, built deliberately
without a consensus library so that every safety argument in the code is one I
had to make myself.

## Why the design looks like this

**The consensus core is a pure state machine.** `internal/raft` performs no I/O,
starts no goroutines, and never reads the clock. Every input arrives through
`Step(Message)` or `Tick()`, and every output — entries to persist, messages to
send, entries to apply — leaves through `Ready()`.

That constraint is the foundation everything else rests on. Because the core
cannot observe the outside world, a test can place five nodes in one goroutine,
drive them with a seeded scheduler, and reproduce a network partition, a clock
skew, and a crash-restart in an exact interleaving. Consensus bugs are
overwhelmingly *ordering* bugs, and ordering bugs that only appear under real
timing are effectively undebuggable. Here a failing run is a seed.

**Durability ordering is a contract, not a convention.** `Ready` specifies what
must be fsynced before the accompanying messages may be sent. A node that
promises a vote, crashes, and forgets it can elect a second leader in the same
term; a node that acknowledges an entry it later loses can lose a committed
write. The ordering requirement is documented at the type and enforced by the
driver.

**Failure handling is designed, not bolted on.** Rejected `AppendEntries`
responses carry a divergence hint so log repair costs one round trip per
conflicting *term* rather than per entry. New members join as non-voting
learners, so a node still transferring a snapshot cannot stall the cluster's
ability to commit. Membership changes go through joint consensus, so no
transition can produce two disjoint majorities.

## Architecture

```
                 ┌──────────────────────────────────────────┐
   client ──────▶│  KVService (gRPC)                        │
                 │  leader redirection, ReadIndex reads,    │
                 │  client-session dedup for exactly-once   │
                 └───────────────────┬──────────────────────┘
                                     │ propose / read
                 ┌───────────────────▼──────────────────────┐
                 │  raft core  (internal/raft)              │
                 │                                          │
                 │  Step(Message) ─▶ [deterministic FSM]    │
                 │  Tick()        ─▶  election / heartbeat  │
                 │                    timers as tick counts │
                 │  Ready()       ─▶ HardState, Entries,    │
                 │                   Messages, Committed    │
                 │                                          │
                 │  no I/O · no goroutines · no clock       │
                 └──┬──────────────┬───────────────┬────────┘
                    │ persist      │ send          │ apply
         ┌──────────▼───────┐ ┌────▼─────────┐ ┌───▼──────────────┐
         │ WAL + snapshots  │ │ gRPC / inmem │ │ FSM: KV store    │
         │ CRC records,     │ │ transport    │ │ MVCC-free map +  │
         │ torn-write       │ │ (swappable)  │ │ session table    │
         │ recovery         │ │              │ │                  │
         └──────────────────┘ └──────────────┘ └──────────────────┘
```

The transport is an interface with two implementations: gRPC for real clusters,
and an in-memory one for the simulator. Nothing in the consensus core knows
which is in use.

## Status

Built in reviewable milestones. Each is a self-contained commit that builds,
passes its tests, and leaves the repository in a presentable state.

| Milestone | Scope | State |
|---|---|---|
| M1 | Module layout, RPC contracts, log and membership data structures | ✅ done |
| M2 | Raft core state machine, leader election, pre-vote, check-quorum, ReadIndex | ✅ done |
| M3 | Log replication under failure, log repair, snapshot fallback | in progress |
| M4 | Crash-safe WAL, snapshots, log compaction | planned |
| M5 | gRPC transport, KV API, linearizable reads, CLI | planned |
| M6 | Joint-consensus membership changes, learners | planned |
| M7 | Deterministic fault injection, linearizability checker | planned |
| M8 | Metrics, benchmarks, Docker cluster, CI, design docs | planned |

## Build and test

Requires Go 1.26+. `protoc` is needed only to regenerate the RPC stubs; the
generated code is committed, so a plain build does not need it.

```bash
make build   # compile the server and CLI into bin/
make test    # unit and integration suites
make race    # the same suites under the race detector
make cover   # coverage, reported separately for the consensus core
make lint    # gofmt, go vet, and module tidiness
make help    # list every target
```

## Testing approach

The consensus core is tested as a state machine, not as a service. A test
constructs a node, feeds it messages, and asserts on the resulting `Ready`
batch — no sleeps, no ports, no flakes. Timers are tick counts, so an election
timeout is `for i := 0; i < timeout; i++ { n.Tick() }`.

On top of that, `internal/sim` (M7) runs whole clusters inside a single
goroutine under a seeded scheduler that can partition the network, drop,
duplicate, reorder and delay messages, skew clocks, and crash and restart nodes.
Every operation history it produces is checked for linearizability, and any
violation reproduces exactly from its seed.

## Repository layout

```
cmd/quorumkvd     cluster node binary
cmd/quorumkvctl   operator CLI
internal/raft     consensus core — deterministic, dependency-free
internal/wal      write-ahead log and snapshot storage
internal/store    the replicated state machine (key-value + client sessions)
internal/server   node wiring: raft core, storage, transport, client API
internal/transport gRPC and in-memory peer transports
internal/sim      deterministic cluster simulator and fault injection
proto/            versioned RPC contracts
docs/DESIGN.md    architecture, invariants, and failure-mode analysis
```

## Further reading

- [`docs/DESIGN.md`](docs/DESIGN.md) — invariants the implementation maintains,
  and the reasoning behind each departure from a textbook transcription of the
  paper.
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*
  (2014) — the Raft paper. Section references throughout the code point here.

## License

MIT — see [LICENSE](LICENSE).
