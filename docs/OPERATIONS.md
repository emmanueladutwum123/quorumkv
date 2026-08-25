# Running quorumkv

This is the operator's document: how to start a cluster, what to watch, and what
to do when something is wrong. The reasoning behind the design is in
[DESIGN.md](DESIGN.md).

## A cluster in one command

```bash
docker compose up --build
```

Three nodes, each with its own volume. The write-ahead log lives on that volume
and a node that loses it has lost its vote and its acknowledged writes with it,
so the volumes are named rather than anonymous: `docker compose down` keeps them
and a restarted cluster recovers instead of forming a new one. `down -v` discards
them, which is the destructive option.

Node 1 publishes `9000` (client and peer API) and `9100` (admin). Any node
accepts client requests and redirects to the leader, so one published entry point
is enough.

```bash
docker compose exec node1 quorumkvctl -endpoints node1:9000 put hello world
docker compose exec node2 quorumkvctl -endpoints node2:9000 get hello
curl -s localhost:9100/metrics
```

Each node binds to its own hostname rather than to every interface, because the
address it listens on is also the address peers reach it at. The CLI's default of
`127.0.0.1:9000` therefore finds nothing from inside a container — pass
`-endpoints` explicitly, as above.

## Running without Docker

```bash
make build
./bin/quorumkvd -id 1 -addr 127.0.0.1:9000 -data-dir /var/lib/quorumkv/1 \
  -admin-addr 127.0.0.1:9100 \
  -peers 1=127.0.0.1:9000,2=127.0.0.1:9001,3=127.0.0.1:9002
```

`-id` must be stable across restarts: it is the node's identity in the
configuration, and a node that comes back with a different one is a new member
that nobody has been told about. `-peers` must include this node, and `-addr`
must match its entry.

## The admin endpoints

`-admin-addr` enables them; leaving it empty disables the listener entirely. It
is deliberately a separate port from the data path, which would be firewalled in
any real deployment — monitoring has to keep working when that path is what has
gone wrong.

| Path | Purpose |
|---|---|
| `/metrics` | Prometheus text format |
| `/healthz` | Liveness: the process is up and its consensus loop has not stopped |
| `/readyz` | Readiness: this node recognises a leader |
| `/status` | JSON snapshot — role, term, indexes, per-peer replication |

The CLI probes both, so a container healthcheck needs no shell or curl in the
image:

```bash
quorumkvctl health 127.0.0.1:9100   # process is up
quorumkvctl ready  127.0.0.1:9100   # a leader is known
```

**Wire your container runtime to `/healthz`, not `/readyz`.** A node that cannot
see a leader is not broken and restarting it will not help. A liveness probe on
readiness restarts every node in the cluster at once during a partition, which is
exactly when the cluster least needs it. `/readyz` belongs on a load balancer,
where "do not send this node traffic" is the correct response. A script that
starts a cluster and immediately writes should wait on `ready` for the same
reason: liveness is green before the first election has finished.

## What to watch

Four questions matter during an incident, and each has a series behind it.

**Is there a leader?** `quorumkv_raft_leader_id` is 0 on a node that knows of
none. Every node reporting 0 for more than a few election timeouts means the
cluster is not forming a quorum — check connectivity and `quorumkv_raft_term`,
which climbs on every failed election.

**Is this node keeping up?** `quorumkv_raft_apply_lag` is committed entries not
yet applied. Persistently non-zero means the state machine is the bottleneck.

**Is replication healthy?** `quorumkv_peer_lag`, exported by the leader per
follower. A single lagging follower is invisible in any aggregate and is exactly
what turns a second failure into an outage. `quorumkv_peer_active` going to 0
means a follower stopped answering.

**Is anything being lost?** `quorumkv_proposals_lost_total` counts writes
accepted by a node and then abandoned, usually because leadership moved before
they committed. Clients are told, so this is not silent data loss — but a rising
rate means the cluster is not holding a stable leader.
`quorumkv_reads_rejected_total` counts reads refused because leadership could not
be confirmed, which is the honest failure: no answer rather than a stale one.

A term that keeps climbing with no election ever completing is the signature of a
node that can receive but not send, or of an election timeout set too close to
the round-trip time between nodes.

## Growing and shrinking a cluster

Add a node as a learner first, wait for it to catch up, then promote it:

```bash
quorumkvctl member add-learner 4 10.0.0.4:9000
quorumkvctl status                     # watch peer 4's match index approach the leader's
quorumkvctl member promote 4
```

The reason is quorum arithmetic. `member add` makes a node a voter immediately,
which raises the quorum requirement the moment it commits — while the new node
may still be receiving a snapshot and unable to help meet it. In a three-node
cluster that means needing three of three, so one further failure stalls commits
entirely. A learner replicates without counting toward quorum, so the risky
window closes before the node has a vote. `member add` exists and warns.

Remove nodes one at a time, and never remove the leader's last peer.

## Backup and restore

The data directory is the backup: segments plus snapshots, and they are
self-describing. Copy it from a stopped node, or from a running one accepting
that you may capture a torn tail — the log's recovery path discards an
incomplete final record, so a copy is recoverable either way.

To restore, put the directory back and start the node with the same `-id`.

## Tuning

`-tick` is the logical clock, and every timeout below is counted in ticks.
`-election-ticks` should be roughly ten times `-heartbeat-ticks`, and the
heartbeat interval should comfortably exceed the round-trip time between nodes —
otherwise followers time out on a healthy leader and elections never settle.

`-snapshot-threshold` trades log growth against stall time. Snapshotting
serialises the whole keyspace on the consensus goroutine, so a large keyspace and
a low threshold means frequent pauses in replication; `make bench` reports what a
snapshot costs at a given size on your hardware.
