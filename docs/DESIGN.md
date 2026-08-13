# quorumkv design

This document records the invariants the implementation maintains, the design
decisions that are not forced by the Raft paper, and the reasoning behind each.
It is written for someone reading the code who wants to know *why* rather than
*what*.

Section references such as §5.4 point to Ongaro & Ousterhout, *In Search of an
Understandable Consensus Algorithm* (2014).

---

## 1. Goals and non-goals

**Goals.**

- Linearizable reads and writes over a replicated key-value map.
- No data loss and no unavailability while a majority of the cluster survives.
- Membership changes with no loss of availability and no possibility of
  split-brain during the transition.
- Failures reproducible from a seed, so that a bug found once can be fixed with
  confidence rather than hopefully.

**Non-goals**, and why each is deliberately excluded:

- *Sharding / horizontal write scaling.* A single Raft group's write throughput
  is bounded by one leader. Multi-group sharding is a well-understood layer
  above consensus, and adding it would dilute the part of this project that is
  actually hard.
- *Transactions across keys.* Compare-and-swap on a single key is provided,
  which is enough for the coordination use cases a store like this serves.
  Multi-key transactions need a concurrency-control protocol that is a separate
  study.
- *Byzantine fault tolerance.* Raft assumes crash-stop failures with a reliable
  eventual network. Nodes may crash, hang, lose messages and see arbitrary
  delays, but they do not lie.

---

## 2. Layering

```
transport ──▶ Step(Message) ──▶ raft core ──▶ Ready() ──▶ WAL, network, FSM
                                    ▲                          │
                                    └────── Advance() ─────────┘
```

The consensus core (`internal/raft`) is a **pure state machine**. It has no
imports beyond the standard library's `errors`, `fmt` and `sort`. It cannot open
a socket, spawn a goroutine, or read a clock.

### 2.1 Why the core is pure

The alternative — a core that owns its own timers and network calls — is easier
to write and far harder to trust. Consensus defects are ordering defects: two
elections that interleave in an unusual way, a message that arrives after a
snapshot has moved the boundary past it, a crash between an fsync and an
acknowledgement. Under real timing, such an interleaving may occur once in
thousands of runs and never again on demand.

With a pure core, an entire cluster runs inside one goroutine. The scheduler
chooses which message is delivered next, and that choice is driven by a seeded
PRNG. A failing test is therefore a seed plus an operation history, replayable
indefinitely. This is the single decision that most shapes the codebase, and
every other testing property follows from it.

The cost is real and worth naming: the driver layer must correctly obey the
`Ready`/`Advance` protocol, and a bug there is a durability bug that core unit
tests cannot catch. That is why the driver is exercised by integration tests
with a real WAL and by crash-restart tests that kill a node between the persist
and send steps.

### 2.2 Time as tick counts

The core has no clock. `Tick()` advances a logical counter, and election and
heartbeat timeouts are expressed as counts of those ticks. The driver calls
`Tick()` from a real ticker in production; the simulator calls it directly, and
can therefore skew one node's clock relative to another by simply ticking it at
a different rate.

---

## 3. The replicated log

### 3.1 Index arithmetic and the compaction boundary

The log is a sequence of entries, each carrying a `(Term, Index)` pair. Indexes
start at 1, so index 0 means "before the log begins".

After compaction the prefix is gone, and the log keeps `snapIndex`/`snapTerm`
describing the last entry folded into the snapshot. These act as a **virtual
entry** preceding the first live one. That detail matters: a leader probing a
follower at exactly `snapIndex` must still receive a correct answer to the
consistency check, and without the virtual entry that probe would look like a
reference to a nonexistent index and force an unnecessary snapshot transfer.

The single invariant that all index arithmetic derives from:

```
entries[i].Index == snapIndex + 1 + i
```

All of it is confined to `internal/raft/log.go`. No other file converts between
a log index and a slice offset.

### 3.2 Entries live in memory

Entries after the compaction boundary are held in memory; durability comes from
the write-ahead log, not from reading back from disk.

This is a deliberate trade. It means every log query is a slice index that
cannot fail with an I/O error or block — so the consensus core never has to
handle a read failure in the middle of a decision, which removes an entire
category of partial-failure paths. The memory cost is bounded by the snapshot
threshold rather than by cluster lifetime: compaction trims the prefix, so
steady-state footprint is roughly `threshold × mean entry size`.

The cost surfaces on restart, where the whole retained log must be replayed from
the WAL into memory before the node can participate. That is bounded by the same
threshold.

### 3.3 Divergence hints: a two-sided walk

When a follower rejects an `AppendEntries` because its log diverges, the textbook
response is for the leader to decrement `nextIndex` and retry — one round trip
per conflicting entry. After a long partition that is thousands of round trips
before a node can catch up.

Repair here is proportional to conflicting *terms* instead, and it takes both
halves of a two-sided walk to get there:

1. **The follower's half.** On rejection it walks back to the highest position
   where its own term is no greater than the probed term, and reports that
   `(HintIndex, HintTerm)`. This skips its own conflicting suffix.
2. **The leader's half.** On receiving the hint, the leader walks back over its
   *own* entries whose term exceeds `HintTerm`, since none of them can possibly
   match a follower sitting at that term.

Either half alone still degrades to roughly one round trip per entry. Together,
the measured cost of reconciling 200 conflicting entries across two terms is
**5 round trips** (`TestRepairSkipsWholeConflictingTermPerRoundTrip`), and the
test fails if it exceeds 15 — so the property is pinned, not merely intended.

### 3.4 Recovering a stalled replication stream

A streaming peer has its `Next` advanced optimistically on send, which is what
makes pipelining possible. It also creates a failure mode that is easy to miss:
if those in-flight messages are dropped, the leader has nothing left to send and
the follower has nothing to acknowledge. Replication to that peer wedges until an
election intervenes.

The difficulty is that the wedged state — everything sent, nothing acknowledged —
is indistinguishable from healthy pipelining at any single instant. So it is
treated as loss only after two consecutive quiet heartbeat intervals.

Recovery rewinds to `Match + 1`, not to `Next - 1`. The follower is known to hold
`Match`, so the retry passes the consistency check on its first attempt and
carries real entries with it. Retrying at `Next - 1` would anchor on an entry the
follower may never have received, and the resulting rejection would drop a
perfectly healthy stream into probing for no reason.

The same reasoning explains why heartbeats are anchored at `Match` rather than
`Next - 1` (§3.5).

### 3.5 Heartbeats anchored at Match

Heartbeats are empty `AppendEntries`. The natural anchor would be `Next - 1`, but
under load `Next` runs ahead of what the follower has acknowledged, so such a
heartbeat fails the consistency check whenever entries are in flight. Every
heartbeat would become a spurious rejection, knocking replication back into
probing precisely when the cluster is busiest.

Anchoring at `Match` avoids this entirely: that entry is by definition already on
the follower, so the check always passes and the heartbeat does its real job —
proving leadership and carrying the commit index forward. The advertised commit
index is capped at `Match`, since a follower may only commit what it actually
holds.

This also keeps the peer RPC surface at the paper's three calls, with no separate
heartbeat message type.

### 3.4 Truncation rewinds durability

When a follower discards a conflicting suffix, any entry in that suffix which was
already reported durable must be un-reported: `stable` rewinds to just before the
truncation point, so `Ready` re-offers the replacement entries for persistence.

Skipping this is a subtle and severe bug. The discarded entries are still in the
WAL. If the node crashes before the replacement entries are written, replay
resurrects entries the cluster has already decided against.

---

## 4. Safety invariants

The implementation maintains the five properties from Figure 3 of the paper.
Where the code enforces each:

| Property | Enforced by |
|---|---|
| **Election Safety** — at most one leader per term | `HardState.Vote` persisted before a vote is sent; a node votes at most once per term |
| **Leader Append-Only** — a leader never overwrites or deletes its own entries | the leader path only appends; `truncateFrom` is reachable only from the follower path |
| **Log Matching** — identical `(term, index)` implies identical prefixes | the `AppendEntries` consistency check in `raftLog.maybeAppend` |
| **Leader Completeness** — a committed entry is present in every future leader's log | the §5.4.1 election restriction in `raftLog.isUpToDate` |
| **State Machine Safety** — no two nodes apply different commands at the same index | Log Matching plus a commit index that only advances |

Two of these deserve their reasoning written out, because they are the ones that
look like optimisations and are not.

### 4.1 The election restriction (§5.4.1)

A vote is granted only to a candidate whose log is at least as up to date as the
voter's, compared by last term first and length only as a tiebreak.

This is what makes leader election *sufficient* for safety, with no extra
mechanism to recover committed-but-missing entries. A committed entry is on a
majority. A candidate needs votes from a majority. Any two majorities intersect.
So a candidate missing a committed entry must ask at least one node that has it
— and that node refuses. A candidate that wins therefore cannot be missing
anything committed.

### 4.2 No commitment from a previous term (§5.4.2)

A new leader may **not** conclude that an entry from an earlier term is committed
merely because it is now replicated on a majority. The paper's Figure 8 shows
the failure: such an entry can still be overwritten by a later leader, so
committing it can un-commit an applied write.

The fix is that a leader commits a blank `EntryNoOp` of its own term on election.
Once that entry is committed by the normal majority rule, everything preceding
it is committed transitively and safely. The no-op also has a useful side
effect: it establishes the new leader's commit index promptly, so linearizable
reads become available without waiting for the first client write.

### 4.3 Committed entries are never overwritten

`maybeAppend` panics if an incoming entry contradicts one at or below the commit
index. This is not defensive coding — it is an assertion that the protocol has
been violated. A committed entry is durable on a majority and may already have
been served to a client. Continuing past that point would silently corrupt the
log, so the process fails loudly instead, which is the correct behaviour for a
storage system that has detected it cannot keep its promises.

---

## 5. Durability contract

`Ready` is not a set of suggestions. Its ordering is required:

1. Persist `HardState`, `Snapshot` and `Entries`, and fsync.
2. Send `Messages`.
3. Apply `Committed` to the state machine; deliver `ReadStates`.
4. Call `Advance()`.

Step 1 must precede step 2. Two concrete failures if it does not:

- A node sends a vote, crashes before the vote is durable, restarts, and votes
  again in the same term for a different candidate — two leaders in one term.
- A follower acknowledges an entry, the leader counts that acknowledgement
  toward a majority and commits, the follower crashes before the entry is
  durable — a committed entry now missing from a node the leader counted.

Persisting `HardState.Commit` is an optimisation rather than a requirement: a
restarted node could relearn the commit index from the leader, but persisting it
lets the node re-apply to its state machine immediately rather than waiting.

---

## 6. Membership

`Config` holds up to two voter sets. `Voters[0]` is the incoming configuration
and is always in force; `Voters[1]` is the outgoing configuration and is
non-empty only while a change is in flight. During that window **every quorum
decision must be satisfied in both sets independently**.

A majority of the union is explicitly not sufficient, and `HasQuorum` rejects it.
That is the whole point of the joint step: with sets `{1,2,3}` and `{3,4,5}`, the
groups `{1,2}` and `{4,5}` are each a majority of one set, and if either alone
counted, both could elect a leader in the same term.

**Learners** replicate the log but neither vote nor count toward quorum. A node
joins as a learner and is promoted only once caught up. Adding a voter directly
is a genuine availability hazard: in a 3-node cluster, adding a fourth voter that
is still transferring a snapshot raises the quorum requirement to 3 while
leaving only 3 nodes able to satisfy it, so a single further failure stalls
commits entirely.

---

## 7. Reads

Writes go through the log, so they are linearizable by construction. Reads have
three options, and the cheap ones are subtly wrong:

- *Read from any replica* — fast, scalable, and not linearizable: a follower can
  be arbitrarily behind.
- *Read from the leader's local state* — also not linearizable. A leader that has
  been partitioned away may not yet know it has been deposed, and will happily
  serve a value that a new leader has already overwritten.
- *ReadIndex* (§6.4) — the leader records its current commit index, confirms with
  a heartbeat round trip that it still holds leadership, waits for the state
  machine to apply through that index, then serves the read locally. One round
  trip to a quorum, no log append, and linearizable.

quorumkv uses ReadIndex by default and exposes stale local reads as an explicit
per-request opt-in, so the weaker guarantee is always a choice the caller makes
knowingly.

---

## 8. Exactly-once client semantics

Raft guarantees a committed entry is applied *at least* once. It does not prevent
a client from committing the same command twice: if a client's write succeeds but
the response is lost, the client retries and the command is committed again.

For a blind `Put` that is harmless. For anything read-modify-write it is not — a
duplicated compare-and-swap can succeed twice against different states.

Every mutating request therefore carries a `(client_id, sequence)` pair, and the
state machine keeps a session table of the last sequence applied per client along
with its result. A replayed command is recognised and its original result
returned without reapplying. The session table is part of the state machine, so
it is replicated and included in snapshots — a failover must not forget which
requests it has already served.

---

## 9. Failure modes

To be extended as the corresponding machinery lands (M4–M7). Currently
documented: leader crash, follower crash, symmetric and asymmetric partitions,
disk loss, clock skew, and the log-repair path after a long partition.
