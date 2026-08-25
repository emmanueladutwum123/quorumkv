package sim

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sort"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
	"github.com/emmanueladutwum123/quorumkv/internal/store"
)

// Cluster is a whole quorumkv cluster — consensus cores, state machines, durable
// storage and clients — running inside a single goroutine under a seeded
// scheduler.
//
// Nothing here reads a clock, opens a socket or starts a goroutine. Every
// decision about which message is delivered next, which node crashes, and when a
// partition heals comes from one PRNG. A failing run is therefore fully described
// by its seed, which is the difference between a bug you can fix and a bug you
// can only hope not to see again.
type Cluster struct {
	ids   []raft.NodeID
	nodes map[raft.NodeID]*simNode

	rng *rand.Rand
	// now is the logical clock. It orders the history's intervals and has no
	// relationship to wall time.
	now int64

	queue   []inFlight
	faults  Faults
	clients []*simClient

	history History
	stats   Stats

	opts Options
	// reachable models the network topology: it reports whether a message from one
	// node can reach another. A partition is expressed by replacing this function.
	reachable func(from, to raft.NodeID) bool
}

// Stats records what a run actually exercised, so a test can assert that the
// scenario did something rather than silently doing nothing.
type Stats struct {
	Steps          int
	Delivered      int
	Dropped        int
	Duplicated     int
	Crashes        int
	Restarts       int
	Elections      int
	Partitions     int
	OpsCompleted   int
	OpsUnknown     int
	Proposals      int
	SnapshotsSent  int
	SnapshotsTaken int
}

// Faults configures the failure injection. All probabilities are per-message or
// per-step, in [0, 1].
type Faults struct {
	// DropRate is the chance a message is discarded.
	DropRate float64
	// DuplicateRate is the chance a message is delivered twice. Duplicates matter
	// because idempotence bugs hide behind an otherwise reliable network.
	DuplicateRate float64
	// ReorderRate is the chance a message is queued out of order rather than
	// appended, which combined with random delivery produces arbitrary reordering.
	ReorderRate float64
	// PartitionRate is the chance per step of re-partitioning the network.
	PartitionRate float64
	// HealRate is the chance per step of restoring full connectivity.
	HealRate float64
	// CrashRate is the chance per step that a live node crashes, losing all
	// volatile state.
	CrashRate float64
	// RestartRate is the chance per step that a crashed node restarts from its
	// durable state alone.
	RestartRate float64
	// ClockSkew, when true, ticks nodes at different rates, so no node's notion of
	// elapsed time matches another's.
	ClockSkew bool
	// MaxCrashed bounds simultaneous crashes. Left at zero it defaults to a
	// minority, because a cluster that loses its majority is *expected* to stop
	// making progress and a test that allows it measures nothing.
	MaxCrashed int
}

// Options configures a simulated cluster.
type Options struct {
	Nodes   int
	Clients int
	Seed    uint64
	Faults  Faults
	// Keys is the size of the keyspace. A small keyspace is deliberate: it forces
	// contention, and contention is what makes linearizability violations possible
	// at all.
	Keys int
	// SnapshotThreshold triggers compaction, exercising the snapshot path under
	// load. Zero disables compaction.
	SnapshotThreshold uint64
	// OpStartRate is the chance per step that an idle client starts an operation.
	OpStartRate float64
}

func (o *Options) withDefaults() {
	if o.Nodes <= 0 {
		o.Nodes = 3
	}
	if o.Clients <= 0 {
		o.Clients = 3
	}
	if o.Keys <= 0 {
		o.Keys = 4
	}
	if o.OpStartRate <= 0 {
		o.OpStartRate = 0.4
	}
	if o.Faults.MaxCrashed == 0 {
		o.Faults.MaxCrashed = (o.Nodes - 1) / 2
	}
}

// simNode is one node plus the state its driver owns.
type simNode struct {
	id   raft.NodeID
	core *raft.Node
	fsm  *store.Store

	// Durable state. Only what appears here survives a crash, which is what makes
	// a restart a real test of the durability contract.
	hardState raft.HardState
	entries   []raft.Entry
	snapshot  *raft.Snapshot

	down bool
	// tickEvery implements clock skew: this node advances its logical clock once
	// every tickEvery steps.
	tickEvery int

	// pending maps a log index to the client operation waiting on it.
	pending map[raft.Index]*pendingProposal
	// reads maps a read token to the read waiting on it.
	reads map[uint64]*pendingRead

	nextReadID uint64
	opts       raft.Options
}

// Snapshot implements raft.SnapshotProvider for this node.
func (n *simNode) Snapshot() (raft.Snapshot, error) {
	if n.snapshot == nil {
		return raft.Snapshot{}, raft.ErrUnavailable
	}
	return *n.snapshot, nil
}

type pendingProposal struct {
	term   raft.Term
	client *simClient
}

type pendingRead struct {
	index    raft.Index
	resolved bool
	client   *simClient
	key      string
}

// simClient issues one operation at a time, which is what a client with a
// sequence-numbered session does.
type simClient struct {
	id       int
	sequence uint64

	// busy holds the operation in flight, if any.
	busy   *Op
	nodeID raft.NodeID
}

// sortedIndexes returns a map's keys in ascending order.
//
// Map iteration order in Go is randomised, and anything derived from it — the
// order operations are recorded in, which client is failed first — would make two
// runs of the same seed diverge. That would defeat the entire point of the
// simulator, so every traversal here is explicitly ordered.
func sortedIndexes[V any](m map[raft.Index]V) []raft.Index {
	out := make([]raft.Index, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedIDs[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

type inFlight struct {
	msg raft.Message
	// deliverAt is the step at which this message becomes eligible, modelling
	// delay.
	deliverAt int64
}

// New creates a simulated cluster.
func New(opts Options) *Cluster {
	opts.withDefaults()

	c := &Cluster{
		nodes:  make(map[raft.NodeID]*simNode, opts.Nodes),
		rng:    rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x9e3779b97f4a7c15)),
		faults: opts.Faults,
	}
	c.opts = opts

	voters := make([]raft.NodeID, 0, opts.Nodes)
	for i := 1; i <= opts.Nodes; i++ {
		voters = append(voters, raft.NodeID(i))
	}
	cfg := raft.NewConfig(voters...)

	for _, id := range voters {
		n := &simNode{
			id:        id,
			fsm:       store.New(),
			pending:   make(map[raft.Index]*pendingProposal),
			reads:     make(map[uint64]*pendingRead),
			tickEvery: 1,
		}
		if opts.Faults.ClockSkew {
			// Different tick rates mean no node's sense of elapsed time matches
			// another's, which is what stresses timeout-based decisions.
			n.tickEvery = 1 + c.rng.IntN(3)
		}
		n.opts = raft.Options{
			ID:                    id,
			Config:                cfg,
			ElectionTimeoutTicks:  10,
			HeartbeatTimeoutTicks: 1,
			Rand:                  c.rng.IntN,
			Snapshots:             n,
		}
		core, err := raft.NewNode(n.opts)
		if err != nil {
			panic(fmt.Sprintf("sim: create node %d: %v", id, err))
		}
		n.core = core
		c.nodes[id] = n
		c.ids = append(c.ids, id)
	}
	sort.Slice(c.ids, func(i, j int) bool { return c.ids[i] < c.ids[j] })

	for i := 0; i < opts.Clients; i++ {
		c.clients = append(c.clients, &simClient{id: i + 1, sequence: 1})
	}
	c.reachable = func(raft.NodeID, raft.NodeID) bool { return true }
	return c
}

// History returns the recorded operations.
func (c *Cluster) History() *History { return &c.history }

// Stats returns what the run exercised.
func (c *Cluster) Stats() Stats { return c.stats }

// Run advances the simulation by the given number of steps.
func (c *Cluster) Run(steps int) {
	for i := 0; i < steps; i++ {
		c.step()
	}
	// Give the cluster a quiet tail with no faults, so operations in flight can
	// complete and the history is not dominated by artificial uncertainty.
	saved := c.faults
	c.faults = Faults{ClockSkew: saved.ClockSkew}
	c.healAll()
	for _, n := range c.nodes {
		if n.down {
			c.restart(n.id)
		}
	}
	for i := 0; i < 400; i++ {
		c.step()
	}
	c.faults = saved

	// Anything still outstanding never returned; the checker treats such an
	// operation as placeable anywhere, or not at all.
	for _, cl := range c.clients {
		if cl.busy != nil {
			cl.busy.Unknown = true
			cl.busy.Return = 0
			c.history.Add(*cl.busy)
			c.stats.OpsUnknown++
			cl.busy = nil
		}
	}
}

func (c *Cluster) step() {
	c.now++
	c.stats.Steps++

	c.injectFaults()
	c.tickNodes()
	c.startOperations()
	c.drainNodes()
	c.deliverMessages()
}

func (c *Cluster) injectFaults() {
	f := c.faults
	if f.PartitionRate > 0 && c.rng.Float64() < f.PartitionRate {
		c.randomPartition()
	}
	if f.HealRate > 0 && c.rng.Float64() < f.HealRate {
		c.healAll()
	}
	if f.CrashRate > 0 && c.rng.Float64() < f.CrashRate {
		c.crashRandom()
	}
	if f.RestartRate > 0 && c.rng.Float64() < f.RestartRate {
		c.restartRandom()
	}
}

// randomPartition splits the cluster into two groups at a random cut.
func (c *Cluster) randomPartition() {
	side := make(map[raft.NodeID]bool, len(c.ids))
	for _, id := range c.ids {
		side[id] = c.rng.IntN(2) == 0
	}
	c.reachable = func(from, to raft.NodeID) bool { return side[from] == side[to] }
	c.stats.Partitions++
}

func (c *Cluster) healAll() {
	c.reachable = func(raft.NodeID, raft.NodeID) bool { return true }
}

func (c *Cluster) crashRandom() {
	downCount := 0
	for _, n := range c.nodes {
		if n.down {
			downCount++
		}
	}
	if downCount >= c.faults.MaxCrashed {
		// Crashing past a minority would leave the cluster unable to commit, which
		// is expected behaviour rather than a bug, so a test that allowed it would
		// mostly measure downtime.
		return
	}
	live := make([]raft.NodeID, 0, len(c.ids))
	for _, id := range c.ids {
		if !c.nodes[id].down {
			live = append(live, id)
		}
	}
	if len(live) == 0 {
		return
	}
	c.crash(live[c.rng.IntN(len(live))])
}

// crash discards a node's volatile state, keeping only what was persisted.
func (c *Cluster) crash(id raft.NodeID) {
	n := c.nodes[id]
	if n.down {
		return
	}
	n.down = true
	c.stats.Crashes++

	// Clients waiting on this node learn nothing about their operations' fate,
	// which is exactly the "unknown outcome" case the checker must handle.
	c.failAllWaiters(n)
}

// failAllWaiters reports every operation outstanding on a node as unknown.
func (c *Cluster) failAllWaiters(n *simNode) {
	for _, index := range sortedIndexes(n.pending) {
		p := n.pending[index]
		delete(n.pending, index)
		c.finishUnknown(p.client)
	}
	for _, id := range sortedIDs(n.reads) {
		r := n.reads[id]
		delete(n.reads, id)
		c.finishUnknown(r.client)
	}
}

func (c *Cluster) restartRandom() {
	down := make([]raft.NodeID, 0, len(c.ids))
	for _, id := range c.ids {
		if c.nodes[id].down {
			down = append(down, id)
		}
	}
	if len(down) == 0 {
		return
	}
	c.restart(down[c.rng.IntN(len(down))])
}

// restart rebuilds a node from its durable state alone. Anything the driver
// failed to persist is now permanently gone, which is what makes this the real
// test of the durability contract.
func (c *Cluster) restart(id raft.NodeID) {
	n := c.nodes[id]
	if !n.down {
		return
	}

	core, err := raft.NewNode(n.opts)
	if err != nil {
		panic(fmt.Sprintf("sim: restart node %d: %v", id, err))
	}
	n.fsm = store.New()

	// Recovery order mirrors the real driver: snapshot first to establish the
	// compaction boundary, then the log that follows it, then term and vote.
	if n.snapshot != nil {
		if err := core.RestoreSnapshot(*n.snapshot); err != nil && err != raft.ErrSnapshotOutOfDate {
			panic(fmt.Sprintf("sim: node %d restore snapshot: %v", id, err))
		}
		if err := n.fsm.Restore(n.snapshot.Data, n.snapshot.Index); err != nil {
			panic(fmt.Sprintf("sim: node %d restore fsm: %v", id, err))
		}
	}
	core.ReplayEntries(n.entries)
	core.SetHardState(n.hardState)

	n.core = core
	n.down = false
	n.pending = make(map[raft.Index]*pendingProposal)
	n.reads = make(map[uint64]*pendingRead)
	c.stats.Restarts++
}

func (c *Cluster) tickNodes() {
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.down {
			continue
		}
		if n.tickEvery <= 1 || c.now%int64(n.tickEvery) == 0 {
			n.core.Tick()
		}
	}
}

// leader returns a node that currently believes it leads, preferring the highest
// term when several do.
func (c *Cluster) leader() *simNode {
	var best *simNode
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.down || n.core.Role() != raft.Leader {
			continue
		}
		if best == nil || n.core.Term() > best.core.Term() {
			best = n
		}
	}
	return best
}

// Leaders returns every node claiming leadership, for safety assertions.
func (c *Cluster) Leaders() map[raft.NodeID]raft.Term {
	out := make(map[raft.NodeID]raft.Term)
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.down && n.core.Role() == raft.Leader {
			out[id] = n.core.Term()
		}
	}
	return out
}

func (c *Cluster) startOperations() {
	for _, cl := range c.clients {
		if cl.busy != nil || c.rng.Float64() > c.opts.OpStartRate {
			continue
		}
		leader := c.leader()
		if leader == nil {
			// No leader to accept work. The client simply waits, which is what a real
			// client does during an election.
			continue
		}
		c.beginOp(cl, leader)
	}
}

func (c *Cluster) beginOp(cl *simClient, leader *simNode) {
	key := fmt.Sprintf("k%d", c.rng.IntN(c.opts.Keys))
	op := Op{
		ClientID: cl.id,
		Key:      key,
		Invoke:   c.now,
	}

	switch c.rng.IntN(10) {
	case 0, 1, 2, 3:
		op.Kind = OpPut
		op.Value = fmt.Sprintf("c%d-s%d", cl.id, cl.sequence)
	case 4, 5, 6:
		op.Kind = OpGet
	case 7:
		op.Kind = OpDelete
	default:
		op.Kind = OpCAS
		op.Value = fmt.Sprintf("c%d-s%d", cl.id, cl.sequence)
		if c.rng.IntN(2) == 0 {
			op.ExpectAbsent = true
		} else {
			// Expecting a value that may or may not be current is the interesting
			// case: it makes the outcome depend on the exact linearization.
			op.Expected = fmt.Sprintf("c%d-s%d", c.rng.IntN(len(c.clients))+1, c.rng.IntN(4)+1)
		}
	}

	cl.busy = &op
	cl.nodeID = leader.id

	if op.Kind == OpGet {
		c.beginRead(cl, leader, op.Key)
		return
	}
	c.beginWrite(cl, leader, &op)
}

func (c *Cluster) beginWrite(cl *simClient, leader *simNode, op *Op) {
	cmd := &kvv1.Command{
		Key:    []byte(op.Key),
		Header: &kvv1.RequestHeader{ClientId: uint64(cl.id), Sequence: cl.sequence},
	}
	switch op.Kind {
	case OpPut:
		cmd.Op = kvv1.OpType_OP_TYPE_PUT
		cmd.Value = []byte(op.Value)
	case OpDelete:
		cmd.Op = kvv1.OpType_OP_TYPE_DELETE
	case OpCAS:
		cmd.Op = kvv1.OpType_OP_TYPE_CAS
		cmd.Value = []byte(op.Value)
		cmd.ExpectedValue = []byte(op.Expected)
		cmd.ExpectAbsent = op.ExpectAbsent
	}

	data, err := store.EncodeCommand(cmd)
	if err != nil {
		panic(fmt.Sprintf("sim: encode command: %v", err))
	}
	index, err := leader.core.Propose(data)
	if err != nil {
		// Not the leader after all. The operation never started, so it is not
		// recorded — a client that got an immediate rejection simply retries.
		cl.busy = nil
		return
	}
	cl.sequence++
	c.stats.Proposals++
	leader.pending[index] = &pendingProposal{term: leader.core.Term(), client: cl}
}

func (c *Cluster) beginRead(cl *simClient, leader *simNode, key string) {
	leader.nextReadID++
	id := leader.nextReadID
	token := make([]byte, 8)
	binary.BigEndian.PutUint64(token, id)
	leader.reads[id] = &pendingRead{client: cl, key: key}
	leader.core.ReadIndex(token)
}

// drainNodes performs the driver's side of the Ready contract for every live
// node, in id order so the schedule does not depend on map iteration.
func (c *Cluster) drainNodes() {
	for iteration := 0; iteration < 8; iteration++ {
		progressed := false
		for _, id := range c.ids {
			n := c.nodes[id]
			if n.down || !n.core.HasReady() {
				continue
			}
			c.processReady(n)
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

func (c *Cluster) processReady(n *simNode) {
	rd := n.core.Ready()

	// 1. Persist, before anything is sent or applied.
	if rd.Snapshot != nil {
		n.snapshot = rd.Snapshot
		n.entries = nil
		if err := n.fsm.Restore(rd.Snapshot.Data, rd.Snapshot.Index); err != nil {
			panic(fmt.Sprintf("sim: node %d restore snapshot: %v", n.id, err))
		}
		c.failPendingBelow(n, rd.Snapshot.Index)
	}
	if rd.HardState != nil {
		n.hardState = *rd.HardState
	}
	for _, e := range rd.Entries {
		// A rewritten index replaces rather than appends, which is how the real WAL
		// resolves a truncation on replay.
		if len(n.entries) > 0 && e.Index <= n.entries[len(n.entries)-1].Index {
			base := n.entries[0].Index
			n.entries = n.entries[:e.Index-base]
		}
		n.entries = append(n.entries, e)
	}

	// 2. Send.
	for _, m := range rd.Messages {
		if m.Type == raft.MsgSnapReq {
			c.stats.SnapshotsSent++
		}
		c.send(m)
	}

	// 3. Apply.
	if rd.SoftState != nil && rd.SoftState.Role == raft.Leader {
		c.stats.Elections++
	}
	if rd.SoftState != nil && rd.SoftState.Role != raft.Leader {
		// No longer leader, so nothing can be promised about accepted proposals or
		// pending reads.
		c.failAllWaiters(n)
	}

	for _, e := range rd.Committed {
		result, err := n.fsm.Apply(e)
		if err != nil {
			panic(fmt.Sprintf("sim: node %d apply entry %d: %v", n.id, e.Index, err))
		}
		if e.Type == raft.EntryConfChange {
			cc, err := raft.DecodeConfChange(e.Data)
			if err != nil {
				panic(fmt.Sprintf("sim: node %d decode conf change: %v", n.id, err))
			}
			if _, err := n.core.ApplyConfChange(cc); err != nil {
				panic(fmt.Sprintf("sim: node %d apply conf change: %v", n.id, err))
			}
		}
		c.resolveProposal(n, e, result)
	}

	for _, rs := range rd.ReadStates {
		c.resolveReadState(n, rs)
	}
	c.releaseReads(n)

	// 4. Acknowledge.
	n.core.Advance(rd)

	c.maybeSnapshot(n)
}

func (c *Cluster) resolveProposal(n *simNode, e raft.Entry, result *kvv1.CommandResult) {
	p, ok := n.pending[e.Index]
	if !ok {
		return
	}
	delete(n.pending, e.Index)
	if p.term != e.Term {
		// A different leader's entry occupies this index, so this client's
		// proposal was overwritten and never committed.
		c.finishUnknown(p.client)
		return
	}
	op := p.client.busy
	if op == nil {
		return
	}
	switch op.Kind {
	case OpDelete:
		op.Existed = result.GetExisted()
	case OpCAS:
		op.Swapped = result.GetSwapped()
	}
	op.Return = c.now
	c.history.Add(*op)
	c.stats.OpsCompleted++
	p.client.busy = nil
}

func (c *Cluster) resolveReadState(n *simNode, rs raft.ReadState) {
	if len(rs.ReadCtx) != 8 {
		return
	}
	id := binary.BigEndian.Uint64(rs.ReadCtx)
	r, ok := n.reads[id]
	if !ok {
		return
	}
	if rs.Index == 0 {
		// Leadership could not be proven, so the read cannot be served
		// linearizably. The client learns nothing about a value, which is a clean
		// failure rather than a wrong answer.
		delete(n.reads, id)
		c.abandonRead(r.client)
		return
	}
	r.index = rs.Index
	r.resolved = true
}

// releaseReads serves reads whose index the state machine has now applied.
func (c *Cluster) releaseReads(n *simNode) {
	if len(n.reads) == 0 {
		return
	}
	applied := n.fsm.AppliedIndex()
	for _, id := range sortedIDs(n.reads) {
		r := n.reads[id]
		if !r.resolved || r.index > applied {
			continue
		}
		delete(n.reads, id)
		op := r.client.busy
		if op == nil {
			continue
		}
		value, found := n.fsm.Get([]byte(r.key))
		op.GotValue = string(value)
		op.GotFound = found
		op.Return = c.now
		c.history.Add(*op)
		c.stats.OpsCompleted++
		r.client.busy = nil
	}
}

// abandonRead drops a read that could not be served. It is not recorded at all:
// the client never observed a value, so there is nothing for the checker to
// verify, and recording it as unknown would only add noise.
func (c *Cluster) abandonRead(cl *simClient) {
	cl.busy = nil
}

// finishUnknown records an operation whose outcome the client never learned.
func (c *Cluster) finishUnknown(cl *simClient) {
	op := cl.busy
	if op == nil {
		return
	}
	if op.Kind == OpGet {
		// A read that never returned reveals nothing.
		cl.busy = nil
		return
	}
	op.Unknown = true
	op.Return = c.now
	c.history.Add(*op)
	c.stats.OpsUnknown++
	cl.busy = nil
}

func (c *Cluster) failPendingBelow(n *simNode, index raft.Index) {
	for _, i := range sortedIndexes(n.pending) {
		if i > index {
			continue
		}
		p := n.pending[i]
		delete(n.pending, i)
		c.finishUnknown(p.client)
	}
}

// maybeSnapshot compacts once enough entries have accumulated past the boundary,
// exercising the snapshot and log-compaction paths under load.
func (c *Cluster) maybeSnapshot(n *simNode) {
	if c.opts.SnapshotThreshold == 0 {
		return
	}
	applied := n.core.AppliedIndex()
	boundary := n.core.SnapshotIndex()
	if applied <= boundary || uint64(applied-boundary) < c.opts.SnapshotThreshold {
		return
	}
	term, err := n.core.TermAt(applied)
	if err != nil {
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		panic(fmt.Sprintf("sim: node %d snapshot: %v", n.id, err))
	}
	snap := raft.Snapshot{Index: applied, Term: term, Conf: n.core.Membership(), Data: data}
	n.snapshot = &snap
	// The persisted log is now redundant up to the boundary.
	trimmed := n.entries
	for i, e := range n.entries {
		if e.Index > applied {
			trimmed = append([]raft.Entry(nil), n.entries[i:]...)
			break
		}
		if i == len(n.entries)-1 {
			trimmed = nil
		}
	}
	n.entries = trimmed
	if err := n.core.Compact(applied); err != nil && err != raft.ErrCompacted {
		panic(fmt.Sprintf("sim: node %d compact: %v", n.id, err))
	}
	c.stats.SnapshotsTaken++
}

// send queues a message, applying drop, duplicate and reorder faults.
func (c *Cluster) send(m raft.Message) {
	if !c.reachable(m.From, m.To) {
		c.stats.Dropped++
		return
	}
	if c.faults.DropRate > 0 && c.rng.Float64() < c.faults.DropRate {
		c.stats.Dropped++
		return
	}

	delay := int64(0)
	if c.faults.ReorderRate > 0 && c.rng.Float64() < c.faults.ReorderRate {
		delay = int64(1 + c.rng.IntN(5))
	}
	c.queue = append(c.queue, inFlight{msg: m, deliverAt: c.now + delay})

	if c.faults.DuplicateRate > 0 && c.rng.Float64() < c.faults.DuplicateRate {
		c.queue = append(c.queue, inFlight{msg: m, deliverAt: c.now + int64(c.rng.IntN(4))})
		c.stats.Duplicated++
	}
}

// deliverMessages delivers a random selection of eligible messages, which is what
// produces arbitrary interleavings.
func (c *Cluster) deliverMessages() {
	if len(c.queue) == 0 {
		return
	}
	budget := 4 + c.rng.IntN(8)
	for i := 0; i < budget && len(c.queue) > 0; i++ {
		// Choose randomly rather than FIFO: a queue drained in order would only ever
		// explore one interleaving.
		idx := c.rng.IntN(len(c.queue))
		item := c.queue[idx]
		if item.deliverAt > c.now {
			continue
		}
		c.queue = append(c.queue[:idx], c.queue[idx+1:]...)

		n := c.nodes[item.msg.To]
		if n == nil || n.down || !c.reachable(item.msg.From, item.msg.To) {
			c.stats.Dropped++
			continue
		}
		if err := n.core.Step(item.msg); err != nil {
			panic(fmt.Sprintf("sim: node %d rejected %s: %v", item.msg.To, item.msg.Type, err))
		}
		c.stats.Delivered++
	}
}

// AssertNoTwoLeadersInOneTerm checks the Election Safety property directly.
func (c *Cluster) AssertNoTwoLeadersInOneTerm() error {
	byTerm := make(map[raft.Term][]raft.NodeID)
	for id, term := range c.Leaders() {
		byTerm[term] = append(byTerm[term], id)
	}
	for term, ids := range byTerm {
		if len(ids) > 1 {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			return fmt.Errorf("nodes %v all claim leadership in term %d", ids, term)
		}
	}
	return nil
}

// AssertAppliedPrefixesAgree checks State Machine Safety: no two live nodes may
// hold different state at the same applied index.
func (c *Cluster) AssertAppliedPrefixesAgree() error {
	type snap struct {
		id      raft.NodeID
		applied raft.Index
		data    []byte
	}
	var snaps []snap
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.down {
			continue
		}
		data, err := n.fsm.Snapshot()
		if err != nil {
			return fmt.Errorf("node %d snapshot: %w", id, err)
		}
		snaps = append(snaps, snap{id: id, applied: n.fsm.AppliedIndex(), data: data})
	}
	// Two nodes that have applied the same number of entries must have identical
	// state, since they applied the same commands in the same order.
	for i := 0; i < len(snaps); i++ {
		for j := i + 1; j < len(snaps); j++ {
			if snaps[i].applied != snaps[j].applied {
				continue
			}
			if string(snaps[i].data) != string(snaps[j].data) {
				return fmt.Errorf("nodes %d and %d both applied through index %d but hold different state",
					snaps[i].id, snaps[j].id, snaps[i].applied)
			}
		}
	}
	return nil
}
