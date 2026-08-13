package raft

import (
	"math/rand/v2"
	"sort"
	"testing"
)

// This file provides the deterministic cluster harness the rest of the tests are
// written against.
//
// An entire cluster runs inside the calling goroutine. There are no timers, no
// sockets and no sleeps: the harness alternates between draining each node's
// Ready batch and delivering one queued message, in a fixed order, until the
// cluster goes quiet. Every scheduling decision is therefore a consequence of
// the seed, so a failure reproduces exactly.
//
// The harness also plays the role of the driver, which means it exercises the
// Ready/Advance durability contract on every test: persist, send, apply, then
// acknowledge.

// maxHarnessSteps bounds a single run so that a livelock — two nodes endlessly
// deposing each other, say — fails the test instead of hanging it.
const maxHarnessSteps = 100000

// memStorage stands in for the write-ahead log, recording exactly what the
// driver was told to persist and nothing more. Tests that care about crash
// recovery restart a node from this and no other state.
type memStorage struct {
	hardState HardState
	entries   []Entry
	snapshot  *Snapshot
}

func (s *memStorage) persist(rd Ready) {
	if rd.Snapshot != nil {
		s.snapshot = rd.Snapshot
		// A snapshot supersedes the log it covers.
		s.entries = nil
	}
	if rd.HardState != nil {
		s.hardState = *rd.HardState
	}
	for _, e := range rd.Entries {
		// Entries are addressed by index, so a rewritten index replaces rather
		// than appends. This is what makes a truncation durable, and it is the
		// behaviour a real WAL must reproduce on replay.
		if len(s.entries) > 0 && e.Index <= s.entries[len(s.entries)-1].Index {
			base := s.entries[0].Index
			s.entries = s.entries[:e.Index-base]
		}
		s.entries = append(s.entries, e)
	}
}

// peer is one node plus the state its driver owns.
type peer struct {
	node    *Node
	store   *memStorage
	applied []Entry
	reads   []ReadState
	down    bool
}

// appliedData returns the payloads of applied normal entries, which is what a
// state machine would actually have consumed.
func (p *peer) appliedData() []string {
	var out []string
	for _, e := range p.applied {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

type cluster struct {
	t     testing.TB
	peers map[NodeID]*peer
	ids   []NodeID
	queue []Message
	// blocked reports whether a message from one node to another is dropped,
	// which is how partitions and one-way link failures are expressed.
	blocked func(from, to NodeID) bool
	rng     *rand.Rand
	// dropped counts messages discarded by the network, for assertions about
	// whether a scenario actually exercised a partition.
	dropped int
}

type clusterOption func(*Options)

// withoutPreVote disables the straw poll, so tests can demonstrate the
// disruption that pre-vote exists to prevent.
func withoutPreVote() clusterOption {
	return func(o *Options) { o.PreVote = boolPtr(false) }
}

// withoutCheckQuorum disables the leader's quorum self-check.
func withoutCheckQuorum() clusterOption {
	return func(o *Options) { o.CheckQuorum = boolPtr(false) }
}

func withConfig(cfg Config) clusterOption {
	return func(o *Options) { o.Config = cfg.Clone() }
}

func withMaxEntriesPerMessage(k int) clusterOption {
	return func(o *Options) { o.MaxEntriesPerMessage = k }
}

// newCluster builds a cluster of voters with a fixed seed.
func newCluster(t testing.TB, voters []NodeID, opts ...clusterOption) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		peers:   make(map[NodeID]*peer),
		blocked: func(NodeID, NodeID) bool { return false },
		// A fixed seed keeps every run identical. Tests that want to explore
		// many interleavings vary the seed explicitly rather than relying on
		// ambient randomness.
		rng: rand.New(rand.NewPCG(0x5eed, 0xc0ffee)),
	}
	cfg := NewConfig(voters...)
	for _, id := range cfg.Members() {
		o := Options{ID: id, Config: cfg, ElectionTimeoutTicks: 10, HeartbeatTimeoutTicks: 1}
		for _, opt := range opts {
			opt(&o)
		}
		o.Rand = c.rng.IntN
		node, err := NewNode(o)
		if err != nil {
			t.Fatalf("NewNode(%d): %v", id, err)
		}
		c.peers[id] = &peer{node: node, store: &memStorage{}}
		c.ids = append(c.ids, id)
	}
	sort.Slice(c.ids, func(i, j int) bool { return c.ids[i] < c.ids[j] })
	return c
}

// node returns a peer, failing the test if the id is unknown.
func (c *cluster) node(id NodeID) *peer {
	c.t.Helper()
	p, ok := c.peers[id]
	if !ok {
		c.t.Fatalf("no such node: %d", id)
	}
	return p
}

// run drives the cluster until nothing more can happen without a tick.
func (c *cluster) run() {
	c.t.Helper()
	for step := 0; step < maxHarnessSteps; step++ {
		progressed := false

		// Drain every node's pending work first, in id order so the schedule
		// does not depend on map iteration.
		for _, id := range c.ids {
			p := c.peers[id]
			if p.down || !p.node.HasReady() {
				continue
			}
			c.processReady(p)
			progressed = true
		}

		// Then deliver a single message, so sends and receives interleave
		// rather than the queue draining in one burst.
		if len(c.queue) > 0 {
			m := c.queue[0]
			c.queue = c.queue[1:]
			c.deliver(m)
			progressed = true
		}

		if !progressed {
			return
		}
	}
	c.t.Fatalf("cluster did not settle within %d steps (livelock?)", maxHarnessSteps)
}

// processReady performs the driver's side of the Ready contract in the required
// order: persist, then send, then apply, then acknowledge.
func (c *cluster) processReady(p *peer) {
	c.t.Helper()
	rd := p.node.Ready()

	p.store.persist(rd)

	for _, m := range rd.Messages {
		c.send(m)
	}

	if rd.Snapshot != nil {
		// A restored snapshot replaces the state machine wholesale, so anything
		// applied from the discarded log no longer describes current state.
		p.applied = nil
	}
	p.applied = append(p.applied, rd.Committed...)
	p.reads = append(p.reads, rd.ReadStates...)

	p.node.Advance(rd)
}

func (c *cluster) send(m Message) {
	if c.blocked(m.From, m.To) {
		c.dropped++
		return
	}
	c.queue = append(c.queue, m)
}

func (c *cluster) deliver(m Message) {
	c.t.Helper()
	p, ok := c.peers[m.To]
	if !ok || p.down {
		c.dropped++
		return
	}
	// Re-check reachability at delivery: a partition raised while the message
	// was in flight must still drop it.
	if c.blocked(m.From, m.To) {
		c.dropped++
		return
	}
	if err := p.node.Step(m); err != nil {
		c.t.Fatalf("node %d rejected %s from %d: %v", m.To, m.Type, m.From, err)
	}
}

// tick advances every live node by k ticks, running the cluster to quiescence
// after each one so that messages are not batched across tick boundaries.
func (c *cluster) tick(k int) {
	c.t.Helper()
	for i := 0; i < k; i++ {
		for _, id := range c.ids {
			if p := c.peers[id]; !p.down {
				p.node.Tick()
			}
		}
		c.run()
	}
}

// tickOne advances a single node, which is how clock skew is expressed: one node
// experiences time passing while the others do not.
func (c *cluster) tickOne(id NodeID, k int) {
	c.t.Helper()
	p := c.node(id)
	for i := 0; i < k; i++ {
		if !p.down {
			p.node.Tick()
		}
		c.run()
	}
}

// partition splits the cluster into mutually unreachable groups. Any node not
// named stays reachable from everyone.
func (c *cluster) partition(groups ...[]NodeID) {
	group := make(map[NodeID]int, len(c.ids))
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi + 1
		}
	}
	c.blocked = func(from, to NodeID) bool {
		gf, gt := group[from], group[to]
		if gf == 0 || gt == 0 {
			return false
		}
		return gf != gt
	}
}

// isolate cuts one node off from the rest of the cluster in both directions.
func (c *cluster) isolate(id NodeID) {
	c.blocked = func(from, to NodeID) bool { return from == id || to == id }
}

// blockOneWay drops traffic from one node to another while leaving the reverse
// direction intact, which is the asymmetric failure that breaks naive failure
// detectors.
func (c *cluster) blockOneWay(from, to NodeID) {
	c.blocked = func(f, t NodeID) bool { return f == from && t == to }
}

// heal restores full connectivity.
func (c *cluster) heal() {
	c.blocked = func(NodeID, NodeID) bool { return false }
}

// crash takes a node offline, discarding all volatile state but keeping what its
// driver had persisted.
func (c *cluster) crash(id NodeID) {
	c.node(id).down = true
}

// restart brings a crashed node back, rebuilding it from durable storage alone.
// This is the operation that catches a durability bug: anything the node needed
// but never persisted is now gone.
func (c *cluster) restart(id NodeID, opts ...clusterOption) {
	c.t.Helper()
	p := c.node(id)

	o := Options{ID: id, Config: NewConfig(c.ids...), ElectionTimeoutTicks: 10, HeartbeatTimeoutTicks: 1}
	for _, opt := range opts {
		opt(&o)
	}
	o.Rand = c.rng.IntN
	node, err := NewNode(o)
	if err != nil {
		c.t.Fatalf("restart node %d: %v", id, err)
	}
	if p.store.snapshot != nil {
		if err := node.RestoreSnapshot(*p.store.snapshot); err != nil {
			c.t.Fatalf("restart node %d: restore snapshot: %v", id, err)
		}
	}
	node.ReplayEntries(p.store.entries)
	node.SetHardState(p.store.hardState)

	p.node = node
	p.down = false
	// Applied state is rebuilt by re-applying the recovered log, so it is
	// cleared rather than carried across the restart.
	p.applied = nil
	p.reads = nil
}

// leaders returns every node that currently believes it is leader.
func (c *cluster) leaders() []NodeID {
	var out []NodeID
	for _, id := range c.ids {
		p := c.peers[id]
		if !p.down && p.node.Role() == Leader {
			out = append(out, id)
		}
	}
	return out
}

// leader returns the single leader, failing the test if there is not exactly
// one. Two leaders in the same term is a safety violation; two leaders in
// different terms is a legitimate transient, so the check is term-aware.
func (c *cluster) leader() *peer {
	c.t.Helper()
	ls := c.leaders()
	if len(ls) == 0 {
		c.t.Fatalf("no leader elected")
	}
	if len(ls) > 1 {
		byTerm := make(map[Term][]NodeID)
		for _, id := range ls {
			t := c.peers[id].node.Term()
			byTerm[t] = append(byTerm[t], id)
		}
		for term, ids := range byTerm {
			if len(ids) > 1 {
				c.t.Fatalf("SAFETY VIOLATION: nodes %v all claim leadership in term %d", ids, term)
			}
		}
		// Distinct terms: the highest is the real one, the others are deposed
		// leaders that have not yet learned it.
		best := ls[0]
		for _, id := range ls[1:] {
			if c.peers[id].node.Term() > c.peers[best].node.Term() {
				best = id
			}
		}
		return c.peers[best]
	}
	return c.peers[ls[0]]
}

// electLeader ticks until a leader emerges, failing if none does.
func (c *cluster) electLeader() *peer {
	c.t.Helper()
	for i := 0; i < 200; i++ {
		if len(c.leaders()) > 0 {
			return c.leader()
		}
		c.tick(1)
	}
	c.t.Fatalf("no leader after 200 ticks")
	return nil
}

// propose submits a command through the current leader and runs the cluster
// until it settles.
func (c *cluster) propose(data string) Index {
	c.t.Helper()
	l := c.leader()
	idx, err := l.node.Propose([]byte(data))
	if err != nil {
		c.t.Fatalf("propose %q on node %d: %v", data, l.node.ID(), err)
	}
	c.run()
	return idx
}

// assertNoDivergence checks the State Machine Safety property directly: no two
// live nodes may hold different entries at the same index.
func (c *cluster) assertNoDivergence() {
	c.t.Helper()
	type held struct {
		term Term
		data string
		by   NodeID
	}
	seen := make(map[Index]held)
	for _, id := range c.ids {
		p := c.peers[id]
		if p.down {
			continue
		}
		for _, e := range p.applied {
			got := held{term: e.Term, data: string(e.Data), by: id}
			if prev, ok := seen[e.Index]; ok {
				if prev.term != got.term || prev.data != got.data {
					c.t.Fatalf("SAFETY VIOLATION at index %d: node %d applied (term %d, %q) but node %d applied (term %d, %q)",
						e.Index, prev.by, prev.term, prev.data, got.by, got.term, got.data)
				}
				continue
			}
			seen[e.Index] = got
		}
	}
}

// assertCommittedEverywhere checks that every live node has applied the same
// prefix of commands.
func (c *cluster) assertCommittedEverywhere(want ...string) {
	c.t.Helper()
	for _, id := range c.ids {
		p := c.peers[id]
		if p.down {
			continue
		}
		got := p.appliedData()
		if len(got) != len(want) {
			c.t.Fatalf("node %d applied %v, want %v", id, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				c.t.Fatalf("node %d applied %v, want %v", id, got, want)
			}
		}
	}
}
