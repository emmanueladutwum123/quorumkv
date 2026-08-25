package sim

import (
	"fmt"
	"testing"
)

// These are the tests that would catch a consensus bug nobody thought to write a
// unit test for. Each run drives a whole cluster through a randomised sequence of
// partitions, drops, duplicates, reorderings, crashes and clock skew, records what
// the clients actually observed, and checks that history against the sequential
// specification.
//
// Every run is reproducible: the seed appears in the failure message, and re-running
// with it replays the exact interleaving.

// runScenario drives a cluster and checks every invariant it can.
func runScenario(t testing.TB, opts Options, steps int) *Cluster {
	t.Helper()
	c := New(opts)
	c.Run(steps)

	if err := c.AssertNoTwoLeadersInOneTerm(); err != nil {
		t.Fatalf("seed %d: SAFETY VIOLATION (election safety): %v", opts.Seed, err)
	}
	if err := c.AssertAppliedPrefixesAgree(); err != nil {
		t.Fatalf("seed %d: SAFETY VIOLATION (state machine safety): %v", opts.Seed, err)
	}
	res := CheckLinearizable(c.History())
	if !res.OK {
		t.Fatalf("seed %d: %s", opts.Seed, res)
	}
	if res.Inconclusive {
		// Not a failure -- the checker ran out of search, which says nothing about
		// the store. It is reported because a scenario whose history could not be
		// decided verified less than it appears to.
		t.Logf("seed %d: %s", opts.Seed, res)
	}
	return c
}

func TestHealthyClusterIsLinearizable(t *testing.T) {
	// The baseline. If this fails, nothing below is meaningful.
	c := runScenario(t, Options{Nodes: 3, Clients: 4, Seed: 1, Keys: 3}, 1500)

	st := c.Stats()
	if st.OpsCompleted == 0 {
		t.Fatal("no operation completed, so the scenario verified nothing")
	}
	if st.Elections == 0 {
		t.Error("no leader was ever elected")
	}
	t.Logf("completed %d ops (%d unknown), %d messages delivered, %d elections",
		st.OpsCompleted, st.OpsUnknown, st.Delivered, st.Elections)
}

func TestLossyNetworkIsLinearizable(t *testing.T) {
	c := runScenario(t, Options{
		Nodes: 3, Clients: 4, Seed: 2, Keys: 3,
		Faults: Faults{DropRate: 0.1, DuplicateRate: 0.1, ReorderRate: 0.3},
	}, 2000)

	st := c.Stats()
	if st.Dropped == 0 || st.Duplicated == 0 {
		t.Errorf("the scenario dropped %d and duplicated %d messages; it was meant to do both",
			st.Dropped, st.Duplicated)
	}
	if st.OpsCompleted == 0 {
		t.Fatal("no operation completed under a lossy network")
	}
	t.Logf("completed %d ops with %d dropped and %d duplicated messages",
		st.OpsCompleted, st.Dropped, st.Duplicated)
}

func TestPartitionsAreLinearizable(t *testing.T) {
	c := runScenario(t, Options{
		Nodes: 5, Clients: 4, Seed: 3, Keys: 3,
		Faults: Faults{PartitionRate: 0.03, HealRate: 0.05, DropRate: 0.05},
	}, 3000)

	st := c.Stats()
	if st.Partitions == 0 {
		t.Fatal("no partition ever occurred")
	}
	if st.Elections < 2 {
		t.Errorf("only %d elections occurred; partitions should have forced several", st.Elections)
	}
	t.Logf("survived %d partitions and %d elections, completing %d ops",
		st.Partitions, st.Elections, st.OpsCompleted)
}

func TestCrashRestartIsLinearizable(t *testing.T) {
	// The durability test: a crashed node loses everything volatile, so anything
	// the driver failed to persist is gone for good.
	c := runScenario(t, Options{
		Nodes: 5, Clients: 4, Seed: 4, Keys: 3,
		Faults: Faults{CrashRate: 0.02, RestartRate: 0.06, DropRate: 0.03},
	}, 3000)

	st := c.Stats()
	if st.Crashes == 0 || st.Restarts == 0 {
		t.Fatalf("the scenario produced %d crashes and %d restarts; it was meant to produce both",
			st.Crashes, st.Restarts)
	}
	t.Logf("survived %d crashes and %d restarts, completing %d ops (%d unknown)",
		st.Crashes, st.Restarts, st.OpsCompleted, st.OpsUnknown)
}

func TestClockSkewIsLinearizable(t *testing.T) {
	// Nodes tick at different rates, so no node's sense of elapsed time matches
	// another's. Timeout-based decisions must still be safe.
	c := runScenario(t, Options{
		Nodes: 5, Clients: 3, Seed: 5, Keys: 3,
		Faults: Faults{ClockSkew: true, DropRate: 0.05, PartitionRate: 0.02, HealRate: 0.05},
	}, 2500)

	if c.Stats().OpsCompleted == 0 {
		t.Fatal("no operation completed under clock skew")
	}
}

func TestSnapshotAndCompactionUnderChaos(t *testing.T) {
	// A low threshold forces repeated compaction while the cluster is being
	// partitioned and crashed, which is what drives followers behind the boundary
	// and exercises InstallSnapshot.
	c := runScenario(t, Options{
		Nodes: 5, Clients: 4, Seed: 6, Keys: 3,
		SnapshotThreshold: 20,
		Faults: Faults{
			PartitionRate: 0.02, HealRate: 0.05,
			CrashRate: 0.01, RestartRate: 0.05, DropRate: 0.05,
		},
	}, 3000)

	st := c.Stats()
	if st.SnapshotsTaken == 0 {
		t.Error("no snapshot was taken despite a threshold of 20 entries")
	}
	t.Logf("took %d snapshots and sent %d, completing %d ops",
		st.SnapshotsTaken, st.SnapshotsSent, st.OpsCompleted)
}

func TestEverythingAtOnce(t *testing.T) {
	// Every fault enabled simultaneously.
	c := runScenario(t, Options{
		Nodes: 5, Clients: 5, Seed: 7, Keys: 4,
		SnapshotThreshold: 30,
		Faults: Faults{
			DropRate: 0.08, DuplicateRate: 0.08, ReorderRate: 0.25,
			PartitionRate: 0.02, HealRate: 0.04,
			CrashRate: 0.015, RestartRate: 0.05,
			ClockSkew: true,
		},
	}, 4000)

	st := c.Stats()
	t.Logf("ops=%d unknown=%d delivered=%d dropped=%d dup=%d partitions=%d crashes=%d restarts=%d elections=%d snapshots=%d",
		st.OpsCompleted, st.OpsUnknown, st.Delivered, st.Dropped, st.Duplicated,
		st.Partitions, st.Crashes, st.Restarts, st.Elections, st.SnapshotsTaken)
	if st.OpsCompleted == 0 {
		t.Fatal("no operation completed with every fault enabled")
	}
}

func TestManySeeds(t *testing.T) {
	// Breadth matters more than depth for finding ordering bugs: many short runs
	// explore many more distinct interleavings than one long run.
	for seed := uint64(100); seed < 140; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runScenario(t, Options{
				Nodes: 3, Clients: 3, Seed: seed, Keys: 2,
				SnapshotThreshold: 40,
				Faults: Faults{
					DropRate: 0.07, DuplicateRate: 0.05, ReorderRate: 0.2,
					PartitionRate: 0.02, HealRate: 0.05,
					CrashRate: 0.01, RestartRate: 0.06,
				},
			}, 900)
		})
	}
}

func TestFiveNodeManySeeds(t *testing.T) {
	for seed := uint64(200); seed < 220; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runScenario(t, Options{
				Nodes: 5, Clients: 4, Seed: seed, Keys: 3,
				SnapshotThreshold: 50,
				Faults: Faults{
					DropRate: 0.06, DuplicateRate: 0.05, ReorderRate: 0.2,
					PartitionRate: 0.025, HealRate: 0.05,
					CrashRate: 0.012, RestartRate: 0.05,
					ClockSkew: true,
				},
			}, 1200)
		})
	}
}

// --- the simulator's own properties ---------------------------------------

func TestSimulationIsDeterministic(t *testing.T) {
	// The property everything else depends on. If two runs with the same seed
	// diverged, a reported failure could not be reproduced, and the seed in a
	// failure message would be worthless.
	opts := Options{
		Nodes: 5, Clients: 4, Seed: 999, Keys: 3,
		SnapshotThreshold: 30,
		Faults: Faults{
			DropRate: 0.08, DuplicateRate: 0.08, ReorderRate: 0.2,
			PartitionRate: 0.02, HealRate: 0.05,
			CrashRate: 0.015, RestartRate: 0.05, ClockSkew: true,
		},
	}

	first := New(opts)
	first.Run(1200)
	second := New(opts)
	second.Run(1200)

	if first.Stats() != second.Stats() {
		t.Fatalf("two runs of seed 999 produced different statistics:\n  %+v\n  %+v",
			first.Stats(), second.Stats())
	}
	fh, sh := first.History(), second.History()
	if fh.Len() != sh.Len() {
		t.Fatalf("histories differ in length: %d vs %d", fh.Len(), sh.Len())
	}
	for i := range fh.Ops {
		if fh.Ops[i].String() != sh.Ops[i].String() {
			t.Fatalf("histories diverge at operation %d:\n  %s\n  %s",
				i, fh.Ops[i], sh.Ops[i])
		}
	}
}

func TestDifferentSeedsExploreDifferentInterleavings(t *testing.T) {
	// If every seed produced the same run, breadth would be an illusion.
	base := Options{
		Nodes: 5, Clients: 4, Keys: 3,
		Faults: Faults{DropRate: 0.08, ReorderRate: 0.2, PartitionRate: 0.02, HealRate: 0.05},
	}
	seen := make(map[string]bool)
	for seed := uint64(1); seed <= 8; seed++ {
		opts := base
		opts.Seed = seed
		c := New(opts)
		c.Run(600)
		st := c.Stats()
		seen[fmt.Sprintf("%d-%d-%d", st.Delivered, st.Elections, st.OpsCompleted)] = true
	}
	if len(seen) < 4 {
		t.Errorf("8 seeds produced only %d distinct runs; the seed barely affects the schedule", len(seen))
	}
}

func TestMinorityCrashLimitIsRespected(t *testing.T) {
	// Crashing past a minority would stop the cluster committing, which is expected
	// behaviour rather than a bug — a scenario that allowed it would mostly measure
	// downtime.
	c := New(Options{
		Nodes: 5, Clients: 3, Seed: 42, Keys: 2,
		Faults: Faults{CrashRate: 0.5, RestartRate: 0.01, MaxCrashed: 2},
	})
	c.Run(500)

	down := 0
	for _, id := range c.ids {
		if c.nodes[id].down {
			down++
		}
	}
	if down > 2 {
		t.Errorf("%d nodes are down, above the configured limit of 2", down)
	}
}

func TestHistoryRecordsRealWork(t *testing.T) {
	// A scenario that recorded nothing would pass the checker trivially.
	c := New(Options{Nodes: 3, Clients: 4, Seed: 11, Keys: 2, OpStartRate: 0.6})
	c.Run(1200)

	h := c.History()
	if h.Len() < 20 {
		t.Fatalf("recorded only %d operations; the checker needs real traffic to verify", h.Len())
	}
	kinds := make(map[OpKind]int)
	for _, op := range h.Ops {
		kinds[op.Kind]++
	}
	for _, kind := range []OpKind{OpPut, OpGet} {
		if kinds[kind] == 0 {
			t.Errorf("no %s operations were recorded", kind)
		}
	}
	t.Logf("recorded %d ops: %d puts, %d gets, %d deletes, %d cas",
		h.Len(), kinds[OpPut], kinds[OpGet], kinds[OpDelete], kinds[OpCAS])
}
