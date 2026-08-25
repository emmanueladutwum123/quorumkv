package server

import (
	"context"
	"sort"
	"strconv"

	"github.com/emmanueladutwum123/quorumkv/internal/metrics"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// Observe collects everything worth exporting about this node.
//
// The counters are read from the same atomics the running node increments, and
// the gauges from a status snapshot taken on the node's own goroutine, so a
// scrape observes a consistent view rather than a torn one assembled while the
// node kept moving.
//
// Choosing what to export is the substance here. A metric earns its place by
// answering a question an operator asks during an incident, and the questions
// that matter for a consensus system are: is there a leader, is this node
// keeping up, is replication making progress, and is anything being lost. Every
// series below answers one of those. Per-peer replication progress is the
// exception worth the cardinality — a single lagging follower is invisible in
// any aggregate, and it is the thing that turns into an outage when a second
// node fails.
func (s *Server) Observe(ctx context.Context) ([]metrics.Sample, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return nil, err
	}

	m := s.Metrics()
	node := []metrics.Label{{Name: "node", Value: strconv.FormatUint(uint64(s.cfg.NodeID), 10)}}

	counter := func(name, help string, v uint64) metrics.Sample {
		return metrics.Sample{Name: name, Help: help, Kind: metrics.Counter, Value: float64(v), Labels: node}
	}
	gauge := func(name, help string, v float64) metrics.Sample {
		return metrics.Sample{Name: name, Help: help, Kind: metrics.Gauge, Value: v, Labels: node}
	}

	out := []metrics.Sample{
		counter("quorumkv_proposals_total", "Client commands submitted to the log.", m.Proposals.Load()),
		// The one counter to alert on. A lost proposal is a client whose write was
		// accepted by this node and then dropped, usually because leadership moved
		// before it committed. It is not corruption -- the client is told -- but a
		// rising rate means the cluster is not holding a stable leader.
		counter("quorumkv_proposals_lost_total", "Commands abandoned before commit, typically on losing leadership.", m.ProposalsLost.Load()),
		counter("quorumkv_entries_committed_total", "Log entries that reached a quorum.", m.CommittedEntries.Load()),
		counter("quorumkv_entries_applied_total", "Log entries handed to the state machine.", m.AppliedEntries.Load()),
		counter("quorumkv_snapshots_taken_total", "Snapshots written by this node.", m.Snapshots.Load()),
		counter("quorumkv_snapshots_applied_total", "Snapshots received from a leader and installed.", m.SnapshotsApplied.Load()),
		counter("quorumkv_reads_linearizable_total", "Reads served after confirming leadership.", m.LinearizedReads.Load()),
		// Rejected reads are the honest failure: leadership could not be proven, so
		// no value was returned rather than a stale one.
		counter("quorumkv_reads_rejected_total", "Reads refused because leadership could not be confirmed.", m.RejectedReads.Load()),
		counter("quorumkv_elections_total", "Elections this node started.", m.Elections.Load()),
		counter("quorumkv_fsync_total", "Durable syncs of the write-ahead log.", m.FsyncCount.Load()),
		counter("quorumkv_conf_changes_total", "Membership changes applied.", m.ConfChanges.Load()),

		gauge("quorumkv_raft_term", "Current Raft term.", float64(status.Term)),
		gauge("quorumkv_raft_commit_index", "Highest log index known to be committed.", float64(status.Commit)),
		gauge("quorumkv_raft_applied_index", "Highest log index applied to the state machine.", float64(status.Applied)),
		gauge("quorumkv_raft_last_log_index", "Highest index in this node's log.", float64(status.LastLog)),
		gauge("quorumkv_raft_snapshot_index", "Index the log is compacted up to.", float64(status.Snapshot)),
		// Apply lag is derivable from the two indexes above, but an operator
		// staring at a dashboard mid-incident should not have to derive it.
		gauge("quorumkv_raft_apply_lag", "Committed entries not yet applied.", float64(status.Commit-status.Applied)),
		gauge("quorumkv_raft_is_leader", "1 when this node is the leader, 0 otherwise.", boolValue(status.Role == raft.Leader)),
		// Zero means this node believes there is no leader, which is exactly the
		// condition worth alerting on when it persists.
		gauge("quorumkv_raft_leader_id", "Id of the leader this node recognises, 0 if none.", float64(status.Leader)),
		gauge("quorumkv_raft_voters", "Voting members in the current configuration.", float64(len(status.Config.Voters[0]))),
		gauge("quorumkv_raft_learners", "Non-voting members in the current configuration.", float64(len(status.Config.Learners))),
		// A configuration change that never leaves the joint state has stalled, and
		// the cluster is running on two quorums until it does.
		gauge("quorumkv_raft_joint_config", "1 while a membership change is in flight.", boolValue(status.Config.IsJoint())),
	}

	// Role as a labelled gauge rather than four separate series, so a dashboard
	// can show the role without knowing which one to look for.
	out = append(out, metrics.Sample{
		Name:   "quorumkv_raft_role",
		Help:   "Current role, as a label carrying the value 1.",
		Kind:   metrics.Gauge,
		Value:  1,
		Labels: append(append([]metrics.Label{}, node...), metrics.Label{Name: "role", Value: status.Role.String()}),
	})

	// Progress is populated only on a leader, which is the only node that tracks
	// it. Followers export nothing here rather than exporting zeros that would
	// read as a stalled cluster.
	peers := make([]raft.NodeID, 0, len(status.Progress))
	for id := range status.Progress {
		// The leader tracks itself in the same map. Exporting that as a peer would
		// put a series on every dashboard that is trivially caught up by
		// construction and can never indicate anything.
		if id == status.ID {
			continue
		}
		peers = append(peers, id)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	for _, id := range peers {
		p := status.Progress[id]
		labels := append(append([]metrics.Label{}, node...), metrics.Label{Name: "peer", Value: strconv.FormatUint(uint64(id), 10)})
		out = append(out,
			metrics.Sample{Name: "quorumkv_peer_match_index", Help: "Highest index known replicated on a peer.", Kind: metrics.Gauge, Value: float64(p.Match), Labels: labels},
			metrics.Sample{Name: "quorumkv_peer_next_index", Help: "Next index the leader will send to a peer.", Kind: metrics.Gauge, Value: float64(p.Next), Labels: labels},
			// How far behind a follower is, which is the number that decides whether
			// losing another node costs availability.
			metrics.Sample{Name: "quorumkv_peer_lag", Help: "Entries by which a peer trails the leader's log.", Kind: metrics.Gauge, Value: float64(status.LastLog - p.Match), Labels: labels},
			metrics.Sample{Name: "quorumkv_peer_active", Help: "1 when a peer answered within the last quorum check.", Kind: metrics.Gauge, Value: boolValue(p.RecentActive), Labels: labels},
			metrics.Sample{Name: "quorumkv_peer_learner", Help: "1 when a peer is a non-voting learner.", Kind: metrics.Gauge, Value: boolValue(p.IsLearner), Labels: labels},
		)
	}
	return out, nil
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
