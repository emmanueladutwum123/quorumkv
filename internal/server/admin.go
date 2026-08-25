package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/emmanueladutwum123/quorumkv/internal/metrics"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// adminTimeout bounds a status snapshot. The request is served on the node's own
// goroutine, so a node wedged in a long fsync would otherwise hold a scrape open
// until the scraper gave up — and a monitoring endpoint that hangs during an
// incident is worse than one that reports the failure.
const adminTimeout = 2 * time.Second

// AdminHandler serves the operational HTTP surface: metrics, liveness and
// readiness probes, and a status snapshot.
//
// It is deliberately separate from the gRPC port. Client and peer traffic
// belong to the data path and would be firewalled in any real deployment;
// scraping and probing must keep working when that path is exactly what has
// gone wrong.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)
	return mux
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), adminTimeout)
	defer cancel()

	samples, err := s.Observe(ctx)
	if err != nil {
		// A node that cannot describe itself is unhealthy, and saying so is more
		// useful than an empty scrape that looks like a healthy idle node.
		http.Error(w, "collect metrics: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_ = metrics.Render(w, samples)
}

// handleHealth answers liveness: the process is up and its main loop has not
// stopped. It deliberately does not consult the cluster — a node in a minority
// partition is unhealthy in no sense that restarting it would fix, and a
// liveness probe that fails during a partition turns one outage into a
// restart loop across every node at once.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.stopCh:
		http.Error(w, "stopped", http.StatusServiceUnavailable)
	default:
		writeText(w, http.StatusOK, "ok")
	}
	_ = r
}

// handleReady answers readiness: this node can take part in serving requests,
// which means it recognises a leader. A node that does not is still alive and
// should keep its process, but sending it traffic only produces redirects.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), adminTimeout)
	defer cancel()

	st, err := s.Status(ctx)
	if err != nil {
		http.Error(w, "status: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	if st.Leader == 0 {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
		return
	}
	writeText(w, http.StatusOK, "ready")
}

// statusReport is the human-readable snapshot behind /status.
type statusReport struct {
	ID            uint64                `json:"id"`
	Role          string                `json:"role"`
	Term          uint64                `json:"term"`
	Leader        uint64                `json:"leader"`
	CommitIndex   uint64                `json:"commit_index"`
	AppliedIndex  uint64                `json:"applied_index"`
	LastLogIndex  uint64                `json:"last_log_index"`
	SnapshotIndex uint64                `json:"snapshot_index"`
	Voters        []uint64              `json:"voters"`
	Learners      []uint64              `json:"learners"`
	Joint         bool                  `json:"joint_config"`
	Peers         map[string]peerReport `json:"peers,omitempty"`
}

type peerReport struct {
	Match        uint64 `json:"match_index"`
	Next         uint64 `json:"next_index"`
	State        string `json:"state"`
	RecentActive bool   `json:"recent_active"`
	Learner      bool   `json:"learner"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), adminTimeout)
	defer cancel()

	st, err := s.Status(ctx)
	if err != nil {
		http.Error(w, "status: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	report := statusReport{
		ID:            uint64(st.ID),
		Role:          st.Role.String(),
		Term:          uint64(st.Term),
		Leader:        uint64(st.Leader),
		CommitIndex:   uint64(st.Commit),
		AppliedIndex:  uint64(st.Applied),
		LastLogIndex:  uint64(st.LastLog),
		SnapshotIndex: uint64(st.Snapshot),
		Voters:        sortedNodes(st.Config.Voters[0]),
		Learners:      sortedNodes(st.Config.Learners),
		Joint:         st.Config.IsJoint(),
	}
	if len(st.Progress) > 0 {
		report.Peers = make(map[string]peerReport, len(st.Progress))
		for id, p := range st.Progress {
			if id == st.ID {
				continue // the leader's own entry, always trivially up to date
			}
			report.Peers[strconv.FormatUint(uint64(id), 10)] = peerReport{
				Match:        uint64(p.Match),
				Next:         uint64(p.Next),
				State:        p.State,
				RecentActive: p.RecentActive,
				Learner:      p.IsLearner,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}

func sortedNodes(set raft.NodeSet) []uint64 {
	out := make([]uint64, 0, len(set))
	for id := range set {
		out = append(out, uint64(id))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func writeText(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}
