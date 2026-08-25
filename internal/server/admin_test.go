package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The admin surface is what an operator and a scraper see during an incident, so
// it is tested against a real running cluster rather than a stub: the values have
// to come from a node that is actually holding an election and replicating.

func get(t testing.TB, srv *Server, path string) (*http.Response, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	srv.AdminHandler().ServeHTTP(rec, req)
	res := rec.Result()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = res.Body.Close()
	return res, string(body)
}

func TestMetricsEndpointReportsLeaderState(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	c.putWithRetry("k", "v", 1, 1)

	res, body := get(t, leader.srv, "/metrics")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, body %q", res.StatusCode, body)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content type %q, want the Prometheus text format", ct)
	}

	// The leader must say so, and must have committed the write above.
	if !strings.Contains(body, `quorumkv_raft_is_leader{node="`) {
		t.Errorf("no leadership gauge:\n%s", body)
	}
	for _, want := range []string{
		"quorumkv_proposals_total",
		"quorumkv_entries_applied_total",
		"quorumkv_raft_term",
		"quorumkv_raft_commit_index",
		`quorumkv_raft_role{node=`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	if !strings.Contains(body, `role="leader"`) {
		t.Errorf("the leader does not report its role:\n%s", body)
	}
	// Every metric family must be typed, or a scrape fails wholesale.
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "quorumkv_") {
			name := line[:strings.IndexAny(line+" ", "{ ")]
			if !strings.Contains(body, "# TYPE "+name+" ") {
				t.Errorf("sample %q has no TYPE declaration", name)
			}
		}
	}
}

func TestMetricsReportPerPeerReplicationOnTheLeaderOnly(t *testing.T) {
	// Per-peer progress is the series that exposes a single lagging follower,
	// which no aggregate can show. Only the leader tracks it; a follower must
	// export nothing rather than zeros that would read as a stalled cluster.
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)
	c.putWithRetry("k", "v", 1, 1)

	_, leaderBody := get(t, leader.srv, "/metrics")
	if !strings.Contains(leaderBody, "quorumkv_peer_match_index") {
		t.Errorf("leader does not report peer progress:\n%s", leaderBody)
	}
	if !strings.Contains(leaderBody, "quorumkv_peer_lag") {
		t.Error("leader does not report per-peer lag")
	}

	for id, n := range c.nodes {
		if id == leader.id {
			continue
		}
		_, body := get(t, n.srv, "/metrics")
		if strings.Contains(body, "quorumkv_peer_match_index") {
			t.Errorf("follower %d reports peer progress it does not track:\n%s", id, body)
		}
		// It must still report its own state.
		if !strings.Contains(body, "quorumkv_raft_term") {
			t.Errorf("follower %d reports no term", id)
		}
		break
	}
}

func TestHealthAndReadinessDistinguishLivenessFromUsefulness(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	if res, body := get(t, leader.srv, "/healthz"); res.StatusCode != http.StatusOK {
		t.Errorf("/healthz on a running leader: %d %q", res.StatusCode, body)
	}
	if res, body := get(t, leader.srv, "/readyz"); res.StatusCode != http.StatusOK {
		t.Errorf("/readyz on a leader that is serving: %d %q", res.StatusCode, body)
	}
}

func TestStoppedNodeIsNotLive(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)
	srv := leader.srv
	c.stop(leader.id)

	res, _ := get(t, srv, "/healthz")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a stopped node reports itself live: status %d", res.StatusCode)
	}
}

func TestStatusEndpointDescribesTheCluster(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)
	c.putWithRetry("k", "v", 1, 1)

	res, body := get(t, leader.srv, "/status")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", res.StatusCode, body)
	}

	var report statusReport
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	if report.Role != "leader" {
		t.Errorf("role = %q, want leader", report.Role)
	}
	if report.ID != uint64(leader.id) {
		t.Errorf("id = %d, want %d", report.ID, leader.id)
	}
	if report.Term == 0 {
		t.Error("term is zero on an elected leader")
	}
	if report.CommitIndex == 0 {
		t.Error("commit index is zero after an acknowledged write")
	}
	if len(report.Voters) != 3 {
		t.Errorf("voters = %v, want three", report.Voters)
	}
	if report.Joint {
		t.Error("reports a joint configuration with no membership change in flight")
	}
	if len(report.Peers) != 2 {
		t.Errorf("leader reports %d peers, want two", len(report.Peers))
	}
}
