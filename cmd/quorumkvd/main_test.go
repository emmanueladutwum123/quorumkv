package main

import (
	"testing"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// Cluster configuration is parsed once at startup, and a mistake here produces a
// node that appears healthy while talking to the wrong peers — or worse, one that
// silently omits a member and so computes quorum against the wrong set.

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name string
		spec string
		want map[raft.NodeID]string
	}{
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{
			"single peer",
			"1=127.0.0.1:9000",
			map[raft.NodeID]string{1: "127.0.0.1:9000"},
		},
		{
			"three peers",
			"1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003",
			map[raft.NodeID]string{1: "127.0.0.1:9001", 2: "127.0.0.1:9002", 3: "127.0.0.1:9003"},
		},
		{
			"tolerates surrounding whitespace",
			" 1 = 127.0.0.1:9001 , 2 = 127.0.0.1:9002 ",
			map[raft.NodeID]string{1: "127.0.0.1:9001", 2: "127.0.0.1:9002"},
		},
		{
			"tolerates a trailing comma",
			"1=127.0.0.1:9001,",
			map[raft.NodeID]string{1: "127.0.0.1:9001"},
		},
		{
			"hostname with port",
			"1=node-a.internal:9000",
			map[raft.NodeID]string{1: "node-a.internal:9000"},
		},
		{
			"ipv6 address keeps its colons",
			"1=[::1]:9000",
			map[raft.NodeID]string{1: "[::1]:9000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePeers(tt.spec)
			if err != nil {
				t.Fatalf("parsePeers(%q): %v", tt.spec, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parsed %d peers, want %d: %v", len(got), len(tt.want), got)
			}
			for id, addr := range tt.want {
				if got[id] != addr {
					t.Errorf("peer %d = %q, want %q", id, got[id], addr)
				}
			}
		})
	}
}

func TestParsePeersRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		spec string
	}{
		{"no equals sign", "127.0.0.1:9000"},
		{"non-numeric id", "abc=127.0.0.1:9000"},
		{"reserved id zero", "0=127.0.0.1:9000"},
		{"empty address", "1="},
		// A duplicated id is the dangerous case: silently keeping one would give the
		// node a smaller cluster than the operator wrote, and quorum would be
		// computed against the wrong set.
		{"duplicate id", "1=127.0.0.1:9001,1=127.0.0.1:9002"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parsePeers(tt.spec); err == nil {
				t.Errorf("parsePeers(%q) succeeded, want an error", tt.spec)
			}
		})
	}
}

func TestNewLoggerAcceptsLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error", "DEBUG", "nonsense", ""} {
		if newLogger(level) == nil {
			t.Errorf("newLogger(%q) returned nil", level)
		}
	}
}
