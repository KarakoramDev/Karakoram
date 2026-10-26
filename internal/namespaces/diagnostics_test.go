// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/relay"
)

func TestRelayDisconnectedDiagnostics(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.DeployedExecutableHash = "deployed"
	n.PendingOpID = ""
	for _, tc := range []struct {
		name   string
		status relay.SessionStatus
		want   string
	}{
		{"unobserved", relay.SessionStatus{}, "no authenticated session observed"},
		{"dropped", relay.SessionStatus{LastSeen: time.Now()}, "authenticated session dropped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.syncSessionSnapshot(t.Context(), n, tc.status)
			got := n.Relay.LastError
			if n.Relay.Phase != "Error" || !strings.Contains(got, tc.want) || !strings.Contains(got, relay.DiagnosticEndpoint(r.o.PublicURL)+"/relay/v1/connect") || !strings.Contains(got, "inspect relay runtime logs") || !strings.Contains(got, "not visible to the control plane") {
				t.Fatalf("unhelpful disconnected status: %s", got)
			}
		})
	}
	r.syncSessionSnapshot(t.Context(), n, relay.SessionStatus{Connected: true})
	if n.Relay.LastError != "" || n.Relay.Phase != "Ready" {
		t.Fatal("connected session retained failure diagnosis")
	}
}

func TestRelayDisconnectedDiagnosticBounds(t *testing.T) {
	r, _, _ := setupRegistry(t)
	r.o.PublicURL = "https://" + strings.Repeat("a", 10000) + ".example"
	n := createTestNamespace(t, r)
	n.DeployedExecutableHash = "deployed"
	n.PendingOpID = ""
	r.syncSessionSnapshot(t.Context(), n, relay.SessionStatus{})
	if len(n.Relay.LastError) > 512 || !strings.Contains(n.Relay.LastError, ".../relay/v1/connect") || !strings.Contains(n.Relay.LastError, "inspect relay runtime logs") {
		t.Fatalf("unbounded or incomplete diagnostic (%d bytes): %s", len(n.Relay.LastError), n.Relay.LastError)
	}
}
