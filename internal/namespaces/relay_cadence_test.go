// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type cadenceBackend struct {
	*projectBackend
	gets, polls int
	getError    error
}

func (b *cadenceBackend) GetService(ctx context.Context, hostname string) (*infra.Service, error) {
	b.gets++
	if b.getError != nil {
		return nil, b.getError
	}
	return b.projectBackend.GetService(ctx, hostname)
}

func (b *cadenceBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	b.polls++
	return b.projectBackend.OpStatus(ctx, op)
}

// A synchronous deployment keeps a mismatched hello exercising service checks
// rather than changing the test into asynchronous operation polling.
func (b *cadenceBackend) Deploy(context.Context, string, infra.Deployment) (infra.Op, error) {
	return infra.Op{}, nil
}

func connectCadenceRelay(t *testing.T, r *Registry, n *domain.Namespace, hash string) {
	t.Helper()
	key, err := r.o.Store.GetNamespaceRelayKey(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(r.Hub.Handler())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- relay.Run(ctx, relay.Options{URL: server.URL, Namespace: n.Name, Hostname: n.Relay.Hostname, CertPEM: n.ClientCert, KeyPEM: key, CAPEM: r.pki.CACert, ListenAgent: "127.0.0.1:0", ExecutableSHA256: hash, HTTPClient: server.Client(), Logger: r.o.Logger})
	}()
	t.Cleanup(func() {
		cancel()
		r.Close()
		server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	deadline, stop := context.WithTimeout(t.Context(), 3*time.Second)
	defer stop()
	for {
		select {
		case notice := <-r.notices:
			if notice.ns == n.Name && notice.event.Type == "connected" {
				return
			}
		case <-deadline.Done():
			t.Fatal("relay did not publish its connected session")
		}
	}
}

func setupCadenceRelay(t *testing.T, connected bool, hash string) (*Registry, *cadenceBackend, string, *time.Time) {
	t.Helper()
	r, backend, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.PendingOpID = ""
	n.PendingOpKind = ""
	n.PendingExecutableHash = ""
	n.SecretsPending = false
	n.DeployedExecutableHash = r.o.ExecutableHash
	n.Relay.ServiceID = "service-test"
	n.Token.LastValidatedAt = time.Now()
	if err := r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	backend.service = &infra.Service{ID: n.Relay.ServiceID, Hostname: n.Relay.Hostname, Kind: infra.KindRuntime, Status: infra.StatusRunning, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
	b := &cadenceBackend{projectBackend: backend}
	r.mu.Lock()
	r.backends[n.Name] = b
	r.mu.Unlock()
	now := time.Now()
	r.now = func() time.Time { return now }
	if connected {
		connectCadenceRelay(t, r, n, hash)
	}
	return r, b, n.Name, &now
}

func cadenceNamespace(t *testing.T, r *Registry, name string, change func(*domain.Namespace)) {
	t.Helper()
	n, err := r.o.Store.GetNamespaceForUpdate(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	change(n)
	if err = r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
}

func TestSettledRelayPlatformCadence(t *testing.T) {
	for _, hash := range []string{"", "test-hash"} {
		t.Run("hello-"+hash, func(t *testing.T) {
			r, b, name, now := setupCadenceRelay(t, true, hash)
			start := *now
			for tick := range 4 {
				*now = start.Add(time.Duration(tick) * 15 * time.Second)
				r.step(t.Context(), name)
			}
			if b.gets != 1 {
				t.Fatalf("four settled ticks: GetService = %d, want 1", b.gets)
			}
			*now = start.Add(infra.SnapshotListingInterval)
			r.step(t.Context(), name)
			if b.gets != 2 {
				t.Fatalf("60 s boundary: GetService = %d, want 2", b.gets)
			}
			for tick := 5; tick < 240; tick++ {
				*now = start.Add(time.Duration(tick) * 15 * time.Second)
				r.step(t.Context(), name)
			}
			if b.gets != 60 {
				t.Fatalf("one simulated hour: GetService = %d, want 60 (ungated: 240)", b.gets)
			}
		})
	}
}

func TestUnsettledRelayChecksEveryEligibleTick(t *testing.T) {
	cases := []struct {
		name         string
		disconnected bool
		hash         string
		change       func(*domain.Namespace)
		checkError   bool
	}{
		{name: "disconnected", disconnected: true},
		{name: "hello hash mismatch", hash: "old-hash"},
		{name: "deployed hash mismatch", change: func(n *domain.Namespace) { n.DeployedExecutableHash = "old-hash" }},
		{name: "secrets pending", change: func(n *domain.Namespace) { n.SecretsPending = true }},
		{name: "missing service identity", change: func(n *domain.Namespace) { n.Relay.ServiceID = "" }},
		{name: "invalid certificate", change: func(n *domain.Namespace) { n.ClientCert = []byte("invalid") }},
		{name: "certificate renewal", change: func(n *domain.Namespace) {}},
		{name: "previous check error", checkError: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, b, name, now := setupCadenceRelay(t, !tc.disconnected, tc.hash)
			r.step(t.Context(), name) // Prime a successful platform check.
			before := b.gets
			if tc.checkError {
				b.getError = errors.New("platform unavailable")
				*now = now.Add(infra.SnapshotListingInterval)
			}
			if tc.name == "certificate renewal" {
				*now = now.Add(400 * 24 * time.Hour)
			}
			for range 3 {
				cadenceNamespace(t, r, name, func(n *domain.Namespace) {
					// Advance to an eligible retry without changing production backoff.
					n.RetryAt = time.Time{}
					n.PendingOpID = ""
					n.PendingOpKind = ""
					if tc.change != nil {
						tc.change(n)
					}
				})
				*now = now.Add(15 * time.Second)
				r.step(t.Context(), name)
			}
			if b.gets-before != 3 {
				t.Fatalf("unsettled GetService = %d, want 3", b.gets-before)
			}
		})
	}
}

func TestPendingRelayKeepsOperationPolling(t *testing.T) {
	r, b, name, now := setupCadenceRelay(t, true, "")
	r.step(t.Context(), name)
	b.opDone = false
	cadenceNamespace(t, r, name, func(n *domain.Namespace) {
		n.PendingOpID = "pending"
		n.PendingOpKind = "deploy"
	})
	for range 3 {
		*now = now.Add(15 * time.Second)
		r.step(t.Context(), name)
	}
	if b.polls != 3 || b.gets != 1 {
		t.Fatalf("pending ticks: OpStatus=%d GetService=%d, want 3 and 1", b.polls, b.gets)
	}
	cadenceNamespace(t, r, name, func(n *domain.Namespace) { n.PendingOpID = ""; n.PendingOpKind = "" })
	r.step(t.Context(), name)
	if b.gets != 2 {
		t.Fatal("pending transition retained cached platform success")
	}
}

func TestDeletingRelayBypassesSettledCadence(t *testing.T) {
	r, b, name, _ := setupCadenceRelay(t, true, "")
	r.step(t.Context(), name)
	cadenceNamespace(t, r, name, func(n *domain.Namespace) { n.Phase = "Deleting" })
	r.step(t.Context(), name) // Service deletion.
	r.step(t.Context(), name) // Pending deletion completion.
	r.step(t.Context(), name) // Confirm service absence and forget namespace.
	if b.gets != 3 || b.deletes != 1 {
		t.Fatalf("deletion: GetService=%d deletes=%d, want 3 and 1", b.gets, b.deletes)
	}
	if _, err := r.o.Store.GetNamespaceForUpdate(t.Context(), name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("namespace not deleted: %v", err)
	}
	r.mu.RLock()
	_, remembered := r.relayChecks[name]
	r.mu.RUnlock()
	if remembered {
		t.Fatal("deleted namespace retained platform check")
	}
}

func TestRelayCadencePreservesRetryAndLocalTicks(t *testing.T) {
	r, b, name, now := setupCadenceRelay(t, true, "")
	r.step(t.Context(), name)
	cadenceNamespace(t, r, name, func(n *domain.Namespace) {
		n.Relay.Connected = false
		n.DeployedExecutableHash = "old-hash"
		n.RetryAt = time.Now().Add(time.Hour)
	})
	*now = now.Add(15 * time.Second)
	r.step(t.Context(), name)
	if b.gets != 1 {
		t.Fatal("RetryAt did not fence platform reconciliation")
	}
	n, err := r.o.Store.GetNamespaceForUpdate(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	if !n.Relay.Connected {
		t.Fatal("retrying tick did not synchronize and save the live hub session")
	}
	cadenceNamespace(t, r, name, func(n *domain.Namespace) {
		n.DeployedExecutableHash = r.o.ExecutableHash
		n.RetryAt = time.Time{}
	})
	r.step(t.Context(), name)
	if b.gets != 2 {
		t.Fatal("state change observed during backoff retained cached success")
	}
	cadenceNamespace(t, r, name, func(n *domain.Namespace) { n.Relay.Connected = false })
	r.step(t.Context(), name)
	n, err = r.o.Store.GetNamespaceForUpdate(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	if b.gets != 2 || !n.Relay.Connected {
		t.Fatal("settled tick failed to skip only platform work while saving session state")
	}
}
