// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type preimportDNSAgents struct {
	t     *testing.T
	calls int
}

func (a *preimportDNSAgents) Dial(ctx context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	a.calls++
	a.t.Errorf("dialed undeployed member %s", name)
	// Model name resolution that consumes the caller's entire budget.
	<-ctx.Done()
	return nil, &net.DNSError{Name: name, Err: "no such host", IsNotFound: true}
}

func TestCreateImportsBeforeNativeDNSObservation(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, "group")
			}
			agents := rc.Agents
			dns := &preimportDNSAgents{t: t}
			rc.Agents = dns
			// A resumed create already has planned identities from its first tick.
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.allocate(rc.Cluster)
			if err = save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			imported := false
			for range 12 {
				before, err := json.Marshal(rc.Cluster.Internal)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				_, err = p.Sync(ctx, rc)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				status, err := p.Status(ctx, rc)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if status.Phase == domain.PhaseReady {
					t.Fatal("unimported cluster reported ready")
				}
				after, err := json.Marshal(rc.Cluster.Internal)
				if err != nil {
					t.Fatal(err)
				}
				if string(before) == string(after) {
					t.Fatal("reconcile did not advance a durable provisioning checkpoint")
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				imported = true
				for _, m := range s.data() {
					if b.services[m.Name] == nil || b.services[m.Volume] == nil {
						imported = false
					}
				}
				if imported {
					break
				}
			}
			if !imported || dns.calls != 0 {
				t.Fatalf("data imports=%v, pre-runtime probes=%d", imported, dns.calls)
			}
			if !s.LastGTIDSafeAt.IsZero() || activeGTIDIncident(s) != nil {
				t.Fatal("missing native observations changed GTID history safety")
			}
			// After the durable imports, ordinary deployment/formation imports the
			// proxy too, without bypassing its joined-writer admission gate.
			rc.Agents = agents
			converge(t, p, rc, b)
			if b.services[rc.Cluster.Name+"mx"] == nil {
				t.Fatal("proxy was not imported")
			}
		})
	}
}

type deadlineGaleraAgents struct{ t *testing.T }

func (a deadlineGaleraAgents) Dial(ctx context.Context, _ domain.ClusterRef, _ string) (*agentapi.Client, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > engine.ObservationTimeout {
		a.t.Error("Galera dial has no bounded observation deadline")
	}
	return nil, engine.ErrUnavailable
}

func TestGaleraStatusBoundsDial(t *testing.T) {
	_, rc, _ := setupPXC(t)
	rc.Agents = deadlineGaleraAgents{t: t}
	if _, err := galeraStatus(t.Context(), rc, "demom0"); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestGaleraStatusBoundsNativeRequest(t *testing.T) {
	_, rc, b := setupPXC(t)
	release := make(chan struct{})
	defer close(release)
	b.action = func(_ http.ResponseWriter, r *http.Request, _ string) bool {
		if r.URL.Path != "/v1/mysql/galera-status" {
			return false
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
		return true
	}
	ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationTimeout+2*time.Second)
	defer cancel()
	started := time.Now()
	_, err := galeraStatus(ctx, rc, "demom0")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("native probe error=%v", err)
	}
	if elapsed := time.Since(started); elapsed > engine.ObservationTimeout+time.Second {
		t.Fatalf("native probe consumed %s", elapsed)
	}
	if ctx.Err() != nil {
		t.Fatal("native probe exhausted parent reconciliation budget")
	}
}
