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
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
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
			// Match live creation: PITR observation also selects an archive
			// writer, independently of ordinary status and GTID observation.
			storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Backup.Storage = storage.Name
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			action := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/archive-status" {
					if err := json.NewEncoder(w).Encode(myspec.BinlogArchiveStatus{}); err != nil {
						t.Error(err)
					}
					return true
				}
				return action != nil && action(w, r, name)
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

func TestPXCInitialProcessAdmissionFollowsFormation(t *testing.T) {
	p, rc, _ := setupPXC(t)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	receipt := s.Formation.GTIDCertification
	if receipt == nil {
		t.Fatal("formation did not capture empty identities before deployment")
	}
	data := s.data()
	for _, m := range data {
		m.ServiceID, m.Stage = m.Name+"-id", "deployed"
	}
	phases := []struct {
		name, state     string
		published       bool
		joined, running int
	}{
		{"unformed", "unformed", false, 0, 0},
		{"bootstrap-intent-only", "bootstrapping", false, 0, 0},
		{"bootstrap-published", "bootstrapping", true, 0, 1},
		{"formed-first-follower", "formed", true, 1, 2},
		{"formed-next-follower", "formed", true, 2, 3},
		{"formed-all-synced", "formed", true, 3, 3},
	}
	for _, phase := range phases {
		s.Formation.State = phase.state
		receipt.Checkpoint.GTIDCertification = &pxcRestoreCertification{Negotiated: true, FencesOwned: true}
		s.RestoreGTIDFence = ""
		if phase.published {
			s.RestoreGTIDFence = receipt.Owner
		}
		for i, m := range data {
			m.Stage, m.Initialized = "deployed", false
			if i < phase.joined {
				m.Stage, m.Initialized = "joined", true
			}
			m.GaleraSSTDonor = ""
			if phase.published && m.Name != s.Formation.Seed {
				m.GaleraSSTDonor = s.Formation.Seed
			}
		}
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		for i, m := range data {
			cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec myspec.Spec
			if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if err := spec.Validate(); err != nil {
				t.Fatalf("%s real member validator: %v", phase.name, err)
			}
			if running := len(cfg.DesiredProcesses) > 0; running != (i < phase.running) {
				t.Fatalf("%s member %s desired processes=%v", phase.name, m.Name, cfg.DesiredProcesses)
			}
			if want := phase.state == "bootstrapping" && m.Name == s.Formation.Seed; spec.Galera.Bootstrap != want {
				t.Fatalf("%s member %s bootstrap=%v", phase.name, m.Name, spec.Galera.Bootstrap)
			}
			if len(cfg.DesiredProcesses) == 0 && cfg.DesiredProcesses == nil {
				t.Fatal("nil desired processes would allow implicit startup")
			}
		}
	}
}
