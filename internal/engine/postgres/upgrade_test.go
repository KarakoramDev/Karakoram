// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func upgradeFenceFixture(t *testing.T) (*Core, *engine.Context, *fakeBackend) {
	t.Helper()
	p, rc, b := setup(t, "ha")
	rc.Cluster.Spec.Version = "16.15.1"
	s := converge(t, p, rc)
	rc.Cluster.Spec.Version = "17.11.1"
	s.Upgrade = &upgradeState{From: "16.15.1", To: "17.11.1", Receipt: "upgrade-fence", Stage: "pause", Primary: "demop0", Locale: "C"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.actions = nil
	return p, rc, b
}

func TestUpgradePauseAcknowledgedBeforeReplicaFirstShutdown(t *testing.T) {
	p, rc, b := upgradeFenceFixture(t)
	b.upgradePausePending = map[string]bool{"demop2": true}
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	for _, m := range s.data() {
		if m.Offline {
			t.Fatal("stop intent published before every pause acknowledgement")
		}
	}
	// Leadership can change during the pause handshake, before fencing completes.
	b.leader = "demop1"
	delete(b.upgradePausePending, "demop2")
	p = New(p.options)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Upgrade.Primary != "demop1" || s.Upgrade.Timeline != 1 || s.find("demop1").Offline {
		t.Fatal("authoritative paused writer not retained", s.Upgrade)
	}
	b.statusOverrides = map[string]pgspec.StatusResponse{"demop0": {Running: true, Role: "replica"}}
	if _, err := New(p.options).Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Upgrade.Stage != "stop-replicas" || s.find("demop1").Offline {
		t.Fatal("writer stopped while a replica still ran")
	}
	delete(b.statusOverrides, "demop0")
	if _, err := New(p.options).Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Upgrade.Stage != "stop" || !s.find("demop1").Offline {
		t.Fatal("writer stop intent missing after all replicas stopped")
	}
	if slices.Contains(b.actions, "upgrade-stop:demop1") {
		t.Fatal("writer stop issued before replica shutdown checkpoint was durable")
	}
	if _, err := New(p.options).Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Upgrade.Stage != "check" {
		t.Fatal(s.Upgrade)
	}
	// A previously stopped observation cannot authorize a later destructive step.
	for _, stage := range []string{"check", "link"} {
		s.Upgrade.Stage = stage
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		b.actions = nil
		b.statusOverrides["demop2"] = pgspec.StatusResponse{Running: true, Role: "replica"}
		if _, err := New(p.options).Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		for _, action := range b.actions {
			if strings.Contains(action, "/upgrade-check") || strings.Contains(action, "/upgrade-run") {
				t.Fatal("upgrade used a stale all-stopped observation", action)
			}
		}
		delete(b.statusOverrides, "demop2")
	}
}

func TestUpgradeFinalWriterChangeFailsClosed(t *testing.T) {
	for _, changed := range []string{"role", "timeline", "pause"} {
		t.Run(changed, func(t *testing.T) {
			p, rc, b := upgradeFenceFixture(t)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			switch changed {
			case "role":
				b.leader = "demop1"
			case "timeline":
				b.statusOverrides = map[string]pgspec.StatusResponse{"demop0": {Running: true, Role: "primary", Timeline: 2}}
			case "pause":
				b.upgradePausePending = map[string]bool{"demop0": true}
			}
			if _, err := New(p.options).Sync(t.Context(), rc); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("changed final writer accepted: %v", err)
			}
			s, _ := load(rc.Cluster)
			if s.find("demop0").Offline || s.Upgrade.Stage != "stop-replicas" {
				t.Fatal("writer changed after a failed fence", s.Upgrade)
			}
		})
	}
}

func TestUpgradeExcludesEveryImperativeOperation(t *testing.T) {
	p, rc, b := upgradeFenceFixture(t)
	s, _ := load(rc.Cluster)
	for _, stage := range []string{"prepare", "pause", "stop-replicas", "stop", "check", "link", "dcs-reset", "primary", "replicas", "stanza", "backup", "finalize"} {
		s.Upgrade.Stage = stage
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		allowed, err := New(p.options).CanClaimOperation(rc.Cluster)
		if err != nil || allowed {
			t.Fatalf("claim allowed during %s: %v", stage, err)
		}
		before := len(b.actions)
		for _, kind := range []string{domain.OperationBackup, domain.OperationRestore, domain.OperationReplaceMember, domain.OperationDeleteBackup, domain.OperationRestart, domain.OperationRotateTLS, "reinit-member", "initial-restore"} {
			if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: kind}); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("%s dispatched during %s: %v", kind, stage, err)
			}
		}
		if len(b.actions) != before {
			t.Fatal("excluded work reached a member")
		}
	}
	s.Upgrade = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
		t.Fatal("queue ownership was not released", err)
	}
}

func TestUpgradeUnprovenStoppedCheckpointFailsClosed(t *testing.T) {
	for _, stage := range []string{"stop", "check", "link"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, b := upgradeFenceFixture(t)
			s, _ := load(rc.Cluster)
			s.Upgrade.Stage = stage
			for _, m := range s.data() {
				m.Offline = true
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("unproven stopped checkpoint accepted: %v", err)
			}
			if len(b.actions) != 0 {
				t.Fatal("unproven upgrade checkpoint reached a member", b.actions)
			}
		})
	}
}
