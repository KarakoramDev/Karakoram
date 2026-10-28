// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
)

func TestMaintenanceHTTPSPatchDoesNotMutateRuntime(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			converge(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members[tspec.UI]
			// Exercise enable and disable at the provider's ingress boundary. A nil
			// backend makes any accidental platform call fail instead of hiding a restart.
			m.Public = !public
			o, err := optionsFor(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			o.UI.PublicHTTPS = public
			if public {
				o.UI.OIDC = &OIDCOptions{Issuer: "https://identity.example", ClientID: "temporal"}
				if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), UIOIDCClientSecretKey, []byte("test-oidc-secret")); err != nil {
					t.Fatal(err)
				}
			}
			rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Backend = nil
			for _, stage := range []engine.ComponentStage{engine.ComponentJoined, engine.ComponentImported} {
				if public && stage == engine.ComponentImported {
					// Imported after stop-public is already past admission and
					// must finish even if the window has since closed.
					continue
				}
				m.Stage = stage
				busy, err := syncUIExposure(t.Context(), rc, s, m)
				if err != nil || !busy || s.Pending != nil || m.Public == public {
					t.Fatal("closed window crossed ingress boundary", busy, err)
				}
			}
		})
	}
}

func TestMaintenanceHTTPSAdmittedTransitionCompletesAfterClosure(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members[tspec.UI]
	o, err := optionsFor(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	o.UI.PublicHTTPS = true
	o.UI.OIDC = &OIDCOptions{Issuer: "https://identity.example", ClientID: "temporal"}
	rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), UIOIDCClientSecretKey, []byte("test-oidc-secret")); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation = 2
	rc.Cluster.Status.ObservedGeneration = 1
	if busy, err := syncUIExposure(t.Context(), rc, s, m); err != nil || !busy || s.Pending == nil || s.Pending.Action != "stop-public" {
		t.Fatal("open window did not admit private UI stop", busy, err)
	}
	for range 4 {
		if _, err := observePending(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
		if s.Pending == nil {
			break
		}
	}
	if s.Pending != nil || m.Stage != engine.ComponentImported {
		t.Fatal("admitted stop did not complete")
	}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	if busy, err := syncUIExposure(t.Context(), rc, s, m); err != nil || !busy || s.Pending == nil || s.Pending.Action != "public" {
		t.Fatal("window closure stranded the admitted UI transition", busy, err)
	}
}
