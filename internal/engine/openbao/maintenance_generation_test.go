// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"testing"
	"time"
)

func TestMaintenancePostgresScaleInKeepsGenerationPending(t *testing.T) {
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	ca, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	p.options.Bindings = &sealBinding{c: pgbinding.Connection{BindingID: "binding", Host: "postgres.example.test", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca}, CredentialGeneration: 1}}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	rc.Cluster.Generation++
	for _, m := range agents.members {
		m.voter = false
		m.healthy = false
	}
	agents.holdVoter = "demom1"
	ready(t, p, rc)
	generation := rc.Cluster.Status.ObservedGeneration
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	rc.Cluster.Generation++
	for range 20 {
		if _, err = p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if len(state(t, rc).Members) != 3 || rc.Cluster.Status.ObservedGeneration != generation {
			t.Fatal("deferred PostgreSQL-storage scale-in was acknowledged")
		}
	}
	rc.Cluster.Spec.Maintenance.Windows = nil
	ready(t, p, rc)
	if len(state(t, rc).Members) != 1 || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
		t.Fatal("open-window membership did not converge")
	}
}
