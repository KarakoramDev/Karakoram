// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("raft/replicas%d", replicas), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = replicas
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("postgresql/replicas%d", replicas), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = replicas
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
			ca, err := pki.NewCA("storage-test", 0)
			if err != nil {
				t.Fatal(err)
			}
			p.options.Bindings = &sealBinding{c: pgbinding.Connection{Host: "postgres.example", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca.CertPEM}, CredentialGeneration: 1}}
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
