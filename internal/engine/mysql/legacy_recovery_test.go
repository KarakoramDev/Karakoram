// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestLegacyGroupCredentialBlockedCondition(t *testing.T) {
	p, rc, b := setup(t, "group")
	if err := putSecret(t.Context(), rc, "users/kk_repl", []byte(strings.Repeat("a", 40))); err != nil {
		t.Fatal(err)
	}
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		s, _ := load(rc.Cluster)
		if s.Formation.State != "bootstrapping" {
			continue
		}
		st := b.native[s.Formation.Seed]
		st.DataIdentity.FormationEpoch = s.Formation.FormationEpoch
		b.native[s.Formation.Seed] = st
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		for _, condition := range status.Conditions {
			if condition.Reason == "BootstrapEpochConsumed" {
				for _, want := range []string{"32-character", "3972", "delete and recreate", "preserve valuable data"} {
					if !strings.Contains(condition.Message, want) {
						t.Fatalf("blocked condition lacks %q", want)
					}
				}
				if b.bootstrap[s.Formation.FormationEpoch] != 0 {
					t.Fatal("legacy blocked epoch was retried")
				}
				return
			}
		}
		t.Fatal("missing actionable blocked condition")
	}
	t.Fatal("bootstrap checkpoint not reached")
}

func TestLegacySingletonCredentialAdoptionCrashReplay(t *testing.T) {
	p, rc, b := setup(t, "single")
	if err := putSecret(t.Context(), rc, "users/kk_repl", []byte(strings.Repeat("a", 40))); err != nil {
		t.Fatal(err)
	}
	converge(t, p, rc, b)
	before := append(json.RawMessage(nil), rc.Cluster.Internal...)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Topology.Type = "group"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 1
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	if err := p.adoptSingleton(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	credential, err := secret(t.Context(), rc, "users/kk_repl")
	if err != nil || len(credential) != 32 {
		t.Fatal("legacy singleton did not persist a native-compatible credential")
	}
	generatedRecoveryPasswordContract.Assert(t, "adopted recovery password", string(credential))
	// Crash after secret persistence but before the adoption state is saved.
	rc.Cluster.Internal = before
	p = New(Options{})
	restarted := false
	b.action = func(_ http.ResponseWriter, r *http.Request, _ string) bool {
		if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			restarted = true
		}
		if r.URL.Path == "/v1/mysql/bootstrap" && !restarted {
			t.Fatal("bootstrap preceded the adoption account-rekey restart")
		}
		return false
	}
	converge(t, p, rc, b)
	after, err := secret(t.Context(), rc, "users/kk_repl")
	if err != nil || string(after) != string(credential) || !restarted {
		t.Fatal("adoption replay changed the credential or skipped account rekey")
	}
	config, err := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	var native myspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &native); err != nil {
		t.Fatal(err)
	}
	generatedRecoveryPasswordContract.Assert(t, "native adopted recovery password", native.Replication.Password)
	if native.Replication.Password != string(credential) {
		t.Fatal("native adoption credential differs from persisted value")
	}
	// Once formed, reconciliation must never migrate its recovery credential.
	if err := putSecret(t.Context(), rc, "users/kk_repl", []byte(strings.Repeat("b", 40))); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	after, err = secret(t.Context(), rc, "users/kk_repl")
	if err != nil || len(after) != 40 {
		t.Fatal("formed group's recovery credential was changed")
	}
}
