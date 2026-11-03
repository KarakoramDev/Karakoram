// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

func TestAdministratorRotationRefreshesSupabaseConsumers(t *testing.T) {
	p, rc, _ := setupProvider(t)
	for _, name := range []string{"meta", "studio", "functions"} {
		spec := rc.Cluster.Spec.Components.Extra[name]
		spec.Replicas = 1
		rc.Cluster.Spec.Components.Extra[name] = spec
	}
	convergeProvider(t, p, rc)
	before, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]string{}
	for _, name := range []string{"meta", "studio", "functions"} {
		hashes[name] = before.Members[name].ConfigHash
	}
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "users/postgres", []byte("rotated-synthetic-admin")); err != nil {
		t.Fatal(err)
	}
	f := supabaseFlavour{core: p.Core, provider: p}
	complete := false
	for range 100 {
		pending, err := f.AdminConsumers(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if pending == "" {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("component acknowledgements did not converge")
	}
	after, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"meta", "studio", "functions"} {
		m := after.Members[name]
		if m.ConfigHash == hashes[name] {
			t.Fatalf("%s was not reconfigured", name)
		}
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec sbspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.DBRoles["postgres"] != "rotated-synthetic-admin" {
			t.Fatalf("%s retained stale administrator credential", name)
		}
	}
}
