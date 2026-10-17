// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestBootstrapPlanChangeInvalidatesExistingDigest(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	credentials, err := loadCredentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		t.Fatal(err)
	}
	request := pgspec.SQLBootstrapRequest{
		Components: stackComponents.Enabled(rc.Cluster.Spec.Components),
		Extensions: options.Extensions, Passwords: credentials.Roles,
	}
	config, err := p.Core.MemberConfig(t.Context(), rc, p.Core.DataMembers(rc.Cluster)[0])
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	generation := 0
	if spec.Bootstrap != nil {
		generation = spec.Bootstrap.Generation
	}
	// An existing cluster's recorded digest predates the additive baseline
	// graphql_public migration, but its selections/credentials are unchanged.
	legacy := configDigest(struct {
		Request    pgspec.SQLBootstrapRequest
		Generation int
	}{Request: request, Generation: generation})
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest", []byte(legacy)); err != nil {
		t.Fatal(err)
	}
	flavour := supabaseFlavour{core: p.Core}
	if err := flavour.PostReady(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	current, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest")
	if err != nil || string(current) == legacy {
		t.Fatal("updated migration plan did not reapply bootstrap on an existing cluster", err)
	}
	// The new digest is stable: the next reconciliation must not contact any
	// member just to reapply already-acknowledged migrations.
	rc.Agents = nil
	if err := flavour.PostReady(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
}
