// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
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
	// The previous CREATE-only receipt had the same immutable migration plan.
	legacy := configDigest(struct {
		Request    pgspec.SQLBootstrapRequest
		Generation int
		Plan       []initsql.Step
	}{request, generation, supabaseProfile.Plan(request.Components, request.Extensions, 17)})
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

func TestSameVersionRestoreUpdatesExtensionsBeforeBootstrapCompletion(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	version := rc.Cluster.Spec.Version
	previous, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest")
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]json.RawMessage
	var core map[string]any
	if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(root["postgres"], &core); err != nil {
		t.Fatal(err)
	}
	core["bootstrapGeneration"] = core["bootstrapGeneration"].(float64) + 1
	root["postgres"], _ = json.Marshal(core)
	rc.Cluster.Internal, _ = json.Marshal(root)
	backend.prepares = nil
	vectorVersion := "0.8.6"
	failUpdate := true
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
		case "/v1/postgres/" + pgspec.ActionSQLBootstrap:
			actions = append(actions, "bootstrap")
			// CREATE EXTENSION IF NOT EXISTS preserves the restored SQL version.
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/" + pgspec.ActionUpdateExtensions:
			var request pgspec.AccessRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if !slices.Contains(request.Extensions, "vector") {
				t.Error("restored vector omitted from installed-extension update")
			}
			actions = append(actions, "update")
			if failUpdate {
				http.Error(w, "extension SQL failed", http.StatusInternalServerError)
				return
			}
			vectorVersion = "0.8.7"
			json.NewEncoder(w).Encode([]pgspec.ExtensionUpdateResult{{Database: "postgres", Updated: []string{"vector"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	flavour := supabaseFlavour{core: p.Core}
	if err := flavour.PostReady(t.Context(), rc); err == nil {
		t.Fatal("restore bootstrap acknowledged failed extension SQL")
	}
	current, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest")
	if err != nil || string(current) != string(previous) || vectorVersion != "0.8.6" {
		t.Fatal("failed extension update advanced restored completion", err)
	}
	failUpdate = false
	if err := flavour.PostReady(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	current, err = rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest")
	if err != nil || string(current) == string(previous) || vectorVersion != "0.8.7" {
		t.Fatal("retry failed to update restored SQL and acknowledge completion", err)
	}
	if !slices.Equal(actions, []string{"bootstrap", "update", "bootstrap", "update"}) {
		t.Fatal("restore SQL retry order", actions)
	}
	if rc.Cluster.Spec.Version != version || len(backend.prepares) != 0 {
		t.Fatal("same-version SQL retry redeployed members")
	}
	rc.Agents = nil
	if err := flavour.PostReady(t.Context(), rc); err != nil {
		t.Fatal("completed restored bootstrap was not stable", err)
	}
}

func TestBootstrapDigestTracksBundleBytes(t *testing.T) {
	bundles, err := bootstrapBundles()
	if err != nil {
		t.Fatal(err)
	}
	bundles = append(bundles, nativeartifacts.Artifact{Name: "pgvector-pg17", SHA256: "qualified-vector-bytes"})
	request := pgspec.SQLBootstrapRequest{Extensions: []string{"vector", "set_user"}}
	before := bootstrapDigest(request, 0, bundles)
	bundles[len(bundles)-1].SHA256 = "different-qualified-bytes"
	if before == bootstrapDigest(request, 0, bundles) {
		t.Fatal("new extension binaries did not invalidate SQL bootstrap")
	}
}
