// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestCompanionCleanupAfterExternalNomadPurge(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	cluster := &domain.Cluster{Name: "demo", Namespace: domain.DefaultNamespace, Spec: domain.ClusterSpec{DataMcp: &domain.DataMcpSpec{Enabled: true}}}
	member := companion.Member(cluster.Name)
	const token = "companion-token"
	spec := runtimeSpec(member, "")
	spec.Env = engine.OwnershipEnv(cluster.Name, member, token)
	spec.Vars = engine.OwnerVars(token, cluster.Name, member, member)
	importFake(t, b, spec)
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	deploy, err := b.Deploy(t.Context(), member, testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		state, err := b.OpStatus(t.Context(), deploy)
		if err != nil || state.Done || state.Failed {
			t.Fatalf("prepare pending companion: %+v %v", state, err)
		}
		if strings.Contains(state.Message, "placement blocked") {
			break
		}
	}
	r, err := b.load(t.Context(), member)
	if err != nil || r == nil || r.Pending == nil || r.Pending.Phase != "starting" {
		t.Fatalf("missing starting companion: %+v %v", r, err)
	}
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err = st.CreateCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if err = st.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: cluster.Namespace, Cluster: cluster.Name, Member: member, Token: token, Hash: secrets.Hash(token)}); err != nil {
		t.Fatal(err)
	}
	state, err := json.Marshal(map[string]any{"id": r.ID, "stage": "deploy", "pending": deploy, "plan": engine.NewImportPlan([]infra.ServiceSpec{spec})})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string][]byte{"datamcp/state": state, "datamcp/config": []byte(`{}`)} {
		if err = st.SetClusterSecret(t.Context(), cluster.Ref(), key, value); err != nil {
			t.Fatal(err)
		}
	}
	// Purge the job without touching the durable service record or CP receipt.
	if err = b.c.delete(t.Context(), "/v1/job/"+member, query("purge", "true"), nil); err != nil {
		t.Fatal(err)
	}
	b = fakeBackend(t, f, nil) // Control-plane restart.
	cluster.Spec.DataMcp.Enabled = false
	rc := &engine.Context{Cluster: cluster, Backend: b, Store: st, Secrets: st}
	controller := &companion.Controller{}
	done := false
	for range 30 {
		done, err = controller.Cleanup(t.Context(), rc)
		if err != nil {
			t.Fatalf("cleanup externally purged companion: %v", err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("companion cleanup did not converge")
	}
	if service, err := b.GetService(t.Context(), member); err != nil || service != nil {
		t.Fatalf("companion service remains: %+v %v", service, err)
	}
	if _, err = st.GetAgentToken(t.Context(), cluster.Ref(), member); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("companion identity was not revoked: %v", err)
	}
	for _, key := range []string{"datamcp/state", "datamcp/config"} {
		if _, err = st.GetClusterSecret(t.Context(), cluster.Ref(), key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("companion secret %s remains: %v", key, err)
		}
	}
}
