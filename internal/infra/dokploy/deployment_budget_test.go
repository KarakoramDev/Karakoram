// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type budgetArtifact []byte

func (a budgetArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(a)), nil
}

type budgetEvents struct{ events []model.Event }

func (e *budgetEvents) Record(_ context.Context, event model.Event) error {
	e.events = append(e.events, event)
	return nil
}

// Exercise real Neo4j Sync/Status and the adapter's HTTP fixture. Successful
// submit/wait cycles and complete CP reconstruction must not reset the budget.
func TestNeo4jBuildFailuresStopAcrossReconstructionAndRearm(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://mirror.example/native")
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := neo4j.New(neo4j.Options{ControlPlaneURL: "https://control.example.test"})
	cluster := &model.Cluster{Name: "demo", Namespace: model.DefaultNamespace}
	p.ApplyDefaults(&cluster.Spec)
	cluster.Spec.Version = "2026.09.0"
	if err = st.CreateCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	events := &budgetEvents{}
	rc := &engine.Context{Cluster: cluster, Store: st, Secrets: st, Backend: b, Artifact: budgetArtifact(renderTestArtifact(t)), Events: events}
	reconcile := func() {
		t.Helper()
		_, err := p.Sync(t.Context(), rc)
		if err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) && !strings.Contains(err.Error(), "Neo4j infrastructure deploy failed") {
			t.Fatalf("sync: %v", err)
		}
		// Model the exact external 404 outcome before the next observation.
		f.mu.Lock()
		for _, a := range f.apps {
			a.holdDeployments = true
			for _, d := range a.deployments {
				d.status = "error"
				d.finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
				a.row["applicationStatus"] = "error"
			}
		}
		f.mu.Unlock()
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Status = status
		if err = st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
			t.Fatal(err)
		}
		// Recreate both provider and backend, reload only durable cluster state,
		// and discard the process-wide admission reservations like a CP restart.
		deploymentServers.Delete(b.target.Config["url"])
		b = fakeBackend(t, f, nil)
		p = neo4j.New(neo4j.Options{ControlPlaneURL: "https://control.example.test"})
		rc.Backend = b
		rc.Cluster, err = st.GetCluster(t.Context(), cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
	}
	for budget := 1; budget <= 3; budget++ {
		for range 40 {
			reconcile()
		}
		if got := fakeCalls(f, "application.dropDeployment"); got != deploymentAttemptLimit*budget {
			t.Fatalf("unbounded or missing builds: got %d want %d", got, deploymentAttemptLimit*budget)
		}
		failure, ok := rc.Cluster.Status.DeploymentFailures["demon0"]
		if !ok || failure.Attempts != deploymentAttemptLimit || !strings.Contains(failure.LastError, "Dokploy deployment failed") || failure.RetryEpoch != rc.Cluster.Spec.RetryEpoch {
			t.Fatalf("missing durable exhaustion diagnosis: %+v", failure)
		}
		m := fakeMeta(t, f, "demon0")
		if m.DeploymentBudget == nil || m.DeploymentBudget.Attempts != deploymentAttemptLimit || m.DeploymentBudget.Digest == "" || m.DeploymentBudget.RetryEpoch != rc.Cluster.Spec.RetryEpoch {
			t.Fatalf("adapter lost durable budget: %+v", m.DeploymentBudget)
		}
		if err := prepareDeploymentBudget(&m, m.DeploymentBudget.Digest, rc.Cluster.Spec.RetryEpoch); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatalf("adapter reconstruction re-armed exhausted intent: %v", err)
		}
		conditions := 0
		for _, condition := range rc.Cluster.Status.Conditions {
			if condition.Reason == "DeploymentRetriesExhausted" {
				conditions++
			}
		}
		if conditions != 1 {
			t.Fatalf("missing terminal deployment status: %+v", rc.Cluster.Status.Conditions)
		}
		warnings := 0
		for _, event := range events.events {
			if event.Reason == "DeploymentRetriesExhausted" {
				warnings++
			}
		}
		if warnings != budget {
			t.Fatalf("exhaustion event repeated or missing: got %d want %d", warnings, budget)
		}
		if budget == 1 {
			// A new desired generation/runtime gets a fresh budget.
			spec := rc.Cluster.Spec
			spec.Version = "2026.10.0"
			if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), spec); err != nil {
				t.Fatal(err)
			}
		} else if budget == 2 {
			// Reuse the existing public operator-retry boundary for unchanged intent.
			if err := st.IncrementClusterRetryEpoch(t.Context(), rc.Cluster.Ref()); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		rc.Cluster, err = st.GetCluster(t.Context(), cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
	}
}
