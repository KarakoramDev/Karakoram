// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type receiptEngineArtifact struct{}

func (receiptEngineArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("archive")), nil
}

type receiptEngineClock struct{ now time.Time }

func (c *receiptEngineClock) Now() time.Time { return c.now }

type receiptEngineBackend struct {
	*Backend
	deployments, failed int
	versions            []string
}

func (b *receiptEngineBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.deployments++
	op, err := b.Backend.Deploy(ctx, name, d)
	if err == nil {
		b.versions = append(b.versions, op.ID)
	}
	return op, err
}
func (b *receiptEngineBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	state, err := b.Backend.OpStatus(ctx, op)
	if state.Failed {
		b.failed++
	}
	return state, err
}

// Exercise the real Mongo provider's observePending -> failPending -> retryPending
// path against the REST fixture, not a hand-written approximation of that loop.
func TestVersionRequestRealEngineRetriesUnsubmittedDeploy(t *testing.T) {
	server := newFakeAPI(t)
	attempted := 0
	b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy") {
			attempted++
			if attempted == 1 {
				return nil, io.ErrUnexpectedEOF
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	}))
	b.client.http.Timeout = 100 * time.Millisecond
	backend := &receiptEngineBackend{Backend: b}
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	stateStore, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stateStore.Close() })
	provider := psmdb.New(psmdb.Options{Store: stateStore, InternalCAPEM: "control-plane-ca"})
	cluster := &domain.Cluster{Name: "demo"}
	provider.ApplyDefaults(&cluster.Spec)
	cluster.Spec.Components.Engine.Replicas = 1
	if err = stateStore.CreateCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	clock := &receiptEngineClock{now: time.Now()}
	rc := &engine.Context{Cluster: cluster, Store: stateStore, Secrets: stateStore, Backend: backend, Artifact: receiptEngineArtifact{}, Clock: clock}
	converged := false
	for range 40 {
		_, err = provider.Sync(t.Context(), rc)
		if at := infra.RetryAt(err); !at.IsZero() {
			if delay := time.Until(at); delay > 0 {
				time.Sleep(delay + 5*time.Millisecond)
			}
		}
		clock.now = clock.now.Add(time.Minute)
		var observed struct {
			Members []struct {
				Stage string `json:"stage"`
			} `json:"members"`
		}
		if e := json.Unmarshal(cluster.Internal, &observed); e != nil {
			t.Fatal(e)
		}
		if len(observed.Members) == 1 && observed.Members[0].Stage == "deployed" {
			converged = true
			break
		}
	}
	if !converged || backend.deployments != 2 || backend.failed != 1 || attempted != 2 || len(backend.versions) != 2 || backend.versions[0] != backend.versions[1] {
		t.Fatalf("real engine failed to retry same selected version: converged=%v backendCalls=%d failedAttempts=%d requests=%d versions=%v lastError=%v", converged, backend.deployments, backend.failed, attempted, backend.versions, err)
	}
	service, err := b.GetService(t.Context(), "demom0")
	if err != nil || service == nil {
		t.Fatalf("deployed member missing: %v", err)
	}
	versions, err := getPages[AppVersion](t.Context(), b.client, "/service-stack/"+service.ID+"/app-version")
	if err != nil || len(versions) != 1 || versions[0].Status != "ACTIVE" {
		t.Fatalf("engine convergence allocated wrong versions: %+v %v", versions, err)
	}
}
