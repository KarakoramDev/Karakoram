// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPostgresCleanupSupersedesFailedDokployBuild(t *testing.T) {
	for _, phase := range []string{"error", "cancelled", "running", "partial-error"} {
		for _, persistedFailure := range []bool{false, true} {
			if phase == "running" && persistedFailure {
				continue
			}
			t.Run(phase+"/persisted="+map[bool]string{false: "no", true: "yes"}[persistedFailure], func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				id := importFake(t, b, runtimeSpec("proxy", "")).Services["proxy"]
				deploy, err := b.Deploy(t.Context(), "proxy", testDeployment(t, ""))
				if err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				a := f.apps[id]
				a.holdDeployments = true
				if phase == "partial-error" {
					// The platform created tasks but the initial rollout never
					// became healthy, so metadata.Deployed is still false.
					f.apply(a)
					m, _ := parseMetadata(fakeString(a.row, "description"))
					m.Deployed = false
					a.row["description"] = m.description()
				}
				a.deployments[0].status = strings.TrimPrefix(phase, "partial-")
				if phase != "running" {
					a.deployments[0].finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
				}
				a.row["applicationStatus"] = strings.TrimPrefix(phase, "partial-")
				if phase != "partial-error" {
					delete(f.services, fakeString(a.row, "appName"))
					a.tasks = nil
				}
				if phase == "running" {
					m, _ := parseMetadata(fakeString(a.row, "description"))
					m.PortsPending = true
					a.row["description"] = m.description()
				}
				f.mu.Unlock()
				if phase == "partial-error" {
					if fakeMeta(t, f, "proxy").Deployed {
						t.Fatal("partial initial rollout incorrectly records a successful deployment")
					}
					f.mu.Lock()
					hasServiceAndTasks := f.services[fakeString(a.row, "appName")] != nil && len(a.tasks) > 0
					f.mu.Unlock()
					if !hasServiceAndTasks {
						t.Fatal("partial initial rollout must retain its service and tasks")
					}
				}
				if persistedFailure {
					state, err := b.OpStatus(t.Context(), deploy)
					if err != nil || !state.Failed || !fakeMeta(t, f, "proxy").Pending.Failed {
						t.Fatalf("failure not persisted: %+v %v", state, err)
					}
				}
				// Restart the adapter to exclude an in-memory recovery shortcut.
				b = fakeBackend(t, f, nil)
				internal, err := json.Marshal(map[string]any{
					"postgres": map[string]any{
						"members": []map[string]any{{"name": "proxy", "serviceId": id, "stage": "imported", "role": "proxy"}},
						"pending": map[string]any{"op": deploy, "action": "deploy", "member": "proxy"},
					},
				})
				if err != nil {
					t.Fatal(err)
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
				cluster := &model.Cluster{Name: "demo", Deleting: true, Internal: internal}
				p := postgres.New(postgres.Options{Store: st})
				p.ApplyDefaults(&cluster.Spec)
				rc := &engine.Context{Cluster: cluster, Backend: b, Store: st, Secrets: st}
				uploads, builds, reloads := fakeCalls(f, "application.dropDeployment"), fakeCalls(f, "application.deploy"), fakeCalls(f, "application.reload")
				if phase == "running" {
					for range 3 {
						done, err := p.Cleanup(t.Context(), rc, model.DeleteOptions{KeepBackups: true})
						if done || infra.RetryAt(err).IsZero() {
							t.Fatalf("active build must wait without consuming failure budget: done=%v err=%v", done, err)
						}
						if fakeCalls(f, "application.delete") != 0 || fakeCalls(f, "application.stop") != 0 {
							t.Fatal("mutation overtook active build")
						}
					}
					f.mu.Lock()
					a.deployments[0].status = "error"
					a.deployments[0].finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
					a.row["applicationStatus"] = "error"
					f.mu.Unlock()
				}
				for range 30 {
					done, err := p.Cleanup(t.Context(), rc, model.DeleteOptions{KeepBackups: true})
					if err != nil {
						t.Fatal(err)
					}
					if fakeCalls(f, "application.dropDeployment") != uploads || fakeCalls(f, "application.deploy") != builds || fakeCalls(f, "application.reload") != reloads {
						t.Fatal("cleanup submitted replacement deployment")
					}
					if done {
						service, err := b.GetService(t.Context(), "proxy")
						if err != nil || service != nil || fakeCalls(f, "application.delete") != 1 {
							t.Fatalf("failed application remains: %+v %v", service, err)
						}
						if phase == "partial-error" {
							f.mu.Lock()
							remainingService := f.services[fakeString(a.row, "appName")] != nil
							f.mu.Unlock()
							if remainingService || fakeCalls(f, "application.stop") != 1 {
								t.Fatal("never-successful partial rollout was not stopped and removed")
							}
						}
						return
					}
				}
				t.Fatal("failed build blocked supported engine cleanup")
			})
		}
	}
}

func TestCleanupObservationRequiresExactDokployIdentity(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
	deployFake(t, b, "runtime", "")
	ctx := infra.WithCleanupIntent(t.Context(), infra.CleanupIntent{FullDelete: true, Services: map[string]string{"runtime": id + "-stale"}})
	if _, err := b.GetService(ctx, "runtime"); err == nil {
		t.Fatal("cleanup observation accepted replaced identity")
	}
}

func TestPSMDBCleanupSupersedesFailedDokployBuild(t *testing.T) {
	for _, phase := range []string{"error", "running"} {
		t.Run(phase, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("router", "")).Services["router"]
			deploy, err := b.Deploy(t.Context(), "router", testDeployment(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a := f.apps[id]
			a.holdDeployments = true
			a.deployments[0].status = phase
			a.row["applicationStatus"] = phase
			m, _ := parseMetadata(fakeString(a.row, "description"))
			m.PortsPending = true
			a.row["description"] = m.description()
			f.mu.Unlock()
			internal, err := json.Marshal(map[string]any{
				"members":        []map[string]any{{"name": "router", "serviceId": id, "stage": "imported", "role": "mongos"}},
				"pending":        map[string]any{"op": deploy, "action": "deploy", "member": "router"},
				"cleanupBackups": true,
			})
			if err != nil {
				t.Fatal(err)
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
			cluster := &model.Cluster{Name: "demo", Deleting: true, Internal: internal}
			p := psmdb.New(psmdb.Options{Store: st})
			p.ApplyDefaults(&cluster.Spec)
			rc := &engine.Context{Cluster: cluster, Backend: b, Store: st, Secrets: st}
			// The reconciler supplies this shared observation intent for every
			// provider; PSMDB does not add PostgreSQL's exact-ID escalation intent.
			ctx := infra.WithCleanupIntent(t.Context(), infra.CleanupIntent{})
			uploads := fakeCalls(f, "application.dropDeployment")
			if phase == "running" {
				for range 3 {
					done, err := p.Cleanup(ctx, rc, model.DeleteOptions{KeepBackups: true})
					if done || infra.RetryAt(err).IsZero() {
						t.Fatalf("public-port cleanup must schedule an active receipt wait: done=%v err=%v", done, err)
					}
					m := fakeMeta(t, f, "router")
					if m.Desired != "deleting" || !m.PortsPending || fakeCalls(f, "application.stop") != 0 || fakeCalls(f, "application.delete") != 0 {
						t.Fatal("route withdrawal lost deletion intent or overtook the build")
					}
				}
				f.mu.Lock()
				a.deployments[0].status = "error"
				a.row["applicationStatus"] = "error"
				f.mu.Unlock()
			}
			for range 30 {
				done, err := p.Cleanup(ctx, rc, model.DeleteOptions{KeepBackups: true})
				if err != nil {
					t.Fatal(err)
				}
				if fakeCalls(f, "application.dropDeployment") != uploads || fakeCalls(f, "application.deploy") != 0 || fakeCalls(f, "application.reload") != 0 {
					t.Fatal("non-PostgreSQL cleanup submitted another deployment")
				}
				if done {
					service, err := b.GetService(t.Context(), "router")
					if err != nil || service != nil || fakeCalls(f, "application.delete") != 1 {
						t.Fatalf("PSMDB failed runtime remains: %+v %v", service, err)
					}
					return
				}
			}
			t.Fatal("non-PostgreSQL cleanup remained blocked by failed deployment")
		})
	}
}
