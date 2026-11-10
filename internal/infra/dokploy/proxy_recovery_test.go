// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// This is the real store mutation / serialized reconciler / PG / Dokploy path.
// Native agents are substituted; no generation is incremented by the test.
func TestOrdinaryProxyChangedTargetRecovery(t *testing.T) {
	for _, action := range []string{"count", "resources"} {
		t.Run(action, func(t *testing.T) {
			p := postgres.New(postgres.Options{})
			rc, f, b := providerBudgetContext(t, p)
			rc.Artifact = budgetArtifact(renderTestArtifact(t))
			rc.Cluster.Spec.Topology.Type = "single"
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			createRuntime := func(name, volume string, replicas int, resources infra.Resources, marked bool) string {
				spec := runtimeSpec(name, volume)
				spec.MinContainers, spec.MaxContainers, spec.Resources = replicas, replicas, resources
				if volume != "" {
					disk := volumeSpec(volume)
					disk.Resources.MinDisk, disk.Resources.MaxDisk = resources.MinDisk, resources.MaxDisk
					importFake(t, b, disk)
				}
				imported := importFake(t, b, spec)
				deployment := testDeployment(t, volume)
				if marked {
					deployment.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
				}
				receipt, err := b.Deploy(t.Context(), name, deployment)
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, receipt)
				return imported.Services[name]
			}
			dataID := createRuntime("samplep0", "samplep0v", 1, rc.Cluster.Spec.Components.Engine.Resources, false)
			proxyID := createRuntime("samplepx", "", 2, rc.Cluster.Spec.Components.Extra["proxy"].Resources, true)
			volume, volumeErr := b.GetService(t.Context(), "samplep0v")
			if volumeErr != nil || volume == nil {
				t.Fatalf("missing imported volume: %v", volumeErr)
			}
			dataResources, proxyResources := fakeMeta(t, f, "samplep0").Resources, fakeMeta(t, f, "samplepx").Resources
			rc.Cluster.Spec.Components.Engine.Resources = dataResources
			proxy := rc.Cluster.Spec.Components.Extra["proxy"]
			proxy.Replicas, proxy.Resources = 2, proxyResources
			rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			st := rc.Store.(*store.Store)
			if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			current, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster = current
			providerBudgetState(t, rc, "postgres", map[string]any{"uid": "sample-identity", "configVersion": 1, "topology": "single", "formed": true, "members": []map[string]any{
				{"name": "samplep0", "role": "data", "stage": "joined", "serviceId": dataID, "volume": "samplep0v", "volumeId": volume.ID, "disk": volume.Resources, "version": current.Spec.Version, "replicas": 1, "configVersion": 1, "resources": dataResources},
				{"name": "samplepx", "role": "proxy", "stage": "joined", "serviceId": proxyID, "version": current.Spec.Version, "replicas": 2, "configVersion": 1, "resources": proxyResources},
			}})
			if err := st.UpdateClusterState(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			var holdAcknowledgement atomic.Bool
			version := current.Spec.Version
			providerBudgetServer(t, rc, func(w http.ResponseWriter, r *http.Request) {
				member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				switch r.URL.Path {
				case "/v1/status":
					revision := int64(1000000)
					if holdAcknowledgement.Load() && member == "samplepx" {
						revision = 1
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: revision})
				case "/v1/postgres/status":
					s := pgspec.StatusResponse{Running: true, Role: "standalone", Version: version, CredentialReceipts: true, SlotStatusKnown: true, HealthCapabilities: &pgspec.HealthCapabilities{ReplicationSlots: true}}
					if member == "samplepx" {
						s.Role = "proxy"
						s.ProxyBackends = []pgspec.ProxyBackend{{Host: "samplep0", Hostgroup: pgspec.ProxyWriterGroup, Status: "ONLINE"}}
					}
					_ = json.NewEncoder(w).Encode(s)
				case "/v1/proxy-serving":
					f.mu.Lock()
					taskID := "incumbent"
					for _, task := range f.apps[proxyID].tasks {
						if strings.HasPrefix(strings.ToLower(fakeString(task, "currentState")), "starting") {
							taskID = fakeString(task, "containerId")
							break
						}
					}
					f.mu.Unlock()
					_ = json.NewEncoder(w).Encode(agentapi.ProxyServingProof{Member: member, TaskID: taskID, ConfigVersion: 1000000, Ready: true})
				default:
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
				}
			})
			registry, err := engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			manager, err := reconciler.New(reconciler.Options{Store: st, Registry: registry, Context: *rc, BackoffMin: time.Millisecond, BackoffMax: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			stopped := make(chan error, 1)
			go func() { stopped <- manager.Run(ctx) }()
			defer func() { cancel(); <-stopped }()
			wait := func(predicate func(*model.Cluster) bool) *model.Cluster {
				t.Helper()
				deadline := time.NewTimer(20 * time.Second)
				defer deadline.Stop()
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					manager.Trigger(current.Ref())
					c, err := st.GetCluster(t.Context(), current.Ref())
					if err != nil {
						t.Fatal(err)
					}
					if predicate(c) {
						return c
					}
					select {
					case <-deadline.C:
						t.Fatalf("did not converge: status=%+v state=%s", c.Status, c.Internal)
					case <-ticker.C:
					}
				}
			}
			current = wait(func(c *model.Cluster) bool {
				return c.Status.Phase == model.PhaseReady && c.Status.ObservedGeneration == c.Generation
			})
			good := current.Spec
			failedSpec := good
			failedSpec.Components.Extra = map[string]model.ComponentSpec{}
			for name, component := range good.Components.Extra {
				failedSpec.Components.Extra[name] = component
			}
			changed := failedSpec.Components.Extra["proxy"]
			if action == "count" {
				changed.Replicas = 3
			} else {
				changed.Resources.MinCPU++
				changed.Resources.MaxCPU++
			}
			failedSpec.Components.Extra["proxy"] = changed
			f.mu.Lock()
			f.apps[proxyID].neverHealthyProxy = true
			f.apps[proxyID].taskCapacity = 4
			f.mu.Unlock()
			failed := &model.Operation{Kind: model.OperationUpdate}
			if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), failed), current.Ref(), failedSpec); err != nil {
				t.Fatal(err)
			}
			previousGeneration := current.Status.ObservedGeneration
			current = wait(func(c *model.Cluster) bool {
				op, _ := st.GetOperation(t.Context(), failed.ID)
				return op != nil && op.Terminal()
			})
			op, err := st.GetOperation(t.Context(), failed.ID)
			if err != nil || op.Phase != model.PhaseFailed || op.CompletedAt == nil || op.Failures != 1 || !strings.Contains(op.Message, "manual recovery required") {
				t.Fatalf("not immediately failed: %+v %v", op, err)
			}
			if current.Status.ObservedGeneration != previousGeneration {
				t.Fatal("failed generation acknowledged")
			}
			first := fakeMeta(t, f, "samplepx").Pending
			if first == nil || !first.Failed {
				t.Fatal("missing retained failed receipt")
			}
			before := fakeCalls(f, "application.reload")
			// Ordinary observation is not a retry, even with healthy incumbents.
			for range 3 {
				_, _ = p.Status(t.Context(), &engine.Context{Cluster: current, Backend: b, Store: st, Secrets: st, Agents: rc.Agents, Clock: engine.RealClock{}})
			}
			if fakeCalls(f, "application.reload") != before {
				t.Fatal("status replayed failed rollout")
			}
			failedGeneration := current.Generation
			unchanged := &model.Operation{Kind: model.OperationUpdate}
			if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), unchanged), current.Ref(), failedSpec); err != nil {
				t.Fatal(err)
			}
			reads := fakeCalls(f, "application.one")
			// This is the manager entrypoint used by `clusters reconcile`:
			// it schedules fresh observation, never re-arms failed receipts.
			manager.ForceReconcile(current.Ref())
			current = wait(func(c *model.Cluster) bool { return fakeCalls(f, "application.one") > reads })
			retained, err := st.GetOperation(t.Context(), failed.ID)
			if err != nil || retained.Phase != model.PhaseFailed || current.Generation != failedGeneration ||
				fakeCalls(f, "application.reload") != before || !sameOperation(fakeMeta(t, f, "samplepx").Pending, *first) {
				t.Fatalf("identical apply or forced reconcile re-armed failed work: %+v %v", retained, err)
			}
			f.mu.Lock()
			f.apps[proxyID].neverHealthyProxy = false
			f.mu.Unlock()
			holdAcknowledgement.Store(true)
			recovery := &model.Operation{Kind: model.OperationUpdate}
			if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), recovery), current.Ref(), good); err != nil {
				t.Fatal(err)
			}
			current = wait(func(c *model.Cluster) bool {
				return c.Generation > op.TargetGeneration && strings.Contains(c.Status.Message, "configuration")
			})
			if fakeCalls(f, "application.reload") != before {
				t.Fatal("corrected target bypassed latest configuration acknowledgement")
			}
			holdAcknowledgement.Store(false)
			current = wait(func(c *model.Cluster) bool {
				op, _ := st.GetOperation(t.Context(), recovery.ID)
				return op != nil && op.Terminal()
			})
			recovered, err := st.GetOperation(t.Context(), recovery.ID)
			if err != nil || recovered.Phase != model.PhaseSucceeded || current.Status.ObservedGeneration != current.Generation {
				t.Fatalf("corrected target did not converge: %+v status=%+v %v", recovered, current.Status, err)
			}
			if fakeCalls(f, "application.reload") != before+1 {
				t.Fatal("recovery did not submit exactly one replacement")
			}
			if m := fakeMeta(t, f, "samplepx"); m.Pending != nil || m.ProxyAdmissionFailure != nil {
				t.Fatalf("successful replacement retained failure authority: %+v", m)
			}
		})
	}
}
