// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"testing"
	"time"
)

func TestMaintenanceResourceChange(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, b, a := setup(t)
			ready(t, p, rc)
			old := ordered(state(t, rc))[0].Resources
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			b.log.clear()
			// A CPU-only PATCH reaches the gate on its first evaluation.
			rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
			result, err := p.Sync(t.Context(), rc)
			if err != nil || result.ConvergenceError != nil || result.RequeueAfter <= 0 {
				t.Fatal("first resource evaluation did not defer successfully", result, err)
			}
			waiting := false
			for _, condition := range rc.Cluster.Status.Conditions {
				waiting = waiting || condition.Type == "MaintenanceWindow" && condition.Reason == "WaitingForWindow"
			}
			if !waiting {
				t.Fatal("first CPU evaluation did not publish maintenance wait")
			}
			for _, member := range state(t, rc).Members {
				if member.Change != nil {
					t.Fatal("closed window persisted an unadmitted resource plan")
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady || status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatalf("deferred CPU change lost serving phase or advanced generation: %+v %v", status, err)
			}
			for range 8 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			for _, m := range state(t, rc).Members {
				if m.Resources != old || a.member(m.Name).sealed {
					t.Fatal("closed window disrupted member")
				}
			}
			found := false
			for _, c := range rc.Cluster.Status.Conditions {
				found = found || c.Type == "MaintenanceWindow" && c.Status == "False" && c.Reason == "WaitingForWindow"
			}
			if !found {
				t.Fatal("missing maintenance wait")
			}
			if release == "override" {
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
			} else {
				rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseUpdating || status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatalf("admitted CPU roll lacks updating phase: %+v %v", status, err)
			}
			syncUntil(t, p, rc, func(s *internalState) bool {
				for _, m := range s.Members {
					if m.Change != nil && m.Change.Started {
						return true
					}
				}
				return false
			})
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseUpdating || status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatalf("started CPU roll lacks updating phase: %+v %v", status, err)
			}
			// Inspect every persisted boundary, including the receipt-free gap
			// after a member finishes and before its successor is planned.
			gaps := 0
			for range 200 {
				result, err := p.Sync(t.Context(), rc)
				if err != nil || result.ConvergenceError != nil {
					t.Fatal(result, err)
				}
				s := state(t, rc)
				status, err = p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if status.ObservedGeneration == rc.Cluster.Generation {
					if status.Phase != domain.PhaseReady || s.RollingKind != "" || s.RollingGeneration != 0 {
						t.Fatalf("completed roll retained lifecycle: %+v rolling=%q", status, s.RollingKind)
					}
					break
				}
				if status.Phase != domain.PhaseUpdating {
					t.Fatalf("incomplete roll lost updating phase: %+v", status)
				}
				hasChange := false
				for _, m := range s.Members {
					hasChange = hasChange || m.Change != nil
				}
				if !hasChange {
					gaps++
					if result.RequeueAfter <= 0 || s.RollingKind != "resources" || s.RollingGeneration != rc.Cluster.Generation {
						t.Fatal("member boundary lost durable rolling intent", result, s.RollingKind)
					}
				}
			}
			if gaps < len(state(t, rc).Members) || status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatalf("roll did not traverse member gaps and converge: gaps=%d status=%+v", gaps, status)
			}
			for _, m := range state(t, rc).Members {
				if m.Resources != runtimeResources(rc.Cluster.Spec.Components.Engine.Resources) {
					t.Fatal("resource change did not proceed")
				}
			}
		})
	}
}

func TestMaintenanceSupersedesAdmittedRoll(t *testing.T) {
	for _, storage := range []string{"raft", "postgresql"} {
		for _, boundary := range []string{"last-member", "started-member"} {
			t.Run(storage+"/"+boundary, func(t *testing.T) {
				p, rc, _, a := setup(t)
				ready(t, p, rc)
				if storage == "postgresql" {
					ca, err := secret(t.Context(), rc, "tls/ca.crt")
					if err != nil {
						t.Fatal(err)
					}
					p.options.Bindings = &sealBinding{c: pgbinding.Connection{BindingID: "binding", Host: "postgres.internal", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca}, CredentialGeneration: 1}}
					rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
					rc.Cluster.Generation++
					ready(t, p, rc)
				}
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				rc.Cluster.Generation++
				rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
				admitted := rc.Cluster.Generation
				syncUntil(t, p, rc, func(s *internalState) bool {
					if s.RollingKind == "" {
						return false
					}
					for _, m := range s.Members {
						if boundary == "started-member" && m.Change != nil && m.Change.Started {
							return true
						}
						if boundary == "last-member" && (m.Change != nil || m.Resources != runtimeResources(rc.Cluster.Spec.Components.Engine.Resources)) {
							return false
						}
					}
					return boundary == "last-member"
				})
				status, err := p.Status(t.Context(), rc)
				if err != nil || status.Phase != domain.PhaseUpdating || status.ObservedGeneration >= admitted {
					t.Fatalf("admitted boundary lost updating: %+v %v", status, err)
				}
				rc.Cluster.Generation++
				rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
				// Reconstruct the provider with only durable receipts retained.
				p = New(p.options)
				a.p = p
				if boundary == "started-member" {
					syncUntil(t, p, rc, func(s *internalState) bool {
						for _, m := range s.Members {
							if m.Change != nil {
								return false
							}
						}
						return true
					})
					status, err = p.Status(t.Context(), rc)
					if err != nil || status.Phase != domain.PhaseUpdating || state(t, rc).RollingGeneration != admitted {
						t.Fatalf("supersession discarded unfinished admitted work: %+v %v", status, err)
					}
				}
				for range 3 {
					result, err := p.Sync(t.Context(), rc)
					if err != nil || result.ConvergenceError != nil || result.RequeueAfter <= 0 {
						t.Fatal(result, err)
					}
					s := state(t, rc)
					status, err = p.Status(t.Context(), rc)
					if err != nil || status.Phase != domain.PhaseReady || status.ObservedGeneration >= admitted || s.RollingKind != "" || s.RollingGeneration != 0 {
						t.Fatalf("new deferred generation inherited old roll: %+v rolling=%q/%d err=%v", status, s.RollingKind, s.RollingGeneration, err)
					}
					for _, m := range s.Members {
						if m.Change != nil {
							t.Fatal("superseding closed-window generation created a receipt")
						}
					}
				}
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				status, err = p.Status(t.Context(), rc)
				if err != nil || status.Phase != domain.PhaseUpdating || state(t, rc).RollingGeneration != rc.Cluster.Generation {
					t.Fatalf("newly admitted generation lost its roll: %+v %v", status, err)
				}
				ready(t, p, rc)
				status, err = p.Status(t.Context(), rc)
				s := state(t, rc)
				if err != nil || status.Phase != domain.PhaseReady || status.ObservedGeneration != rc.Cluster.Generation || s.RollingKind != "" || s.RollingGeneration != 0 {
					t.Fatalf("released generation did not complete: %+v %v", status, err)
				}
			})
		}
	}
}
