// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// A failed legacy platform import can still create both services, leaving Local
// Storage without ownership variables and the runtime with only legacy evidence.
type partialImportInfra struct {
	*fakeInfra
	importFailed bool
}

func (f *partialImportInfra) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	op, err := f.fakeInfra.ImportServices(ctx, specs)
	if err != nil {
		return op, err
	}
	for _, spec := range specs {
		service := f.services[spec.Hostname]
		service.ID = spec.Hostname + "-id"
		delete(service.Ownership, engine.OwnerVariable)
		op.Services[spec.Hostname] = service.ID
		service.Status = infra.StatusReadyToDeploy
		if spec.Kind == infra.KindVolume {
			service.Ownership = nil
			service.Status = infra.StatusCreating
		}
	}
	if !f.importFailed {
		f.importFailed = true
		return infra.Op{}, errors.New("platform import failed after creating services")
	}
	return op, nil
}

func failedMemberImport(t *testing.T) (*Provider, *engine.Context, *fakeInfra) {
	t.Helper()
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	rc.Backend = &partialImportInfra{fakeInfra: f}
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal("checkpoint import batch", err)
	}
	if _, err := p.Sync(context.Background(), rc); err == nil {
		t.Fatal("expected partial import failure")
	}
	s := state(t, rc)
	if len(s.Members) != 1 || s.Retry == nil || s.Retry.Action != "import" || s.Pending != nil {
		t.Fatalf("missing failed import checkpoint: %+v", s)
	}
	m := s.Members[0]
	if m.Stage != "planned" || m.ServiceID != "" || m.VolumeID != "" {
		t.Fatalf("failed import unexpectedly recorded services: %+v", m)
	}
	runtime, volume := f.services[m.Name], f.services[m.Volume]
	if !engine.MountsVolume(rc.Backend, runtime, m.Volume) || volume == nil || len(volume.Ownership) != 0 {
		t.Fatal("partial import did not preserve runtime mount evidence and an untagged volume")
	}
	return p, rc, f
}

func TestFailedImportRetryAdoptsUntaggedVolume(t *testing.T) {
	p, rc, f := failedMemberImport(t)
	s := state(t, rc)
	m := s.Members[0]
	runtimeID, volumeID := f.services[m.Name].ID, f.services[m.Volume].ID
	rc.Clock.(*testClock).at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s = state(t, rc)
	m = s.Members[0]
	if m.Stage != "imported" || m.ServiceID != runtimeID || m.VolumeID != volumeID || s.Retry != nil || s.Pending != nil {
		t.Fatalf("retry did not checkpoint adopted services: %+v", s)
	}
	// The partial import fixture intentionally leaves storage CREATING.
	// Complete its platform transition before expecting database bootstrap.
	f.services[m.Volume].Status = infra.StatusActive
	// Continue from the serialized checkpoint rather than an in-memory member.
	rc.Cluster = cloneCluster(t, rc.Cluster)
	ready(t, p, rc)
	if len(f.imports) != 1 || f.services[m.Name].ID != runtimeID || f.services[m.Volume].ID != volumeID {
		t.Fatal("create retry replaced or reimported adopted services")
	}
}

func TestFailedImportCleanupAdoptsAndDeletesUntaggedVolume(t *testing.T) {
	p, rc, f := failedMemberImport(t)
	m := state(t, rc).Members[0]
	runtimeID, volumeID := f.services[m.Name].ID, f.services[m.Volume].ID
	f.log.clear()
	done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true})
	if err != nil || done {
		t.Fatalf("initial cleanup: done=%v err=%v", done, err)
	}
	s := state(t, rc)
	m = s.Members[0]
	if m.ServiceID != runtimeID || m.VolumeID != volumeID || s.Retry != nil || f.services[m.Name] == nil || f.services[m.Volume] == nil {
		t.Fatalf("cleanup lost volume evidence before runtime deletion: %+v", s)
	}
	rc.Cluster = cloneCluster(t, rc.Cluster)
	for range 20 {
		done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done || f.services[m.Name] != nil || f.services[m.Volume] != nil {
		t.Fatalf("cleanup did not delete adopted services: done=%v state=%s", done, rc.Cluster.Internal)
	}
	assertBefore(t, f.log.snapshot(), "infra:"+m.Name+":delete", "infra:"+m.Volume+":delete")
}

func TestUntaggedVolumeRequiresVerifiedRuntimeMount(t *testing.T) {
	for _, stage := range []string{"planned", "imported", "ready"} {
		for _, evidence := range []string{"absent-runtime", "missing-token", "wrong-token", "wrong-cluster", "wrong-member", "missing-mount", "wrong-mount", "recorded-runtime-only"} {
			t.Run(stage+"/"+evidence, func(t *testing.T) {
				p, rc, f := failedMemberImport(t)
				s := state(t, rc)
				m := s.Members[0]
				m.Stage = stage
				runtime := f.services[m.Name]
				switch evidence {
				case "absent-runtime":
					delete(f.services, m.Name)
				case "missing-token":
					delete(runtime.Ownership, "KARAKORAM_AGENT_TOKEN")
				case "wrong-token":
					runtime.Ownership["KARAKORAM_AGENT_TOKEN"] = "another-token"
				case "wrong-cluster":
					runtime.Ownership["KARAKORAM_CLUSTER"] = "another-cluster"
				case "wrong-member":
					runtime.Ownership["KARAKORAM_MEMBER"] = "another-member"
				case "missing-mount":
					delete(runtime.Ownership, "KARAKORAM_DATA_DIR")
				case "wrong-mount":
					runtime.Ownership[engine.DataDirVariable] = rc.Backend.Layout().VolumePath("another-volume")
				case "recorded-runtime-only":
					m.ServiceID = runtime.ID
					delete(runtime.Ownership, "KARAKORAM_AGENT_TOKEN")
				}
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				rc.Clock.(*testClock).at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
				before := f.mutations
				f.log.clear()
				if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("create adopted unverifiable services: %v", err)
				}
				if done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true}); done || !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("cleanup accepted unverifiable services: done=%v err=%v", done, err)
				}
				if f.mutations != before || len(f.log.snapshot()) != 0 || state(t, rc).Members[0].VolumeID != "" {
					t.Fatal("unverifiable volume was adopted or services were mutated")
				}
			})
		}
	}
}

func TestFailedImportCleanupCheckpointsOwnershipBeforeDeletingRuntime(t *testing.T) {
	p, rc, f := failedMemberImport(t)
	m := state(t, rc).Members[0]
	runtimeID, volumeID := f.services[m.Name].ID, f.services[m.Volume].ID
	f.log.clear()
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
	if err != nil || done {
		t.Fatal(done, err)
	}
	m = state(t, rc).Members[0]
	if m.ServiceID != runtimeID || m.VolumeID != volumeID || f.services[m.Name] == nil || f.services[m.Volume] == nil {
		t.Fatal("runtime deleted before returning its adopted ownership checkpoint", m, f.log.snapshot())
	}
	rc.Cluster = cloneCluster(t, rc.Cluster)
	done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
	if err != nil || done || f.services[m.Name] != nil || f.services[m.Volume] == nil {
		t.Fatal("checkpointed cleanup did not delete only the runtime", done, err, f.log.snapshot())
	}
}

type volumeLossObservationInfra struct {
	*fakeInfra
	name string
	err  error
}

func (f *volumeLossObservationInfra) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := f.fakeInfra.GetService(ctx, name)
	if name == f.name && service != nil {
		return service, f.err
	}
	return service, err
}

func TestOwnershipToleratesOnlyRecordedMatchingVolumeLoss(t *testing.T) {
	for _, kind := range []string{"runtime", "volume"} {
		for _, observation := range []string{"matched", "unclassified", "wrong-volume", "wrong-id", "generic", "foreign-service"} {
			t.Run(kind+"/"+observation, func(t *testing.T) {
				p, rc, f, _ := setup(t)
				ready(t, p, rc)
				m := state(t, rc).find("demom0")
				runtimeID, volumeID := m.ServiceID, m.VolumeID
				m.RuntimeRecovery.VolumeLost = observation != "unclassified"
				lost := &infra.VolumeLostError{Volume: m.Volume, ServiceID: m.VolumeID, Message: "original physical volume is absent"}
				var err error = lost
				switch observation {
				case "wrong-volume":
					lost.Volume = "different-volume"
				case "wrong-id":
					lost.ServiceID = "different-incarnation"
				case "generic":
					err = errors.New("temporary platform observation failure")
				}
				name := m.Name
				if kind == "volume" {
					name = m.Volume
				}
				if observation == "foreign-service" {
					f.services[name].ID = "foreign-incarnation"
					f.services[name].Ownership = nil
				}
				rc.Backend = &volumeLossObservationInfra{fakeInfra: f, name: name, err: err}
				err = verifyMemberServices(t.Context(), rc, m)
				if observation == "matched" {
					if err != nil {
						t.Fatalf("confirmed loss blocked existing recovery journal: %v", err)
					}
				} else if err == nil {
					t.Fatal("unmatched, unclassified or foreign loss bypassed ownership")
				}
				if m.ServiceID != runtimeID || m.VolumeID != volumeID {
					t.Fatal("loss observation adopted a new runtime or volume incarnation")
				}
			})
		}
	}
}

func TestDepartureUsesConfirmedLostIncarnationWithoutAgent(t *testing.T) {
	for _, kind := range []string{"runtime", "volume"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			m := state(t, rc).find("demom0")
			m.RuntimeRecovery.VolumeLost = true
			name := m.Name
			if kind == "volume" {
				name = m.Volume
			}
			rc.Backend = &volumeLossObservationInfra{fakeInfra: f, name: name, err: &infra.VolumeLostError{Volume: m.Volume, ServiceID: m.VolumeID}}
			agents := &lifecycleAgents{fakeAgents: a}
			rc.Agents = agents
			lost, err := p.departureUnavailable(t.Context(), rc, m)
			if err != nil || !lost || len(agents.dials) != 0 {
				t.Fatalf("confirmed physical loss blocked native removal guards: lost=%v err=%v dials=%v", lost, err, agents.dials)
			}
			m.RuntimeRecovery.VolumeLost = false
			if _, err = p.departureUnavailable(t.Context(), rc, m); err == nil {
				t.Fatal("unclassified physical-loss observation was suppressed")
			}
		})
	}
}

func TestCleanupConfirmedPhysicalLossDeletesButCannotRetainData(t *testing.T) {
	for _, kind := range []string{"runtime", "volume"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			m := s.find("demom0")
			m.RuntimeRecovery.VolumeLost = true
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			name := m.Name
			if kind == "volume" {
				name = m.Volume
			}
			rc.Backend = &volumeLossObservationInfra{fakeInfra: f, name: name, err: &infra.VolumeLostError{Volume: m.Volume, ServiceID: m.VolumeID}}
			before := f.mutations
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
			if done || err == nil || f.mutations != before {
				t.Fatalf("suspension claimed to preserve missing data: done=%v err=%v mutations=%d", done, err, f.mutations-before)
			}
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepBackups: true})
			if len(f.services) != 0 {
				t.Fatalf("confirmed physical loss prevented destructive cleanup: %+v", f.services)
			}
		})
	}
}
