// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// failedImportInfra models a legacy import error after both services were
// created, with readable ownership secrets available only on the runtime.
type failedImportInfra struct{ *coreInfra }

func (f *failedImportInfra) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	op, err := f.coreInfra.ImportServices(ctx, specs)
	if err != nil {
		return op, err
	}
	for _, spec := range specs {
		service := f.services[spec.Hostname]
		if spec.Kind == infra.KindVolume {
			service.ID = "volume-id"
			service.Ownership = nil
		} else {
			service.ID = "runtime-id"
			delete(service.Ownership, engine.OwnerVariable)
		}
	}
	return op, errors.New("internalServerError")
}

func failedMemberImport(t *testing.T) (*Provider, *engine.Context, *coreInfra, *state) {
	t.Helper()
	p, rc, f, _ := coreSetup(t, 1)
	rc.Backend = &failedImportInfra{f}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("partial import failure was not returned")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members[0]
	if m.Stage != stagePlanned || m.ServiceID != "" || m.VolumeID != "" || s.Pending != nil || s.Retry == nil {
		t.Fatal("failed import unexpectedly recorded service ownership", m, s.Pending, s.Retry)
	}
	if f.services[m.Name] == nil || f.services[m.Volume] == nil || len(f.services[m.Volume].Ownership) != 0 {
		t.Fatal("partial import did not leave a runtime and untagged volume")
	}
	if got := f.services[m.Name].Ownership["KARAKORAM_DATA_DIR"]; got != f.Layout().VolumePath(m.Volume) {
		t.Fatalf("runtime import must identify its volume mount, got %q", got)
	}
	return p, rc, f, s
}

func TestPartialImportRetryAdoptsUntaggedVolume(t *testing.T) {
	p, rc, f, s := failedMemberImport(t)
	name, volume := s.Members[0].Name, s.Members[0].Volume
	before := f.mutations
	rc.Clock.(*coreClock).at = rc.Clock.Now().Add(time.Minute)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.find(name)
	if m.Stage != stageImported || m.ServiceID != f.services[name].ID || m.VolumeID != f.services[volume].ID || s.Pending != nil || s.Retry != nil || f.mutations != before {
		t.Fatal("retry did not persist ownership without reimporting", m, s.Pending, s.Retry, f.calls)
	}
	s = coreConverge(t, p, rc, f)
	if s.find(name).Stage != stageJoined || len(f.imports) != 1 {
		t.Fatal("adopted member did not finish provisioning", s.find(name), f.calls)
	}
}

func TestPartialImportCleanupAdoptsAndDeletesUntaggedVolume(t *testing.T) {
	p, rc, f, s := failedMemberImport(t)
	name, volume := s.Members[0].Name, s.Members[0].Volume
	before := len(f.calls)
	adopted := false
	for range 20 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if f.services[name] == nil && f.services[volume] != nil {
			m := s.find(name)
			if m.ServiceID != "runtime-id" || m.VolumeID != "volume-id" {
				t.Fatal("ownership was not persisted before runtime deletion", m)
			}
			adopted = true
		}
		if done {
			if !adopted || len(f.services) != 0 || !reflect.DeepEqual(f.calls[before:], []string{"delete:" + name, "delete:" + volume}) {
				t.Fatal("cleanup did not adopt and delete runtime before volume", f.calls[before:])
			}
			return
		}
	}
	t.Fatal("cleanup did not complete")
}

func TestVolumeAdoptionDoesNotDependOnMemberStage(t *testing.T) {
	_, rc, f, s := failedMemberImport(t)
	for _, stage := range []string{stagePlanned, stageImported, stageDeployed, stageJoined, stageDeleting} {
		t.Run(stage, func(t *testing.T) {
			m := *s.Members[0]
			m.Stage, m.ServiceID, m.VolumeID = stage, "", ""
			if err := verifyMemberServices(t.Context(), rc, &m); err != nil {
				t.Fatal(err)
			}
			if m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatal("verified service IDs were not adopted", m)
			}
		})
	}
}

func TestUntaggedVolumeRequiresTokenVerifiedRuntimeMount(t *testing.T) {
	for _, scenario := range []string{"runtime-absent", "wrong-token", "empty-token", "wrong-cluster", "wrong-member", "missing-mount", "wrong-mount", "mount-prefix"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, f, s := failedMemberImport(t)
			m := s.Members[0]
			runtime := f.services[m.Name]
			// Even a recorded runtime ID cannot replace token ownership as
			// evidence for adopting a previously unrecorded volume.
			m.ServiceID = runtime.ID
			switch scenario {
			case "runtime-absent":
				delete(f.services, m.Name)
			case "wrong-token":
				runtime.Ownership["KARAKORAM_AGENT_TOKEN"] = "foreign-token"
			case "empty-token":
				runtime.Ownership["KARAKORAM_AGENT_TOKEN"] = ""
			case "wrong-cluster":
				runtime.Ownership["KARAKORAM_CLUSTER"] = "other"
			case "wrong-member":
				runtime.Ownership["KARAKORAM_MEMBER"] = "other"
			case "missing-mount":
				delete(runtime.Ownership, "KARAKORAM_DATA_DIR")
			case "wrong-mount":
				runtime.Ownership["KARAKORAM_DATA_DIR"] = f.Layout().VolumePath("other")
			case "mount-prefix":
				runtime.Ownership["KARAKORAM_DATA_DIR"] = f.Layout().VolumePath(m.Volume) + "/data"
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			before := f.mutations
			rc.Clock.(*coreClock).at = rc.Clock.Now().Add(time.Minute)
			if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrConflict) {
				t.Fatal("retry accepted an unverifiable volume", err)
			}
			if done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true}); done || !errors.Is(err, engine.ErrConflict) {
				t.Fatal("cleanup accepted an unverifiable volume", done, err)
			}
			if f.mutations != before || f.services[m.Volume] == nil {
				t.Fatal("unverifiable services were mutated", f.calls)
			}
		})
	}
}
