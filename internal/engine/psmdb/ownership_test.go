// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type partialImportInfra struct {
	*fakeInfra
	processFailure bool
}

func (f *partialImportInfra) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	op, err := f.fakeInfra.ImportServices(ctx, specs)
	if err != nil {
		return op, err
	}
	for _, spec := range specs {
		service := f.services[spec.Hostname]
		service.ID = spec.Hostname + "-id"
		if spec.Type == "local-storage:single@1" {
			service.Ownership = nil
		}
	}
	if f.processFailure {
		return op, nil
	}
	return infra.Op{}, errors.New("import failed after creating services")
}

func failedMemberImport(t *testing.T) (*Provider, *engine.Context, *fakeInfra) {
	t.Helper()
	p, rc, f, _ := setup(t)
	rc.Backend = &partialImportInfra{fakeInfra: f}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("expected partial import failure")
	}
	s := retryState(t, rc)
	m := s.Members[0]
	if m.Stage != "planned" || m.ServiceID != "" || m.VolumeID != "" || s.Pending != nil || s.Retry != nil {
		t.Fatal("unexpected partial import checkpoint", m, s.Pending, s.Retry)
	}
	if f.services[m.Name].Ownership["KARAKORAM_DATA_DIR"] != "/mnt/"+m.Volume || len(f.services[m.Volume].Ownership) != 0 {
		t.Fatal("missing runtime mount evidence or untagged volume")
	}
	return p, rc, f
}

func TestPartialImportRetryAdoptsUntaggedVolume(t *testing.T) {
	p, rc, f := failedMemberImport(t)
	var s *internalState
	before := f.mutations
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	m := s.Members[0]
	if m.Stage != "imported" || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID || s.Retry != nil || s.Pending != nil || f.mutations != before {
		t.Fatal("retry did not adopt services without reimporting", m, s.Retry, s.Pending)
	}
	if len(f.imports) != 1 {
		t.Fatal("adopted services were reimported")
	}
}

func TestPartialImportProcessRetryAdoptsUntaggedVolume(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Backend = &partialImportInfra{fakeInfra: f, processFailure: true}
	rc.Clock = &retryClock{at: time.Now()}
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	f.failed = true
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("failed import process was not reported")
	}
	s := retryState(t, rc)
	if s.Retry == nil || s.Retry.Action != "import" || s.Members[0].Stage != "planned" {
		t.Fatal("failed import process was not checkpointed", s)
	}
	f.failed = false
	rc.Clock.(*retryClock).at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
	before := f.mutations
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	m := retryState(t, rc).Members[0]
	if m.Stage != "imported" || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID || f.mutations != before || len(f.imports) != 1 {
		t.Fatal("process retry did not preserve adopted services", m, f.imports)
	}
}

func TestPartialImportCleanupCheckpointsThenDeletesUntaggedVolume(t *testing.T) {
	p, rc, f := failedMemberImport(t)
	m := retryState(t, rc).Members[0]
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
	if err != nil || done {
		t.Fatal(done, err)
	}
	m = retryState(t, rc).Members[0]
	if m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID || retryState(t, rc).Retry != nil {
		t.Fatal("ownership not checkpointed before runtime removal", m)
	}
	cluster := *rc.Cluster
	cluster.Internal = append([]byte(nil), cluster.Internal...)
	rc.Cluster = &cluster
	done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
	if err != nil || done || f.services[m.Name] != nil || f.services[m.Volume] == nil {
		t.Fatal("runtime must be removed before volume", done, err)
	}
	for range 20 {
		done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done || len(f.services) != 0 {
		t.Fatal("cleanup failed to remove adopted services", done, f.services)
	}
}

func TestUntaggedVolumeRequiresTokenVerifiedRuntimeMount(t *testing.T) {
	for _, stage := range []string{"planned", "imported", "ready", "deleting"} {
		for _, evidence := range []string{"valid", "absent-runtime", "missing-token", "wrong-token", "wrong-cluster", "wrong-member", "missing-mount", "wrong-mount", "mount-prefix", "recorded-runtime-only"} {
			t.Run(stage+"/"+evidence, func(t *testing.T) {
				p, rc, f := failedMemberImport(t)
				s := retryState(t, rc)
				m := s.Members[0]
				m.Stage = stage
				runtime := f.services[m.Name]
				switch evidence {
				case "absent-runtime":
					delete(f.services, m.Name)
				case "missing-token":
					delete(runtime.Ownership, "KARAKORAM_AGENT_TOKEN")
				case "wrong-token":
					runtime.Ownership["KARAKORAM_AGENT_TOKEN"] = "foreign-token"
				case "wrong-cluster":
					runtime.Ownership["KARAKORAM_CLUSTER"] = "other"
				case "wrong-member":
					runtime.Ownership["KARAKORAM_MEMBER"] = "other"
				case "missing-mount":
					delete(runtime.Ownership, "KARAKORAM_DATA_DIR")
				case "wrong-mount":
					runtime.Ownership["KARAKORAM_DATA_DIR"] = "/mnt/other"
				case "mount-prefix":
					runtime.Ownership["KARAKORAM_DATA_DIR"] += "/data"
				case "recorded-runtime-only":
					m.ServiceID = runtime.ID
					delete(runtime.Ownership, "KARAKORAM_AGENT_TOKEN")
				}
				if evidence == "valid" {
					if err := verifyMemberServices(t.Context(), rc, m); err != nil {
						t.Fatal(err)
					}
					if m.VolumeID != f.services[m.Volume].ID {
						t.Fatal("volume not adopted at stage", stage)
					}
					return
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				before := f.mutations
				if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrConflict) {
					t.Fatal("retry accepted foreign services", err)
				}
				if done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true}); done || !errors.Is(err, engine.ErrConflict) {
					t.Fatal("cleanup accepted foreign services", done, err)
				}
				if f.mutations != before || retryState(t, rc).Members[0].VolumeID != "" {
					t.Fatal("unverified volume adopted or mutated")
				}
			})
		}
	}
}

func TestRecordedVolumeMismatchRejectsVerifiedRuntimeMount(t *testing.T) {
	p, rc, f := failedMemberImport(t)
	s := retryState(t, rc)
	m := s.Members[0]
	m.VolumeID = "original-volume-id"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	before := f.mutations
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrConflict) {
		t.Fatal("runtime mount replaced a recorded volume identity", err)
	}
	if done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true}); done || !errors.Is(err, engine.ErrConflict) {
		t.Fatal("cleanup accepted a replacement volume", done, err)
	}
	if f.mutations != before || retryState(t, rc).Members[0].VolumeID != "original-volume-id" {
		t.Fatal("recorded volume identity was changed or replacement was mutated")
	}
}
