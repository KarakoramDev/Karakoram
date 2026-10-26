package companion

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// partialResourceBackend models backends that preserve omitted settings instead
// of replacing the complete resource object, using the infrastructure fake for
// service storage and asynchronous operation receipts.
type partialResourceBackend struct {
	infra.Backend
	updates int
}

func (b *partialResourceBackend) SetResources(ctx context.Context, member string, desired infra.Resources) (infra.Op, error) {
	service, err := b.Backend.GetService(ctx, member)
	if err != nil {
		return infra.Op{}, err
	}
	merged := service.Resources
	if desired.CPUMode != "" {
		merged.CPUMode = desired.CPUMode
	}
	for _, bound := range []struct {
		field *float64
		value float64
	}{
		{&merged.MinCPU, desired.MinCPU}, {&merged.MaxCPU, desired.MaxCPU},
		{&merged.MinRAM, desired.MinRAM}, {&merged.MaxRAM, desired.MaxRAM},
		{&merged.MinDisk, desired.MinDisk}, {&merged.MaxDisk, desired.MaxDisk},
	} {
		if bound.value != 0 {
			*bound.field = bound.value
		}
	}
	if desired.StartCPU != nil {
		value := *desired.StartCPU
		merged.StartCPU = &value
	}
	b.updates++
	return b.Backend.SetResources(ctx, member, merged)
}

func TestRemovedResourceFieldsDoNotBlockCredentialRotation(t *testing.T) {
	f := setup(t)
	backend := &partialResourceBackend{Backend: f.backend}
	f.rc.Backend = backend
	start := 1.0
	f.rc.Cluster.Spec.DataMcp.Resources = &domain.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 2, MinRAM: 0.5, MaxRAM: 1, StartCPU: &start}
	f.converge(t)
	initial, err := MemberConfig(t.Context(), f.rc, "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	// Omitting previously managed CPU fields leaves backend values untouched,
	// while the newly managed RAM bounds and credentials must still reconcile.
	f.rc.Cluster.Spec.DataMcp.Resources = &domain.Resources{MinRAM: 0.25, MaxRAM: 0.5}
	f.provider.password = "rotated-after-resource-removal"
	f.converge(t)
	if backend.updates != 1 {
		t.Fatalf("partial update did not converge: %d updates", backend.updates)
	}
	service, err := f.backend.GetService(t.Context(), "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	if service.Resources.CPUMode != infra.CPUShared || service.Resources.MinCPU != 1 || service.Resources.MaxCPU != 2 || service.Resources.StartCPU == nil || *service.Resources.StartCPU != start {
		t.Fatal("backend did not retain omitted fields", service.Resources)
	}
	if service.Resources.MinRAM != 0.25 || service.Resources.MaxRAM != 0.5 {
		t.Fatal("managed fields not applied", service.Resources)
	}
	cfg, err := MemberConfig(t.Context(), f.rc, "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigVersion != initial.ConfigVersion+1 {
		t.Fatal("rotation config did not advance", cfg.ConfigVersion)
	}
	var spec companionspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	for _, process := range spec.Processes {
		if process.Password != f.provider.password {
			t.Fatal("rotation stranded behind resource update")
		}
	}
	if f.reloads.Load() < 2 {
		t.Fatal("rotation was not reloaded")
	}
	// Returning to controller defaults manages CPU/RAM again but does not reset
	// the omitted start reservation. Later passes must not repeatedly resize.
	f.rc.Cluster.Spec.DataMcp.Resources = nil
	f.provider.password = "rotated-after-defaults"
	f.converge(t)
	if backend.updates != 2 {
		t.Fatalf("defaults did not converge: %d updates", backend.updates)
	}
	after, err := MemberConfig(t.Context(), f.rc, "demomcp")
	if err != nil || after.ConfigVersion != cfg.ConfigVersion+1 {
		t.Fatal(after, err)
	}
	f.converge(t)
	if backend.updates != 2 {
		t.Fatal("stable partial resources triggered another resize")
	}
}

// The default companion resources must be accepted by real backends, which
// take only the infra CPU mode constants.
func TestDefaultResourcesUseBackendCPUMode(t *testing.T) {
	f := setup(t)
	f.rc.Cluster.Spec.DataMcp.Resources = nil
	f.converge(t)
	service, err := f.backend.GetService(t.Context(), "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	if r := service.Resources; r.CPUMode != infra.CPUShared || r.MinCPU != 1 || r.MaxCPU != 1 || r.MinRAM != 0.25 || r.MaxRAM != 0.5 {
		t.Fatal("unexpected default companion resources", r)
	}
}

func TestResourcesMatchExplicitManagement(t *testing.T) {
	observedStart := 1.0
	desiredStart := 1.0
	observed := domain.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 2, MinRAM: 0.25, MaxRAM: 0.5, MinDisk: 1, MaxDisk: 2, StartCPU: &observedStart}
	if !resourcesMatch(observed, domain.Resources{}) {
		t.Fatal("omitted fields treated as drift")
	}
	desired := observed
	desired.StartCPU = &desiredStart
	if !resourcesMatch(observed, desired) {
		t.Fatal("equal start reservations compared by pointer identity")
	}
	for _, test := range []struct {
		name   string
		change func(*domain.Resources)
	}{
		{"mode", func(r *domain.Resources) { r.CPUMode = infra.CPUDedicated }},
		{"minCPU", func(r *domain.Resources) { r.MinCPU = 2 }},
		{"maxCPU", func(r *domain.Resources) { r.MaxCPU = 3 }},
		{"minRAM", func(r *domain.Resources) { r.MinRAM = 0.5 }},
		{"maxRAM", func(r *domain.Resources) { r.MaxRAM = 1 }},
		{"minDisk", func(r *domain.Resources) { r.MinDisk = 2 }},
		{"maxDisk", func(r *domain.Resources) { r.MaxDisk = 3 }},
		{"startCPU", func(r *domain.Resources) { value := 2.0; r.StartCPU = &value }},
		{"invalidNegative", func(r *domain.Resources) { r.MinRAM = -1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := desired
			test.change(&changed)
			if resourcesMatch(observed, changed) {
				t.Fatal("managed change treated as converged")
			}
		})
	}
	observed.StartCPU = nil
	if resourcesMatch(observed, desired) {
		t.Fatal("missing managed start reservation treated as converged")
	}
}
