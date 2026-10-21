package companion

import "github.com/KarakoramDev/Karakoram/internal/domain"

// resourcesMatch compares only explicitly managed resource fields. Backends may
// preserve omitted bounds, CPU mode and start reservation during partial updates;
// their retained values are not drift. A nonzero bound (including an invalid
// negative value) remains managed so backend validation is not bypassed.
func resourcesMatch(observed, desired domain.Resources) bool {
	if desired.CPUMode != "" && observed.CPUMode != desired.CPUMode {
		return false
	}
	for _, bound := range [...]struct{ observed, desired float64 }{
		{observed.MinCPU, desired.MinCPU}, {observed.MaxCPU, desired.MaxCPU},
		{observed.MinRAM, desired.MinRAM}, {observed.MaxRAM, desired.MaxRAM},
		{observed.MinDisk, desired.MinDisk}, {observed.MaxDisk, desired.MaxDisk},
	} {
		if bound.desired != 0 && bound.observed != bound.desired {
			return false
		}
	}
	return desired.StartCPU == nil || (observed.StartCPU != nil && *observed.StartCPU == *desired.StartCPU)
}
