// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"fmt"
	"math"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ElasticsearchHeapMB computes an explicit equal minimum/maximum JVM heap from
// minimum RAM in GB (1024 MB). Dedicated masters use the same rule.
func ElasticsearchHeapMB(ramGB float64) int {
	fraction := 0.4
	if ramGB > 4 {
		fraction = 0.5
	}
	mb := math.Floor(ramGB*1024*fraction/64) * 64
	if math.IsNaN(mb) || mb < 512 {
		return 512
	}
	if mb > 31744 {
		return 31744
	}
	return int(mb)
}

// KibanaHeapMB computes Node.js' maximum old-space size, with a 512 MB minimum.
func KibanaHeapMB(ramGB float64) int { return heapMB(ramGB, 0.6, 512) }

// LogstashHeapMB computes an equal minimum/maximum JVM heap, minimum 384 MB.
func LogstashHeapMB(ramGB float64) int { return heapMB(ramGB, 0.5, 384) }

func heapMB(ramGB, fraction float64, minimum int) int {
	mb := math.Floor(ramGB * 1024 * fraction)
	if math.IsNaN(mb) || mb < float64(minimum) {
		return minimum
	}
	// Avoid a float-to-int overflow even for invalid, unbounded input. Resource
	// validation rejects non-finite RAM before these values reach an agent.
	if mb >= float64(math.MaxInt) {
		return math.MaxInt
	}
	return int(mb)
}

// DefaultResources returns defaults for spec component names (engine, master,
// kibana, logstash, apm). Unknown components return an error.
func DefaultResources(component string) (domain.Resources, error) {
	r := domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinDisk: 1, MaxDisk: 5}
	switch component {
	case "engine":
		r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk = 2, 2, 5, 20
	case "master":
		r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk = 1.5, 1.5, 5, 20
	case "kibana":
		r.MinRAM, r.MaxRAM = 1.5, 2
	case "logstash":
		r.MinRAM, r.MaxRAM = 1.5, 1.5
	case "apm":
		r.MinRAM, r.MaxRAM = 0.5, 0.5
	default:
		return domain.Resources{}, fmt.Errorf("unknown Elasticsearch component %q", component)
	}
	return r, nil
}

// ValidateMinimumRAM checks each spec component's minimum RAM requirement in GB.
func ValidateMinimumRAM(component string, ramGB float64) error {
	minimum := 1.0
	switch component {
	case "engine":
		minimum = 1.5
	case "master", "kibana", "logstash":
	case "apm":
		minimum = 0.25
	default:
		return fmt.Errorf("unknown Elasticsearch component %q", component)
	}
	if math.IsNaN(ramGB) || math.IsInf(ramGB, 0) || ramGB < minimum {
		return fmt.Errorf("%s minimum RAM must be finite and at least %g GB", component, minimum)
	}
	return nil
}
