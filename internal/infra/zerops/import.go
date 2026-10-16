// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"gopkg.in/yaml.v3"
)

type importVertical struct {
	CPUMode  string   `yaml:"cpuMode,omitempty"`
	MinCPU   float64  `yaml:"minCpu,omitempty"`
	MaxCPU   float64  `yaml:"maxCpu,omitempty"`
	MinRAM   float64  `yaml:"minRam,omitempty"`
	MaxRAM   float64  `yaml:"maxRam,omitempty"`
	MinDisk  float64  `yaml:"minDisk,omitempty"`
	MaxDisk  float64  `yaml:"maxDisk,omitempty"`
	StartCPU *float64 `yaml:"startCpuCoreCount,omitempty"`
}
type importService struct {
	Hostname      string            `yaml:"hostname"`
	Type          string            `yaml:"type"`
	MinContainers int               `yaml:"minContainers,omitempty"`
	MaxContainers int               `yaml:"maxContainers,omitempty"`
	Vertical      importVertical    `yaml:"verticalAutoscaling"`
	Env           map[string]string `yaml:"envSecrets,omitempty"`
	Vars          map[string]string `yaml:"envVariables,omitempty"`
}
type importDocument struct {
	Services []importService `yaml:"services"`
}

func serviceType(kind infra.ServiceKind, base string) (string, error) {
	switch {
	case kind == infra.KindVolume:
		return "local-storage:single@1", nil
	case kind == infra.KindRuntime && base == infra.BaseUbuntu2404:
		return "ubuntu@24.04", nil
	default:
		return "", &infra.RejectedError{Message: "zerops: invalid or protected service type"}
	}
}

func buildImport(specs []infra.ServiceSpec) (importDocument, string, error) {
	doc := importDocument{Services: make([]importService, 0, len(specs))}
	seen := make(map[string]bool, len(specs))
	for _, s := range specs {
		if !hostnamePattern.MatchString(s.Hostname) || s.Hostname == "core" || seen[s.Hostname] {
			return doc, "", &infra.RejectedError{Message: "zerops: invalid or duplicate import hostname"}
		}
		seen[s.Hostname] = true
		typ, err := serviceType(s.Kind, s.Base)
		if err != nil {
			return doc, "", err
		}
		r := s.Resources
		v := importVertical{CPUMode: r.CPUMode, MinCPU: r.MinCPU, MaxCPU: r.MaxCPU, MinRAM: r.MinRAM, MaxRAM: r.MaxRAM, MinDisk: r.MinDisk, MaxDisk: r.MaxDisk, StartCPU: r.StartCPU}
		minCount, maxCount := s.MinContainers, s.MaxContainers
		if s.Kind == infra.KindVolume {
			v = importVertical{MinDisk: max(r.MinDisk, 5), MaxDisk: max(r.MaxDisk, max(r.MinDisk, 5))}
			minCount, maxCount = 0, 0
		} else {
			if minCount == 0 {
				minCount = 1
			}
			if maxCount == 0 {
				maxCount = minCount
			}
			if v.CPUMode == "" {
				v.CPUMode = "SHARED"
			}
		}
		if minCount < 0 || maxCount < minCount {
			return doc, "", &infra.RejectedError{Message: "zerops: invalid container bounds"}
		}
		for k := range s.Vars {
			if _, dup := s.Env[k]; dup {
				return doc, "", &infra.RejectedError{Message: "zerops: variable declared both sensitive and non-sensitive"}
			}
		}
		doc.Services = append(doc.Services, importService{s.Hostname, typ, minCount, maxCount, v, s.Env, s.Vars})
	}
	data, err := yaml.Marshal(doc)
	if err != nil {
		return doc, "", &infra.RejectedError{Message: "zerops: cannot encode import YAML"}
	}
	return doc, string(data), nil
}
