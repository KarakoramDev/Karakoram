// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"gopkg.in/yaml.v3"
)

var zeropsLayout = infra.Layout{AppDir: "/var/www", VolumeRoot: "/mnt", User: "zerops", PrivateAddressFamily: infra.AddressFamilyIPv6}
var zeropsCapabilities = infra.Capabilities{PublicTCP: true, TCPAllowlist: true, PublicHTTPS: true, DiskGrowth: true, VolumeQuota: true, Logs: true, ScopedTokens: true, DedicatedCPU: true, HorizontalScaling: true}

// RenderYAML translates a neutral runtime description into a Zerops deployment.
func RenderYAML(hostname string, layout infra.Layout, binary string, spec infra.RuntimeSpec) (string, error) {
	base, err := serviceType(infra.KindRuntime, spec.Base)
	if err != nil {
		return "", err
	}
	ports := make([]map[string]any, 0, len(spec.Ports))
	for _, p := range spec.Ports {
		port := map[string]any{"port": p.Port, "protocol": p.Protocol}
		if p.HTTP {
			port["httpSupport"] = true
		}
		ports = append(ports, port)
	}
	deploy := map[string]any{"temporaryShutdown": spec.StopBeforeStart}
	if r := spec.Readiness; r != nil {
		deploy["readinessCheck"] = map[string]any{
			"exec":           map[string]any{"command": path.Join(layout.AppDir, binary) + " " + strings.Join(r.Args, " ")},
			"failureTimeout": fmt.Sprintf("%ds", r.FailureTimeout/time.Second),
			"retryPeriod":    fmt.Sprintf("%ds", r.RetryPeriod/time.Second),
		}
	}
	run := map[string]any{"base": base, "ports": ports, "envVariables": spec.Env, "start": "./" + binary + " " + strings.Join(spec.Args, " ")}
	if len(spec.Prepare) > 0 {
		run["prepareCommands"] = spec.Prepare
	}
	if spec.Volume != "" {
		run["volume"] = map[string]any{"hostname": spec.Volume}
	}
	data, err := yaml.Marshal(map[string]any{"zerops": []any{map[string]any{"setup": hostname, "deploy": deploy, "run": run}}})
	return string(data), err
}
