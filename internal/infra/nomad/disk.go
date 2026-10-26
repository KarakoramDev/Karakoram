// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// maxEphemeralDiskMB bounds operator scratch reservations to 64 GiB. This is
// not a placement guarantee; node capacity and persistent data remain separate.
const maxEphemeralDiskMB = 64 * 1024

// allocationDiskMB includes the private rootfs, engine installation, package
// caches and both downloaded and extracted artifacts, not persistent data.
// Freeze this at Deploy: changing defaults or target settings must not rewrite
// an existing member's job merely because the control plane was upgraded.
func (b *Backend) allocationDiskMB(spec infra.RuntimeSpec) int {
	size := 3072 // Unknown/custom native preparation: conservative fallback.
	if len(spec.Args) > 0 && spec.Args[0] == "relay" {
		size = 1024
	} else {
		switch spec.Env["KARAKORAM_ENGINE"] {
		case "rabbitmq":
			size = 2048
		case "postgres":
			if spec.Volume == "" {
				size = 1024 // ProxySQL; data nodes retain the larger budget.
			}
		case "elasticsearch":
			size = 4096
			for _, p := range spec.Ports {
				switch p.Port {
				case 8200: // APM server.
					size = 1024
				case 5044: // Logstash.
					size = 3072
				}
			}
		case "supabase":
			size = 2048
			for _, p := range spec.Ports {
				switch p.Port {
				case 3002: // Studio, including its Node.js dependency tree.
					size = 4096
				case 4000, 9000: // Realtime/analytics and edge runtime.
					size = 3072
				}
			}
		}
	}
	// The operator can raise, but not silently undercut, component budgets.
	minimum, _ := strconv.Atoi(b.target.Config["ephemeralDiskMB"])
	return max(size, minimum)
}
