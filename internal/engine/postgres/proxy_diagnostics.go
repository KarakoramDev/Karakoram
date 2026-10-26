// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func proxyFailureSummary(proxy string, backends []pgspec.ProxyBackend) string {
	var parts []string
	size := 0
	for _, b := range backends {
		if len(parts) == 4 {
			break
		}
		host := b.Host
		if len(host) > 64 {
			host = host[:64]
		}
		state := b.Status
		switch state {
		case "ONLINE", "SHUNNED", "OFFLINE_SOFT", "OFFLINE_HARD":
		default:
			state = "unknown"
		}
		detail := fmt.Sprintf("%q/hg%d=%s", host, b.Hostgroup, state)
		if d := b.Diagnostics; d != nil {
			if d.PoolObserved {
				detail += fmt.Sprintf(" ConnERR=%d", d.ConnectionErrors)
			}
			for _, check := range []struct {
				name   string
				sample *pgspec.ProxyMonitorError
			}{{"connect", d.Connect}, {"ping", d.Ping}, {"read_only", d.ReadOnly}} {
				if check.sample == nil || check.sample.TimeUnixMicro <= 0 {
					continue
				}
				class := check.sample.Class
				switch class {
				case "timeout", "authentication", "tls", "dns", "capacity", "refused", "connection", "other":
				default:
					class = "other"
				}
				detail += " " + check.name + "=" + class + "@" + time.UnixMicro(check.sample.TimeUnixMicro).UTC().Format(time.RFC3339)
			}
		}
		if size+len(detail) > 1400 {
			break
		}
		parts = append(parts, detail)
		size += len(detail)
	}
	if len(parts) == 0 {
		return ""
	}
	if len(proxy) > 64 {
		proxy = proxy[:64]
	}
	return fmt.Sprintf("%q: %s", proxy, strings.Join(parts, "; "))
}
