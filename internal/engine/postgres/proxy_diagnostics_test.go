// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestProxyFailureSummaryBoundsUntrustedFields(t *testing.T) {
	var backends []pgspec.ProxyBackend
	for range 50 {
		backends = append(backends, pgspec.ProxyBackend{Host: strings.Repeat("x", 100) + "\nprivate", Hostgroup: 10, Status: "private-native-message", Diagnostics: &pgspec.ProxyDiagnostics{Connect: &pgspec.ProxyMonitorError{Class: "password=private", TimeUnixMicro: 1792300090000000}, ConnectionErrors: 17, PoolObserved: true}})
	}
	text := proxyFailureSummary("proxy", backends)
	if len(text) > 1500 || strings.Contains(text, "private") || !strings.Contains(text, "connect=other@") || !strings.Contains(text, "ConnERR=17") {
		t.Fatalf("unsafe summary: %s", text)
	}
	legacy := proxyFailureSummary("proxy", []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "SHUNNED"}})
	if !strings.Contains(legacy, "SHUNNED") || strings.Contains(legacy, "ConnERR") {
		t.Fatalf("legacy compatibility: %s", legacy)
	}
}
