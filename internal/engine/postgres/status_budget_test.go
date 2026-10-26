// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestRecoveryGenericStatusBudgets(t *testing.T) {
	for _, scenario := range []string{"upgrade-offline", "retirement-fallback", "resource-recovery", "certificate-controls", "certificate-acknowledgement", "proxy-primary", "proxy-clock", "proxy-native"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.data()[0]
			if scenario == "proxy-clock" || scenario == "proxy-native" {
				p, rc, backend, s, _, _ = proxyClockFixture(t)
				m = s.data()[0]
			}
			var genericReads, blockedReads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				block := false
				switch r.URL.Path {
				case "/v1/status":
					read := genericReads.Add(1)
					block = scenario != "proxy-native" && (scenario != "certificate-acknowledgement" || read == 2)
				case "/v1/postgres/status":
					block = scenario == "proxy-native" && name == "demopx"
				}
				if block {
					blockedReads.Add(1)
					<-r.Context().Done()
					return
				}
				switch r.URL.Path {
				case "/v1/status":
					_ = json.NewEncoder(w).Encode(agentapi.Status{})
				case "/v1/postgres/status":
					status := pgspec.StatusResponse{Running: true, Role: "primary", Leader: name, EtcdHealthy: true}
					if name == "demopx" {
						status = pgspec.StatusResponse{Role: "proxy", ProxyBackends: []pgspec.ProxyBackend{{Host: "demop0", Diagnostics: &pgspec.ProxyDiagnostics{Connect: &pgspec.ProxyMonitorError{TimeUnixMicro: rc.Clock.Now().Add(8 * time.Hour).UnixMicro()}}}}}
					}
					_ = json.NewEncoder(w).Encode(status)
				default:
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
				}
			}))
			defer server.Close()
			rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationTimeout+2*time.Second)
			defer cancel()
			start := time.Now()
			switch scenario {
			case "upgrade-offline":
				m.Offline = true
				_, _ = awaitUpgradeOffline(ctx, rc, m)
			case "retirement-fallback":
				_, _ = retirementRuntimeFallback(ctx, rc, s, m, errors.New("native status unavailable"))
			case "resource-recovery":
				m.Offline = true
				s.Rolling = &rollingState{}
				s.Members = []*member{m}
				rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
				backend.services[m.Name].Resources = m.Resources
				_, _ = p.reconcileRecoveryResources(ctx, rc, s)
			case "certificate-controls", "certificate-acknowledgement":
				s.Members = []*member{m}
				m.TLSReloadPending, m.TLSPending = true, true
				_, _ = p.recoverNativeCertificates(ctx, rc, s)
			case "proxy-primary":
				s.Members = []*member{m}
				_ = healthyProxyRecoveryPrimary(ctx, rc, s)
			case "proxy-clock", "proxy-native":
				_, _ = p.recoverProxyClock(ctx, rc, s)
			}
			if got := blockedReads.Load(); got != 1 {
				t.Fatalf("blocked reads=%d; want one to exercise %s", got, scenario)
			}
			if elapsed := time.Since(start); elapsed < engine.ObservationTimeout-250*time.Millisecond || elapsed > engine.ObservationTimeout+time.Second {
				t.Fatalf("status read took %s; want %s observation budget", elapsed, engine.ObservationTimeout)
			}
		})
	}
}
