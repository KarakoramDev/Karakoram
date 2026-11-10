// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestConfigNoticeBackoffAllowsDistinctTargetImmediately(t *testing.T) {
	var policy configNoticeBackoff
	now := time.Now()
	if !policy.allowed(now, "old-target", 1) {
		t.Fatal("initial notice blocked")
	}
	policy.completed(now, configNoticeResult{target: "old-target", unchanged: true})
	policy.completed(now.Add(time.Second), configNoticeResult{target: "old-target", unchanged: true})
	if policy.allowed(now.Add(2*time.Second), "old-target", 1) {
		t.Fatal("same target escaped repeated304 bound")
	}
	if policy.allowed(now.Add(2*time.Second), "", 1) {
		t.Fatal("legacy unknown target escaped bound")
	}
	if !policy.allowed(now.Add(2*time.Second), "new-peer-or-credential-target", 1) {
		t.Fatal("real change was delayed by unrelated304s")
	}
	policy.completed(now.Add(2*time.Second), configNoticeResult{target: "new-peer-or-credential-target", unchanged: true})
	if !policy.allowed(now.Add(3*time.Second), "new-peer-or-credential-target", 1) {
		t.Fatal("different targets shared the304 counter")
	}
	policy.completed(now.Add(3*time.Second), configNoticeResult{target: "new-peer-or-credential-target", unchanged: true})
	if policy.allowed(now.Add(4*time.Second), "new-peer-or-credential-target", 1) {
		t.Fatal("new target never acquired its own bound")
	}
}

func TestConfigNoticeBackoffResetsAfterPeriodicFullApply(t *testing.T) {
	for _, target := range []string{"known-target", ""} {
		t.Run(target, func(t *testing.T) {
			a, cfg, _ := fakeNode(t, "/bin/true")
			cfg.DesiredProcesses = []string{}
			cfg.PullRevision, cfg.ChangeRevision = "payload-A", "applied-A"
			if _, err := a.apply(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			var policy configNoticeBackoff
			now := time.Now()
			a.mu.RLock()
			before := a.configApplyGeneration
			a.mu.RUnlock()
			policy.allowed(now, target, before)
			policy.completed(now, configNoticeResult{target: target, unchanged: true})
			policy.completed(now.Add(time.Second), configNoticeResult{target: target, unchanged: true})
			if policy.allowed(now.Add(2*time.Second), target, before) {
				t.Fatal("two304s did not establish the bound")
			}
			newer := cfg
			newer.PullRevision, newer.ChangeRevision = "payload-B", "applied-B"
			newer.ConfigVersion++
			remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(newer) }))
			defer remote.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw})
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: remote.URL, Token: "token", CAPEM: ca})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			a.client = client
			// The ordinary minute pull uses this path, not heartbeat's completed callback.
			if _, err := a.reload(t.Context()); err != nil {
				t.Fatal(err)
			}
			a.mu.RLock()
			after := a.configApplyGeneration
			applied := a.pullRevision
			a.mu.RUnlock()
			if after == before || applied != "payload-B" {
				t.Fatal("periodic full apply did not publish its success")
			}
			if !policy.allowed(now.Add(2*time.Second), target, after) {
				t.Fatal("periodic full apply retained an obsolete304 bound")
			}
		})
	}
}
