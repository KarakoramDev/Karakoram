// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestObsoleteConfigNoticeRechecksAppliedTarget(t *testing.T) {
	a, cfg, _ := fakeNode(t, "/bin/true")
	cfg.DesiredProcesses = []string{}
	cfg.PullRevision, cfg.ChangeRevision = "old-payload", "old"
	if _, err := a.apply(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	captured, release := make(chan struct{}), make(chan struct{})
	var gets atomic.Int32
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == agentapi.HeartbeatPath {
			close(captured)
			<-release
			json.NewEncoder(w).Encode(agentapi.HeartbeatResponse{Token: "token", IntervalMillis: 3000, ReloadConfig: true, ConfigRevision: "new"})
			return
		}
		gets.Add(1)
		w.WriteHeader(http.StatusNotModified)
	}))
	defer remote.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: remote.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a.client = client
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.heartbeatLoop(t.Context(), a.handler(), func(context.Context, time.Duration) bool { return false })
	}()
	<-captured
	newer := cfg
	newer.PullRevision, newer.ChangeRevision = "new-payload", "new"
	_, applyErr := a.apply(t.Context(), newer)
	close(release)
	<-done
	if applyErr != nil {
		t.Fatal(applyErr)
	}
	if gets.Load() != 0 {
		t.Fatal("obsolete in-flight heartbeat notice launched a redundant reload", gets.Load())
	}
}

func TestConfigNoticeUnchangedBackoffBoundsLegacyCP(t *testing.T) {
	var policy configNoticeBackoff
	now := time.Now()
	allowed := func(at time.Time) bool { return policy.allowed(at, "", 0) }
	if !allowed(now) || policy.completed(now, configNoticeResult{unchanged: true}) {
		t.Fatal("first304 was prematurely throttled/logged")
	}
	if !allowed(now.Add(3 * time.Second)) {
		t.Fatal("second304 was prevented")
	}
	if !policy.completed(now.Add(3*time.Second), configNoticeResult{unchanged: true}) {
		t.Fatal("second304 did not enable one-time warning")
	}
	if allowed(now.Add(62*time.Second)) || !allowed(now.Add(63*time.Second)) {
		t.Fatal("unchanged notices were not limited to60s")
	}
	if policy.completed(now.Add(63*time.Second), configNoticeResult{unchanged: true}) {
		t.Fatal("repeated304 warning logged more than once")
	}
	if allowed(now.Add(122*time.Second)) || !allowed(now.Add(123*time.Second)) {
		t.Fatal("third304 resumed the heartbeat-rate loop")
	}
	policy.completed(now.Add(124*time.Second), configNoticeResult{})
	if !allowed(now.Add(124 * time.Second)) {
		t.Fatal("a real full apply did not reset the304-only backoff")
	}
}
