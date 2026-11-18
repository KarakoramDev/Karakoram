// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent/tombstone"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type stalledResolutionWriter struct {
	install func()
	calls   atomic.Int32
}

func (*stalledResolutionWriter) Kind() string                    { return "nomad-variable" }
func (*stalledResolutionWriter) Preflight(context.Context) error { return nil }
func (w *stalledResolutionWriter) Install(context.Context, []string) error {
	w.calls.Add(1)
	w.install()
	return nil
}

func boundedResolutionCall(t *testing.T, call func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); call() }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resolver work blocked the observation/configuration path")
	}
}

func TestResolutionTombstoneStalledWriterDoesNotBlockStatusOrPull(t *testing.T) {
	for _, stage := range []string{"install", "verify"} {
		t.Run(stage, func(t *testing.T) {
			a, c, e := fakeNode(t, "/bin/true")
			c.ConfigVersion, c.Maintenance, c.ResolutionTombstonesPresent = 1, true, true
			c.ResolutionTombstones = []string{"retired"}
			entered, release := make(chan struct{}), make(chan struct{})
			var once, unblock sync.Once
			defer unblock.Do(func() { close(release) })
			stall := func() { once.Do(func() { close(entered) }); <-release }
			writer := &stalledResolutionWriter{install: func() {}}
			a.resolutionWriter = writer
			a.resolutionVerify = func(context.Context, []string) error {
				if stage == "verify" {
					stall()
				}
				return nil
			}
			if stage == "install" {
				writer.install = stall
			}
			native := 0
			e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
			if _, err := a.apply(t.Context(), c); err == nil {
				t.Fatal("pending resolver work acknowledged configuration")
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("writer did not start")
			}
			// A newer empty set must be accepted for retry even while an old
			// writer ignores cancellation. Its proof cannot be inherited.
			c.ConfigVersion++
			c.ResolutionTombstones = nil
			cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(c)
			}))
			defer cp.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cp.Certificate().Raw})
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: cp.URL, Token: "fixture", CAPEM: ca})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			a.client = client
			handler := a.handler()
			for range 4 {
				boundedResolutionCall(t, func() {
					r := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
					r.Header.Set("Authorization", "Bearer "+a.o.Token)
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if w.Code != http.StatusOK {
						t.Errorf("status: %d", w.Code)
					}
				})
				boundedResolutionCall(t, func() {
					_, err := a.reload(t.Context())
					var retry *RetryableApplyError
					if !errors.As(err, &retry) {
						t.Errorf("pending pull: %v", err)
					}
				})
			}
			deadline := time.After(5 * time.Second)
			tick := time.NewTicker(5 * time.Millisecond)
			defer tick.Stop()
			for {
				proof := a.status(t.Context()).ResolutionTombstones
				if proof != nil && strings.Contains(proof.Reason, "deadline") && proof.SetHash == "" && proof.VerifiedAt.IsZero() {
					break
				}
				select {
				case <-deadline:
					t.Fatal("stalled verification did not publish its timeout")
				case <-tick.C:
				}
			}
			for range 4 {
				_ = a.refreshResolutionTombstones(t.Context())
			}
			if writer.calls.Load() != 1 || native != 0 {
				t.Fatal("stalled work accumulated flights or started native engine", writer.calls.Load(), native)
			}
			unblock.Do(func() { close(release) })
			retryTombstoneApply(t, a)
			proof := a.status(t.Context()).ResolutionTombstones
			if proof.Reason != "" || proof.SetHash != agentapi.ResolutionSetHash(nil) || native != 1 {
				t.Fatal("superseded flight acknowledged the wrong set", proof, native)
			}
		})
	}
}

func TestResolutionTombstoneNomadReleaseKeepsStatusResponsive(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "restarted"}[restart], func(t *testing.T) {
			testNomadReleaseStatus(t, restart)
		})
	}
}

func testNomadReleaseStatus(t *testing.T, restart bool) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.Maintenance, c.ResolutionTombstonesPresent = 1, true, true
	c.ResolutionTombstones = []string{"retired"}
	path := filepath.Join(t.TempDir(), "hosts")
	project := func(hosts string) {
		t.Helper()
		if err := os.WriteFile(path+".next", []byte("# karakoram resolution tombstones: enabled\n127.0.0.1 localhost\n"+hosts), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".next", path); err != nil {
			t.Fatal(err)
		}
	}
	project("")
	a.resolutionWriter = tombstone.NewNomadVariableWriter(path)
	// Native NSS verification is covered separately; this exercises the real
	// Nomad writer's exact-set handshake on a private watched projection.
	a.resolutionVerify = func(context.Context, []string) error { return nil }
	native := 0
	e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("unprojected install acknowledged")
	}
	boundedResolutionCall(t, func() { _ = a.status(t.Context()) })
	project("239.192.0.1 retired\nff18::4b4b:1 retired\n")
	retryTombstoneApply(t, a)
	c.ConfigVersion++
	c.ResolutionTombstones = nil
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("unprojected release acknowledged")
	}
	wantNative := 1
	if restart {
		// Run reloads the previously applied full-set cache after restart,
		// then receives the still-pending reduced config before projection.
		data, err := os.ReadFile(filepath.Join(a.o.DataDir, "karakoram", "config.json"))
		if err != nil {
			t.Fatal(err)
		}
		var cached agentapi.MemberConfig
		if err := json.Unmarshal(data, &cached); err != nil {
			t.Fatal(err)
		}
		restarted, _, engine := fakeNode(t, "/bin/true")
		restarted.o = a.o
		restarted.resolutionWriter = tombstone.NewNomadVariableWriter(path)
		restarted.resolutionVerify = func(context.Context, []string) error { return nil }
		engine.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
		if _, err := restarted.apply(t.Context(), cached); err == nil {
			t.Fatal("cached startup unexpectedly skipped asynchronous verification")
		}
		a = restarted
		if _, err := a.apply(t.Context(), c); err == nil {
			t.Fatal("restarted agent acknowledged an unprojected release")
		}
		wantNative++
	}
	boundedResolutionCall(t, func() {
		proof := a.status(t.Context()).ResolutionTombstones
		if proof.SetHash != "" || proof.Reason == "" {
			t.Error("release inherited an install proof", proof)
		}
	})
	project("")
	retryTombstoneApply(t, a)
	proof := a.status(t.Context()).ResolutionTombstones
	if proof.SetHash != agentapi.ResolutionSetHash(nil) || proof.Reason != "" || native != wantNative {
		t.Fatal("release failed or restarted the native engine", proof, native)
	}
}

func TestResolutionTombstoneShutdownDoesNotRestartSupersededFlight(t *testing.T) {
	a, c, _ := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
	c.ResolutionTombstones = []string{"retired"}
	lifetime, cancel := context.WithCancel(t.Context())
	defer cancel()
	a.resolutionContext = lifetime
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	writer := &stalledResolutionWriter{install: func() { close(entered); <-release }}
	a.resolutionWriter = writer
	a.resolutionVerify = func(context.Context, []string) error { return nil }
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("pending install acknowledged")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("install did not start")
	}
	c.ConfigVersion++
	c.ResolutionTombstones = nil
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("pending release acknowledged")
	}
	cancel()
	once.Do(func() { close(release) })
	awaitTombstoneCondition(t, func() bool {
		a.resolutionMu.Lock()
		defer a.resolutionMu.Unlock()
		return !a.resolutionRunning
	})
	if writer.calls.Load() != 1 {
		t.Fatal("shutdown launched another resolver installation", writer.calls.Load())
	}
	proof := a.status(t.Context()).ResolutionTombstones
	if proof.SetHash != "" || !proof.VerifiedAt.IsZero() {
		t.Fatal("shutdown published a stale successful proof", proof)
	}
}
