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
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra/hoststombstones"
)

func awaitTombstoneCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("resolver operation did not complete")
}

func tombstoneOperationIdle(a *nodeAgent) bool {
	a.resolutionMu.Lock()
	defer a.resolutionMu.Unlock()
	return !a.resolutionRunning
}

func awaitTombstoneReason(t *testing.T, a *nodeAgent, reason string) {
	t.Helper()
	awaitTombstoneCondition(t, func() bool {
		status := a.status(t.Context()).ResolutionTombstones
		return status != nil && strings.Contains(status.Reason, reason) && status.VerifiedAt.IsZero() && status.SetHash == "" && tombstoneOperationIdle(a)
	})
}

func retryTombstoneApply(t *testing.T, a *nodeAgent) {
	t.Helper()
	awaitTombstoneCondition(t, func() bool {
		attempted, err := a.retryApply(t.Context())
		if !attempted {
			t.Fatal("resolver retry was lost")
		}
		if err == nil {
			return true
		}
		var retryable *RetryableApplyError
		if !errors.As(err, &retryable) {
			t.Fatal("non-retryable resolver apply failure", err)
		}
		return false
	})
	awaitTombstoneCondition(t, func() bool { return tombstoneOperationIdle(a) })
}

func applyTombstonesEventually(t *testing.T, a *nodeAgent, c agentapi.MemberConfig) {
	t.Helper()
	_, err := a.apply(t.Context(), c)
	var retryable *RetryableApplyError
	if !errors.As(err, &retryable) {
		t.Fatal("supported resolver apply did not defer completion", err)
	}
	retryTombstoneApply(t, a)
}

func TestResolutionTombstoneApplyReportsWithoutNativeRestart(t *testing.T) {
	t.Setenv("KARAKORAM_RESOLVER_TOMBSTONES", "")
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion = 1
	c.Maintenance = true
	c.ResolutionTombstonesPresent = true
	calls := 0
	e.onApply = func(*Node, agentapi.MemberConfig) error { calls++; return nil }
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.ConfigVersion++
	c.ResolutionTombstones = []string{"retired"}
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || a.config().ConfigVersion != 2 {
		t.Fatal("tombstone config invoked native Apply", calls, a.config().ConfigVersion)
	}
	status := a.status(t.Context())
	if status.ResolutionTombstones == nil || status.ResolutionTombstones.Supported || status.ResolutionTombstones.Reason != "no resolver mechanism on this platform" {
		t.Fatal(status.ResolutionTombstones)
	}
	// Retrying resolver verification must not re-render or restart native processes.
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("retry restarted native renderer", calls)
	}
	c.ConfigVersion++
	c.ResolutionTombstones = nil
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("release restarted native renderer", calls)
	}
}

func TestResolutionTombstoneSupportedFailureBlocksStartup(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, failure := range []string{"preflight", "install", "verify"} {
			t.Run(map[bool]string{false: "fresh", true: "cached"}[cached]+"/"+failure, func(t *testing.T) {
				a, c, e := fakeNode(t, "/bin/true")
				c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
				c.ResolutionTombstones = []string{"retired"}
				if cached {
					raw, _ := json.Marshal(c)
					c = agentapi.MemberConfig{}
					if err := json.Unmarshal(raw, &c); err != nil {
						t.Fatal(err)
					}
				}
				var calls tombstoneCalls
				writer := testTombstoneWriter{calls: &calls}
				if failure == "preflight" {
					writer.preflight = errors.New("transient preflight failure")
				}
				if failure == "install" {
					writer.install = errors.New("transient installation failure")
				}
				a.resolutionWriter = writer
				var fail atomic.Bool
				fail.Store(true)
				a.resolutionVerify = func(context.Context, []string) error {
					if fail.Load() && failure == "verify" {
						return errors.New("transient resolver failure")
					}
					return nil
				}
				native := 0
				e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
				if _, err := a.apply(t.Context(), c); err == nil {
					t.Fatal("supported failure accepted")
				}
				awaitTombstoneReason(t, a, map[string]string{
					"preflight": "transient preflight failure",
					"install":   "transient installation failure",
					"verify":    "transient resolver failure",
				}[failure])
				if native != 0 || a.config().ConfigVersion != 0 || a.applyRetry == nil {
					t.Fatal("failed startup started/acknowledged native config", native, a.config().ConfigVersion)
				}
				fail.Store(false)
				a.resolutionWriter = testTombstoneWriter{calls: &calls}
				retryTombstoneApply(t, a)
				if native != 1 || a.config().ConfigVersion != 1 {
					t.Fatal(native, a.config())
				}
			})
		}
	}
}

func TestResolutionTombstoneReload304RepairsAndInvalidates(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.ResolutionTombstonesPresent, c.PullRevision = 1, true, "same"
	c.ResolutionTombstones = []string{"retired"}
	var calls tombstoneCalls
	a.resolutionWriter = testTombstoneWriter{calls: &calls}
	var fail atomic.Bool
	a.resolutionVerify = func(context.Context, []string) error {
		if fail.Load() {
			return errors.New("projection disappeared")
		}
		return nil
	}
	native := 0
	e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
	applyTombstonesEventually(t, a, c)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("revision") != "same" {
			t.Error("missing conditional revision")
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "fixture", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a.client = client
	fail.Store(true)
	if _, err := a.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := a.refreshResolutionTombstones(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitTombstoneReason(t, a, "projection disappeared")
	status := a.status(t.Context()).ResolutionTombstones
	if status == nil || status.Reason == "" || !status.VerifiedAt.IsZero() || status.SetHash != "" {
		t.Fatal("stale proof advertised", status)
	}
	fail.Store(false)
	if _, err := a.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := a.refreshResolutionTombstones(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitTombstoneCondition(t, func() bool {
		status := a.status(t.Context()).ResolutionTombstones
		return status != nil && status.Reason == "" && !status.VerifiedAt.IsZero()
	})
	status = a.status(t.Context()).ResolutionTombstones
	if status.Reason != "" || status.SetHash != agentapi.ResolutionSetHash(c.ResolutionTombstones) || status.Generation == "" {
		t.Fatal("304 did not retry resolver", status)
	}
	if native != 1 {
		t.Fatal("resolver retry rendered native config", native)
	}
}

func TestResolutionTombstoneLegacyConfigPreservesSet(t *testing.T) {
	a, c, _ := fakeNode(t, "/bin/true")
	c.ResolutionTombstonesPresent = true
	c.ResolutionTombstones = []string{"retired"}
	a.c = c
	var legacy agentapi.MemberConfig
	if err := json.Unmarshal([]byte(`{"configVersion":2}`), &legacy); err != nil {
		t.Fatal(err)
	}
	got := a.preserveResolutionTombstones(legacy)
	if len(got.ResolutionTombstones) != 1 || !got.ResolutionTombstonesPresent {
		t.Fatal(got)
	}
	legacy.ResolutionTombstonesPresent = true
	if got := a.preserveResolutionTombstones(legacy); len(got.ResolutionTombstones) != 0 {
		t.Fatal(got)
	}
}

func TestResolutionTombstoneRefreshRepairsVanishedProjection(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
	c.ResolutionTombstones = []string{"retired"}
	var present atomic.Bool
	var calls tombstoneCalls
	a.resolutionWriter = testTombstoneWriter{calls: &calls, onInstall: func() { present.Store(true) }}
	a.resolutionVerify = func(context.Context, []string) error {
		if !present.Load() {
			return errors.New("hosts block vanished")
		}
		return nil
	}
	native := 0
	e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
	applyTombstonesEventually(t, a, c)
	initialVerifiedAt := a.status(t.Context()).ResolutionTombstones.VerifiedAt
	present.Store(false)
	before := len(calls.snapshot())
	status := a.status(t.Context()).ResolutionTombstones
	if present.Load() || len(calls.snapshot()) != before || status.Reason != "" || status.VerifiedAt.IsZero() {
		t.Fatal("status unexpectedly refreshed resolver projection", present.Load(), calls.snapshot(), status)
	}
	if err := a.refreshResolutionTombstones(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitTombstoneCondition(t, func() bool {
		status = a.status(t.Context()).ResolutionTombstones
		return status != nil && status.Reason == "" && status.VerifiedAt.After(initialVerifiedAt)
	})
	if !present.Load() || len(calls.snapshot()) <= before || status.VerifiedAt.IsZero() {
		t.Fatal("refresh did not repair resolver projection", present.Load(), calls.snapshot(), status)
	}
	if native != 1 {
		t.Fatal("resolver repair invoked native renderer", native)
	}
}

// Adapt only the privileged transport for the existing injected Writer seam.
// Install runs the production helper, with the same private-copy literal-path
// substitutions used by the provider helper tests; no host sudo is executed.
type restartHostsHelperWriter struct {
	run func(context.Context, []string) error
}

func (restartHostsHelperWriter) Kind() string                    { return "hosts-helper" }
func (restartHostsHelperWriter) Preflight(context.Context) error { return nil }
func (w restartHostsHelperWriter) Install(ctx context.Context, hosts []string) error {
	return w.run(ctx, hosts)
}

func TestResolutionTombstoneRestartReplaysBeforeNativeStart(t *testing.T) {
	for _, failInstall := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "install-failure"}[failInstall], func(t *testing.T) {
			a, c, e := fakeNode(t, "/bin/true")
			c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
			c.ResolutionTombstones = []string{"retired"}
			c.PullRevision, c.ChangeRevision, c.ContentRevision = "cached", "cached", "cached"
			cachePath := filepath.Join(a.o.DataDir, "karakoram", "config.json")
			if err := os.MkdirAll(filepath.Dir(cachePath), 0700); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(cachePath, data, 0600); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			hostsPath := filepath.Join(dir, "hosts")
			const original = "127.0.0.1 localhost\n192.0.2.1 member\n"
			const block = "# BEGIN karakoram resolution tombstones\n239.192.0.1 retired\nff18::4b4b:1 retired\n# END karakoram resolution tombstones\n"
			if err := os.WriteFile(hostsPath, []byte(original+block), 0600); err != nil {
				t.Fatal(err)
			}
			// Zerops preserves the agent cache/helper but recreates /etc/hosts
			// without the installed block when the member stops and starts.
			if err := os.WriteFile(hostsPath, []byte(original), 0600); err != nil {
				t.Fatal(err)
			}
			lockDir := filepath.Join(dir, "lock-parent")
			if !failInstall {
				if err := os.Mkdir(lockDir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			script := strings.Replace(hoststombstones.Script, "hosts=/etc/hosts", "hosts='"+hostsPath+"'", 1)
			script = strings.Replace(script, "lock=/etc/.karakoram-hosts-tombstones.lock", "lock='"+filepath.Join(lockDir, "lock")+"'", 1)
			scriptPath := filepath.Join(dir, "helper.sh")
			if err := os.WriteFile(scriptPath, []byte(script), 0600); err != nil {
				t.Fatal(err)
			}
			var order tombstoneCalls
			a.resolutionWriter = restartHostsHelperWriter{run: func(ctx context.Context, hosts []string) error {
				order.add("install")
				if !slices.Equal(hosts, []string{"retired"}) {
					return errors.New("cached complete resolver set was lost")
				}
				cmd := exec.CommandContext(ctx, "/bin/sh", scriptPath)
				cmd.Stdin = strings.NewReader(strings.Join(hosts, "\n") + "\n")
				if output, err := cmd.CombinedOutput(); err != nil {
					return errors.New("hosts helper failed: " + err.Error() + ": " + string(output))
				}
				return nil
			}}
			a.resolutionVerify = func(_ context.Context, hosts []string) error {
				data, err := os.ReadFile(hostsPath)
				if err != nil {
					return err
				}
				if !slices.Equal(hosts, []string{"retired"}) || string(data) != original+block {
					return errors.New("reset hosts file was not repaired before verification")
				}
				order.add("verify")
				return nil
			}
			e.onApply = func(*Node, agentapi.MemberConfig) error {
				steps := order.snapshot()
				if len(steps) < 2 || !slices.Equal(steps[len(steps)-2:], []string{"install", "verify"}) {
					t.Fatalf("native runner reached before successful install/verify: %v", steps)
				}
				a.mu.RLock()
				proof := a.resolutionTombstones
				a.mu.RUnlock()
				if proof == nil || proof.VerifiedAt.IsZero() || proof.Reason != "" {
					t.Fatal("native runner reached without successful resolver proof", proof)
				}
				order.add("native")
				return nil
			}
			// Mirror cached startup's read/decode/revision clearing and invoke
			// the real startup apply entry point, with no applied native config.
			data, err = os.ReadFile(cachePath)
			if err != nil {
				t.Fatal(err)
			}
			var cached agentapi.MemberConfig
			if err := json.Unmarshal(data, &cached); err != nil {
				t.Fatal(err)
			}
			cached.PullRevision, cached.ChangeRevision, cached.ContentRevision = "", "", ""
			_, err = a.apply(t.Context(), cached)
			if failInstall {
				awaitTombstoneReason(t, a, "hosts helper failed")
				var retryable *RetryableApplyError
				if !errors.As(err, &retryable) || !slices.Equal(order.snapshot(), []string{"install"}) {
					t.Fatal("failed install reached verification/native start", err, order.snapshot())
				}
				a.mu.RLock()
				pending := a.applyRetry
				a.mu.RUnlock()
				if pending == nil || !slices.Equal(pending.ResolutionTombstones, []string{"retired"}) || a.config().ConfigVersion != 0 {
					t.Fatal("failed startup acknowledged config or lost its retry", pending, a.config().ConfigVersion)
				}
				if data, err := os.ReadFile(hostsPath); err != nil || string(data) != original {
					t.Fatal("failed helper changed reset hosts file", string(data), err)
				}
				if err := os.Mkdir(lockDir, 0700); err != nil {
					t.Fatal(err)
				}
				retryTombstoneApply(t, a)
				if !slices.Equal(order.snapshot(), []string{"install", "install", "verify", "native"}) {
					t.Fatal("incorrect recovery order", order.snapshot())
				}
			} else {
				var retryable *RetryableApplyError
				if !errors.As(err, &retryable) {
					t.Fatal("cached startup did not defer native start", err)
				}
				retryTombstoneApply(t, a)
				if !slices.Equal(order.snapshot(), []string{"install", "verify", "native"}) {
					t.Fatal("incorrect cached startup order", order.snapshot())
				}
			}
		})
	}
}

func TestResolutionTombstoneFailedReducedInstallCannotRefreshProof(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
	c.ResolutionTombstones = []string{"retired-a", "retired-b"}
	var installed []string
	var fail atomic.Bool
	var installs, verifies atomic.Int64
	native := 0
	a.resolutionWriter = restartHostsHelperWriter{run: func(_ context.Context, hosts []string) error {
		installs.Add(1)
		if fail.Load() {
			return errors.New("reduced-set installation failed")
		}
		installed = slices.Clone(hosts)
		return nil
	}}
	a.resolutionVerify = func(_ context.Context, hosts []string) error {
		verifies.Add(1)
		// Like getent, this proves each desired name still resolves; it
		// cannot prove that an omitted managed name has been removed.
		for _, host := range hosts {
			if !slices.Contains(installed, host) {
				return errors.New("desired sentinel mapping is missing")
			}
		}
		return nil
	}
	e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
	applyTombstonesEventually(t, a, c)
	fail.Store(true)
	c.ConfigVersion++
	c.ResolutionTombstones = []string{"retired-b"}
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("failed reduced-set installation was acknowledged")
	}
	awaitTombstoneReason(t, a, "reduced-set installation failed")
	before := installs.Load()
	if err := a.refreshResolutionTombstones(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitTombstoneCondition(t, func() bool {
		status := a.status(t.Context()).ResolutionTombstones
		return status != nil && strings.Contains(status.Reason, "reduced-set installation failed") && installs.Load() > before
	})
	status := a.status(t.Context()).ResolutionTombstones
	if status == nil || !status.Supported || status.Reason == "" || !status.VerifiedAt.IsZero() || status.SetHash != "" {
		t.Fatal("lookup-only refresh certified a failed reduced install", status)
	}
	if installs.Load() <= before || verifies.Load() != 1 || !slices.Equal(installed, []string{"retired-a", "retired-b"}) {
		t.Fatal("refresh bypassed failed Install because retained name resolved", installs.Load(), verifies.Load(), installed)
	}
	fail.Store(false)
	retryTombstoneApply(t, a)
	status = a.status(t.Context()).ResolutionTombstones
	if status.SetHash != agentapi.ResolutionSetHash(c.ResolutionTombstones) || status.Reason != "" || !slices.Equal(installed, []string{"retired-b"}) {
		t.Fatal("successful reduced installation was not acknowledged", status, installed)
	}
	if native != 1 {
		t.Fatal("reduced resolver retry invoked native renderer", native)
	}
}
