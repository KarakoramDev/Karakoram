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
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra/hoststombstones"
)

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
				var calls []string
				writer := testTombstoneWriter{calls: &calls}
				if failure == "preflight" {
					writer.preflight = errors.New("transient preflight failure")
				}
				if failure == "install" {
					writer.install = errors.New("transient installation failure")
				}
				a.resolutionWriter = writer
				fail := true
				a.resolutionVerify = func(context.Context, []string) error {
					if fail && failure == "verify" {
						return errors.New("transient resolver failure")
					}
					return nil
				}
				native := 0
				e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
				if _, err := a.apply(t.Context(), c); err == nil {
					t.Fatal("supported failure accepted")
				}
				if native != 0 || a.config().ConfigVersion != 0 || a.applyRetry == nil {
					t.Fatal("failed startup started/acknowledged native config", native, a.config().ConfigVersion)
				}
				fail = false
				a.resolutionWriter = testTombstoneWriter{calls: &calls}
				if attempted, err := a.retryApply(t.Context()); err != nil || !attempted {
					t.Fatal("normal apply retry did not recover", attempted, err)
				}
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
	var calls []string
	a.resolutionWriter = testTombstoneWriter{calls: &calls}
	fail := false
	a.resolutionVerify = func(context.Context, []string) error {
		if fail {
			return errors.New("projection disappeared")
		}
		return nil
	}
	native := 0
	e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
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
	fail = true
	if _, err := a.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	status := a.status(t.Context()).ResolutionTombstones
	if status == nil || status.Reason == "" || !status.VerifiedAt.IsZero() || status.SetHash != "" {
		t.Fatal("stale proof advertised", status)
	}
	fail = false
	if _, err := a.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
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

func TestResolutionTombstoneStatusRepairsVanishedProjection(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
	c.ResolutionTombstones = []string{"retired"}
	present := false
	var calls []string
	a.resolutionWriter = testTombstoneWriter{calls: &calls, onInstall: func() { present = true }}
	a.resolutionVerify = func(context.Context, []string) error {
		if !present {
			return errors.New("hosts block vanished")
		}
		return nil
	}
	native := 0
	e.onApply = func(*Node, agentapi.MemberConfig) error { native++; return nil }
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	present = false
	before := len(calls)
	status := a.status(t.Context()).ResolutionTombstones
	if !present || len(calls) <= before || status.Reason != "" || status.VerifiedAt.IsZero() {
		t.Fatal("status did not repair resolver projection", present, calls, status)
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
			var order []string
			a.resolutionWriter = restartHostsHelperWriter{run: func(ctx context.Context, hosts []string) error {
				order = append(order, "install")
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
				order = append(order, "verify")
				return nil
			}
			e.onApply = func(*Node, agentapi.MemberConfig) error {
				if len(order) < 2 || !slices.Equal(order[len(order)-2:], []string{"install", "verify"}) {
					t.Fatalf("native runner reached before successful install/verify: %v", order)
				}
				a.mu.RLock()
				proof := a.resolutionTombstones
				a.mu.RUnlock()
				if proof == nil || proof.VerifiedAt.IsZero() || proof.Reason != "" {
					t.Fatal("native runner reached without successful resolver proof", proof)
				}
				order = append(order, "native")
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
				var retryable *RetryableApplyError
				if !errors.As(err, &retryable) || !slices.Equal(order, []string{"install"}) {
					t.Fatal("failed install reached verification/native start", err, order)
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
				if attempted, err := a.retryApply(t.Context()); err != nil || !attempted {
					t.Fatal("cached startup retry did not recover", attempted, err)
				}
				if !slices.Equal(order, []string{"install", "install", "verify", "native"}) {
					t.Fatal("incorrect recovery order", order)
				}
			} else if err != nil || !slices.Equal(order, []string{"install", "verify", "native"}) {
				t.Fatal("incorrect cached startup order", err, order)
			}
		})
	}
}

func TestResolutionTombstoneFailedReducedInstallCannotRefreshProof(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion, c.ResolutionTombstonesPresent = 1, true
	c.ResolutionTombstones = []string{"retired-a", "retired-b"}
	var installed []string
	fail, installs, verifies, native := false, 0, 0, 0
	a.resolutionWriter = restartHostsHelperWriter{run: func(_ context.Context, hosts []string) error {
		installs++
		if fail {
			return errors.New("reduced-set installation failed")
		}
		installed = slices.Clone(hosts)
		return nil
	}}
	a.resolutionVerify = func(_ context.Context, hosts []string) error {
		verifies++
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
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	fail = true
	c.ConfigVersion++
	c.ResolutionTombstones = []string{"retired-b"}
	if _, err := a.apply(t.Context(), c); err == nil {
		t.Fatal("failed reduced-set installation was acknowledged")
	}
	before := installs
	status := a.status(t.Context()).ResolutionTombstones
	if status == nil || !status.Supported || status.Reason == "" || !status.VerifiedAt.IsZero() || status.SetHash != "" {
		t.Fatal("lookup-only refresh certified a failed reduced install", status)
	}
	if installs <= before || verifies != 1 || !slices.Equal(installed, []string{"retired-a", "retired-b"}) {
		t.Fatal("refresh bypassed failed Install because retained name resolved", installs, verifies, installed)
	}
	fail = false
	if attempted, err := a.retryApply(t.Context()); err != nil || !attempted {
		t.Fatal("reduced-set retry did not recover", attempted, err)
	}
	status = a.status(t.Context()).ResolutionTombstones
	if status.SetHash != agentapi.ResolutionSetHash(c.ResolutionTombstones) || status.Reason != "" || !slices.Equal(installed, []string{"retired-b"}) {
		t.Fatal("successful reduced installation was not acknowledged", status, installed)
	}
	if native != 1 {
		t.Fatal("reduced resolver retry invoked native renderer", native)
	}
}
