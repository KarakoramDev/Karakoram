// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestFakeStartOnlyScalesToOne(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MaxContainers = 3
	op := importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	id := op.Services["runtime"]
	if err := b.c.post(t.Context(), "application.stop", map[string]any{"applicationId": id}, nil); err != nil {
		t.Fatal(err)
	}
	if row := fakeAppSnapshot(t, f, "runtime"); row["applicationStatus"] != "idle" || fakeInt(row, "replicas") != 3 {
		t.Fatalf("stop database shape: %+v", row)
	}
	if err := b.c.post(t.Context(), "application.start", map[string]any{"applicationId": id}, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	running := 0
	for _, task := range f.apps[id].tasks {
		if task["state"] == "running" {
			running++
		}
	}
	if running != 1 || fakeInt(f.apps[id].row, "replicas") != 3 {
		t.Fatalf("wire start must create exactly one task without changing replicas: running=%d", running)
	}
}

func TestImportMountFailureRetainsResumableApplication(t *testing.T) {
	for _, preexisting := range []bool{false, true} {
		t.Run(map[bool]string{false: "holder", true: "runtime"}[preexisting], func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			specs := []infra.ServiceSpec{volumeSpec("volume"), runtimeSpec("runtime", "volume")}
			if preexisting {
				importFake(t, b, volumeSpec("volume"))
				specs = specs[1:]
			}
			f.mu.Lock()
			f.failureOnce["mounts.create"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "temporary mount failure"}
			f.mu.Unlock()
			if _, err := b.ImportServices(t.Context(), specs); !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("mount failure: %v", err)
			}
			services, err := b.ListServices(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if preexisting {
				want = 2
			}
			if len(services) != want {
				t.Fatalf("failed import lost its durable creation receipt: %+v", services)
			}
			if fakeCalls(f, "application.delete") != 0 {
				t.Fatal("partial import deleted its resumable ownership receipt")
			}
			importFake(t, b, specs...)
			holder := fakeAppSnapshot(t, f, "volume")
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, a := range f.apps {
				if a.row["name"] == "runtime" {
					if len(a.mounts) != 1 || a.mounts[0]["volumeName"] != holder["appName"] {
						t.Fatalf("retried runtime is not mounted: %+v", a.mounts)
					}
				}
			}
		})
	}
}

func TestDoneDeploymentFailedWipeTaskDoesNotRetry(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"))
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	for _, a := range f.apps {
		f.apply(a)
		a.deployments[0].status = "done"
		a.tasks[0]["state"] = "shutdown"
		a.tasks[0]["currentState"] = "Failed 1 second ago"
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed {
		t.Fatalf("failed task: %+v %v", state, err)
	}
	retry, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	state, err = b.OpStatus(t.Context(), retry)
	if err != nil || !state.Failed || fakeCalls(f, "application.deploy") != 1 {
		t.Fatal("failed wipe must remain terminal without resubmission")
	}
}

func TestDeleteKeepsApplicationUntilTasksStopped(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	id := imported.Services["runtime"]
	f.mu.Lock()
	f.apps[id].holdTasks = true
	f.mu.Unlock()
	op, err := b.DeleteService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	present := f.apps[id] != nil
	f.mu.Unlock()
	if !present || fakeCalls(f, "application.delete") != 0 {
		t.Fatal("delete removed application before running task stopped")
	}
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("still-running delete: %+v %v", state, err)
	}
	f.mu.Lock()
	for _, task := range f.apps[id].tasks {
		task["state"] = "shutdown"
		task["currentState"] = "Shutdown 1 second ago"
	}
	f.apps[id].holdTasks = false
	f.mu.Unlock()
	awaitFake(t, fakeBackend(t, f, nil), roundTripOp(t, op))
	if fakeCalls(f, "application.delete") != 1 {
		t.Fatal("quiescent runtime was not deleted")
	}
}

func TestStoppedSettingsDoNotRestartService(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	port := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}
	if err := b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
		t.Fatal(err)
	}
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	reloads, starts := fakeCalls(f, "application.reload"), fakeCalls(f, "application.start")
	op, err = b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 1, MaxCPU: 2})
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	op, err = b.SetContainerCount(t.Context(), "runtime", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if err = b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	port.Port = 18081
	if err = b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
		t.Fatal(err)
	}
	if err = b.SetEnvVars(t.Context(), "runtime", map[string]string{"KARAKORAM_OWNER": "owner"}); err != nil {
		t.Fatal(err)
	}
	if err = b.SetEnvSecrets(t.Context(), "runtime", map[string]string{"TOKEN": "fixture"}); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != reloads || fakeCalls(f, "application.start") != starts {
		t.Fatal("settings update restarted intentionally stopped service")
	}
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 0 {
		t.Fatalf("stopped containers: %+v %v", containers, err)
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil || s.Status != infra.StatusStopped || s.MaxContainers != 3 {
		t.Fatalf("stopped updated service: %+v %v", s, err)
	}
}

func TestOperationsAwaitAllNewReplicas(t *testing.T) {
	for _, action := range []string{"deploy", "restart", "start"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.MaxContainers = 3
			imported := importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			var op infra.Op
			var err error
			switch action {
			case "deploy":
				op, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			case "restart":
				op, err = b.Restart(t.Context(), "runtime")
			case "start":
				stopped, e := b.Stop(t.Context(), "runtime")
				if e != nil {
					t.Fatal(e)
				}
				awaitFake(t, b, stopped)
				op, err = b.Start(t.Context(), "runtime")
			}
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a := f.apps[imported.Services["runtime"]]
			if action == "deploy" {
				a.deployments[0].status = "done"
				f.apply(a)
			}
			a.holdTasks = true
			newRunning := 0
			for _, task := range a.tasks {
				if task["state"] == "running" {
					task["currentState"] = "Preparing 1 second ago"
					if newRunning == 0 {
						task["currentState"] = "Running 1 second ago"
					}
					newRunning++
				}
			}
			f.mu.Unlock()
			if newRunning != 3 {
				t.Fatalf("%s should request all configured replicas, got %d", action, newRunning)
			}
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || state.Done || state.Failed {
				t.Fatalf("one of three ready: %+v %v", state, err)
			}
			f.mu.Lock()
			for _, task := range a.tasks {
				if task["state"] == "running" {
					task["currentState"] = "Running 1 second ago"
				}
			}
			f.mu.Unlock()
			state, err = b.OpStatus(t.Context(), op)
			if err != nil || !state.Done || state.Failed {
				t.Fatalf("all three ready: %+v %v", state, err)
			}
		})
	}
}

func TestQueuedDeploymentRemainsPendingPastTwoMinutes(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	a := f.apps[imported.Services["runtime"]]
	a.deployments = nil
	a.tasks = nil
	f.mu.Unlock()
	handle, err := decodeOp(op)
	if err != nil {
		t.Fatal(err)
	}
	handle.Issued = time.Now().Add(-3 * time.Minute)
	state, err := b.OpStatus(t.Context(), encodeOp(handle))
	if err != nil || state.Done || state.Failed {
		t.Fatalf("accepted upload without deployment row can still be queued: %+v %v", state, err)
	}
}

func TestLogsPreserveMessagePipe(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	const message = "left | right | final"
	f.mu.Lock()
	f.apps[imported.Services["runtime"]].logs = "2026-10-16T12:00:00.123456789Z " + message + "\n"
	f.mu.Unlock()
	lines, err := b.Logs(t.Context(), "runtime", infra.LogQuery{})
	if err != nil || len(lines) != 1 || lines[0].Message != message || lines[0].Timestamp.IsZero() {
		t.Fatalf("pipe-containing timestamped message: %+v %v", lines, err)
	}
}

func TestLogsSelectNewestAcrossRetainedSwarmTasks(t *testing.T) {
	const current = "2026-10-27T15:23:00Z current first\n2026-10-27T15:23:00Z current second\n"
	const retained = "2026-10-27T14:58:00Z retained task stopped\n2026-10-27T14:58:01Z retained task exited\n"
	for _, tc := range []struct{ name, raw string }{
		{"current-batch-first", current + retained},
		{"retained-batch-first", retained + current},
		{"interleaved", "2026-10-27T15:23:00Z current first\n2026-10-27T14:58:00Z retained task stopped\n2026-10-27T15:23:00Z current second\n2026-10-27T14:58:01Z retained task exited\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.mu.Lock()
			f.apps[imported.Services["runtime"]].logs = tc.raw
			f.mu.Unlock()
			lines, err := b.Logs(t.Context(), "runtime", infra.LogQuery{Limit: 2})
			if err != nil || len(lines) != 2 {
				t.Fatalf("merged task tail: %+v %v", lines, err)
			}
			if lines[0].Message != "current first" || lines[1].Message != "current second" {
				t.Fatalf("retained task batch hid current output or timestamp ties reordered: %+v", lines)
			}
		})
	}
}

func TestLogsPreserveUnknownTimestampNeighbours(t *testing.T) {
	const older = "2026-10-27T14:58:00Z older\n"
	const current = "2026-10-27T15:23:00Z current\n"
	for _, tc := range []struct {
		name, raw string
		want      [2]string
	}{
		{"missing-trailing", older + current + "diagnostic\n", [2]string{"current", "diagnostic"}},
		{"malformed-trailing", older + current + "not-a-time diagnostic\n", [2]string{"current", "not-a-time diagnostic"}},
		{"leading", "diagnostic\n" + current + older, [2]string{"diagnostic", "current"}},
		{"mixed-task-batches", current + "diagnostic\n" + older, [2]string{"current", "diagnostic"}},
		{"mixed-missing-malformed", current + "diagnostic\nbad-timestamp continuation\n" + older, [2]string{"diagnostic", "bad-timestamp continuation"}},
		{"all-unknown", "first\ndiagnostic\nbad-timestamp continuation\n", [2]string{"diagnostic", "bad-timestamp continuation"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.mu.Lock()
			f.apps[imported.Services["runtime"]].logs = tc.raw
			f.mu.Unlock()
			lines, err := b.Logs(t.Context(), "runtime", infra.LogQuery{Limit: 2})
			if err != nil || len(lines) != 2 {
				t.Fatalf("mixed timestamp tail: %+v %v", lines, err)
			}
			if lines[0].Message != tc.want[0] || lines[1].Message != tc.want[1] {
				t.Fatalf("unknown timestamp moved away from transport neighbours: %+v", lines)
			}
			wantTime := time.Date(2026, 10, 27, 15, 23, 0, 0, time.UTC)
			if tc.name == "all-unknown" {
				wantTime = time.Time{}
			}
			for _, line := range lines {
				if !line.Timestamp.Equal(wantTime) {
					t.Fatalf("unexpected diagnostic timestamp anchor: %+v", line)
				}
			}
		})
	}
}

func TestFailedUploadDoesNotMarkServiceDeployed(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	f.mu.Lock()
	f.failureOnce["application.dropDeployment"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "upload rejected"}
	f.mu.Unlock()
	if _, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "")); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("failed upload: %v", err)
	}
	if fakeMeta(t, f, "runtime").Deployed {
		t.Fatal("failed first upload left deployed metadata")
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil || s.Status != infra.StatusCreating {
		t.Fatalf("ambiguous first upload service: %+v %v", s, err)
	}
	if op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "")); err != nil || op.ID == "" || fakeCalls(f, "application.dropDeployment") != 1 {
		t.Fatalf("ambiguous upload did not recover its original attempt without replay: %+v %v", op, err)
	}
}

func TestExistingExternalOverlayIsTolerated(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		accepted      bool
	}{
		{"external-overlay", "(HTTP code 409) unexpected - network with name race-overlay already exists ", true},
		{"unrelated-rejection", "overlay creation is prohibited", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t)
			// Docker has the external overlay, but network.all only lists Dokploy DB
			// rows and therefore cannot prove whether this overlay exists.
			f.failureOnce["network.create"] = fakeResponse{400, "BAD_REQUEST", tc.message}
			b := fakeBackend(t, f, map[string]string{"network": "race-overlay"})
			_, err := b.validate(t.Context())
			if tc.accepted && err != nil {
				t.Fatalf("existing external overlay must be accepted: %v", err)
			}
			if !tc.accepted && !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("unrelated network failure must remain rejected: %v", err)
			}
			if fakeCalls(f, "network.create") != 1 {
				t.Fatal("network creation was not attempted")
			}
		})
	}
}

func TestSubdomainLabelsFitDNSLimit(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	names := []string{strings.Repeat("a", 63), strings.Repeat("a", 62) + "b"}
	hosts := map[string]bool{}
	for _, name := range names {
		importFake(t, b, runtimeSpec(name, ""))
		deployFake(t, b, name, "")
		if _, err := b.SetSubdomainAccess(t.Context(), name, true); err != nil {
			t.Fatal(err)
		}
		urls, err := b.SubdomainURLs(t.Context(), name)
		if err != nil || len(urls) != 1 {
			t.Fatalf("long hostname URLs: %v %v", urls, err)
		}
		host := strings.TrimPrefix(urls[0], "https://")
		for _, label := range strings.Split(host, ".") {
			if len(label) > 63 || len(label) == 0 {
				t.Fatalf("invalid DNS label length %d", len(label))
			}
		}
		if hosts[host] {
			t.Fatal("truncated hostname domain collides")
		}
		hosts[host] = true
	}
}

func TestUnquotedUnicodeWhitespaceIsRejected(t *testing.T) {
	for _, r := range []rune{'\ufeff', '\u0085'} {
		t.Run(string(r), func(t *testing.T) {
			value := "a'b\"c`d" + string(r)
			if _, err := encodeEnvironment(map[string]string{"SECRET": value}); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("ambiguous Unicode whitespace in unquoted value accepted: %v", err)
			}
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.Env = map[string]string{"SECRET": value}
			if _, err := b.ImportServices(t.Context(), []infra.ServiceSpec{spec}); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("ambiguous environment import: %v", err)
			}
			if fakeCalls(f, "application.create") != 0 {
				t.Fatal("invalid environment import created partial service")
			}
		})
	}
}

func appliedNetworks(t *testing.T, f *fakeAPI, hostname string) []map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.apps {
		if a.row["name"] == hostname {
			return fakeRowsCopy(a.appliedNetworks)
		}
	}
	t.Fatalf("application %q missing", hostname)
	return nil
}

func assertIsolatedNetwork(t *testing.T, b *Backend, networks []map[string]any) {
	t.Helper()
	if b.network() == "dokploy-network" || len(networks) != 1 || networks[0]["Target"] != b.network() {
		t.Fatalf("application should use only target overlay %q: %+v", b.network(), networks)
	}
}

func TestDefaultNetworkIsIsolatedUntilSubdomainEnable(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	target := b.target
	target.ID = "different-environment"
	target.Config = make(map[string]string, len(b.target.Config))
	for key, value := range b.target.Config {
		if key != "network" {
			target.Config[key] = value
		}
	}
	factory := newFactory(func(key string) string {
		if key == "KARAKORAM_DOKPLOY_ALLOWED_URLS" {
			return f.server.URL
		}
		return ""
	})
	created, err := factory.New(target)
	if err != nil {
		t.Fatal(err)
	}
	other := created.(*Backend)
	if b.network() == other.network() {
		t.Fatal("default overlay shared across distinct target environments")
	}
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	op, err := b.SetSubdomainAccess(t.Context(), "runtime", true)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	networks := appliedNetworks(t, f, "runtime")
	if len(networks) != 2 || networks[0]["Target"] != b.network() || networks[1]["Target"] != "dokploy-network" {
		t.Fatalf("enabled HTTPS network attachments: %+v", networks)
	}
	before := fakeCalls(f, "application.reload")
	if _, err = b.SetSubdomainAccess(t.Context(), "runtime", true); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before {
		t.Fatal("idempotent HTTPS enable reloaded service")
	}
	op, err = b.SetSubdomainAccess(t.Context(), "runtime", false)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	urls, err := b.SubdomainURLs(t.Context(), "runtime")
	if err != nil || len(urls) != 0 {
		t.Fatalf("disabled HTTPS domains retained: %v %v", urls, err)
	}
}

func TestNonHTTPRuntimeNeverAttachesSharedNetwork(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	d := testDeployment(t, "")
	d.Runtime.Ports = []infra.Port{{Port: 9090, Protocol: "tcp"}}
	op, err := b.Deploy(t.Context(), "runtime", d)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	op, err = b.SetSubdomainAccess(t.Context(), "runtime", true)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	if fakeCalls(f, "domain.create") != 0 {
		t.Fatal("non-HTTP runtime created domain")
	}
}

func TestStoppedSubdomainEnableDoesNotRestart(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	reloads := fakeCalls(f, "application.reload")
	op, err = b.SetSubdomainAccess(t.Context(), "runtime", true)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if fakeCalls(f, "application.reload") != reloads {
		t.Fatal("HTTPS setting restarted stopped service")
	}
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	op, err = b.Start(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	networks := appliedNetworks(t, f, "runtime")
	if len(networks) != 2 || networks[1]["Target"] != "dokploy-network" {
		t.Fatalf("start did not apply deferred HTTPS network setting: %+v", networks)
	}
}

func TestDeployDisablesEnvFileCreation(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	// Simulate an operator changing Dokploy's default between import and deploy.
	if err := b.c.post(t.Context(), "application.update", map[string]any{"applicationId": imported.Services["runtime"], "createEnvFile": true}, nil); err != nil {
		t.Fatal(err)
	}
	deployFake(t, b, "runtime", "")
	if row := fakeAppSnapshot(t, f, "runtime"); row["createEnvFile"] != false {
		t.Fatal("deployment must not write secret environment values into an image env file")
	}
}

func TestQueuedWipeStaysPendingWithoutResubmission(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, volumeSpec("volume"))
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	a := f.apps[imported.Services["volume"]]
	a.deployments = nil
	a.tasks = nil
	f.mu.Unlock()
	handle, err := decodeOp(op)
	if err != nil {
		t.Fatal(err)
	}
	handle.Issued = time.Now().Add(-3 * time.Minute)
	state, err := b.OpStatus(t.Context(), encodeOp(handle))
	if err != nil || state.Done || state.Failed {
		t.Fatalf("accepted queued wipe without deployment or task must stay pending: %+v %v", state, err)
	}
	if !fakeMeta(t, f, "volume").Wiping {
		t.Fatal("queued wipe lost persistent wiping state")
	}
	retry, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.deploy") != 1 {
		t.Fatal("explicit delete duplicated an already accepted queued wipe")
	}
	state, err = fakeBackend(t, f, nil).OpStatus(t.Context(), roundTripOp(t, retry))
	if err != nil || state.Done || state.Failed {
		t.Fatalf("retried queued wipe: %+v %v", state, err)
	}
	if fakeCalls(f, "application.delete") != 0 {
		t.Fatal("queued wipe deleted the holder before a task completed")
	}
}

func TestDataNodeLabelsExcludeUnlabelledFallback(t *testing.T) {
	for _, condition := range []string{"drained", "down"} {
		t.Run(condition, func(t *testing.T) {
			f := newFakeAPI(t)
			for _, n := range f.nodes {
				n["Spec"].(map[string]any)["Labels"] = map[string]any{}
			}
			f.nodes[0]["Spec"].(map[string]any)["Labels"] = map[string]any{"karakoram.data": "true"}
			if condition == "drained" {
				f.nodes[0]["Spec"].(map[string]any)["Availability"] = "drain"
			} else {
				f.nodes[0]["Status"].(map[string]any)["State"] = "down"
			}
			// Both other nodes remain ready/active but lack the data label.
			// An unavailable explicit data tier must not silently change tiers.
			b := fakeBackend(t, f, nil)
			nodes, err := b.nodes(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got := eligibleNodes(nodes); len(got) != 0 {
				t.Fatalf("unavailable labelled tier fell back to %d unlabelled nodes", len(got))
			}
			if _, err = b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("volume")}); !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("volume import must reject unavailable labelled data tier: %v", err)
			}
			if fakeCalls(f, "application.create") != 0 {
				t.Fatal("unavailable data tier created a partial volume holder")
			}
		})
	}
}

func TestScaleBackUsesTemporaryStopFirstWithoutSpareCapacity(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 2, 2
	id := importFake(t, b, spec).Services["runtime"]
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	f.apps[id].taskCapacity = 2
	f.mu.Unlock()
	up, err := b.SetContainerCount(t.Context(), "runtime", 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.OpStatus(t.Context(), up)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("scale-up should be placement blocked: %+v %v", state, err)
	}
	down, err := b.SetContainerCount(t.Context(), "runtime", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if policy := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"].(map[string]any); policy["Parallelism"] != float64(1) {
		t.Fatalf("ordinary scale-back must remain a per-slot update: %#v", policy)
	}
	b = fakeBackend(t, f, nil)
	awaitFake(t, b, roundTripOp(t, down))
	f.mu.Lock()
	orders := append([]string(nil), f.apps[id].reloadOrders...)
	f.mu.Unlock()
	if len(orders) != 2 || orders[0] != "start-first" || orders[1] != "stop-first" {
		t.Fatalf("unexpected rollout request sequence: %v", orders)
	}
	row := fakeAppSnapshot(t, f, "runtime")
	if policy := row["updateConfigSwarm"].(map[string]any); policy["Order"] != "start-first" {
		t.Fatalf("temporary policy was not restored without reload: %v", policy)
	}
	if metadata := fakeMeta(t, f, "runtime"); metadata.Pending != nil || metadata.Max != 2 {
		t.Fatalf("scale-back did not finish durably: %+v", metadata)
	}
	f.mu.Lock()
	f.apps[id].taskCapacity = 0
	f.mu.Unlock()
	update, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 1, MaxCPU: 2})
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, update)
	f.mu.Lock()
	defer f.mu.Unlock()
	if orders := f.apps[id].reloadOrders; len(orders) != 3 || orders[2] != "start-first" {
		t.Fatalf("ordinary specification change inherited stop-first: %v", orders)
	}
}

func TestScaleBackPreservesStartFirstForSingletonAndPendingSpecification(t *testing.T) {
	for _, scenario := range []string{"singleton", "pending-specification", "staged-environment"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.MinContainers, spec.MaxContainers = 2, 2
			id := importFake(t, b, spec).Services["runtime"]
			deployFake(t, b, "runtime", "")
			f.mu.Lock()
			f.apps[id].taskCapacity = 2
			f.mu.Unlock()
			target := 1
			if scenario == "pending-specification" {
				target = 2
				if _, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 1, MaxCPU: 2}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "staged-environment" {
				target = 2
				if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{"EXAMPLE_SETTING": "changed"}); err != nil {
					t.Fatal(err)
				}
			}
			op, err := b.SetContainerCount(t.Context(), "runtime", target, target)
			if err != nil {
				t.Fatal(err)
			}
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || state.Done || state.Failed {
				t.Fatalf("unplaceable safe update should wait: %+v %v", state, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			orders := f.apps[id].reloadOrders
			if len(orders) == 0 || orders[len(orders)-1] != "start-first" {
				t.Fatalf("unsafe stop-first update: %v", orders)
			}
		})
	}
}

func TestLifecycleSupersessionRestoresScaleBackPolicyAndReceipt(t *testing.T) {
	for _, action := range []string{"start", "restart", "stop-start"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.MinContainers, spec.MaxContainers = 2, 2
			id := importFake(t, b, spec).Services["runtime"]
			deployFake(t, b, "runtime", "")
			applied := fakeMeta(t, f, "runtime").AppliedSpecification
			if !strings.HasPrefix(applied, "hmac-sha256:v1:") {
				t.Fatalf("specification receipt is not keyed: %q", applied)
			}
			f.mu.Lock()
			f.apps[id].taskCapacity = 2
			f.mu.Unlock()
			if _, err := b.SetContainerCount(t.Context(), "runtime", 3, 3); err != nil {
				t.Fatal(err)
			}
			if _, err := b.SetContainerCount(t.Context(), "runtime", 2, 2); err != nil {
				t.Fatal(err)
			}
			if pending := fakeMeta(t, f, "runtime").Pending; pending == nil || len(pending.RestoreUpdateConfig) == 0 {
				t.Fatal("scale-back did not retain its temporary policy")
			}
			f.mu.Lock()
			f.apps[id].taskCapacity = 0
			f.mu.Unlock()
			b = fakeBackend(t, f, nil)
			if action == "stop-start" {
				stop, err := b.Stop(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				if policy := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"].(map[string]any); policy["Order"] != "start-first" {
					t.Fatalf("Stop lost policy restoration: %v", policy)
				}
				awaitFake(t, b, stop)
				if got := fakeMeta(t, f, "runtime").AppliedSpecification; got != applied {
					t.Fatalf("Stop erased the applied receipt: %q", got)
				}
				b = fakeBackend(t, f, nil)
			}
			if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{"EXAMPLE_SETTING": "next"}); err != nil {
				t.Fatal(err)
			}
			var op infra.Op
			var err error
			if action == "restart" {
				op, err = b.Restart(t.Context(), "runtime")
			} else {
				op, err = b.Start(t.Context(), "runtime")
			}
			if err != nil {
				t.Fatal(err)
			}
			requested := fakeMeta(t, f, "runtime")
			if requested.Pending == nil || requested.Pending.Specification == "" || requested.Pending.Specification == applied {
				t.Fatal("lifecycle reload did not record its changed requested specification")
			}
			if requested.AppliedSpecification != applied {
				t.Fatal("lifecycle reload installed its receipt before completion")
			}
			want := requested.Pending.Specification
			awaitFake(t, fakeBackend(t, f, nil), roundTripOp(t, op))
			if got := fakeMeta(t, f, "runtime").AppliedSpecification; got != want {
				t.Fatalf("successful lifecycle reload lost its receipt: got=%q want=%q", got, want)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			orders := f.apps[id].reloadOrders
			if len(orders) == 0 || orders[len(orders)-1] != "start-first" {
				t.Fatalf("superseding lifecycle used stale stop-first policy: %v", orders)
			}
		})
	}
}
