// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func awaitFake(t *testing.T, b *Backend, op infra.Op) infra.OpState {
	t.Helper()
	for range 100 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
		if state.Failed {
			t.Fatalf("operation failed: %+v", state)
		}
		if state.Done {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("operation did not converge: %+v", op)
	return infra.OpState{}
}
func importFake(t *testing.T, b *Backend, specs ...infra.ServiceSpec) infra.Op {
	t.Helper()
	op, err := b.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	return op
}
func runtimeSpec(name, volume string) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Volume: volume, MinContainers: 1, MaxContainers: 1}
}
func volumeSpec(name string) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: name, Kind: infra.KindVolume, SpreadGroup: "same-cluster", Resources: infra.Resources{MinDisk: 1, MaxDisk: 1}}
}
func deployFake(t *testing.T, b *Backend, hostname, volume string) infra.Op {
	t.Helper()
	op, err := b.Deploy(t.Context(), hostname, testDeployment(t, volume))
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	return op
}
func fakeAppSnapshot(t *testing.T, f *fakeAPI, name string) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.apps {
		if a.row["name"] == name {
			return fakeCopy(a.row)
		}
	}
	t.Fatalf("fake application %q not found", name)
	return nil
}
func fakeMeta(t *testing.T, f *fakeAPI, name string) metadata {
	t.Helper()
	row := fakeAppSnapshot(t, f, name)
	m, ok := parseMetadata(fakeString(row, "description"))
	if !ok {
		t.Fatal("missing metadata")
	}
	return m
}
func fakeCalls(f *fakeAPI, proc string) int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls[proc] }
func roundTripOp(t *testing.T, op infra.Op) infra.Op {
	t.Helper()
	data, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var decoded infra.Op
	if err = json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestVolumeSpreadAndRuntimePlacement(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("vol-a"), volumeSpec("vol-b"), volumeSpec("vol-c"))
	counts := map[string]int{}
	for _, name := range []string{"vol-a", "vol-b", "vol-c"} {
		counts[fakeMeta(t, f, name).Node]++
	}
	if len(counts) != 3 {
		t.Fatalf("three volumes not spread across three nodes: %v", counts)
	}
	importFake(t, b, volumeSpec("vol-d"))
	counts[fakeMeta(t, f, "vol-d").Node]++
	for node, count := range counts {
		if count < 1 || count > 2 {
			t.Fatalf("unbalanced placement %s: %v", node, counts)
		}
	}
	// Deliberately list the runtime first; import must create its holder first.
	importFake(t, b, runtimeSpec("runtime", "vol-e"), volumeSpec("vol-e"))
	m := fakeMeta(t, f, "vol-e")
	row := fakeAppSnapshot(t, f, "runtime")
	placement := row["placementSwarm"].(map[string]any)
	if !reflect.DeepEqual(placement["Constraints"], []any{"node.hostname==" + m.Node}) {
		t.Fatalf("runtime not pinned to holder: %v", placement)
	}
	deployFake(t, b, "runtime", "vol-e")
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 {
		t.Fatalf("Containers: %+v %v", containers, err)
	}
	if containers[0].PlacementGroup != m.Node || containers[0].Status != infra.ContainerRunning {
		t.Fatalf("container placement/status: %+v, volume node %s", containers[0], m.Node)
	}
	if containers[0].Hostname == "runtime" || !strings.HasSuffix(containers[0].Hostname, "."+containers[0].ID) {
		t.Fatalf("container address is not task-specific overlay DNS: %+v", containers[0])
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var holder, runtime *fakeApplication
	for _, a := range f.apps {
		switch a.row["name"] {
		case "vol-e":
			holder = a
		case "runtime":
			runtime = a
		}
	}
	if holder.row["appName"] == "vol-e" {
		t.Fatal("fake must suffix appName")
	}
	if len(runtime.mounts) != 1 || runtime.mounts[0]["volumeName"] != holder.row["appName"] || runtime.mounts[0]["mountPath"] != b.Layout().VolumePath("vol-e") {
		t.Fatalf("wrong runtime mount: %+v", runtime.mounts)
	}
}

func TestPlacementFilters(t *testing.T) {
	for _, tc := range []struct {
		name        string
		change      func(*fakeAPI)
		want        string
		unavailable bool
	}{
		{"data-label", func(f *fakeAPI) {
			for i := range f.nodes {
				f.nodes[i]["Spec"].(map[string]any)["Labels"] = map[string]any{}
			}
			f.nodes[1]["Spec"].(map[string]any)["Labels"] = map[string]any{"karakoram.data": "true"}
		}, "node-b", false},
		{"drain-and-down", func(f *fakeAPI) {
			f.nodes[0]["Spec"].(map[string]any)["Availability"] = "drain"
			f.nodes[1]["Status"].(map[string]any)["State"] = "down"
		}, "node-c", false},
		{"pause-excluded", func(f *fakeAPI) {
			f.nodes[0]["Spec"].(map[string]any)["Availability"] = "pause"
			f.nodes[2]["Status"].(map[string]any)["State"] = "down"
		}, "node-b", false},
		{"unlabelled-fallback", func(f *fakeAPI) {
			for i := range f.nodes {
				f.nodes[i]["Spec"].(map[string]any)["Labels"] = map[string]any{}
			}
		}, "node-a", false},
		{"none-eligible", func(f *fakeAPI) {
			for i := range f.nodes {
				f.nodes[i]["Status"].(map[string]any)["State"] = "down"
			}
		}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI(t)
			tc.change(f)
			b := fakeBackend(t, f, nil)
			_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("volume")})
			if tc.unavailable {
				if !errors.Is(err, infra.ErrUnavailable) {
					t.Fatalf("ImportServices: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := fakeMeta(t, f, "volume").Node; got != tc.want {
				t.Fatalf("node=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestOwnershipMasksSecretsAndKeepsVars(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Env = map[string]string{"KARAKORAM_CLUSTER": "private-cluster", "KARAKORAM_AGENT_TOKEN": "private-token", "UNRELATED_SECRET": "private-other"}
	spec.Vars = map[string]string{"KARAKORAM_OWNER": "readable-owner", "KARAKORAM_MEMBER": "readable-member", "PLAIN": "readable-var"}
	importFake(t, b, spec)
	check := func(s infra.Service) {
		t.Helper()
		if !s.Masked {
			t.Fatal("secret identity was not marked masked")
		}
		for key := range spec.Env {
			if _, ok := s.Ownership[key]; ok {
				t.Fatalf("secret key %q leaked in Ownership", key)
			}
		}
		if s.Ownership["KARAKORAM_OWNER"] != "readable-owner" || s.Ownership["KARAKORAM_MEMBER"] != "readable-member" {
			t.Fatalf("readable vars absent: %+v", s.Ownership)
		}
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil {
		t.Fatalf("GetService: %+v %v", s, err)
	}
	check(*s)
	list, err := b.ListServices(t.Context(), "runtime")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListServices: %+v %v", list, err)
	}
	check(list[0])
	before := fakeCalls(f, "application.saveEnvironment")
	for key := range spec.Env {
		if err = b.SetEnvVars(t.Context(), "runtime", map[string]string{key: "do-not-expose"}); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("secret promotion %s: %v", key, err)
		}
	}
	if fakeCalls(f, "application.saveEnvironment") != before {
		t.Fatal("rejected secret promotion changed environment")
	}
	if err = b.SetEnvVars(t.Context(), "runtime", map[string]string{"KARAKORAM_OWNER": "changed", "PLAIN": "changed-var"}); err != nil {
		t.Fatal(err)
	}
	if err = b.SetEnvSecrets(t.Context(), "runtime", map[string]string{"KARAKORAM_MEMBER": "now-secret"}); err != nil {
		t.Fatal(err)
	}
	s, err = b.GetService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	if s.Ownership["KARAKORAM_OWNER"] != "changed" {
		t.Fatal("readable owner update lost")
	}
	if _, ok := s.Ownership["KARAKORAM_MEMBER"]; ok {
		t.Fatal("secret conversion still readable")
	}
	values, err := decodeEnvironment(fakeString(fakeAppSnapshot(t, f, "runtime"), "env"))
	if err != nil {
		t.Fatal(err)
	}
	if values["PLAIN"] != "changed-var" || values["UNRELATED_SECRET"] != "private-other" {
		t.Fatal("environment replacement dropped existing values")
	}
}

func TestPrivateDNSAndPublicEndpointModes(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	check := func(want string) {
		t.Helper()
		row := fakeAppSnapshot(t, f, "runtime")
		if want == "vip" {
			if value, exists := row["endpointSpecSwarm"]; !exists || value != nil {
				t.Fatalf("public endpoint override = %#v, want cleared override", value)
			}
			return
		}
		endpoint, ok := row["endpointSpecSwarm"].(map[string]any)
		if !ok || endpoint["Mode"] != want {
			t.Fatalf("endpoint mode = %#v, want %s", row["endpointSpecSwarm"], want)
		}
	}
	check("dnsrr")
	public := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}
	if err := b.EnsurePublicPort(t.Context(), "runtime", public); err != nil {
		t.Fatal(err)
	}
	check("vip")
	deployFake(t, b, "runtime", "")
	check("vip")
	if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	check("dnsrr")
}

func TestPublicPortAllowlistAndReload(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	port := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
	before := fakeCalls(f, "application.reload")
	if err := b.EnsurePublicPort(t.Context(), "runtime", port); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatalf("restrictive allowlist: %v", err)
	}
	if fakeCalls(f, "port.create") != 0 || fakeCalls(f, "application.reload") != before {
		t.Fatal("unsupported allowlist mutated service")
	}
	port.Allowlist = []string{"0.0.0.0/1", "128.0.0.0/1"}
	if err := b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before+1 {
		t.Fatal("new route was not applied by reload")
	}
	if err := b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before+1 || fakeCalls(f, "port.create") != 1 {
		t.Fatal("idempotent route reloaded/recreated service")
	}
	f.mu.Lock()
	for _, a := range f.apps {
		if a.row["name"] == "runtime" {
			if len(a.appliedPorts) != 1 || fakeInt(a.appliedPorts[0], "publishedPort") != port.Port {
				t.Errorf("route not applied to Swarm: %+v", a.appliedPorts)
			}
		}
	}
	f.mu.Unlock()
	if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before+2 {
		t.Fatal("port deletion not reloaded")
	}
	if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before+2 {
		t.Fatal("idempotent delete reloaded")
	}
	port.Port = 18081
	port.IPType = "IPV6"
	port.Allowlist = []string{"::/1", "8000::/1"}
	if err := b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
		t.Fatalf("whole IPv6 family: %v", err)
	}
}

func TestVolumeDeleteMountedAndWipe(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	before := fakeCalls(f, "application.deploy")
	if _, err := b.DeleteService(t.Context(), "volume"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("delete mounted holder: %v", err)
	}
	if fakeCalls(f, "application.deploy") != before {
		t.Fatal("mounted volume started wiping")
	}
	op, err := b.DeleteService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	op, err = b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	row := fakeAppSnapshot(t, f, "volume")
	if row["command"] != "find" || !reflect.DeepEqual(row["args"], []any{"/data", "-mindepth", "1", "-delete"}) {
		t.Fatalf("wipe command: %v %v", row["command"], row["args"])
	}
	if fakeString(row, "description") == "" || !fakeMeta(t, f, "volume").Wiping {
		t.Fatal("wipe state not persisted")
	}
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("wipe must await Complete task: %+v %v", state, err)
	}
	awaitFake(t, b, roundTripOp(t, op))
	s, err := b.GetService(t.Context(), "volume")
	if err != nil || s != nil {
		t.Fatalf("wiped volume retained: %+v %v", s, err)
	}
	if fakeCalls(f, "dockerVolume.removeVolume") != 1 {
		t.Fatal("manager volume not removed after wiping")
	}
}

func TestWipeRetainsHolderOnFailedTask(t *testing.T) {
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
		for _, d := range a.deployments {
			d.status = "done"
		}
		a.tasks[0]["state"] = "shutdown"
		a.tasks[0]["currentState"] = "Failed 1 second ago"
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed || state.Done {
		t.Fatalf("failed wipe: %+v %v", state, err)
	}
	if fakeCalls(f, "application.delete") != 0 || fakeCalls(f, "dockerVolume.removeVolume") != 0 {
		t.Fatal("failed wipe deleted data holder")
	}
}

func TestFencedDeleteRejectsReplacement(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	first := importFake(t, b, runtimeSpec("runtime", ""))
	if _, err := b.DeleteServiceByID(t.Context(), "runtime", first.Services["runtime"]+"wrong"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("wrong fence: %v", err)
	}
	op, err := b.DeleteServiceByID(t.Context(), "runtime", first.Services["runtime"])
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	second := importFake(t, b, runtimeSpec("runtime", ""))
	if second.Services["runtime"] == first.Services["runtime"] {
		t.Fatal("fake reused application ID")
	}
	if _, err = b.DeleteServiceByID(t.Context(), "runtime", first.Services["runtime"]); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("stale fence: %v", err)
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil || s.ID != second.Services["runtime"] {
		t.Fatalf("fence altered replacement: %+v %v", s, err)
	}
}

func TestOperationsSurviveControlPlaneRestart(t *testing.T) {
	for _, action := range []string{"import", "deploy", "stop", "start", "restart", "delete", "wipe"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			var op infra.Op
			var err error
			if action == "wipe" {
				importFake(t, b, volumeSpec("volume"))
				op, err = b.DeleteService(t.Context(), "volume")
			} else {
				op = importFake(t, b, runtimeSpec("runtime", ""))
				if action != "import" {
					if action != "deploy" {
						deployFake(t, b, "runtime", "")
					}
					switch action {
					case "deploy":
						op, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
					case "stop":
						op, err = b.Stop(t.Context(), "runtime")
					case "start":
						stopped, e := b.Stop(t.Context(), "runtime")
						if e != nil {
							t.Fatal(e)
						}
						awaitFake(t, b, stopped)
						op, err = b.Start(t.Context(), "runtime")
					case "restart":
						op, err = b.Restart(t.Context(), "runtime")
					case "delete":
						op, err = b.DeleteService(t.Context(), "runtime")
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if op.ID == "" {
				t.Fatalf("%s has no durable handle", action)
			}
			restarted := fakeBackend(t, f, nil)
			state := awaitFake(t, restarted, roundTripOp(t, op))
			if state.Expired {
				t.Fatalf("live operation expired: %+v", state)
			}
		})
	}
}

func TestExpiredOperationSemantics(t *testing.T) {
	for _, action := range []string{"import", "deploy", "stop", "start", "restart"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			op := importFake(t, b, runtimeSpec("runtime", ""))
			var err error
			if action != "import" {
				deployFake(t, b, "runtime", "")
				switch action {
				case "deploy":
					op, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
				case "stop":
					op, err = b.Stop(t.Context(), "runtime")
				case "start":
					op, err = b.Start(t.Context(), "runtime")
				case "restart":
					op, err = b.Restart(t.Context(), "runtime")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.apps = map[string]*fakeApplication{}
			f.mu.Unlock()
			state, err := fakeBackend(t, f, nil).OpStatus(t.Context(), roundTripOp(t, op))
			if err != nil {
				t.Fatal(err)
			}
			if !state.Expired || state.Done != (action == "import") || state.Failed != (action != "import") {
				t.Fatalf("expired %s: %+v", action, state)
			}
		})
	}
}

func TestStopPreservesDatabaseReplicas(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MaxContainers = 3
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if fakeInt(fakeAppSnapshot(t, f, "runtime"), "replicas") != 3 {
		t.Fatal("stop must not mutate DB replica count")
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil || s.Status != infra.StatusStopped {
		t.Fatalf("stopped GetService: %+v %v", s, err)
	}
	list, err := b.ListServices(t.Context(), "runtime")
	if err != nil || len(list) != 1 || list[0].Status != infra.StatusStopped {
		t.Fatalf("stopped ListServices: %+v %v", list, err)
	}
	op, err = b.Start(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 3 {
		t.Fatalf("start must restore replicas: %+v %v", containers, err)
	}
}

func TestSubdomainLifecycleAndUnsupported(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	if !b.Capabilities().PublicHTTPS {
		t.Fatal("configured domain suffix missing capability")
	}
	op, err := b.SetSubdomainAccess(t.Context(), "runtime", true)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	urls, err := b.SubdomainURLs(t.Context(), "runtime")
	if err != nil || len(urls) != 1 || !strings.HasPrefix(urls[0], "https://runtime-8080-") || !strings.HasSuffix(urls[0], ".apps.example.test") {
		t.Fatalf("HTTPS URLs: %v %v", urls, err)
	}
	if _, err = b.SetSubdomainAccess(t.Context(), "runtime", true); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "domain.create") != 1 {
		t.Fatal("idempotent enable created another domain")
	}
	if _, err = b.SetSubdomainAccess(t.Context(), "runtime", false); err != nil {
		t.Fatal(err)
	}
	urls, err = b.SubdomainURLs(t.Context(), "runtime")
	if err != nil || len(urls) != 0 {
		t.Fatalf("disabled URLs: %v %v", urls, err)
	}
	without := fakeBackend(t, f, map[string]string{"domainSuffix": ""})
	if without.Capabilities().PublicHTTPS {
		t.Fatal("HTTPS capability without suffix")
	}
	if _, err = without.SetSubdomainAccess(t.Context(), "runtime", true); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatalf("no suffix enable: %v", err)
	}
	if _, err = without.SubdomainURLs(t.Context(), "runtime"); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatalf("no suffix URLs: %v", err)
	}
}

func TestBackendErrorsRedactCredentialsAndEnvironment(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   error
	}{{400, "BAD_REQUEST", infra.ErrRejected}, {401, "UNAUTHORIZED", infra.ErrUnauthorized}, {404, "NOT_FOUND", infra.ErrNotFound}, {409, "CONFLICT", infra.ErrConflict}, {500, "INTERNAL_SERVER_ERROR", infra.ErrUnavailable}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			value := "private-env-fixture"
			f.mu.Lock()
			f.failure["application.saveEnvironment"] = fakeResponse{tc.status, tc.code, "rejected " + fakeAPIKey + " " + value}
			f.mu.Unlock()
			err := b.SetEnvSecrets(t.Context(), "runtime", map[string]string{"SECRET": value})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error classification: %v, want %v", err, tc.want)
			}
			for _, private := range []string{fakeAPIKey, value} {
				if strings.Contains(fmt.Sprintf("%+v", err), private) {
					t.Fatal("backend error exposed a private value")
				}
			}
		})
	}
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	bad := "${{private-env-fixture}}"
	_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "runtime", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Env: map[string]string{"SECRET": bad}}})
	if err == nil || strings.Contains(err.Error(), bad) || strings.Contains(err.Error(), fakeAPIKey) {
		t.Fatalf("unsafe validation error: %v", err)
	}
}

func TestEnvironmentLosslessEncoding(t *testing.T) {
	values := map[string]string{"EMPTY": "", "SPACE": " a b ", "EQ": "a=b=c", "HASH": "a#b", "QUOTES": "he said \"hello\"", "BACKSLASH": `a\b\c`, "LITERAL_NEWLINE": `first\nsecond`, "MULTILINE": "first\nsecond\nthird", "DOLLAR": "$HOME ${HOME} $$", "BOTH_QUOTES": "a'b\"c", "ALL_QUOTES": "a'b\"c`d"}
	encoded, err := encodeEnvironment(values)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeEnvironment(encoded)
	if err != nil || !reflect.DeepEqual(decoded, values) {
		t.Fatalf("environment roundtrip mismatch: %v", err)
	}
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Env = values
	importFake(t, b, spec)
	stored, err := decodeEnvironment(fakeString(fakeAppSnapshot(t, f, "runtime"), "env"))
	if err != nil || !reflect.DeepEqual(stored, values) {
		t.Fatalf("saved environment mismatch: %v", err)
	}
	for _, bad := range []map[string]string{{"INVALID-KEY": "value"}, {"SECRET": "${{OTHER}}"}, {"SECRET": "a\x00b"}, {"SECRET": "a\rb"}, {"SECRET": "a'b\"c`d#tail"}} {
		if _, err := encodeEnvironment(bad); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("unsafe environment accepted: %v", err)
		}
	}
}

func TestFakeWireErrorsAndProjectShape(t *testing.T) {
	f := newFakeAPI(t)
	c := newClient(f.server.URL, fakeAPIKey)
	var created map[string]any
	if err := c.post(t.Context(), "project.create", map[string]any{"name": "additional"}, &created); err != nil {
		t.Fatal(err)
	}
	project, ok := created["project"].(map[string]any)
	if !ok {
		t.Fatal("project.create must return wrapped project")
	}
	env, ok := created["environment"].(map[string]any)
	if !ok || env["name"] != "production" {
		t.Fatal("project.create must create default environment")
	}
	var one map[string]any
	if err := c.get(t.Context(), "project.one", map[string]string{"projectId": fakeString(project, "projectId")}, &one); err != nil {
		t.Fatal(err)
	}
	if len(one["environments"].([]any)) != 1 {
		t.Fatal("project.one environments shape")
	}
	for _, tc := range []struct {
		name, proc, key string
		body            map[string]any
		status          int
		code            string
	}{
		{"unauthorized", "environment.one?environmentId=environment", "wrong", nil, 401, ""},
		{"missing", "application.one?applicationId=missing", fakeAPIKey, nil, 404, "NOT_FOUND"},
		{"bad-input", "application.create", fakeAPIKey, map[string]any{}, 400, "BAD_REQUEST"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			var err error
			if tc.body == nil {
				req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, f.server.URL+"/api/"+tc.proc, nil)
			} else {
				data, _ := json.Marshal(tc.body)
				req, err = http.NewRequestWithContext(t.Context(), http.MethodPost, f.server.URL+"/api/"+tc.proc, strings.NewReader(string(data)))
			}
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("x-api-key", tc.key)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			var wire map[string]any
			if err = json.NewDecoder(res.Body).Decode(&wire); err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != tc.status || fakeString(wire, "code") != tc.code || wire["message"] == nil {
				t.Fatalf("error wire shape: %d %+v", res.StatusCode, wire)
			}
			if tc.status == 401 {
				if len(wire) != 1 || wire["message"] != "Unauthorized" {
					t.Fatalf("bad key must have minimal 401 shape: %+v", wire)
				}
			} else {
				data, ok := wire["data"].(map[string]any)
				if !ok || data["httpStatus"] != float64(tc.status) || data["path"] != strings.Split(tc.proc, "?")[0] {
					t.Fatalf("missing tRPC error metadata: %+v", wire)
				}
				if tc.status == 400 && (wire["issues"] == nil || data["zodError"] == nil) {
					t.Fatalf("validation error missing issues: %+v", wire)
				}
			}
		})
	}
	if err := c.post(t.Context(), "network.create", map[string]any{"name": "custom", "driver": "overlay"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.post(t.Context(), "network.create", map[string]any{"name": "custom", "driver": "overlay"}, nil); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("duplicate network is actual HTTP400 despite Docker409 in message: %v", err)
	}
}

func TestNetworkOverridesAndPendingPortSettings(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, map[string]string{"network": "custom-overlay"})
	if _, err := b.validate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "registry.one") != 1 || fakeCalls(f, "network.create") != 1 {
		t.Fatal("validation did not inspect registry/create network")
	}
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	a := f.apps[imported.Services["runtime"]]
	networks := fakeRowsCopy(a.appliedNetworks)
	f.mu.Unlock()
	if len(networks) != 1 || networks[0]["Target"] != "custom-overlay" || !reflect.DeepEqual(networks[0]["Aliases"], []any{"runtime"}) {
		t.Fatalf("private service must have only its target network and alias: %+v", networks)
	}
	id := imported.Services["runtime"]
	// Exercise Dokploy's raw port/reload semantics without a custom EndpointSpec,
	// which otherwise takes precedence over its separate port settings.
	if err := b.c.post(t.Context(), "application.update", map[string]any{"applicationId": id, "endpointSpecSwarm": nil}, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.c.post(t.Context(), "port.create", map[string]any{"applicationId": id, "publishedPort": 19090, "targetPort": 9090, "protocol": "tcp", "publishMode": "ingress"}, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	pending := len(f.apps[id].ports)
	applied := len(f.apps[id].appliedPorts)
	f.mu.Unlock()
	if pending != 1 || applied != 0 {
		t.Fatalf("port.create should only update database: pending=%d applied=%d", pending, applied)
	}
	row := fakeAppSnapshot(t, f, "runtime")
	if err := b.c.post(t.Context(), "application.reload", map[string]any{"applicationId": id, "appName": row["appName"]}, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	applied = len(f.apps[id].appliedPorts)
	f.mu.Unlock()
	if applied != 1 {
		t.Fatal("reload did not apply pending ports")
	}
	if err := b.c.post(t.Context(), "application.saveBuildType", map[string]any{"applicationId": id, "buildType": "dockerfile", "dockerfile": "Dockerfile", "dockerContextPath": ".", "dockerBuildStage": nil, "herokuVersion": nil, "railpackVersion": nil}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSplitImportUsesHolderAppName(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	holderOp := importFake(t, b, volumeSpec("volume"))
	importFake(t, b, runtimeSpec("runtime", "volume"))
	holder := fakeAppSnapshot(t, f, "volume")
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.apps {
		if a.row["name"] == "runtime" {
			if len(a.mounts) != 1 || a.mounts[0]["volumeName"] != holder["appName"] {
				t.Fatalf("split import lost suffixed volume name: %+v", a.mounts)
			}
		}
	}
	if holderOp.Services["volume"] == "" {
		t.Fatal("holder import omitted identity")
	}
}

func TestWipeWaitsForManagerVolumeRelease(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"))
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failure["dockerVolume.removeVolume"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "volume is in use"}
	f.mu.Unlock()
	var state infra.OpState
	for range 10 {
		state, err = b.OpStatus(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
		if fakeCalls(f, "dockerVolume.removeVolume") > 0 {
			break
		}
	}
	if state.Done || state.Failed || fakeCalls(f, "dockerVolume.removeVolume") != 1 {
		t.Fatalf("busy manager volume should stay pending: %+v", state)
	}
	f.mu.Lock()
	delete(f.failure, "dockerVolume.removeVolume")
	f.mu.Unlock()
	awaitFake(t, fakeBackend(t, f, nil), roundTripOp(t, op))
	if fakeCalls(f, "dockerVolume.removeVolume") != 2 {
		t.Fatal("manager volume removal was not retried")
	}
}

func TestWorkerWipeDoesNotRemoveManagerVolume(t *testing.T) {
	f := newFakeAPI(t)
	// Only node-b is data-labelled, so the holder is worker-local.
	f.nodes[0]["Spec"].(map[string]any)["Labels"] = map[string]any{}
	f.nodes[2]["Spec"].(map[string]any)["Labels"] = map[string]any{}
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"))
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if fakeCalls(f, "dockerVolume.removeVolume") != 0 {
		t.Fatal("worker wipe tried manager-only volume removal")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.volumes) != 1 {
		t.Fatal("worker-local empty volume object should remain")
	}
}

func TestPublicPortsIncludesUnmanagedApplications(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	var app map[string]any
	if err := b.c.post(t.Context(), "application.create", map[string]any{"name": "outside-app", "environmentId": "environment", "description": "not managed by Karakoram"}, &app); err != nil {
		t.Fatal(err)
	}
	if err := b.c.post(t.Context(), "port.create", map[string]any{"applicationId": app["applicationId"], "publishedPort": 18080, "targetPort": 8080, "protocol": "tcp", "publishMode": "ingress"}, nil); err != nil {
		t.Fatal(err)
	}
	routes, err := b.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 1 || routes[0].Hostname != "outside-app" || routes[0].Port != 18080 {
		t.Fatalf("target-wide unmanaged ports: %+v %v", routes, err)
	}
	services, err := b.ListServices(t.Context(), "")
	if err != nil || len(services) != 0 {
		t.Fatalf("unmanaged app leaked into managed services: %+v %v", services, err)
	}
}

func TestDeployFailureDoesNotReportSuccess(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	for _, a := range f.apps {
		a.deployments[0].status = "error"
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed || state.Done {
		t.Fatalf("failed newest deployment with old running tasks: %+v %v", state, err)
	}
}

func TestFakeQueuesDeploymentsAndReturnsEnvironmentSummaries(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	body := map[string]any{"applicationId": imported.Services["runtime"]}
	for range 2 {
		if err := b.c.post(t.Context(), "application.deploy", body, nil); err != nil {
			t.Fatal(err)
		}
	}
	var deployments []map[string]any
	if err := b.c.get(t.Context(), "deployment.all", map[string]string{"applicationId": imported.Services["runtime"]}, &deployments); err != nil {
		t.Fatal(err)
	}
	if len(deployments) != 1 || deployments[0]["status"] != "running" {
		t.Fatalf("second accepted submission should queue without immediate deployment row: %+v", deployments)
	}
	for range 3 {
		if err := b.c.get(t.Context(), "deployment.all", map[string]string{"applicationId": imported.Services["runtime"]}, &deployments); err != nil {
			t.Fatal(err)
		}
	}
	if len(deployments) != 2 || deployments[0]["status"] != "done" || deployments[1]["status"] != "done" {
		t.Fatalf("queued deployments did not finish newest-first: %+v", deployments)
	}
	var environment map[string]any
	if err := b.c.get(t.Context(), "environment.one", map[string]string{"environmentId": "environment"}, &environment); err != nil {
		t.Fatal(err)
	}
	summary := environment["applications"].([]any)[0].(map[string]any)
	for _, key := range []string{"appName", "env", "replicas", "mounts", "ports", "domains"} {
		if _, ok := summary[key]; ok {
			t.Fatalf("environment summary unexpectedly includes %s", key)
		}
	}
}

func TestWipeSubmissionFailureCanBeRetried(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"))
	f.mu.Lock()
	f.failure["application.deploy"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "deployment failed"}
	f.mu.Unlock()
	if _, err := b.DeleteService(t.Context(), "volume"); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("failed wipe submission: %v", err)
	}
	f.mu.Lock()
	delete(f.failure, "application.deploy")
	f.mu.Unlock()
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if fakeCalls(f, "application.deploy") != 2 {
		t.Fatal("persistent wiping marker prevented explicit retry")
	}
}

func TestWipeDeploymentFailureRetainsHolder(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"))
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	for _, a := range f.apps {
		a.deployments[0].status = "error"
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed || state.Done {
		t.Fatalf("failed wipe deployment: %+v %v", state, err)
	}
	if fakeCalls(f, "application.delete") != 0 {
		t.Fatal("failed wipe deployment deleted holder")
	}
	retry, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, retry)
}

func TestReplacementVolumeReusesItsSpreadGroupNode(t *testing.T) {
	for _, replaces := range []string{"volume-a", "volume-b", "volume-c"} {
		t.Run(replaces, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			specs := []infra.ServiceSpec{volumeSpec("volume-a"), volumeSpec("volume-b"), volumeSpec("volume-c")}
			for i := range specs {
				specs[i].SpreadGroup = "g"
			}
			importFake(t, b, specs...)
			nodes := map[string]bool{}
			for _, spec := range specs {
				nodes[fakeMeta(t, f, spec.Hostname).Node] = true
			}
			if len(nodes) != 3 {
				t.Fatalf("original group must span three nodes: %v", nodes)
			}
			want := fakeMeta(t, f, replaces).Node
			replacement := volumeSpec("replacement")
			replacement.SpreadGroup = "g"
			replacement.SpreadReplaces = replaces
			importFake(t, b, replacement)
			if got := fakeMeta(t, f, replacement.Hostname).Node; got != want {
				t.Fatalf("replacement placed on %q instead of replaced volume's node %q", got, want)
			}
		})
	}
}

func TestEnvironmentTrailingBackslashUsesUnquotedEncoding(t *testing.T) {
	for _, value := range []string{`abc\`, "a'b\\", "a'b`c\\", `C:\new\`} {
		t.Run(value, func(t *testing.T) {
			values := map[string]string{"A": value, "Z": "x\"`'"}
			encoded, err := encodeEnvironment(values)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(encoded, "A="+value+"\n") {
				t.Fatalf("trailing-backslash value must not have a quoted delimiter: %q", encoded)
			}
			decoded, err := decodeEnvironment(encoded)
			if err != nil || !reflect.DeepEqual(decoded, values) {
				t.Fatalf("following unquoted entry must survive unchanged: %v", err)
			}
		})
	}
	for _, value := range []string{"'quoted\\", "line\nvalue\\", "hash#value\\", " leading\\"} {
		if _, err := encodeEnvironment(map[string]string{"A": value}); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("unrepresentable trailing-backslash value must be rejected: %v", err)
		}
	}
}

func TestContainersAddressEveryStillRunningSwarmTask(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("proxy", ""))
	f.mu.Lock()
	for _, app := range f.apps {
		if app.row["name"] != "proxy" {
			continue
		}
		app.holdTasks = true
		app.tasks = []map[string]any{
			{"containerId": "task-a", "name": "app.1", "state": "running", "currentState": "Running 1 minute ago"},
			{"containerId": "task-b", "name": `\_ app.2`, "state": "shutdown", "currentState": "Running 1 minute ago"},
			{"containerId": "task-c", "name": "app.3", "state": "running", "currentState": "Preparing 1 second ago"},
			{"containerId": "task-d", "name": "app.4", "state": "shutdown", "currentState": "Shutdown 1 minute ago"},
		}
	}
	f.mu.Unlock()
	containers, err := b.Containers(t.Context(), "proxy")
	if err != nil || len(containers) != 3 {
		t.Fatalf("live/stopping/preparing tasks: %+v %v", containers, err)
	}
	for i, want := range []string{"app.1.task-a", "app.2.task-b", "app.3.task-c"} {
		if containers[i].Hostname != want {
			t.Fatalf("task-specific DNS = %q, want %q", containers[i].Hostname, want)
		}
	}
	if containers[1].Status != infra.ContainerRunning || containers[2].Status != infra.StatusCreating {
		t.Fatalf("desired shutdown hid a still-running task: %+v", containers)
	}
}
