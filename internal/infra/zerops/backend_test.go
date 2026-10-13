// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"gopkg.in/yaml.v3"
)

func serviceHandler(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	if strings.HasSuffix(r.URL.Path, "/env") {
		_, _ = w.Write([]byte(`{"items":[]}`))
		return true
	}
	if strings.HasSuffix(r.URL.Path, "/service-stack") {
		reply(t, w, "list")
		return true
	}
	if r.URL.Path == "/service-stack/ExampleID0000000000045" && r.Method == "GET" {
		reply(t, w, "service")
		return true
	}
	if r.URL.Path == "/service-stack/ExampleID0000000000069" && r.Method == "GET" {
		reply(t, w, "member")
		return true
	}
	return false
}
func TestImportNoInlineAndAggregatesEveryProcess(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		var doc struct {
			Services []map[string]any `yaml:"services"`
		}
		if err := yaml.Unmarshal([]byte(body["yaml"]), &doc); err != nil {
			t.Error(err)
		}
		if len(doc.Services) != 2 {
			t.Error("missing services")
		}
		for _, s := range doc.Services {
			if _, ok := s["zeropsYaml"]; ok {
				t.Error("inline pipeline")
			}
			v := s["verticalAutoscaling"].(map[string]any)
			if s["hostname"] == "kkv" && (v["minDisk"] != 5 || v["maxDisk"] != 5) {
				t.Errorf("volume floor: %v", v)
			}
			if s["hostname"] == "kkm" && (s["minContainers"] != 1 || s["envSecrets"].(map[string]any)["TOKEN"] != "secret" || v["cpuMode"] != "SHARED") {
				t.Error("runtime config")
			}
		}
		reply(t, w, "import")
	})
	op, err := b.ImportServices(context.Background(), []infra.ServiceSpec{{Hostname: "kkv", Type: "local-storage:single@1", Resources: infra.Resources{MinDisk: 1, MaxDisk: 2}}, {Hostname: "kkm", Type: "ubuntu@24.04", Env: map[string]string{"TOKEN": "secret"}, ZeropsYAML: "must not import"}})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err = json.Unmarshal([]byte(strings.TrimPrefix(op.ID, "multi:")), &ids); err != nil {
		t.Fatal(err)
	}
	var imported ImportResult
	_ = json.Unmarshal(fixture(t, "import"), &imported)
	want := 0
	for _, s := range imported.ServiceStacks {
		want += len(s.Processes)
	}
	if len(ids) != want {
		t.Fatalf("process IDs %d want %d", len(ids), want)
	}
}
func TestOpAggregateTerminalStates(t *testing.T) {
	statuses := map[string]string{"a": "FINISHED", "b": "RUNNING", "c": "FINISHED"}
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/process/")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "status": statuses[id], "error": map[string]string{"message": "disk failed"}})
	})
	op := infra.Op{ID: `multi:["a","b","c"]`}
	state, err := b.OpStatus(context.Background(), op)
	if err != nil || state.Done {
		t.Fatalf("premature completion %+v %v", state, err)
	}
	for _, status := range []string{"PENDING", "ROLLBACKING", "CANCELING", "UNKNOWN"} {
		statuses["b"] = status
		state, err = b.OpStatus(context.Background(), op)
		if err != nil || state.Done {
			t.Fatalf("pending %s %+v %v", status, state, err)
		}
	}
	statuses["c"] = "FAILED"
	state, err = b.OpStatus(context.Background(), op)
	if err != nil || !state.Done || !state.Failed || state.Message != "disk failed" {
		t.Fatalf("failure %+v %v", state, err)
	}
	statuses["b"], statuses["c"] = "FINISHED", "FINISHED"
	state, err = b.OpStatus(context.Background(), op)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("success %+v %v", state, err)
	}
	statuses["c"] = "CANCELED"
	state, _ = b.OpStatus(context.Background(), op)
	if !state.Failed {
		t.Fatal("canceled not failed")
	}
	state, err = b.OpStatus(context.Background(), infra.Op{})
	if err != nil || !state.Done {
		t.Fatal("no-op not complete")
	}
}
func TestResourcesPreservesCompleteConfig(t *testing.T) {
	var bodies []map[string]json.RawMessage
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if serviceHandler(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/autoscaling") {
			var body struct {
				Custom map[string]json.RawMessage `json:"customAutoscaling"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			bodies = append(bodies, body.Custom)
			reply(t, w, "autoscaling")
			return
		}
		t.Errorf("unexpected %s", r.URL.Path)
	})
	op, err := b.SetResources(context.Background(), "controlplane", infra.Resources{MaxDisk: 3})
	if err != nil || op.ID == "" {
		t.Fatalf("resources %v", err)
	}
	if _, err = b.SetContainerCount(context.Background(), "controlplane", 2, 2); err != nil {
		t.Fatal(err)
	}
	var s ServiceStack
	_ = json.Unmarshal(fixture(t, "service"), &s)
	oldV, _ := decodeObject(s.CurrentAutoscaling["verticalAutoscaling"])
	customV, _ := decodeObject(s.CustomAutoscaling["verticalAutoscaling"])
	oldV["minFreeResource"] = customV["minFreeResource"]
	for _, body := range bodies {
		v, _ := decodeObject(body["verticalAutoscaling"])
		for _, k := range []string{"cpuMode", "startCpuCoreCount", "swapEnabled", "minFreeResource", "minResource"} {
			var got, want any
			_ = json.Unmarshal(v[k], &got)
			_ = json.Unmarshal(oldV[k], &want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("reset %s", k)
			}
		}
	}
	v, _ := decodeObject(bodies[0]["verticalAutoscaling"])
	high, _ := decodeObject(v["maxResource"])
	if number(high, "diskGBytes") != 3 {
		t.Fatal("disk unchanged")
	}
	h, _ := decodeObject(bodies[1]["horizontalAutoscaling"])
	if number(h, "minContainerCount") != 2 || number(h, "maxContainerCount") != 2 {
		t.Fatal("horizontal unchanged")
	}
}
func TestSecretsWaitWithoutRestartAndSkipUnchanged(t *testing.T) {
	var creates, updates, polls int
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/env") {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []EnvItem{{ID: "same", Key: "SAME", Content: "same", Type: "USER", Sensitive: true}, {ID: "old", Key: "OLD", Content: "before", Type: "USER"}}})
			return
		}
		if serviceHandler(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/user-data"):
			creates++
			reply(t, w, "createSecret")
		case strings.HasPrefix(r.URL.Path, "/user-data/"):
			updates++
			reply(t, w, "updateSecret")
		case strings.HasPrefix(r.URL.Path, "/process/"):
			polls++
			reply(t, w, "finished")
		default:
			t.Errorf("unexpected action %s", r.URL.Path)
		}
	})
	if err := b.SetEnvSecrets(context.Background(), "controlplane", map[string]string{"NEW": "new", "OLD": "after", "SAME": "same"}); err != nil {
		t.Fatal(err)
	}
	if creates != 1 || updates != 1 || polls != 2 {
		t.Fatalf("writes %d %d polls %d", creates, updates, polls)
	}
}
func TestRoutingIdempotencySynchronizationAndDeletion(t *testing.T) {
	var current []PublicPortRouting
	var creates, deletes, syncs int
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if serviceHandler(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/public-port-routing") && r.Method == "GET":
			_ = json.NewEncoder(w).Encode(page[PublicPortRouting]{Total: len(current), List: current})
		case strings.HasSuffix(r.URL.Path, "/public-port-routing") && r.Method == "POST":
			creates++
			var p PublicPortRouting
			_ = json.NewDecoder(r.Body).Decode(&p)
			p.ID = "route"
			current = append(current, p)
			reply(t, w, "route")
		case strings.HasPrefix(r.URL.Path, "/public-port-routing/"):
			deletes++
			for i := range current {
				current[i].DeleteOnSync = true
			}
			reply(t, w, "deleteRoute")
		case strings.HasSuffix(r.URL.Path, "/sync-public-port-routing"):
			syncs++
			out := current[:0]
			for _, p := range current {
				if !p.DeleteOnSync {
					p.IsSynced = true
					out = append(out, p)
				}
			}
			current = out
			reply(t, w, "deploy")
		case strings.HasPrefix(r.URL.Path, "/process/"):
			reply(t, w, "finished")
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	})
	p := infra.PublicPort{Port: 19443, TargetPort: 9443, IPType: "IPV6", Protocol: "tcp", Allowlist: []string{"2001:db8::/64"}}
	for range 2 {
		if err := b.EnsurePublicPort(context.Background(), "member", p); err != nil {
			t.Fatal(err)
		}
	}
	if creates != 1 || syncs != 1 {
		t.Fatalf("not idempotent create %d sync %d", creates, syncs)
	}
	p.Allowlist = []string{"2001:db8:1::/64"}
	if err := b.EnsurePublicPort(context.Background(), "member", p); err != nil {
		t.Fatal(err)
	}
	if creates != 2 || deletes != 1 || syncs != 3 {
		t.Fatalf("replacement %d %d %d", creates, deletes, syncs)
	}
	if err := b.DeletePublicPorts(context.Background(), "member"); err != nil {
		t.Fatal(err)
	}
	if len(current) != 0 || deletes != 2 || syncs != 4 {
		t.Fatal("routing cleanup not synchronized")
	}
}
func TestResolveCacheInvalidationAndProtection(t *testing.T) {
	var lists, details, mutations int
	absent := false
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/env") {
			_, _ = w.Write([]byte(`{"items":[]}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/service-stack") {
			lists++
			if absent {
				_ = json.NewEncoder(w).Encode(page[ServiceStack]{})
				return
			}
			reply(t, w, "list")
			return
		}
		if r.Method == "GET" {
			details++
			if absent {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"code":"serviceStackNotFound","message":"Service stack not found"}}`)
				return
			}
			reply(t, w, "service")
			return
		}
		mutations++
		reply(t, w, "deploy")
	})
	for range 2 {
		s, err := b.GetService(context.Background(), "controlplane")
		if err != nil || s == nil {
			t.Fatalf("resolve %v", err)
		}
	}
	if lists != 1 || details != 2 {
		t.Fatalf("cache counts %d %d", lists, details)
	}
	if _, err := b.DeleteService(context.Background(), "core"); err == nil {
		t.Fatal("allowed core")
	}
	if mutations != 0 {
		t.Fatal("mutated core")
	}
	absent = true
	s, err := b.GetService(context.Background(), "controlplane")
	if err != nil || s != nil || lists != 2 {
		t.Fatalf("stale ID not invalidated %v %+v lists %d", err, s, lists)
	}
}

func TestNotFoundPlatformErrors(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   bool
	}{
		{http.StatusNotFound, "", true},
		{http.StatusBadRequest, "serviceStackNotFound", true},
		{http.StatusBadRequest, "invalidCustomAutoscalingValue", false},
		{http.StatusForbidden, "serviceStackNotFound", false},
	} {
		if got := notFound(&APIError{StatusCode: tc.status, Code: tc.code}); got != tc.want {
			t.Fatalf("notFound(%d, %q)=%t want %t", tc.status, tc.code, got, tc.want)
		}
	}
}
func TestDeployStreamsArtifactAndLifecycle(t *testing.T) {
	var uploads, lifecycle int
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if serviceHandler(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/app-version"):
			reply(t, w, "appVersion")
		case strings.HasSuffix(r.URL.Path, "/upload"):
			uploads++
			data, _ := io.ReadAll(r.Body)
			if string(data) != "archive" || r.ContentLength != 7 {
				t.Error("wrong streamed artifact")
			}
			reply(t, w, "upload")
		case strings.HasSuffix(r.URL.Path, "/deploy"):
			reply(t, w, "deploy")
		case strings.HasSuffix(r.URL.Path, "/restart") || strings.HasSuffix(r.URL.Path, "/start") || strings.HasSuffix(r.URL.Path, "/stop"):
			lifecycle++
			reply(t, w, "deploy")
		case r.Method == "DELETE":
			reply(t, w, "deploy")
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	})
	for _, reader := range []io.Reader{strings.NewReader("archive"), io.LimitReader(strings.NewReader("archive"), 7)} {
		if _, err := b.Deploy(context.Background(), "controlplane", infra.Deployment{Artifact: reader, ZeropsYAML: "zerops: []", SetupName: "controlplane", Filename: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, fn := range []func(context.Context, string) (infra.Op, error){b.Start, b.Stop, b.Restart, b.DeleteService} {
		if _, err := fn(context.Background(), "controlplane"); err != nil {
			t.Fatal(err)
		}
	}
	if uploads != 2 || lifecycle != 3 {
		t.Fatalf("calls %d %d", uploads, lifecycle)
	}
}
func TestWaitCancellationAndNullAutoscaling(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if serviceHandler(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/autoscaling") {
			_, _ = w.Write([]byte(`{"process":null}`))
			return
		}
		_ = json.NewEncoder(w).Encode(Process{ID: "p", Status: "RUNNING"})
	})
	op, err := b.SetResources(context.Background(), "controlplane", infra.Resources{MaxDisk: 3})
	if err != nil || op.ID != "" {
		t.Fatalf("null process %v %+v", err, op)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err = b.waitOp(ctx, infra.Op{ID: "p"}); err == nil {
		t.Fatal("wait ignored cancellation")
	}
}

func TestContainerObservedResourcesAndCreatingNulls(t *testing.T) {
	creating := false
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if serviceHandler(t, w, r) {
			return
		}
		if creating {
			_, _ = w.Write([]byte(`{"count":1,"total":1,"limit":20,"offset":0,"list":[{"id":"new","hostname":null,"status":"CREATING","currentHardwareResource":null}]}`))
			return
		}
		reply(t, w, "containers")
	})
	containers, err := b.Containers(context.Background(), "member")
	if err != nil || len(containers) != 1 {
		t.Fatalf("containers: %v", err)
	}
	resources := containers[0].Resources
	if resources == nil || resources.CPUCores != 1 || resources.MemoryGB != 0.25 || resources.DiskGB != 1 {
		t.Fatalf("observed allocation: %+v", resources)
	}
	creating = true
	containers, err = b.Containers(context.Background(), "member")
	if err != nil || len(containers) != 1 || containers[0].Hostname != "" || containers[0].Resources != nil {
		t.Fatalf("creating container: %+v %v", containers, err)
	}
}

func TestAutoscalingPreservesNullableAndExplicitFreeThresholds(t *testing.T) {
	errorFixture, err := os.ReadFile("testdata/autoscaling-error.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, override := range []json.RawMessage{
		json.RawMessage(`{"cpuCoreCount":null,"cpuCorePercent":null,"memoryGBytes":null,"memoryPercent":null}`),
		json.RawMessage(`{"cpuCoreCount":null,"cpuCorePercent":20,"memoryGBytes":null,"memoryPercent":30}`),
		json.RawMessage(`{"cpuCoreCount":0.1,"cpuCorePercent":null,"memoryGBytes":0.125,"memoryPercent":null}`),
	} {
		t.Run(string(override), func(t *testing.T) {
			var service ServiceStack
			_ = json.Unmarshal(fixture(t, "service"), &service)
			custom, _ := decodeObject(service.CustomAutoscaling["verticalAutoscaling"])
			custom["minFreeResource"] = override
			putJSON(service.CustomAutoscaling, "verticalAutoscaling", custom)
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/service-stack/ExampleID0000000000045" {
					_ = json.NewEncoder(w).Encode(service)
					return
				}
				if serviceHandler(t, w, r) {
					return
				}
				var body struct {
					Custom map[string]json.RawMessage `json:"customAutoscaling"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				vertical, _ := decodeObject(body.Custom["verticalAutoscaling"])
				var got, want any
				_ = json.Unmarshal(vertical["minFreeResource"], &got)
				_ = json.Unmarshal(override, &want)
				if !reflect.DeepEqual(got, want) {
					w.WriteHeader(400)
					_, _ = w.Write(errorFixture)
					return
				}
				reply(t, w, "autoscaling")
			})
			if _, err := b.SetResources(context.Background(), "controlplane", infra.Resources{MaxDisk: 3}); err != nil {
				t.Fatalf("resource update lost nullable threshold semantics: %v", err)
			}
			if _, err := b.SetContainerCount(context.Background(), "controlplane", 2, 2); err != nil {
				t.Fatalf("horizontal update lost nullable threshold semantics: %v", err)
			}
		})
	}
}
