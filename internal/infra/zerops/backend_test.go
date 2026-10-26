// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
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
	var imported ImportResult
	if err := json.Unmarshal(fixture(t, "import"), &imported); err != nil {
		t.Fatal(err)
	}
	imported.ServiceStacks[0].Name = "kkv"
	imported.ServiceStacks[1].Name = "kkm"
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
			if s["hostname"] == "kkm" && (s["minContainers"] != 1 || s["envSecrets"].(map[string]any)["TOKEN"] != "secret" || s["envVariables"].(map[string]any)["KARAKORAM_OWNER"] != "marker" || v["cpuMode"] != "SHARED") {
				t.Error("runtime config")
			}
		}
		_ = json.NewEncoder(w).Encode(imported)
	})
	op, err := b.ImportServices(context.Background(), []infra.ServiceSpec{{Hostname: "kkv", Kind: infra.KindVolume, Resources: infra.Resources{MinDisk: 1, MaxDisk: 2}}, {Hostname: "kkm", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Env: map[string]string{"TOKEN": "secret"}, Vars: map[string]string{"KARAKORAM_OWNER": "marker"}}})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err = json.Unmarshal([]byte(strings.TrimPrefix(op.ID, "multi:")), &ids); err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, s := range imported.ServiceStacks {
		want += len(s.Processes)
		if s.Name == "kkv" || s.Name == "kkm" {
			if op.Services[s.Name] != s.ID {
				t.Errorf("imported service %s: got %q want %q", s.Name, op.Services[s.Name], s.ID)
			}
		}
	}
	if len(ids) != want {
		t.Fatalf("process IDs %d want %d", len(ids), want)
	}
	if len(op.Services) != 2 {
		t.Fatalf("import ownership included services outside the request: %v", op.Services)
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
	oldV["startCpuCoreCount"] = customV["startCpuCoreCount"]
	oldV["swapEnabled"] = customV["swapEnabled"]
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

func TestAutoscalingPreservesAndClampsStartCPU(t *testing.T) {
	explicit := 3.0
	for _, tc := range []struct {
		name           string
		customStart    string
		customSwap     string
		customState    string
		currentStart   float64
		resources      infra.Resources
		containerCount bool
		wantStart      string
		wantMinCPU     float64
		wantMaxCPU     float64
	}{
		{name: "platform_default", customStart: "null", resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "null", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "absent_custom_start", customState: "absent_start", resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "null", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "null_custom_vertical", customState: "null_vertical", resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "null", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "absent_custom_vertical", customState: "absent_vertical", resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "null", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "custom_start_clamped_up", customStart: "1", resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "2", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "custom_start_clamped_down", customStart: "4", currentStart: 4, resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "2", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "explicit_start", customStart: "1", resources: infra.Resources{MinCPU: 2, MaxCPU: 4, StartCPU: &explicit, CPUMode: "DEDICATED"}, wantStart: "3", wantMinCPU: 2, wantMaxCPU: 4},
		{name: "explicit_start_not_clamped", customStart: "1", resources: infra.Resources{MinCPU: 2, MaxCPU: 2, StartCPU: &explicit}, wantStart: "3", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "widening_keeps_custom_start", customStart: "1", resources: infra.Resources{MinCPU: 1, MaxCPU: 4}, wantStart: "1", wantMinCPU: 1, wantMaxCPU: 4},
		{name: "effective_start_clamped_up", customState: "no_custom", resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "2", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "effective_start_clamped_down", customState: "no_custom", currentStart: 4, resources: infra.Resources{MinCPU: 2, MaxCPU: 2}, wantStart: "2", wantMinCPU: 2, wantMaxCPU: 2},
		{name: "only_minimum_changed", customStart: "1", resources: infra.Resources{MinCPU: 2}, wantStart: "2", wantMinCPU: 2, wantMaxCPU: 4},
		{name: "only_maximum_changed", customStart: "4", currentStart: 4, resources: infra.Resources{MaxCPU: 2}, wantStart: "2", wantMinCPU: 1, wantMaxCPU: 2},
		{name: "container_count_platform_default", customStart: "null", containerCount: true, wantStart: "null", wantMinCPU: 1, wantMaxCPU: 1},
		{name: "container_count_custom_start", customStart: "1", containerCount: true, wantStart: "1", wantMinCPU: 1, wantMaxCPU: 1},
		{name: "container_count_effective_start", customState: "no_custom", containerCount: true, wantStart: "1", wantMinCPU: 1, wantMaxCPU: 1},
		{name: "container_count_explicit_swap_disabled", customStart: "null", customSwap: "false", containerCount: true, wantStart: "null", wantMinCPU: 1, wantMaxCPU: 1},
		{name: "container_count_explicit_swap_enabled", customStart: "null", customSwap: "true", containerCount: true, wantStart: "null", wantMinCPU: 1, wantMaxCPU: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var service ServiceStack
			if err := json.Unmarshal(fixture(t, "service"), &service); err != nil {
				t.Fatal(err)
			}
			current, _ := decodeObject(service.CurrentAutoscaling["verticalAutoscaling"])
			if tc.currentStart != 0 {
				putJSON(current, "startCpuCoreCount", tc.currentStart)
				high, _ := decodeObject(current["maxResource"])
				putJSON(high, "cpuCoreCount", tc.currentStart)
				putJSON(current, "maxResource", high)
			}
			if tc.resources.MinCPU > 0 && tc.resources.MaxCPU == 0 {
				high, _ := decodeObject(current["maxResource"])
				putJSON(high, "cpuCoreCount", 4)
				putJSON(current, "maxResource", high)
			}
			putJSON(service.CurrentAutoscaling, "verticalAutoscaling", current)
			custom, _ := decodeObject(service.CustomAutoscaling["verticalAutoscaling"])
			if tc.customStart != "" {
				custom["startCpuCoreCount"] = json.RawMessage(tc.customStart)
			}
			if tc.customSwap != "" {
				custom["swapEnabled"] = json.RawMessage(tc.customSwap)
			}
			if tc.customState == "absent_start" {
				delete(custom, "startCpuCoreCount")
			}
			putJSON(service.CustomAutoscaling, "verticalAutoscaling", custom)
			switch tc.customState {
			case "no_custom":
				service.CustomAutoscaling = nil
			case "null_vertical":
				service.CustomAutoscaling["verticalAutoscaling"] = json.RawMessage("null")
			case "absent_vertical":
				delete(service.CustomAutoscaling, "verticalAutoscaling")
			}
			var got map[string]any
			var writes int
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/service-stack/"+service.ID && r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(service)
					return
				}
				if serviceHandler(t, w, r) {
					return
				}
				if r.URL.Path != "/service-stack/"+service.ID+"/autoscaling" || r.Method != http.MethodPut {
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				writes++
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Error(err)
				}
				reply(t, w, "autoscaling")
			})
			var err error
			if tc.containerCount {
				_, err = b.SetContainerCount(context.Background(), service.Name, 2, 2)
			} else {
				_, err = b.SetResources(context.Background(), service.Name, tc.resources)
			}
			if err != nil || writes != 1 {
				t.Fatalf("update: %v, writes %d", err, writes)
			}
			var want map[string]any
			if err := json.Unmarshal([]byte(`{"customAutoscaling":{"verticalAutoscaling":{"minResource":{"cpuCoreCount":1,"memoryGBytes":0.25,"diskGBytes":1},"maxResource":{"cpuCoreCount":1,"memoryGBytes":0.5,"diskGBytes":2},"minFreeResource":{"cpuCoreCount":null,"cpuCorePercent":null,"memoryGBytes":null,"memoryPercent":null},"cpuMode":"SHARED","startCpuCoreCount":null,"swapEnabled":null},"horizontalAutoscaling":{"minContainerCount":1,"maxContainerCount":1}}}`), &want); err != nil {
				t.Fatal(err)
			}
			config := want["customAutoscaling"].(map[string]any)
			vertical := config["verticalAutoscaling"].(map[string]any)
			vertical["minResource"].(map[string]any)["cpuCoreCount"] = tc.wantMinCPU
			vertical["maxResource"].(map[string]any)["cpuCoreCount"] = tc.wantMaxCPU
			var start any
			if err := json.Unmarshal([]byte(tc.wantStart), &start); err != nil {
				t.Fatal(err)
			}
			vertical["startCpuCoreCount"] = start
			if tc.resources.CPUMode != "" {
				vertical["cpuMode"] = tc.resources.CPUMode
			}
			if tc.customSwap != "" {
				var swap bool
				if err := json.Unmarshal([]byte(tc.customSwap), &swap); err != nil {
					t.Fatal(err)
				}
				vertical["swapEnabled"] = swap
			}
			if tc.customState == "no_custom" {
				if err := json.Unmarshal([]byte(`{"cpuCoreCount":0.10000000149011612,"cpuCorePercent":0,"memoryGBytes":0.0625,"memoryPercent":0}`), &start); err != nil {
					t.Fatal(err)
				}
				vertical["minFreeResource"] = start
				vertical["swapEnabled"] = true
			} else if tc.customState == "null_vertical" || tc.customState == "absent_vertical" {
				vertical["minFreeResource"] = nil
			}
			if tc.containerCount {
				config["horizontalAutoscaling"] = map[string]any{"minContainerCount": float64(2), "maxContainerCount": float64(2)}
			}
			if !reflect.DeepEqual(got, want) {
				gotJSON, _ := json.Marshal(got)
				wantJSON, _ := json.Marshal(want)
				t.Fatalf("PUT body:\n got %s\nwant %s", gotJSON, wantJSON)
			}
		})
	}
}

func TestAutoscalingResetsFreeThresholdsExceedingCapacity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		free      string
		resources infra.Resources
		noCustom  bool
		want      string
	}{
		{name: "memory", free: `{"cpuCoreCount":0.1,"cpuCorePercent":20,"memoryGBytes":0.5,"memoryPercent":30}`, resources: infra.Resources{MaxRAM: 0.25}, want: `{"cpuCoreCount":0.1,"cpuCorePercent":20,"memoryGBytes":null,"memoryPercent":30}`},
		{name: "cpu", free: `{"cpuCoreCount":2,"cpuCorePercent":20,"memoryGBytes":0.125,"memoryPercent":30}`, resources: infra.Resources{MaxCPU: 1}, want: `{"cpuCoreCount":null,"cpuCorePercent":20,"memoryGBytes":0.125,"memoryPercent":30}`},
		{name: "both", free: `{"cpuCoreCount":2,"cpuCorePercent":null,"memoryGBytes":1,"memoryPercent":null}`, resources: infra.Resources{MaxCPU: 1, MaxRAM: 0.25}, want: `{"cpuCoreCount":null,"cpuCorePercent":null,"memoryGBytes":null,"memoryPercent":null}`},
		{name: "equal_to_capacity", free: `{"cpuCoreCount":1,"cpuCorePercent":20,"memoryGBytes":0.25,"memoryPercent":30}`, resources: infra.Resources{MaxCPU: 1, MaxRAM: 0.25}, want: `{"cpuCoreCount":1,"cpuCorePercent":20,"memoryGBytes":0.25,"memoryPercent":30}`},
		{name: "effective_fallback", free: `{"cpuCoreCount":2,"cpuCorePercent":0,"memoryGBytes":1,"memoryPercent":0}`, noCustom: true, resources: infra.Resources{MaxCPU: 1, MaxRAM: 0.25}, want: `{"cpuCoreCount":null,"cpuCorePercent":0,"memoryGBytes":null,"memoryPercent":0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var service ServiceStack
			if err := json.Unmarshal(fixture(t, "service"), &service); err != nil {
				t.Fatal(err)
			}
			config := service.CustomAutoscaling
			if tc.noCustom {
				service.CustomAutoscaling = nil
				config = service.CurrentAutoscaling
			}
			vertical, _ := decodeObject(config["verticalAutoscaling"])
			vertical["minFreeResource"] = json.RawMessage(tc.free)
			putJSON(config, "verticalAutoscaling", vertical)
			var got json.RawMessage
			var writes int
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/service-stack/"+service.ID && r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(service)
					return
				}
				if serviceHandler(t, w, r) {
					return
				}
				if r.URL.Path != "/service-stack/"+service.ID+"/autoscaling" || r.Method != http.MethodPut {
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				writes++
				var body struct {
					Custom map[string]json.RawMessage `json:"customAutoscaling"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				vertical, _ := decodeObject(body.Custom["verticalAutoscaling"])
				got = vertical["minFreeResource"]
				reply(t, w, "autoscaling")
			})
			if _, err := b.SetResources(context.Background(), service.Name, tc.resources); err != nil {
				t.Fatal(err)
			}
			var actual, expected any
			if err := json.Unmarshal(got, &actual); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.want), &expected); err != nil {
				t.Fatal(err)
			}
			if writes != 1 || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("free thresholds: got %s, want %s, writes %d", got, tc.want, writes)
			}
		})
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
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/process"):
			fakePage(w, r, []Process{})
		case strings.HasSuffix(r.URL.Path, "/user-data"):
			_ = json.NewEncoder(w).Encode(Process{ID: "receipt"})
		case r.URL.Path == "/process/receipt":
			_ = json.NewEncoder(w).Encode(Process{ID: "receipt", Status: "FINISHED"})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/app-version"):
			fakePage(w, r, []AppVersion{})
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
		if _, err := b.Deploy(context.Background(), "controlplane", infra.Deployment{Artifact: infra.Artifact{Reader: reader, Name: "test", Binary: "karakoram"}, Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}}}); err != nil {
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

func TestContainersAndCreatingNulls(t *testing.T) {
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
	if err != nil || len(containers) != 1 || containers[0].Status != "ACTIVE" {
		t.Fatalf("containers: %+v %v", containers, err)
	}
	creating = true
	containers, err = b.Containers(context.Background(), "member")
	if err != nil || len(containers) != 1 || containers[0].Hostname != "" || containers[0].Status != "CREATING" {
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

func TestSubdomainAccessSchedulesAndPollsDurableOperations(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		action := "disable-subdomain-access"
		if enabled {
			action = "enable-subdomain-access"
		}
		t.Run(action, func(t *testing.T) {
			var writes, polls int
			status := "PENDING"
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/project/ExampleID0000000000077/service-stack":
					_, _ = w.Write([]byte(`{"total":1,"list":[{"id":"service-id","name":"dashboard"}]}`))
				case "/service-stack/service-id":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "service-id", "name": "dashboard", "projectId": "ExampleID0000000000077", "subdomainAccess": !enabled})
				case "/service-stack/service-id/" + action:
					writes++
					if r.Method != "PUT" {
						t.Errorf("method %s", r.Method)
					}
					body, err := io.ReadAll(r.Body)
					if err != nil || string(body) != "{}" {
						t.Errorf("body %q: %v", body, err)
					}
					_, _ = w.Write([]byte(`{"id":"subdomain-process","status":"PENDING"}`))
				case "/process/subdomain-process":
					polls++
					if r.Method != "GET" {
						t.Errorf("poll method %s", r.Method)
					}
					_ = json.NewEncoder(w).Encode(Process{ID: "subdomain-process", Status: status})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			ctx := context.Background()
			op, err := b.SetSubdomainAccess(ctx, "dashboard", enabled)
			want := infra.Op{ID: "subdomain-process", Kind: action, ServiceID: "service-id"}
			if err != nil || !reflect.DeepEqual(op, want) || writes != 1 || polls != 0 {
				t.Fatalf("schedule %+v: %v, writes %d polls %d", op, err, writes, polls)
			}
			// Reconstruct both backend and operation: polling needs no in-memory state.
			restored, err := New(Options{BaseURL: b.client.base, Token: "test-token", ProjectID: b.client.project, MaxRetries: -1})
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			var durable infra.Op
			if err := json.Unmarshal(data, &durable); err != nil {
				t.Fatal(err)
			}
			for _, terminal := range []string{"PENDING", "RUNNING", "FINISHED", "FAILED", "CANCELED"} {
				status = terminal
				state, err := restored.OpStatus(ctx, durable)
				done := terminal == "FINISHED" || terminal == "FAILED" || terminal == "CANCELED"
				failed := terminal == "FAILED" || terminal == "CANCELED"
				if err != nil || state.Done != done || state.Failed != failed || state.Status != terminal {
					t.Fatalf("%s: %+v %v", terminal, state, err)
				}
			}
		})
	}
}

func TestSubdomainAccessMatchingStateIsDone(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/project/ExampleID0000000000077/service-stack":
					_, _ = w.Write([]byte(`{"total":1,"list":[{"id":"service-id","name":"dashboard"}]}`))
				case "/service-stack/service-id":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "service-id", "name": "dashboard", "subdomainAccess": enabled})
				default:
					t.Errorf("no-op made request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			ctx := context.Background()
			op, err := b.SetSubdomainAccess(ctx, "dashboard", enabled)
			if err != nil || op.ID != "" || op.ServiceID != "service-id" {
				t.Fatalf("matching state %+v %v", op, err)
			}
			state, err := b.OpStatus(ctx, op)
			if err != nil || !state.Done || state.Failed {
				t.Fatalf("no-op status %+v %v", state, err)
			}
		})
	}
}

func TestSubdomainAccessRefusesProtectedAndForeignServices(t *testing.T) {
	for _, tc := range []struct {
		name, category, project string
		system                  bool
	}{
		{name: "core"},
		{name: "controlplane", system: true},
		{name: "controlplane", category: "CORE"},
		{name: "controlplane", category: "SYSTEM"},
		{name: "dashboard", project: "other-project"},
	} {
		t.Run(tc.name+tc.category+tc.project, func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/project/ExampleID0000000000077/service-stack":
					_ = json.NewEncoder(w).Encode(map[string]any{"total": 1, "list": []map[string]string{{"id": "service-id", "name": tc.name}}})
				case "/service-stack/service-id":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "service-id", "name": tc.name, "isSystem": tc.system, "projectId": tc.project, "serviceStackTypeInfo": map[string]string{"serviceStackTypeCategory": tc.category}})
				default:
					t.Errorf("protected service made request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			for _, enabled := range []bool{true, false} {
				if _, err := b.SetSubdomainAccess(context.Background(), tc.name, enabled); err == nil {
					t.Fatal("accepted protected or foreign service")
				}
			}
			if _, err := b.SubdomainURLs(context.Background(), tc.name); err == nil {
				t.Fatal("exposed protected or foreign service")
			}
		})
	}
}

func TestSubdomainURLsEffectiveHTTPPorts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		service   string
		project   string
		want      []string
		wantError bool
		wantGETs  int
	}{
		{
			name:     "enabled",
			service:  `{"subdomainAccess":true,"nestId":"test1","ports":[{"port":5672,"scheme":"tcp"},{"port":15672,"scheme":"http"},{"port":8080,"httpSupport":true},{"port":9443,"scheme":"https"},{"port":9000,"httpSupport":false}]}`,
			project:  `{"zeropsSubdomainHost":"example"}`,
			want:     []string{"https://dashboard-example-15672.test1.zerops.app", "https://dashboard-example-8080.test1.zerops.app", "https://dashboard-example-9443.test1.zerops.app"},
			wantGETs: 1,
		},
		{name: "disabled", service: `{"subdomainAccess":false,"nestId":"test1","ports":[{"port":15672,"scheme":"http"}]}`},
		{name: "no-http", service: `{"subdomainAccess":true,"nestId":"test1","ports":[{"port":5672,"scheme":"tcp"}]}`},
		{name: "no-ports", service: `{"subdomainAccess":true,"nestId":"test1","ports":[]}`},
		{name: "missing-host", service: `{"subdomainAccess":true,"nestId":"test1","ports":[{"port":15672,"scheme":"http"}]}`, project: `{}`, wantError: true, wantGETs: 1},
		{name: "missing-region", service: `{"subdomainAccess":true,"ports":[{"port":15672,"scheme":"http"}]}`, project: `{"zeropsSubdomainHost":"example"}`, wantError: true, wantGETs: 1},
		{name: "project-error", service: `{"subdomainAccess":true,"nestId":"test1","ports":[{"port":15672,"scheme":"http"}]}`, wantError: true, wantGETs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var projectGETs int
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("URL observation made %s request", r.Method)
				}
				switch r.URL.Path {
				case "/project/ExampleID0000000000077/service-stack":
					_, _ = w.Write([]byte(`{"total":1,"list":[{"id":"service-id","name":"dashboard"}]}`))
				case "/service-stack/service-id":
					var service map[string]any
					if err := json.Unmarshal([]byte(tc.service), &service); err != nil {
						t.Fatal(err)
					}
					service["id"], service["name"] = "service-id", "dashboard"
					_ = json.NewEncoder(w).Encode(service)
				case "/project/ExampleID0000000000077":
					projectGETs++
					if tc.project == "" {
						w.WriteHeader(http.StatusForbidden)
						_, _ = w.Write([]byte(`{"error":{"code":"accessDenied"}}`))
					} else {
						_, _ = w.Write([]byte(tc.project))
					}
				default:
					t.Errorf("unexpected %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			urls, err := b.SubdomainURLs(context.Background(), "dashboard")
			if (err != nil) != tc.wantError || !reflect.DeepEqual(urls, tc.want) || projectGETs != tc.wantGETs {
				t.Fatalf("URLs %v err %v project GETs %d", urls, err, projectGETs)
			}
		})
	}
}

func resourceDiagnosticService() ServiceStack {
	return ServiceStack{ID: "service", Name: "dbzero", CurrentAutoscaling: map[string]json.RawMessage{
		"verticalAutoscaling":   json.RawMessage(`{"cpuMode":"SHARED","startCpuCoreCount":1,"minResource":{"cpuCoreCount":1,"memoryGBytes":0.5,"diskGBytes":1},"maxResource":{"cpuCoreCount":1,"memoryGBytes":1,"diskGBytes":4}}`),
		"horizontalAutoscaling": json.RawMessage(`{"minContainerCount":1,"maxContainerCount":1}`),
	}}
}

func TestResourceUpdateRejectionReportsSentValues(t *testing.T) {
	for _, containers := range []bool{false, true} {
		t.Run(map[bool]string{false: "resources", true: "containers"}[containers], func(t *testing.T) {
			var sent map[string]json.RawMessage
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/service-stack"):
					_ = json.NewEncoder(w).Encode(map[string]any{"list": []ServiceStack{resourceDiagnosticService()}})
				case r.Method == http.MethodGet:
					_ = json.NewEncoder(w).Encode(resourceDiagnosticService())
				case strings.HasSuffix(r.URL.Path, "/autoscaling"):
					var body struct {
						Custom map[string]json.RawMessage `json:"customAutoscaling"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					sent = body.Custom
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write(validationFixture(t, "startCpuBelowMin"))
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
			})
			var err error
			if containers {
				_, err = b.SetContainerCount(t.Context(), "dbzero", 2, 3)
			} else {
				start := 1.0
				_, err = b.SetResources(t.Context(), "dbzero", infra.Resources{MinCPU: 2, MaxCPU: 2, StartCPU: &start})
			}
			var api *APIError
			if !errors.As(err, &api) || api.Code != "invalidCustomAutoscalingValue" || !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("structured error lost: %v", err)
			}
			for _, want := range []string{"Zerops rejected the resource update for dbzero", "invalidCustomAutoscalingValue", "startCpuCoreCount: Value must not be less than minResource.cpuCoreCount", "; attempted " + attemptedResources(sent), "startCpu=1", "ram=0.5-1", "disk=1-4"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("missing %q in %s", want, err)
				}
			}
			want := "cpu=2-2"
			if containers {
				want = "containers=2-3"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("missing sent bounds %q in %s", want, err)
			}
		})
	}
}

func TestAutoscalingProcessFailureRetainsRequestDiagnostics(t *testing.T) {
	var failed struct{ Error json.RawMessage }
	if err := json.Unmarshal(validationFixture(t, "startCpuBelowMin"), &failed); err != nil {
		t.Fatal(err)
	}
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/service-stack"):
			_ = json.NewEncoder(w).Encode(map[string]any{"list": []ServiceStack{resourceDiagnosticService()}})
		case strings.HasPrefix(r.URL.Path, "/process/"):
			_ = json.NewEncoder(w).Encode(Process{ID: "update", Status: "FAILED", Error: failed.Error})
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(resourceDiagnosticService())
		case strings.HasSuffix(r.URL.Path, "/autoscaling"):
			_ = json.NewEncoder(w).Encode(map[string]Process{"process": {ID: "update", Status: "PENDING"}})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
	start := 1.0
	op, err := b.SetResources(t.Context(), "dbzero", infra.Resources{MinCPU: 2, MaxCPU: 2, StartCPU: &start})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var durable infra.Op
	if err := json.Unmarshal(raw, &durable); err != nil {
		t.Fatal(err)
	}
	state, err := b.OpStatus(t.Context(), durable)
	if err != nil || !state.Failed {
		t.Fatalf("process state: %+v %v", state, err)
	}
	for _, want := range []string{"for dbzero", "invalidCustomAutoscalingValue", "startCpuCoreCount: Value must not be less", "cpu=2-2 startCpu=1", "containers=1-1"} {
		if !strings.Contains(state.Message, want) {
			t.Fatalf("missing %q in %s", want, state.Message)
		}
	}
	if err := b.waitOp(t.Context(), durable); err == nil || !strings.Contains(err.Error(), state.Message) {
		t.Fatalf("wait lost process diagnostic: %v", err)
	}
	// Operations persisted before request diagnostics can still resolve the host.
	legacy, err := b.OpStatus(t.Context(), infra.Op{ID: "update", Kind: "autoscaling", ServiceID: "service"})
	if err != nil || !strings.Contains(legacy.Message, "for dbzero") {
		t.Fatalf("legacy process host missing: %+v %v", legacy, err)
	}
}

func TestImportRejectionReportsEffectiveValuesWithoutEnvironment(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(validationFixture(t, "startCpuBelowMin"))
	})
	start := 1.0
	_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{
		Hostname: "dbzero", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Env: map[string]string{"PASSWORD": "do-not-show-secret"}, Vars: map[string]string{"DATA": "do-not-show-variable"},
		Resources: infra.Resources{MinCPU: 2, MaxCPU: 2, StartCPU: &start, MinRAM: 0.5, MaxRAM: 1, MinDisk: 1, MaxDisk: 4},
	}})
	if err == nil {
		t.Fatal("missing import rejection")
	}
	for _, want := range []string{"Zerops rejected the service import for dbzero", "invalidCustomAutoscalingValue", "attempted dbzero cpuMode=SHARED cpu=2-2 startCpu=1 ram=0.5-1 disk=1-4 containers=1-1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %q in %s", want, err)
		}
	}
	if strings.Contains(err.Error(), "do-not-show") || strings.Contains(err.Error(), "PASSWORD") {
		t.Fatalf("environment leaked: %s", err)
	}
}

func TestMetadataProcessObservationDoesNotAddDeploymentPollingFloor(t *testing.T) {
	b := &Backend{pollInterval: 2 * time.Second}
	// Replay the four ~50ms receipt writes observed before each Z17 deploy
	// using a fake clock. Process completion must still be observed positively.
	for _, kind := range []string{"receipt", "secret"} {
		t.Run(kind, func(t *testing.T) {
			var elapsed time.Duration
			for range 4 {
				var at time.Duration
				for attempt := 0; at < 50*time.Millisecond; attempt++ {
					at += b.waitOpInterval(infra.Op{Kind: kind}, attempt == 0)
				}
				elapsed += at
			}
			if elapsed > 400*time.Millisecond {
				t.Fatalf("four metadata writes cost %s despite 50ms native completion; want <=400ms", elapsed)
			}
			if got := b.waitOpInterval(infra.Op{Kind: kind}, false); got != b.pollInterval {
				t.Fatalf("slow metadata polling accelerated indefinitely: %s", got)
			}
		})
	}
	for _, kind := range []string{"deploy", "import", "routing", "stop"} {
		if got := b.waitOpInterval(infra.Op{Kind: kind}, true); got != b.pollInterval {
			t.Fatalf("%s polling cadence changed: %s", kind, got)
		}
	}
	b.pollInterval = 10 * time.Millisecond
	if got := b.waitOpInterval(infra.Op{Kind: "receipt"}, true); got != b.pollInterval {
		t.Fatalf("explicit short polling interval was increased: %s", got)
	}
}
