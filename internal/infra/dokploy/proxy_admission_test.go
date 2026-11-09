// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestProxyAdmissionTemplateOptIn(t *testing.T) {
	f, b, _ := markedProxyFake(t, 1)
	cold := fakeAppSnapshot(t, f, "proxy")["env"]
	if text, ok := cold.(string); ok && strings.Contains(text, "KARAKORAM_PROXY_SERVING_ADMISSION=") {
		t.Fatal("cold deployment opted in")
	}
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	row := fakeAppSnapshot(t, f, "proxy")
	env, _ := row["env"].(string)
	values, err := decodeEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	if values["KARAKORAM_TASK_ID"] != "{{.Task.ID}}" || values["KARAKORAM_PROXY_SERVING_ADMISSION"] != "1" {
		t.Fatal("marked rollout lacks task-bound opt-in")
	}
	// Replacing an opted-in template with a cold deployment removes the gate.
	stripped, err := proxyAdmissionEnv(env, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stripped, "KARAKORAM_TASK_ID=") || strings.Contains(stripped, "KARAKORAM_PROXY_SERVING_ADMISSION=") {
		t.Fatal("cold deployment retained gate")
	}
	awaitFake(t, b, op)
}

func TestProxyAdmissionInventoryStartingAndOwnership(t *testing.T) {
	f, b, a := markedProxyFake(t, 1)
	f.mu.Lock()
	a.taskCapacity = 3
	f.mu.Unlock()
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	a.holdTasks = true
	f.mu.Unlock()
	containers, err := b.Containers(infra.WithFreshTaskInventory(t.Context()), "proxy")
	if err != nil {
		t.Fatal(err)
	}
	starting := false
	for _, task := range containers {
		starting = starting || task.Status == infra.ContainerStarting
		if task.Status == infra.ContainerStarting && (task.IPv4 != "192.0.2.50" || !task.ProxyServingAdmission) {
			t.Fatal("starting task lacks inspected overlay IP and task-local opt-in")
		}
	}
	if !starting {
		t.Fatal("starting task not exposed for admission")
	}
	// A metadata-owned application is insufficient if its actual Swarm service
	// has been replaced by a foreign service.
	f.mu.Lock()
	service := f.services[fakeString(a.row, "appName")]
	if service == nil {
		f.mu.Unlock()
		t.Fatal("missing fake service")
	}
	service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"] = map[string]string{"karakoram.owner": "foreign"}
	f.mu.Unlock()
	if _, err = b.Containers(infra.WithFreshTaskInventory(t.Context()), "proxy"); err == nil {
		t.Fatal("foreign service inventory authorized")
	}
}

func TestProxyAdmissionEnvironmentPreservesMultiline(t *testing.T) {
	for _, value := range []string{
		"first\nKARAKORAM_TASK_ID=inside\nlast",
		"first\nKARAKORAM_PROXY_SERVING_ADMISSION=inside\nlast",
		"first\nKARAKORAM_TASK_ID=final",
		"first\nKARAKORAM_PROXY_SERVING_ADMISSION=final",
	} {
		original := map[string]string{"MULTILINE": value, "KARAKORAM_TASK_ID": "old", "KARAKORAM_PROXY_SERVING_ADMISSION": "old"}
		text, err := encodeEnvironment(original)
		if err != nil {
			t.Fatal(err)
		}
		for _, enabled := range []bool{false, true} {
			updated, err := proxyAdmissionEnv(text, enabled)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeEnvironment(updated)
			if err != nil || decoded["MULTILINE"] != value {
				t.Fatal("multiline environment value corrupted")
			}
			if enabled && (decoded["KARAKORAM_TASK_ID"] != "{{.Task.ID}}" || decoded["KARAKORAM_PROXY_SERVING_ADMISSION"] != "1") {
				t.Fatal("missing opt-in")
			}
			if !enabled && (decoded["KARAKORAM_TASK_ID"] != "" || decoded["KARAKORAM_PROXY_SERVING_ADMISSION"] != "") {
				t.Fatal("retained opt-in")
			}
		}
	}
	if _, err := proxyAdmissionEnv("BROKEN='unterminated", true); err == nil {
		t.Fatal("invalid dotenv accepted")
	}
}

func TestProxyAdmissionTaskInspectionFailsClosed(t *testing.T) {
	for _, failure := range []string{"foreign-service", "wrong-task", "foreign-network", "no-address", "bad-address"} {
		t.Run(failure, func(t *testing.T) {
			f, b, a := markedProxyFake(t, 1)
			op, err := b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2, MinRAM: 1, MaxRAM: 1})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a.holdTasks = true
			taskID := fakeString(a.proxyRollout.next[0], "containerId")
			forceUpdate := fakeInt(a.proxyRollout.next[0], "specForceUpdate")
			serviceID := fakeString(f.services[fakeString(a.row, "appName")], "ID")
			f.mu.Unlock()
			id, network := taskID, b.network()
			addresses := []string{"192.0.2.50/24"}
			switch failure {
			case "foreign-service":
				serviceID = "foreign"
			case "wrong-task":
				id = "other"
			case "foreign-network":
				network = "foreign"
			case "no-address":
				addresses = nil
			case "bad-address":
				addresses = []string{"not-an-ip"}
			}
			f.mu.Lock()
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				if r.URL.Path != "/api/docker.getConfig" || r.URL.Query().Get("containerId") != taskID {
					return false, nil
				}
				json.NewEncoder(w).Encode(map[string]any{
					"ID": id, "ServiceID": serviceID, "DesiredState": "running",
					"Status":              map[string]any{"State": "starting"},
					"Spec":                map[string]any{"ForceUpdate": forceUpdate, "ContainerSpec": map[string]any{"Env": []string{"KARAKORAM_PROXY_SERVING_ADMISSION=1"}}},
					"NetworksAttachments": []map[string]any{{"Network": map[string]any{"ID": network, "Spec": map[string]any{"Name": network}, "DriverState": map[string]any{"Name": "overlay"}}, "Addresses": addresses}},
				})
				return true, nil
			}
			f.mu.Unlock()
			if _, err := b.Containers(infra.WithFreshTaskInventory(t.Context()), "proxy"); err == nil {
				t.Fatal("unverified task address entered admission inventory")
			}
		})
	}
}
