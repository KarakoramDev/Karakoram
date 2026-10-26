// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"regexp"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestNativeAllocationDiskReservation(t *testing.T) {
	for _, tc := range []struct {
		name, engine, floor string
		volume              bool
		port, want          int
	}{
		{"mongo", "", "", true, 27017, 3072},
		{"rabbit", "rabbitmq", "", true, 5672, 2048},
		{"postgres", "postgres", "", true, 5432, 3072},
		{"proxy", "postgres", "", false, 5432, 1024},
		{"elasticsearch", "elasticsearch", "", true, 9200, 4096},
		{"kibana", "elasticsearch", "", false, 5601, 4096},
		{"logstash", "elasticsearch", "", false, 5044, 3072},
		{"apm", "elasticsearch", "", false, 8200, 1024},
		{"supabase-auth", "supabase", "", false, 9999, 2048},
		{"supabase-studio", "supabase", "", false, 3002, 4096},
		{"supabase-functions", "supabase", "", false, 9000, 3072},
		{"supabase-realtime", "supabase", "", false, 4000, 3072},
		{"operator-floor", "postgres", "6144", false, 5432, 6144},
		{"floor-does-not-undercut", "postgres", "512", true, 5432, 3072},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeNomad(t)
			config := map[string]string{}
			if tc.floor != "" {
				config["ephemeralDiskMB"] = tc.floor
			}
			b := fakeBackend(t, f, config)
			mount := ""
			if tc.volume {
				mount = "volume"
				importFake(t, b, volumeSpec(mount))
			}
			importFake(t, b, runtimeSpec("runtime", mount))
			deployment := testDeployment(t, mount)
			deployment.Runtime.Env = map[string]string{"KARAKORAM_ENGINE": tc.engine}
			deployment.Runtime.Ports = []infra.Port{{Port: tc.port, Protocol: "tcp"}}
			op, err := b.Deploy(t.Context(), "runtime", deployment)
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, op)
			f.mu.Lock()
			defer f.mu.Unlock()
			group := f.jobs[fakeKey("default", "runtime")].row["TaskGroups"].([]any)[0].(fakeRow)
			disk, ok := group["EphemeralDisk"].(fakeRow)
			if !ok || fakeInt(disk, "SizeMB") != tc.want || disk["Sticky"] != false || disk["Migrate"] != false {
				t.Fatalf("native rootfs/package/artifact disk reservation missing or migrates disposable state: %#v", group["EphemeralDisk"])
			}
		})
	}
}

func TestControlPlaneAllocationDiskReservation(t *testing.T) {
	job := deploymentFile(t, "karakoram.nomad.hcl")
	if !regexp.MustCompile(`(?s)ephemeral_disk\s*\{\s*size\s*=\s*1024\s+sticky\s*=\s*false\s+migrate\s*=\s*false\s*\}`).MatchString(job) {
		t.Fatal("control-plane rootfs/artifact disk reservation missing")
	}
	if !strings.Contains(job, `KARAKORAM_NOMAD_EPHEMERAL_DISK_MB   = var.member_ephemeral_disk_mb > 0 ? tostring(var.member_ephemeral_disk_mb) : ""`) {
		t.Fatal("shipped control-plane job does not forward the optional member disk floor")
	}
}

func TestDiskExhaustionPlacementDiagnostic(t *testing.T) {
	f, b, op := blockedDeployment(t, true)
	f.mu.Lock()
	for _, e := range f.evals {
		if e["Status"] == "blocked" {
			e["FailedTGAllocs"] = fakeRow{"member": fakeRow{"NodesEvaluated": 3, "NodesExhausted": 3, "DimensionExhausted": fakeRow{"disk": 3}}}
		}
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed || state.Status != "BackendPlacementFailed" || !strings.Contains(state.Message, `dimension "disk" exhausted on 3 nodes`) {
		t.Fatalf("disk placement diagnostic: %+v %v", state, err)
	}
	failures, err := b.PlacementFailures(t.Context(), []string{"runtime"})
	if err != nil || len(failures) != 1 || !strings.Contains(failures[0].Message, `dimension "disk" exhausted on 3 nodes`) {
		t.Fatalf("disk placement observation: %+v %v", failures, err)
	}
}

func TestDiskBudgetPreservesExistingDeployment(t *testing.T) {
	b, r := renderFixture(t)
	if got := b.allocationDiskMB(infra.RuntimeSpec{Args: []string{"relay"}}); got != 1024 {
		t.Fatalf("relay scratch disk = %d, want 1024 MiB", got)
	}
	_, group, _ := renderParts(t, b, r)
	if _, ok := group["EphemeralDisk"]; ok {
		t.Fatal("legacy deployment acquired a new disk policy without Deploy")
	}
	r.Deployment.EphemeralDiskMB = 1024
	b.target.Config["ephemeralDiskMB"] = "8192"
	_, group, _ = renderParts(t, b, r)
	renderJSONEqual(t, group["EphemeralDisk"], `{"SizeMB":1024,"Sticky":false,"Migrate":false}`)
}

func TestDiskBudgetConfiguration(t *testing.T) {
	f := newFactory(testEnv(nil))
	for _, value := range []string{"0", "-1", "NaN", "1.5", "65537", "9223372036854775807", "9999999999999999999999999"} {
		target := factoryTarget("https://nomad.example.test")
		target.Config["ephemeralDiskMB"] = value
		if _, err := f.New(target); err == nil {
			t.Errorf("invalid ephemeralDiskMB %q accepted", value)
		}
	}
	for _, value := range []string{"1", "65536"} {
		target := factoryTarget("https://nomad.example.test")
		target.Config["ephemeralDiskMB"] = value
		if _, err := f.New(target); err != nil {
			t.Fatalf("valid ephemeralDiskMB boundary %q rejected: %v", value, err)
		}
	}
}

func TestDiskPolicyUpgradeDoesNotRollRunningMember(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	// Model a persisted deployment/job created by an older control plane.
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	r.Deployment.EphemeralDiskMB = 0
	if err := b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	group := f.jobs[fakeKey("default", "runtime")].row["TaskGroups"].([]any)[0].(fakeRow)
	delete(group, "EphemeralDisk")
	f.calls = nil
	f.mu.Unlock()
	// A fresh backend observes the old persisted records after a CP upgrade.
	upgraded := fakeBackend(t, f, map[string]string{"ephemeralDiskMB": "8192"})
	for range 3 {
		if _, err := upgraded.GetService(t.Context(), "runtime"); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := group["EphemeralDisk"]; ok {
		t.Fatal("observing an old member changed its disk policy")
	}
	for _, call := range f.calls {
		if call.Method != "GET" {
			t.Fatalf("CP upgrade observation mutated backend: %+v", call)
		}
	}
}

func TestDiskBudgetCorrectedOnIntendedRedeploy(t *testing.T) {
	f, b, _ := blockedDeployment(t, true)
	b.target.Config["ephemeralDiskMB"] = "8192"
	if _, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "volume")); err != nil {
		t.Fatal(err)
	}
	r, err := b.load(t.Context(), "runtime")
	if err != nil || r.Deployment.EphemeralDiskMB != 8192 {
		t.Fatalf("initial budget not persisted: %+v %v", r, err)
	}
	// An explicit deployment is new intent, unlike observation or restart.
	corrected := fakeBackend(t, f, map[string]string{"ephemeralDiskMB": "4096"})
	op, err := corrected.Deploy(t.Context(), "runtime", testDeployment(t, "volume"))
	if err != nil {
		t.Fatal(err)
	}
	f.setNodeEligible("node-a", true)
	awaitFake(t, corrected, op)
	r, err = corrected.load(t.Context(), "runtime")
	if err != nil || r.Deployment.EphemeralDiskMB != 4096 {
		t.Fatalf("corrected deployment retained old budget: %+v %v", r, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	group := f.jobs[fakeKey("default", "runtime")].row["TaskGroups"].([]any)[0].(fakeRow)
	disk := group["EphemeralDisk"].(fakeRow)
	if fakeInt(disk, "SizeMB") != 4096 {
		t.Fatalf("registered job retained old budget: %+v", disk)
	}
}
