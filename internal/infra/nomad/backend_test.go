// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	t.Fatalf("operation did not converge: %s", op.Kind)
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
func volumeSpec(name string) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: name, Kind: infra.KindVolume, SpreadGroup: "same-cluster", Resources: infra.Resources{MinDisk: 1, MaxDisk: 1}}
}
func runtimeSpec(name, volume string) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Volume: volume, MinContainers: 1, MaxContainers: 1}
}
func deployFake(t *testing.T, b *Backend, name, volume string) infra.Op {
	t.Helper()
	op, err := b.Deploy(t.Context(), name, testDeployment(t, volume))
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	return op
}
func roundTripOp(t *testing.T, op infra.Op) infra.Op {
	t.Helper()
	data, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var out infra.Op
	if err = json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestVolumeSpreadAndRuntimePlacement(t *testing.T) {
	for _, nodeCount := range []int{3, 2} {
		t.Run(fmt.Sprintf("nodes-%d", nodeCount), func(t *testing.T) {
			f := newFakeNomad(t)
			if nodeCount == 2 {
				f.setNodeEligible("node-c", false)
			}
			b := fakeBackend(t, f, nil)
			counts := map[string]int{}
			for i := range 5 {
				name := fmt.Sprintf("vol-%d", i)
				importFake(t, b, volumeSpec(name))
				r, err := b.load(t.Context(), name)
				if err != nil {
					t.Fatal(err)
				}
				counts[r.Node]++
			}
			if len(counts) != nodeCount {
				t.Fatalf("volume distribution: %v", counts)
			}
			minCount, maxCount := 5, 0
			for _, count := range counts {
				minCount = min(minCount, count)
				maxCount = max(maxCount, count)
			}
			if maxCount-minCount > 1 {
				t.Fatalf("unbalanced volume spread: %v", counts)
			}
			importFake(t, b, runtimeSpec("runtime", "vol-0"))
			deployFake(t, b, "runtime", "vol-0")
			r, err := b.load(t.Context(), "vol-0")
			if err != nil {
				t.Fatal(err)
			}
			containers, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(containers) != 1 || containers[0].PlacementGroup != r.Node {
				t.Fatalf("runtime placement: %+v %v; volume node %s", containers, err, r.Node)
			}
		})
	}
}

func TestChooseNodeSpreadReplaces(t *testing.T) {
	ns := []node{{ID: "a", Name: "a", Status: "ready", SchedulingEligibility: "eligible"}, {ID: "b", Name: "b", Status: "ready", SchedulingEligibility: "eligible"}, {ID: "c", Name: "c", Status: "ready", SchedulingEligibility: "eligible"}}
	rs := []*record{{Hostname: "va", Kind: infra.KindVolume, Node: "a", SpreadGroup: "group"}, {Hostname: "vb", Kind: infra.KindVolume, Node: "b", SpreadGroup: "group"}, {Hostname: "vc", Kind: infra.KindVolume, Node: "c", SpreadGroup: "group"}}
	got, err := chooseNode(ns, rs, "group", "vb")
	if err != nil || got != "b" {
		t.Fatalf("replacement placement = %q, %v; want b", got, err)
	}
	// Other groups affect only the tie-breaker, never anti-affinity.
	rs = append(rs, &record{Hostname: "other", Kind: infra.KindVolume, Node: "b", SpreadGroup: "other"})
	got, err = chooseNode(ns, rs, "group", "vb")
	if err != nil || got != "b" {
		t.Fatalf("replacement ignored group priority: %q %v", got, err)
	}
	for i := range ns {
		ns[i].SchedulingEligibility = "ineligible"
	}
	if _, err = chooseNode(ns, rs, "group", ""); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("unavailable placement: %v", err)
	}
}

func TestOwnershipMasksSecretsAndKeepsVars(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Env = map[string]string{"KARAKORAM_CLUSTER": "private-cluster", "KARAKORAM_AGENT_TOKEN": "private-token", "UNRELATED_SECRET": "private-other"}
	spec.Vars = map[string]string{"KARAKORAM_OWNER": "readable-owner", "KARAKORAM_MEMBER": "readable-member", "PLAIN": "readable-var"}
	importFake(t, b, spec)
	check := func(s infra.Service) {
		t.Helper()
		if !s.Masked {
			t.Fatal("secret ownership was not masked")
		}
		for key := range spec.Env {
			if _, ok := s.Ownership[key]; ok {
				t.Fatalf("secret %s leaked", key)
			}
		}
		if s.Ownership["KARAKORAM_OWNER"] != "readable-owner" || s.Ownership["KARAKORAM_MEMBER"] != "readable-member" {
			t.Fatalf("readable ownership lost: %+v", s.Ownership)
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
	for key := range spec.Env {
		if err = b.SetEnvVars(t.Context(), "runtime", map[string]string{key: "must-not-be-readable"}); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("secret promotion %s: %v", key, err)
		}
	}
	if err = b.SetEnvVars(t.Context(), "runtime", map[string]string{"KARAKORAM_OWNER": "changed"}); err != nil {
		t.Fatal(err)
	}
	if err = b.SetEnvSecrets(t.Context(), "runtime", map[string]string{"KARAKORAM_MEMBER": "now-secret"}); err != nil {
		t.Fatal(err)
	}
	s, err = b.GetService(t.Context(), "runtime")
	if err != nil || s == nil {
		t.Fatal(err)
	}
	if s.Ownership["KARAKORAM_OWNER"] != "changed" {
		t.Fatal("owner update lost")
	}
	if _, ok := s.Ownership["KARAKORAM_MEMBER"]; ok {
		t.Fatal("secret conversion stayed readable")
	}
	v, err := b.readVariable(t.Context(), "nomad/jobs/runtime")
	if err != nil {
		t.Fatal(err)
	}
	if v.Items["UNRELATED_SECRET"] != "private-other" || v.Items["PLAIN"] != "readable-var" {
		t.Fatal("environment merge dropped existing values")
	}
}

func TestDeployPreservesImportedSecretIdentity(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Env = map[string]string{
		"KARAKORAM_CLUSTER":  "private-cluster",
		"KARAKORAM_MEMBER":   "private-member",
		"KARAKORAM_DATA_DIR": b.Layout().DataDir(""),
	}
	spec.Vars = map[string]string{"KARAKORAM_OWNER": "readable-owner"}
	importFake(t, b, spec)
	d := testDeployment(t, "")
	d.Runtime.Env = spec.Env
	op, err := b.Deploy(t.Context(), "runtime", d)
	if err != nil {
		t.Fatal("Deploy rejected repeated imported secret identity:", err)
	}
	awaitFake(t, b, op)
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil {
		t.Fatalf("GetService after deploy: %+v %v", s, err)
	}
	if !s.Masked || s.Ownership["KARAKORAM_OWNER"] != "readable-owner" {
		t.Fatalf("Deploy changed ownership classification: %+v", s)
	}
	v, err := b.readVariable(t.Context(), "nomad/jobs/runtime")
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range spec.Env {
		if _, readable := s.Ownership[key]; readable {
			t.Fatalf("Deploy exposed imported secret identity %s", key)
		}
		if v.Items[key] != value {
			t.Fatalf("Deploy lost imported secret identity %s", key)
		}
		if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{key: "must-remain-secret"}); !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("Deploy allowed later promotion of secret %s: %v", key, err)
		}
	}
}

func TestVolumeNamesAreIsolatedAcrossNamespaces(t *testing.T) {
	f := newFakeNomad(t)
	f.addNamespace("tenant-a")
	f.addNamespace("tenant-b")
	f.addToken(fakeNomadToken, false, "default", "tenant-a", "tenant-b")
	factory := newFactory(testEnv(nil), WithHTTPClient(f.Client()), WithAllowedAddrs(f.URL))
	names := map[string]bool{}
	ids := map[string]bool{}
	node := ""
	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		backend, err := factory.New(infra.Target{
			Kind: "nomad", ID: namespace, Token: fakeNomadToken,
			Config: map[string]string{"address": f.URL},
		})
		if err != nil {
			t.Fatal(err)
		}
		b := backend.(*Backend)
		imported := importFake(t, b, volumeSpec("volume"))
		r, err := b.load(t.Context(), "volume")
		if err != nil || r == nil {
			t.Fatalf("namespace %s lost logical volume: %v", namespace, err)
		}
		if r.Hostname != "volume" || imported.Services["volume"] != r.ID || ids[r.ID] {
			t.Fatalf("namespace %s has an invalid or shared volume identity", namespace)
		}
		ids[r.ID] = true
		var volume struct{ Name, NodeID string }
		if _, err := b.c.get(t.Context(), "/v1/volume/host/"+r.VolumeID, nil, &volume); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(namespace))
		want := fmt.Sprintf("kk-%x-volume", digest[:4])
		if volume.Name != want || names[volume.Name] {
			t.Fatalf("namespace %s physical volume name = %q, want unique %q", namespace, volume.Name, want)
		}
		names[volume.Name] = true
		if node != "" && volume.NodeID != node {
			t.Fatalf("isolation test must exercise one physical node: %q versus %q", node, volume.NodeID)
		}
		node = volume.NodeID
		if node == "" {
			t.Fatal("physical volume has no placement node")
		}
	}
}

func TestImportRejectsUnmanagedJobBeforeMutations(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	f.mu.Lock()
	f.jobs[fakeKey("default", "foreign")] = &fakeJob{row: fakeRow{
		"ID": "foreign", "Namespace": "default", "Status": "running",
		"Meta": fakeRow{"secret": "foreign-job-secret"},
	}}
	f.mu.Unlock()
	_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("volume"), runtimeSpec("foreign", "volume")})
	if !errors.Is(err, infra.ErrConflict) || infra.SafeDetail(err) != "nomad HTTP 409 import: job already exists without a Karakoram service record" {
		t.Fatalf("unmanaged job conflict = %v", err)
	}
	assertSafeMessage(t, err.Error(), f.URL)
	if strings.Contains(err.Error(), fakeNomadToken) || strings.Contains(err.Error(), "foreign-job-secret") {
		t.Fatal("import conflict exposed foreign job data or credentials")
	}
	for _, call := range f.requests() {
		if call.Method != http.MethodGet {
			t.Fatalf("import conflict mutated resources: %+v", call)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobs[fakeKey("default", "foreign")] == nil || len(f.volumes) != 0 || len(f.variables) != 0 {
		t.Fatal("conflicting import changed the unmanaged job or created resources")
	}
}

func TestImportJobLookupFailureIsNotAbsence(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s %s", r.Method, r.URL.Path)
				}
				if r.URL.Path == "/v1/vars" {
					fmt.Fprint(w, "[]")
					return
				}
				http.Error(w, "lookup failed", status)
			}))
			t.Cleanup(server.Close)
			backend, err := newFactory(testEnv(nil), WithAllowedAddrs(server.URL)).New(factoryTarget(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			_, err = backend.ImportServices(t.Context(), []infra.ServiceSpec{runtimeSpec("runtime", "")})
			want := infra.ErrUnavailable
			if status == http.StatusForbidden {
				want = infra.ErrUnauthorized
			}
			if !errors.Is(err, want) {
				t.Fatalf("lookup failure = %v, want %v", err, want)
			}
		})
	}
}

func TestVolumeCreateAlreadyExistsHasSafeConflict(t *testing.T) {
	const rawVolume = "server-volume-identifier"
	const rawNode = "server-node-identifier"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "mkdir: host volume "+rawVolume+" already exists on "+rawNode+"; "+fakeNomadToken+" https://nomad.example.test:4646", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	backend, err := newFactory(testEnv(nil), WithHTTPClient(server.Client()), WithAllowedAddrs(server.URL)).New(factoryTarget(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	b := backend.(*Backend)
	err = b.createVolume(t.Context(), &record{Hostname: "volume", Kind: infra.KindVolume, Node: "node-a"})
	if !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("HTTP 500 host volume already exists did not become conflict: %v", err)
	}
	detail := infra.SafeDetail(err)
	if detail != "nomad HTTP 409 volume create: host volume already exists" {
		t.Fatalf("unstable host-volume conflict diagnostic: %q", detail)
	}
	for _, text := range []string{detail, err.Error()} {
		for _, forbidden := range []string{rawVolume, rawNode, fakeNomadToken, server.URL, "nomad.example.test"} {
			if strings.Contains(text, forbidden) {
				t.Fatal("host-volume conflict exposed raw server identifiers or credentials")
			}
		}
	}
	// The same uncoded RPC message must not turn unrelated API failures into
	// volume conflicts.
	if _, err := b.ProjectInfo(t.Context()); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("unrelated HTTP 500 was reclassified as a volume conflict: %v", err)
	}
}

func TestOwnerMarkersCanBecomeReadable(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Env = map[string]string{"KARAKORAM_OWNER": "secret-owner", "KARAKORAM_RELAY_OWNER": "secret-relay-owner", "KARAKORAM_AGENT_TOKEN": "private-token"}
	importFake(t, b, spec)
	values := map[string]string{"KARAKORAM_OWNER": "owner", "KARAKORAM_RELAY_OWNER": "relay-owner"}
	if err := b.SetEnvVars(t.Context(), "runtime", values); err != nil {
		t.Fatal(err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s == nil {
		t.Fatalf("read owner markers: %+v %v", s, err)
	}
	v, err := b.readVariable(t.Context(), "nomad/jobs/runtime")
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range values {
		if contains(r.SecretKeys, key) || r.Vars[key] != value || s.Ownership[key] != value || v.Items[key] != value {
			t.Fatalf("owner marker %s was not converted to readable", key)
		}
	}
	if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{"KARAKORAM_AGENT_TOKEN": "readable"}); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("non-marker secret promotion = %v", err)
	}
}

func TestRecordsSkipForeignAndUndecodableEntries(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	for hostname, data := range map[string]string{
		"nested/entry": `{}`,
		"Demo":         `{}`,
		"missing":      "",
		"broken":       `{`,
		"mismatch":     `{"ID":"foreign","Hostname":"other","Kind":"runtime"}`,
		"foreign":      `{"ID":"foreign","Hostname":"foreign","Kind":"unmanaged"}`,
	} {
		v := variable{Path: recordPrefix + hostname, Items: map[string]string{"record": data}}
		if err := b.writeVariable(t.Context(), &v); err != nil {
			t.Fatal(err)
		}
	}
	services, err := b.ListServices(t.Context(), "")
	if err != nil || len(services) != 1 || services[0].Hostname != "runtime" {
		t.Fatalf("foreign entries broke listing: %+v %v", services, err)
	}
	importFake(t, b, volumeSpec("volume"))
	if _, err := b.DeleteService(t.Context(), "volume"); !errors.Is(err, infra.ErrConflict) || !strings.Contains(infra.SafeDetail(err), "undecodable karakoram/services record") {
		t.Fatalf("undecodable entries did not explain the volume deletion conflict: %v", err)
	}
	var volumes []fakeRow
	if _, err := b.c.get(t.Context(), "/v1/volumes", query("type", "host"), &volumes); err != nil || len(volumes) != 1 {
		t.Fatalf("conflicted deletion removed the volume: %+v %v", volumes, err)
	}
	for _, hostname := range []string{"missing", "broken", "mismatch", "foreign"} {
		v, err := b.readVariable(t.Context(), recordPrefix+hostname)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.removeVariable(t.Context(), v.Path, v.ModifyIndex); err != nil {
			t.Fatal(err)
		}
	}
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal("non-record paths blocked volume deletion:", err)
	}
	awaitFake(t, b, op)
}
