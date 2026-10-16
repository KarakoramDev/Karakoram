// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package fake_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func importServices(t *testing.T, b *fake.Backend, specs ...infra.ServiceSpec) infra.Op {
	t.Helper()
	op, err := b.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	return op
}
func runtime(name, volume string) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, MinContainers: 1, MaxContainers: 1, Volume: volume}
}
func volume(name string) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: name, Kind: infra.KindVolume}
}
func deploy(volume string, reader io.Reader) infra.Deployment {
	return infra.Deployment{Artifact: infra.Artifact{Reader: reader, Name: "karakoram.tar.gz", Binary: "karakoram"}, Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Env: map[string]string{"VISIBLE": "yes"}, Volume: volume, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9090, Protocol: "tcp"}}, Readiness: &infra.Readiness{Args: []string{"agent", "readiness"}, FailureTimeout: time.Minute, RetryPeriod: time.Second}}}
}
func get(t *testing.T, b *fake.Backend, name string) *infra.Service {
	t.Helper()
	s, err := b.GetService(t.Context(), name)
	if err != nil || s == nil {
		t.Fatalf("GetService: %+v %v", s, err)
	}
	return s
}
func TestDefaults(t *testing.T) {
	b := fake.New(fake.Options{})
	if b.Name() != "fake" || b.Layout() != (infra.Layout{AppDir: "/srv/karakoram", VolumeRoot: "/srv/volumes", User: "karakoram"}) {
		t.Fatalf("defaults: %s %+v", b.Name(), b.Layout())
	}
	c := b.Capabilities()
	if !c.PublicTCP || !c.TCPAllowlist || !c.PublicHTTPS || !c.DiskGrowth || !c.VolumeQuota || !c.Logs || !c.HorizontalScaling || !c.DedicatedCPU {
		t.Fatalf("capabilities: %+v", c)
	}
	b.SetToken("replacement")
}
func TestImportAtomicConflictAndIdentity(t *testing.T) {
	for _, masked := range []bool{false, true} {
		t.Run(fmt.Sprint(masked), func(t *testing.T) {
			b := fake.New(fake.Options{MaskSecrets: masked})
			spec := runtime("runtime", "")
			spec.Env = map[string]string{"KARAKORAM_OWNER": "secretowner", "KARAKORAM_CLUSTER": "cluster", "KARAKORAM_MEMBER": "member", "KARAKORAM_AGENT_TOKEN": "token", "KARAKORAM_DATA_DIR": "/data", "KARAKORAM_RELAY_OWNER": "relay", "PASSWORD": "neverexpose"}
			spec.Vars = map[string]string{"KARAKORAM_OWNER": "publicowner", "ORDINARY": "notidentity"}
			op := importServices(t, b, spec, volume("volume"))
			s := get(t, b, "runtime")
			if len(op.Services) != 2 || op.Services["runtime"] != s.ID || op.Services["volume"] == s.ID {
				t.Fatalf("import identities: %+v", op)
			}
			if s.Masked != masked || s.Ownership["KARAKORAM_OWNER"] != "publicowner" {
				t.Fatalf("ownership: %+v", s)
			}
			for key, value := range spec.Env {
				if key == "PASSWORD" || key == "KARAKORAM_OWNER" {
					continue
				}
				got, ok := s.Ownership[key]
				if masked && ok || !masked && got != value {
					t.Fatalf("identity %s=%q present=%v masked=%v", key, got, ok, masked)
				}
			}
			if _, ok := s.Ownership["PASSWORD"]; ok {
				t.Fatal("password exposed")
			}
			if _, ok := s.Ownership["ORDINARY"]; ok {
				t.Fatal("nonidentity var exposed")
			}
			spec.Env["KARAKORAM_CLUSTER"] = "changed"
			s.Ownership["KARAKORAM_OWNER"] = "changed"
			if get(t, b, "runtime").Ownership["KARAKORAM_OWNER"] != "publicowner" {
				t.Fatal("observed ownership aliases state")
			}
			_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{runtime("new", ""), spec})
			if !errors.Is(err, infra.ErrConflict) {
				t.Fatal(err)
			}
			absent, err := b.GetService(t.Context(), "new")
			if err != nil || absent != nil {
				t.Fatal("conflicting import partially applied")
			}
		})
	}
}
func TestDeploymentConsumesArtifactAndCopiesSpec(t *testing.T) {
	b := fake.New(fake.Options{})
	importServices(t, b, volume("volume"), runtime("runtime", "volume"))
	reader := strings.NewReader(strings.Repeat("archive", 100))
	d := deploy("volume", reader)
	if _, err := b.Deploy(t.Context(), "runtime", d); err != nil {
		t.Fatal(err)
	}
	if reader.Len() != 0 {
		t.Fatal("artifact not fully consumed")
	}
	d.Runtime.Args[0] = "changed"
	d.Runtime.Env["VISIBLE"] = "changed"
	d.Runtime.Readiness.Args[0] = "changed"
	observed, ok := b.Deployed("runtime")
	if !ok || observed.Args[0] != "agent" || observed.Env["VISIBLE"] != "yes" || observed.Readiness.Args[0] != "agent" {
		t.Fatalf("runtime aliases caller: %+v", observed)
	}
	observed.Args[0] = "changed"
	again, _ := b.Deployed("runtime")
	if again.Args[0] != "agent" {
		t.Fatal("Deployed aliases state")
	}
	if _, ok := b.Deployed("absent"); ok {
		t.Fatal("absent service deployed")
	}
	if _, ok := b.Deployed("volume"); ok {
		t.Fatal("volume deployed")
	}
	s := get(t, b, "runtime")
	if s.Status != infra.StatusActive {
		t.Fatal(s.Status)
	}
	if _, err := b.SetContainerCount(t.Context(), "runtime", 3, 4); err != nil {
		t.Fatal(err)
	}
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 3 {
		t.Fatalf("containers: %+v %v", containers, err)
	}
	for _, c := range containers {
		if c.Status != infra.ContainerActive {
			t.Fatal(c)
		}
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func TestDeploymentFailuresLeaveNoRuntime(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*infra.Deployment)
		want  error
	}{
		{"reader", func(d *infra.Deployment) { d.Artifact.Reader = failingReader{} }, io.ErrUnexpectedEOF},
		{"invalid", func(d *infra.Deployment) { d.Artifact.Reader = nil }, infra.ErrRejected},
		{"missingvolume", func(d *infra.Deployment) { d.Runtime.Volume = "missing" }, infra.ErrConflict},
		{"base", func(d *infra.Deployment) { d.Runtime.Base = "unknown" }, infra.ErrRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.New(fake.Options{})
			importServices(t, b, runtime("runtime", ""))
			d := deploy("", strings.NewReader("archive"))
			tc.alter(&d)
			op, err := b.Deploy(t.Context(), "runtime", d)
			if !errors.Is(err, tc.want) || op.ID != "" {
				t.Fatalf("Deploy: %+v %v", op, err)
			}
			if tc.name == "invalid" {
				var rejected *infra.RejectedError
				if !errors.As(err, &rejected) || rejected.Code != "invalidDeployment" {
					t.Fatalf("invalid deployment classification: %v", err)
				}
			}
			if _, ok := b.Deployed("runtime"); ok {
				t.Fatal("failed deployment recorded")
			}
		})
	}
}
func TestSingleWriterAndFencedDelete(t *testing.T) {
	b := fake.New(fake.Options{})
	importServices(t, b, volume("volume"), runtime("first", "volume"), runtime("second", ""))
	if _, err := b.Deploy(t.Context(), "first", deploy("volume", strings.NewReader("archive"))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Deploy(t.Context(), "second", deploy("volume", strings.NewReader("archive"))); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	if _, err := b.DeleteService(t.Context(), "volume"); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	id := get(t, b, "first").ID
	if _, err := b.DeleteServiceByID(t.Context(), "first", "wrong"); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	if _, err := b.DeleteServiceByID(t.Context(), "first", id); err != nil {
		t.Fatal(err)
	}
	importServices(t, b, runtime("first", ""))
	if _, err := b.DeleteServiceByID(t.Context(), "first", id); !errors.Is(err, infra.ErrConflict) {
		t.Fatal("hostname reuse not fenced", err)
	}
	if _, err := b.Deploy(t.Context(), "second", deploy("volume", strings.NewReader("archive"))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DeleteService(t.Context(), "second"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DeleteService(t.Context(), "volume"); err != nil {
		t.Fatal(err)
	}
	if op, err := b.DeleteServiceByID(t.Context(), "absent", "wrong"); err != nil || op.ID != "" {
		t.Fatalf("absent delete: %+v %v", op, err)
	}
}
func TestResourcesPreserveZerosAndRejectNegatives(t *testing.T) {
	b := fake.New(fake.Options{})
	spec := runtime("runtime", "")
	start := 2.0
	spec.Resources = infra.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 4, MinRAM: 2, MaxRAM: 4, MinDisk: 1, MaxDisk: 3, StartCPU: &start}
	importServices(t, b, spec, volume("volume"))
	start = 99
	if _, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 3}); err != nil {
		t.Fatal(err)
	}
	want := spec.Resources
	start = 2
	want.MinRAM = 3
	if got := get(t, b, "runtime").Resources; !reflect.DeepEqual(got, want) {
		t.Fatalf("resources: %+v want %+v", got, want)
	}
	for _, r := range []infra.Resources{{MinCPU: -1}, {MaxCPU: -1}, {MinRAM: -1}, {MaxRAM: -1}, {MinDisk: -1}, {MaxDisk: -1}, {StartCPU: func() *float64 { v := -1.0; return &v }()}} {
		_, err := b.SetResources(t.Context(), "runtime", r)
		var rejected *infra.RejectedError
		if !errors.As(err, &rejected) || !errors.Is(err, infra.ErrRejected) {
			t.Fatalf("negative resources: %v", err)
		}
	}
	for _, bounds := range [][2]int{{0, 0}, {2, 1}, {-1, 1}} {
		if _, err := b.SetContainerCount(t.Context(), "runtime", bounds[0], bounds[1]); !errors.Is(err, infra.ErrRejected) {
			t.Fatal(err)
		}
	}
	restricted := fake.New(fake.Options{Capabilities: infra.Capabilities{VolumeQuota: true}})
	importServices(t, restricted, volume("volume"), runtime("runtime", ""))
	if _, err := restricted.SetResources(t.Context(), "volume", infra.Resources{MinDisk: 2}); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := restricted.SetContainerCount(t.Context(), "runtime", 1, 2); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := restricted.SetContainerCount(t.Context(), "runtime", 1, 1); err != nil {
		t.Fatal(err)
	}
}
func TestRoutesHTTPSAndLogs(t *testing.T) {
	b := fake.New(fake.Options{})
	importServices(t, b, runtime("first", ""), runtime("second", ""))
	port := infra.PublicPort{Port: 18080, TargetPort: 8080, Allowlist: []string{"192.0.2.0/24"}}
	if err := b.EnsurePublicPort(t.Context(), "first", port); err != nil {
		t.Fatal(err)
	}
	port.Allowlist[0] = "changed"
	if err := b.EnsurePublicPort(t.Context(), "second", port); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	routes, err := b.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 1 || routes[0].Allowlist[0] != "192.0.2.0/24" {
		t.Fatalf("routes: %+v %v", routes, err)
	}
	routes[0].Allowlist[0] = "changed"
	routes, _ = b.ListPublicPorts(t.Context())
	if routes[0].Allowlist[0] != "192.0.2.0/24" {
		t.Fatal("routes alias state")
	}
	if _, err := b.Deploy(t.Context(), "first", deploy("", strings.NewReader("archive"))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetSubdomainAccess(t.Context(), "first", true); err != nil {
		t.Fatal(err)
	}
	urls, err := b.SubdomainURLs(t.Context(), "first")
	if err != nil || len(urls) != 1 || !strings.Contains(urls[0], "8080") {
		t.Fatalf("HTTP ports: %v %v", urls, err)
	}
	now := time.Now()
	b.AddLog("first", infra.LogLine{Timestamp: now.Add(-time.Minute), Container: "one", Message: "old"}, infra.LogLine{Timestamp: now, Container: "one", Message: "current"}, infra.LogLine{Timestamp: now, Container: "two", Message: "other"})
	lines, err := b.Logs(t.Context(), "first", infra.LogQuery{Since: now, Container: "one", Limit: 1})
	if err != nil || len(lines) != 1 || lines[0].Message != "current" {
		t.Fatalf("logs: %+v %v", lines, err)
	}
	lines[0].Message = "changed"
	lines, _ = b.Logs(t.Context(), "first", infra.LogQuery{Limit: 1})
	if lines[0].Message != "other" {
		t.Fatal("logs alias state")
	}
	noAllowlist := fake.New(fake.Options{Capabilities: infra.Capabilities{PublicTCP: true}})
	importServices(t, noAllowlist, runtime("runtime", ""))
	if err := noAllowlist.EnsurePublicPort(t.Context(), "runtime", port); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal(err)
	}
}
func TestEnvironmentConversion(t *testing.T) {
	for _, key := range []string{"KARAKORAM_OWNER", "KARAKORAM_RELAY_OWNER"} {
		t.Run(key, func(t *testing.T) {
			b := fake.New(fake.Options{MaskSecrets: true})
			importServices(t, b, runtime("runtime", ""))
			if err := b.SetEnvSecrets(t.Context(), "runtime", map[string]string{key: "secret"}); err != nil {
				t.Fatal(err)
			}
			if s := get(t, b, "runtime"); !s.Masked || s.Ownership[key] != "" {
				t.Fatal(s)
			}
			if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{key: "public"}); err != nil {
				t.Fatal(err)
			}
			if s := get(t, b, "runtime"); s.Masked || s.Ownership[key] != "public" {
				t.Fatal(s)
			}
			if err := b.SetEnvSecrets(t.Context(), "runtime", map[string]string{key: "private"}); err != nil {
				t.Fatal(err)
			}
			if s := get(t, b, "runtime"); !s.Masked || s.Ownership[key] != "" {
				t.Fatal(s)
			}
		})
	}
}
func TestEnvironmentRefusesSecretExposure(t *testing.T) {
	for _, key := range []string{"KARAKORAM_CLUSTER", "KARAKORAM_AGENT_TOKEN", "PASSWORD"} {
		t.Run(key, func(t *testing.T) {
			b := fake.New(fake.Options{})
			spec := runtime("runtime", "")
			spec.Env = map[string]string{key: ""}
			spec.Vars = map[string]string{"KARAKORAM_OWNER": "original"}
			importServices(t, b, spec)
			err := b.SetEnvVars(t.Context(), "runtime", map[string]string{key: "readable", "KARAKORAM_OWNER": "changed"})
			var rejected *infra.RejectedError
			if !errors.Is(err, infra.ErrRejected) || !errors.As(err, &rejected) {
				t.Fatalf("secret exposure accepted: %v", err)
			}
			if s := get(t, b, "runtime"); s.Ownership["KARAKORAM_OWNER"] != "original" || s.Ownership[key] != "" {
				t.Fatalf("rejected write changed environment: %+v", s)
			}
			if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{"KARAKORAM_MEMBER": "new"}); err != nil {
				t.Fatalf("new readable variable rejected: %v", err)
			}
		})
	}
}

func TestServiceStatusChangesOnOperationCompletion(t *testing.T) {
	for _, polls := range []int{0, 2} {
		t.Run(fmt.Sprint(polls), func(t *testing.T) {
			b := fake.New(fake.Options{Polls: polls})
			check := func(op infra.Op, before, after string) {
				t.Helper()
				want := before
				if polls == 0 {
					want = after
				}
				if got := get(t, b, "runtime").Status; got != want {
					t.Fatalf("before polling %s: status %q, want %q", op.Kind, got, want)
				}
				for i := range polls {
					state, err := b.OpStatus(t.Context(), op)
					if err != nil || state.Done {
						t.Fatalf("pending poll %d for %s: %+v %v", i, op.Kind, state, err)
					}
					if got := get(t, b, "runtime").Status; got != before {
						t.Fatalf("pending %s: status %q, want %q", op.Kind, got, before)
					}
				}
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || !state.Done || state.Failed {
					t.Fatalf("completed %s: %+v %v", op.Kind, state, err)
				}
				if got := get(t, b, "runtime").Status; got != after {
					t.Fatalf("completed %s: status %q, want %q", op.Kind, got, after)
				}
			}
			importOp := importServices(t, b, runtime("runtime", ""), volume("volume"))
			if got := get(t, b, "volume").Status; polls > 0 && got != infra.StatusCreating || polls == 0 && got != infra.StatusActive {
				t.Fatalf("pending volume import: %s", got)
			}
			check(importOp, infra.StatusCreating, infra.StatusReadyToDeploy)
			if got := get(t, b, "volume").Status; got != infra.StatusActive {
				t.Fatalf("completed volume import: %s", got)
			}
			op, err := b.Deploy(t.Context(), "runtime", deploy("", strings.NewReader("archive")))
			if err != nil {
				t.Fatal(err)
			}
			check(op, infra.StatusReadyToDeploy, infra.StatusActive)
			stop, err := b.Stop(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			check(stop, infra.StatusActive, infra.StatusStopped)
			if _, err := b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if got := get(t, b, "runtime").Status; got != infra.StatusStopped {
				t.Fatalf("re-polling deploy reverted stop: %s", got)
			}
			start, err := b.Start(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			check(start, infra.StatusStopped, infra.StatusActive)
			stop, err = b.Stop(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			check(stop, infra.StatusActive, infra.StatusStopped)
			restart, err := b.Restart(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			check(restart, infra.StatusStopped, infra.StatusActive)
		})
	}
}
func TestOperationsFailuresAndCalls(t *testing.T) {
	b := fake.New(fake.Options{Polls: 2})
	op := importServices(t, b, runtime("runtime", ""))
	for i := range 3 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil || state.Done != (i == 2) || state.Failed || len(state.ServiceIDs) != 1 {
			t.Fatalf("poll %d: %+v %v", i, state, err)
		}
	}
	for _, kind := range []string{"import", "deploy", ""} {
		s, err := b.OpStatus(t.Context(), infra.Op{ID: "unknown", Kind: kind})
		if err != nil || !s.Done || !s.Expired || s.Failed != (kind != "import") {
			t.Fatalf("unknown op %+v %v", s, err)
		}
	}
	failure := errors.New("injected")
	b.FailNext("Stop", failure)
	b.FailNext("Stop", infra.ErrUnavailable)
	if _, err := b.Stop(t.Context(), "runtime"); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := b.Stop(t.Context(), "runtime"); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := b.Stop(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	calls := b.Calls()
	if !reflect.DeepEqual(calls[len(calls)-3:], []string{"Stop", "Stop", "Stop"}) {
		t.Fatal(calls)
	}
	calls[0] = "changed"
	if b.Calls()[0] != "ImportServices" {
		t.Fatal("calls alias state")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := b.GetService(ctx, "runtime"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestConcurrentStateAccess(t *testing.T) {
	b := fake.New(fake.Options{})
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			name := fmt.Sprintf("runtime%d", i)
			if _, err := b.ImportServices(t.Context(), []infra.ServiceSpec{runtime(name, "")}); err != nil {
				t.Error(err)
			}
			b.SetToken(name)
			b.AddLog(name, infra.LogLine{Message: name})
			if _, err := b.ListServices(t.Context(), "runtime"); err != nil {
				t.Error(err)
			}
			b.Calls()
		})
	}
	wg.Wait()
	services, err := b.ListServices(t.Context(), "runtime")
	if err != nil || len(services) != 20 {
		t.Fatalf("services=%d error=%v", len(services), err)
	}
}
