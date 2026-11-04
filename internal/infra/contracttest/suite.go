// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package contracttest

import (
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func await(t *testing.T, h Harness, op infra.Op) {
	t.Helper()
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	interval := h.PollInterval
	if interval <= 0 {
		interval = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		state, err := h.Backend.OpStatus(t.Context(), op)
		if err != nil {
			t.Fatalf("OpStatus: %v", err)
		}
		if state.Failed {
			t.Fatalf("operation failed: %+v", state)
		}
		if state.Done {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("operation %q did not finish within %s", op.Kind, timeout)
		case <-ticker.C:
		}
	}
}
func complete(t *testing.T, h Harness, op infra.Op, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	await(t, h, op)
}
func neutralStatus(status string) bool {
	switch status {
	case infra.StatusActive, infra.StatusRunning, infra.StatusReadyToDeploy, infra.StatusStopped, infra.StatusCreating, infra.StatusDeleting, infra.StatusFailed:
		return true
	}
	return false
}
func specs(h Harness) (runtime, volume infra.ServiceSpec) {
	runtime = infra.ServiceSpec{Hostname: h.Hostname("runtime"), Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, MinContainers: 1, MaxContainers: 1, Resources: infra.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 1, MaxDisk: 1}, Env: map[string]string{"KARAKORAM_CLUSTER": "contractcluster", "KARAKORAM_AGENT_TOKEN": "contractsecret", "UNRELATED_SECRET": "private"}, Vars: map[string]string{"KARAKORAM_OWNER": "contractowner"}}
	volume = infra.ServiceSpec{Hostname: h.Hostname("volume"), Kind: infra.KindVolume, Resources: infra.Resources{MinDisk: 1, MaxDisk: 1}, Env: map[string]string{"KARAKORAM_CLUSTER": "contractcluster"}, Vars: map[string]string{"KARAKORAM_OWNER": "contractowner"}}
	runtime.Volume = volume.Hostname
	return
}
func imported(t *testing.T, h Harness) (runtime, volume infra.ServiceSpec) {
	t.Helper()
	runtime, volume = specs(h)
	op, err := h.Backend.ImportServices(t.Context(), []infra.ServiceSpec{volume, runtime})
	complete(t, h, op, err)
	return
}
func deployment(h Harness, volume string) infra.Deployment {
	return infra.Deployment{Artifact: h.Artifact(), Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Env: map[string]string{"KARAKORAM_DATA_DIR": h.Backend.Layout().VolumePath(volume)}, Volume: volume, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9090, Protocol: "tcp"}}, Readiness: &infra.Readiness{Args: []string{"agent", "readiness"}, FailureTimeout: time.Minute, RetryPeriod: time.Second}, StopBeforeStart: true}}
}
func deployed(t *testing.T, h Harness) (runtime, volume infra.ServiceSpec) {
	t.Helper()
	runtime, volume = imported(t, h)
	op, err := h.Backend.Deploy(t.Context(), runtime.Hostname, deployment(h, volume.Hostname))
	complete(t, h, op, err)
	return
}
func lookup(t *testing.T, h Harness, name string) *infra.Service {
	t.Helper()
	s, err := h.Backend.GetService(t.Context(), name)
	if err != nil || s == nil {
		t.Fatalf("GetService(%q) = %+v, %v", name, s, err)
	}
	return s
}
func absent(t *testing.T, h Harness, name string) {
	t.Helper()
	s, err := h.Backend.GetService(t.Context(), name)
	if err != nil || s != nil {
		t.Fatalf("absent GetService(%q) = %+v, %v", name, s, err)
	}
}
func run(t *testing.T, newHarness func(*testing.T) Harness) {
	cases := []struct {
		name string
		test func(*testing.T, Harness)
	}{
		{"identity", func(t *testing.T, h Harness) {
			b := h.Backend
			if b.Name() == "" {
				t.Fatal("empty backend name")
			}
			c := b.Capabilities()
			if c.TCPAllowlist && !c.PublicTCP {
				t.Fatal("TCPAllowlist requires PublicTCP")
			}
			if c.PublicHTTPS {
				if _, ok := b.(infra.SubdomainAccess); !ok {
					t.Fatal("PublicHTTPS requires SubdomainAccess")
				}
			}
			l := b.Layout()
			if !path.IsAbs(l.AppDir) || !path.IsAbs(l.VolumeRoot) || l.User == "" {
				t.Fatalf("invalid layout: %+v", l)
			}
			if p := l.VolumePath("contractvolume"); !strings.HasPrefix(p, path.Clean(l.VolumeRoot)+"/") {
				t.Fatalf("volume path %q escapes layout", p)
			}
			p, err := b.ProjectInfo(t.Context())
			if err != nil || p.ID == "" {
				t.Fatalf("ProjectInfo: %+v %v", p, err)
			}
		}},
		{"absent_lookup", func(t *testing.T, h Harness) { absent(t, h, h.Hostname("absent")) }},
		{"import", func(t *testing.T, h Harness) {
			runtime, volume := specs(h)
			op, err := h.Backend.ImportServices(t.Context(), []infra.ServiceSpec{volume, runtime})
			complete(t, h, op, err)
			for _, spec := range []infra.ServiceSpec{runtime, volume} {
				s := lookup(t, h, spec.Hostname)
				if s.ID == "" || s.Kind != spec.Kind || !neutralStatus(s.Status) {
					t.Fatalf("invalid observed service: %+v", s)
				}
				if len(op.Services) > 0 && op.Services[spec.Hostname] != s.ID {
					t.Fatalf("import services missing identity for %q: %+v", spec.Hostname, op.Services)
				}
				if s.Ownership["KARAKORAM_OWNER"] != "contractowner" {
					t.Fatal("owner variable not readable")
				}
				if v, ok := s.Ownership["KARAKORAM_CLUSTER"]; v != "contractcluster" && (!s.Masked || ok) {
					t.Fatalf("secret identity must be readable or omitted and masked: %+v", s)
				}
				if _, ok := s.Ownership["UNRELATED_SECRET"]; ok {
					t.Fatal("unrelated secret exposed in Ownership")
				}
			}
			if lookup(t, h, runtime.Hostname).ID == lookup(t, h, volume.Hostname).ID {
				t.Fatal("volume and runtime share an ID")
			}
			list, err := h.Backend.ListServices(t.Context(), runtime.Hostname)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, s := range list {
				if !strings.HasPrefix(s.Hostname, runtime.Hostname) {
					t.Fatalf("prefix filter returned %q", s.Hostname)
				}
				if s.Hostname == runtime.Hostname {
					found = true
				}
			}
			if !found {
				t.Fatal("prefix lookup omitted runtime")
			}
		}},
		{"duplicate_import", func(t *testing.T, h Harness) {
			runtime, _ := imported(t, h)
			_, err := h.Backend.ImportServices(t.Context(), []infra.ServiceSpec{runtime})
			if !errors.Is(err, infra.ErrConflict) && !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("duplicate import: %v", err)
			}
		}},
		{"deploy", func(t *testing.T, h Harness) {
			runtime, _ := deployed(t, h)
			containers, err := h.Backend.Containers(t.Context(), runtime.Hostname)
			if err != nil || len(containers) == 0 {
				t.Fatalf("Containers: %+v, %v", containers, err)
			}
			for _, c := range containers {
				if c.ID == "" || (c.Status != infra.ContainerActive && c.Status != infra.ContainerRunning) {
					t.Fatalf("invalid active container: %+v", c)
				}
			}
		}},
		{"placement_avoid", func(t *testing.T, h Harness) {
			if !h.Backend.Capabilities().Placement {
				t.Skip("backend has no authoritative placement")
			}
			runtime, oldVolume := deployed(t, h)
			containers, err := h.Backend.Containers(t.Context(), runtime.Hostname)
			if err != nil || len(containers) == 0 || containers[0].PlacementGroup == "" {
				t.Fatalf("authoritative placement unavailable: %+v %v", containers, err)
			}
			group := containers[0].PlacementGroup
			replacement := oldVolume
			replacement.Hostname = h.Hostname("movevolume")
			replacement.SpreadReplaces = oldVolume.Hostname
			replacement.SpreadAvoid = []string{group}
			op, err := h.Backend.ImportServices(t.Context(), []infra.ServiceSpec{replacement})
			if errors.Is(err, infra.ErrPlacementUnsatisfiable) {
				absent(t, h, replacement.Hostname)
				return
			}
			complete(t, h, op, err)
			runtime.Hostname = h.Hostname("moveruntime")
			runtime.Volume = replacement.Hostname
			op, err = h.Backend.ImportServices(t.Context(), []infra.ServiceSpec{runtime})
			complete(t, h, op, err)
			op, err = h.Backend.Deploy(t.Context(), runtime.Hostname, deployment(h, replacement.Hostname))
			complete(t, h, op, err)
			containers, err = h.Backend.Containers(t.Context(), runtime.Hostname)
			if err != nil || len(containers) == 0 {
				t.Fatalf("replacement containers: %+v %v", containers, err)
			}
			for _, container := range containers {
				if container.PlacementGroup == "" || container.PlacementGroup == group {
					t.Fatalf("replacement ignored exclusion %q: %+v", group, container)
				}
			}
		}},
		{"invalid_deployment", func(t *testing.T, h Harness) {
			runtime, volume := imported(t, h)
			d := deployment(h, volume.Hostname)
			d.Artifact.Reader = nil
			op, err := h.Backend.Deploy(t.Context(), runtime.Hostname, d)
			if !errors.Is(err, infra.ErrRejected) || op.ID != "" {
				t.Fatalf("invalid deployment returned op %+v error %v", op, err)
			}
		}},
		{"unknown_base", func(t *testing.T, h Harness) {
			runtime, _ := specs(h)
			runtime.Base = "unsupported-base"
			runtime.Volume = ""
			_, err := h.Backend.ImportServices(t.Context(), []infra.ServiceSpec{runtime})
			if !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("unknown base: %v", err)
			}
		}},
		{"environment", func(t *testing.T, h Harness) {
			runtime, _ := imported(t, h)
			if err := h.Backend.SetEnvVars(t.Context(), runtime.Hostname, map[string]string{"KARAKORAM_OWNER": "changedowner"}); err != nil {
				t.Fatal(err)
			}
			if s := lookup(t, h, runtime.Hostname); s.Ownership["KARAKORAM_OWNER"] != "changedowner" {
				t.Fatal("SetEnvVars not visible in Ownership")
			}
			if err := h.Backend.SetEnvSecrets(t.Context(), runtime.Hostname, map[string]string{"KARAKORAM_AGENT_TOKEN": "replacement"}); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"UNRELATED_SECRET", "KARAKORAM_AGENT_TOKEN"} {
				if err := h.Backend.SetEnvVars(t.Context(), runtime.Hostname, map[string]string{key: "mustremainsecret"}); !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("SetEnvVars exposed existing secret %q: %v", key, err)
				}
			}
		}},
		{"lifecycle", func(t *testing.T, h Harness) {
			runtime, _ := deployed(t, h)
			for _, action := range []func() (infra.Op, error){func() (infra.Op, error) { return h.Backend.Stop(t.Context(), runtime.Hostname) }, func() (infra.Op, error) { return h.Backend.Start(t.Context(), runtime.Hostname) }, func() (infra.Op, error) { return h.Backend.Restart(t.Context(), runtime.Hostname) }} {
				op, err := action()
				complete(t, h, op, err)
			}
		}},
		{"resources", func(t *testing.T, h Harness) {
			runtime, _ := imported(t, h)
			op, err := h.Backend.SetResources(t.Context(), runtime.Hostname, infra.Resources{MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1})
			complete(t, h, op, err)
			_, err = h.Backend.SetResources(t.Context(), runtime.Hostname, infra.Resources{MinCPU: -1})
			if !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("negative resources: %v", err)
			}
			_, err = h.Backend.SetContainerCount(t.Context(), runtime.Hostname, 0, 0)
			if !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("invalid container count: %v", err)
			}
		}},
		{"public_ports", func(t *testing.T, h Harness) {
			runtime, _ := deployed(t, h)
			port := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV6"}
			err := h.Backend.EnsurePublicPort(t.Context(), runtime.Hostname, port)
			if !h.Backend.Capabilities().PublicTCP {
				if !errors.Is(err, infra.ErrUnsupported) {
					t.Fatalf("unsupported public TCP: %v", err)
				}
				if _, err := h.Backend.ListPublicPorts(t.Context()); !errors.Is(err, infra.ErrUnsupported) {
					t.Fatalf("unsupported ListPublicPorts: %v", err)
				}
				if err := h.Backend.DeletePublicPorts(t.Context(), runtime.Hostname); !errors.Is(err, infra.ErrUnsupported) {
					t.Fatalf("unsupported DeletePublicPorts: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Backend.EnsurePublicPort(t.Context(), runtime.Hostname, port); err != nil {
				t.Fatalf("idempotent public route: %v", err)
			}
			routes, err := h.Backend.ListPublicPorts(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, r := range routes {
				if r.Hostname == runtime.Hostname && r.Port == port.Port && r.TargetPort == port.TargetPort {
					found = true
				}
			}
			if !found {
				t.Fatal("public route missing")
			}
			if err := h.Backend.DeletePublicPorts(t.Context(), runtime.Hostname); err != nil {
				t.Fatal(err)
			}
			routes, err = h.Backend.ListPublicPorts(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range routes {
				if r.Hostname == runtime.Hostname {
					t.Fatal("public route survived deletion")
				}
			}
		}},
		{"subdomains", func(t *testing.T, h Harness) {
			runtime, _ := deployed(t, h)
			sub, ok := h.Backend.(infra.SubdomainAccess)
			if !h.Backend.Capabilities().PublicHTTPS {
				if ok {
					if _, err := sub.SetSubdomainAccess(t.Context(), runtime.Hostname, true); !errors.Is(err, infra.ErrUnsupported) {
						t.Fatalf("unsupported subdomain: %v", err)
					}
					if _, err := sub.SubdomainURLs(t.Context(), runtime.Hostname); !errors.Is(err, infra.ErrUnsupported) {
						t.Fatalf("unsupported URLs: %v", err)
					}
				}
				return
			}
			if !ok {
				t.Fatal("PublicHTTPS without SubdomainAccess")
			}
			op, err := sub.SetSubdomainAccess(t.Context(), runtime.Hostname, true)
			complete(t, h, op, err)
			urls, err := sub.SubdomainURLs(t.Context(), runtime.Hostname)
			if err != nil || len(urls) == 0 {
				t.Fatalf("SubdomainURLs: %v %v", urls, err)
			}
			for _, url := range urls {
				if !strings.HasPrefix(url, "https://") {
					t.Fatalf("non-HTTPS URL %q", url)
				}
			}
			op, err = sub.SetSubdomainAccess(t.Context(), runtime.Hostname, false)
			complete(t, h, op, err)
			urls, err = sub.SubdomainURLs(t.Context(), runtime.Hostname)
			if err != nil || len(urls) != 0 {
				t.Fatalf("disabled URLs: %v %v", urls, err)
			}
		}},
		{"logs", func(t *testing.T, h Harness) {
			runtime, _ := deployed(t, h)
			_, err := h.Backend.Logs(t.Context(), runtime.Hostname, infra.LogQuery{Limit: 10})
			if h.Backend.Capabilities().Logs {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, infra.ErrUnsupported) {
				t.Fatalf("unsupported logs: %v", err)
			}
		}},
		{"fenced_delete", func(t *testing.T, h Harness) {
			runtime, _ := imported(t, h)
			before := lookup(t, h, runtime.Hostname)
			_, err := h.Backend.DeleteServiceByID(t.Context(), runtime.Hostname, before.ID+"wrong")
			if !errors.Is(err, infra.ErrConflict) {
				t.Fatalf("fenced delete: %v", err)
			}
			if after := lookup(t, h, runtime.Hostname); after.ID != before.ID {
				t.Fatal("fenced delete altered service")
			}
		}},
		{"delete", func(t *testing.T, h Harness) {
			runtime, volume := deployed(t, h)
			for _, name := range []string{runtime.Hostname, volume.Hostname} {
				op, err := h.Backend.DeleteService(t.Context(), name)
				complete(t, h, op, err)
				absent(t, h, name)
			}
		}},
		{"absent_delete", func(t *testing.T, h Harness) {
			op, err := h.Backend.DeleteService(t.Context(), h.Hostname("absent"))
			complete(t, h, op, err)
		}},
		{"empty_operation", func(t *testing.T, h Harness) {
			s, err := h.Backend.OpStatus(t.Context(), infra.Op{})
			if err != nil || !s.Done || s.Failed {
				t.Fatalf("empty operation: %+v %v", s, err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			if h.Backend == nil || h.Hostname == nil || h.Artifact == nil {
				t.Fatal("incomplete contract harness")
			}
			tc.test(t, h)
		})
	}
}
