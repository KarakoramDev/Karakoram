// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestAgentRuntimeSpec(t *testing.T) {
	l := infra.Layout{AppDir: "/srv/app", VolumeRoot: "/srv/disks", User: "app"}
	a := AgentRuntime{Cluster: "cluster", Member: "member", Engine: "postgres", Volume: "volume", AgentURL: "https://control.example.test", InternalCAPEM: "certificate", Prepare: []string{"prepare"}, Env: map[string]string{"ENGINE_SETTING": "value"}, Ports: []infra.Port{{Port: 5432, Protocol: "tcp"}}, ReadinessTimeout: 2 * time.Minute}
	got := a.Spec(l)
	want := infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Prepare: a.Prepare, Env: map[string]string{"KARAKORAM_URL": a.AgentURL, "KARAKORAM_CLUSTER": "cluster", "KARAKORAM_MEMBER": "member", "KARAKORAM_INTERNAL_CA": "certificate", DataDirVariable: "/srv/disks/volume", "KARAKORAM_ENGINE": "postgres", "ENGINE_SETTING": "value"}, Args: []string{"agent"}, Ports: a.Ports, Volume: "volume", StopBeforeStart: true, Readiness: &infra.Readiness{Args: []string{"agent", "readiness", "--startup", "--data-dir=/srv/disks/volume"}, FailureTimeout: 2 * time.Minute, RetryPeriod: ReadinessRetryPeriod}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Spec: %+v want %+v", got, want)
	}
	if _, exists := a.Env["KARAKORAM_CLUSTER"]; exists {
		t.Fatal("Spec mutated extra environment")
	}
	if err := (infra.Deployment{Artifact: infra.Artifact{Reader: strings.NewReader("archive"), Name: ArtifactName, Binary: ArtifactBinary}, Runtime: got}).Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestAgentRuntimeDefaultsAndStatelessDataDir(t *testing.T) {
	l := infra.Layout{AppDir: "/srv/app", VolumeRoot: "/srv/disks", User: "app"}
	got := (AgentRuntime{}).Spec(l)
	if got.Base != infra.BaseUbuntu2404 || got.Volume != "" || got.Env[DataDirVariable] != "/srv/app/karakoram-data" || !got.StopBeforeStart {
		t.Fatalf("defaults: %+v", got)
	}
	if _, ok := got.Env["KARAKORAM_ENGINE"]; ok {
		t.Fatal("unspecified engine was set")
	}
	if !reflect.DeepEqual(got.Args, []string{"agent"}) || got.Readiness == nil || !reflect.DeepEqual(got.Readiness.Args, []string{"agent", "readiness", "--startup", "--data-dir=/srv/app/karakoram-data"}) || got.Readiness.FailureTimeout != DefaultReadinessTimeout || got.Readiness.RetryPeriod != ReadinessRetryPeriod {
		t.Fatalf("default readiness: %+v", got.Readiness)
	}
	override := (AgentRuntime{AgentURL: "original", Env: map[string]string{"KARAKORAM_URL": "override"}}).Spec(l)
	if override.Env["KARAKORAM_URL"] != "override" {
		t.Fatal("extra environment did not override default")
	}
}
func TestRuntimeAndVolumeServiceSpec(t *testing.T) {
	l := infra.Layout{AppDir: "/srv/app", VolumeRoot: "/srv/disks", User: "app"}
	resources := domain.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 2, MinRAM: 1, MaxRAM: 2, MinDisk: 5, MaxDisk: 10}
	member := MemberService{Cluster: "cluster", Member: "member", Token: "token", Volume: "volume", Resources: resources, Containers: 2}
	got := RuntimeServiceSpec(l, member)
	env := OwnershipEnv("cluster", "member", "token")
	env[DataDirVariable] = "/srv/disks/volume"
	want := infra.ServiceSpec{Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Resources: resources, MinContainers: 2, MaxContainers: 2, Env: env, Vars: OwnerVars("token", "cluster", "member", "member"), Volume: "volume"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime: %+v want %+v", got, want)
	}
	v := VolumeServiceSpec("cluster", "member", "volume", "token", resources)
	wantVolume := infra.ServiceSpec{Hostname: "volume", Kind: infra.KindVolume, Resources: domain.Resources{MinDisk: 5, MaxDisk: 10}, Env: OwnershipEnv("cluster", "member", "token"), Vars: OwnerVars("token", "cluster", "member", "volume"), SpreadGroup: "cluster"}
	if !reflect.DeepEqual(v, wantVolume) {
		t.Fatalf("volume: %+v want %+v", v, wantVolume)
	}
	member.Volume = ""
	stateless := RuntimeServiceSpec(l, member)
	if _, ok := stateless.Env[DataDirVariable]; ok || stateless.Volume != "" {
		t.Fatal("stateless import claims a mount")
	}
	if got := OwnershipEnv("cluster", "member", "token"); !reflect.DeepEqual(got, map[string]string{"KARAKORAM_CLUSTER": "cluster", "KARAKORAM_MEMBER": "member", "KARAKORAM_AGENT_TOKEN": "token"}) {
		t.Fatal(got)
	}
}
func TestMountsVolume(t *testing.T) {
	b := fake.New(fake.Options{})
	for _, tc := range []struct {
		name    string
		service *infra.Service
		volume  string
		want    bool
	}{
		{"mounted", &infra.Service{Ownership: map[string]string{DataDirVariable: b.Layout().VolumePath("volume")}}, "volume", true},
		{"nil", nil, "volume", false},
		{"missing_evidence", &infra.Service{}, "volume", false},
		{"masked", &infra.Service{Masked: true}, "volume", false},
		{"different_volume", &infra.Service{Ownership: map[string]string{DataDirVariable: b.Layout().VolumePath("other")}}, "volume", false},
		{"prefix_is_not_evidence", &infra.Service{Ownership: map[string]string{DataDirVariable: b.Layout().VolumePath("volume") + "/nested"}}, "volume", false},
		{"empty_volume", &infra.Service{}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MountsVolume(b, tc.service, tc.volume); got != tc.want {
				t.Fatalf("MountsVolume=%v want=%v", got, tc.want)
			}
		})
	}
}
func TestCapabilityRequirements(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		caps                        infra.Capabilities
		tcp, allowlist, https, disk bool
	}{
		{"all", fake.New(fake.Options{}).Capabilities(), true, true, true, true},
		{"none", infra.Capabilities{VolumeQuota: true}, false, false, false, false},
		{"tcp_only", infra.Capabilities{PublicTCP: true}, true, false, false, false},
		{"https_only", infra.Capabilities{PublicHTTPS: true}, false, false, true, false},
		{"disk_only", infra.Capabilities{DiskGrowth: true}, false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.New(fake.Options{Capabilities: tc.caps})
			check := func(err error, want bool) {
				t.Helper()
				if want && err != nil || !want && !errors.Is(err, ErrNotSupported) {
					t.Fatalf("capability error=%v supported=%v", err, want)
				}
			}
			check(RequirePublicTCP(b, domain.Exposure{}), tc.tcp)
			check(RequirePublicTCP(b, domain.Exposure{Allowlist: []string{"192.0.2.0/24"}}), tc.allowlist)
			sub, err := PublicHTTPS(b)
			check(err, tc.https)
			if tc.https && sub == nil || !tc.https && sub != nil {
				t.Fatal("unexpected subdomain interface")
			}
			check(RequireDiskGrowth(b), tc.disk)
		})
	}
}

type trackedArtifact struct {
	*strings.Reader
	closed bool
}

func (a *trackedArtifact) Close() error { a.closed = true; return nil }

type runtimeArtifactSource struct {
	artifact *trackedArtifact
	err      error
}

func (s runtimeArtifactSource) Open(context.Context) (io.ReadCloser, error) { return s.artifact, s.err }
func TestDeployOpensAndClosesArtifact(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "backend_error"}[fail], func(t *testing.T) {
			b := fake.New(fake.Options{})
			if _, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}}); err != nil {
				t.Fatal(err)
			}
			artifact := &trackedArtifact{Reader: strings.NewReader("archive")}
			rc := &Context{Cluster: &domain.Cluster{}, Backend: b, Artifact: runtimeArtifactSource{artifact: artifact}}
			failure := errors.New("deployment refused")
			if fail {
				b.FailNext("Deploy", failure)
			}
			spec := (AgentRuntime{Member: "member"}).Spec(b.Layout())
			op, err := Deploy(t.Context(), rc, "member", spec)
			if fail {
				if !errors.Is(err, failure) || op.ID != "" {
					t.Fatalf("Deploy: %+v %v", op, err)
				}
			} else {
				if err != nil || op.ID == "" {
					t.Fatalf("Deploy: %+v %v", op, err)
				}
				got, ok := b.Deployed("member")
				if !ok || !reflect.DeepEqual(got, spec) || artifact.Len() != 0 {
					t.Fatal("runtime or artifact not delivered")
				}
			}
			if !artifact.closed {
				t.Fatal("artifact not closed")
			}
		})
	}
	failure := errors.New("artifact unavailable")
	b := fake.New(fake.Options{})
	rc := &Context{Cluster: &domain.Cluster{}, Backend: b, Artifact: runtimeArtifactSource{err: failure}}
	if op, err := Deploy(t.Context(), rc, "member", infra.RuntimeSpec{}); !errors.Is(err, failure) || op.ID != "" {
		t.Fatalf("artifact error: %+v %v", op, err)
	}
	if len(b.Calls()) != 0 {
		t.Fatal("backend called after artifact failure")
	}
}
