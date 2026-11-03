// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
)

func TestZeropsTemporalReconcile(t *testing.T) {
	for _, topology := range []string{"combined", "split"} {
		for _, version := range []string{"1.31.4", "1.32.1"} {
			t.Run(topology+"/"+version, func(t *testing.T) {
				p, rc, _, _ := fixture(t, topology)
				platform := new(zerops.Backend)
				backend := infrafake.New(infrafake.Options{Name: "zerops", Layout: platform.Layout(), Capabilities: platform.Capabilities()})
				rc.Backend = &testBackend{backend}
				rc.Cluster.Spec.Version = version
				// Replicated roles retain one service identity, but discover a
				// separate membership address inside each runtime instance.
				for name, component := range rc.Cluster.Spec.Components.Extra {
					if component.Replicas > 0 {
						component.Replicas = 2
						rc.Cluster.Spec.Components.Extra[name] = component
					}
				}
				if err := p.ValidateBackend(rc.Backend); err != nil {
					t.Fatal(err)
				}
				converge(t, p, rc)
				state, err := loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				for _, member := range state.Members {
					if member.Replicas == 0 {
						continue
					}
					// Old agents must not receive a configuration whose family they ignore.
					for _, caps := range [][]string{nil, {"unknown"}, {agentapi.ConfigCapabilityTemporalIPv6}, nil} {
						pull := *rc
						pull.AgentConfigCapabilities = caps
						cfg, err := p.MemberConfig(t.Context(), &pull, member.Name)
						if len(caps) == 0 || caps[0] != agentapi.ConfigCapabilityTemporalIPv6 {
							if !errors.Is(err, engine.ErrNotSupported) || len(cfg.EngineSpec) != 0 {
								t.Fatalf("legacy config leaked: %v", err)
							}
							continue
						}
						if err != nil {
							t.Fatal(err)
						}
						var sp tspec.Spec
						if err := json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
							t.Fatal(err)
						}
						if !sp.IPv6 {
							t.Fatal("Zerops configuration did not select IPv6")
						}
						if sp.FrontendAddress == "" || !strings.HasSuffix(sp.FrontendAddress, ":7233") {
							t.Fatal("missing private frontend endpoint")
						}
					}
					runtime, ok := backend.Deployed(member.Name)
					if !ok {
						t.Fatal("member not deployed")
					}
					if runtime.Volume != "" || runtime.Readiness == nil {
						t.Fatal("stateless role runtime contract changed")
					}
					expected := map[int]bool{9443: true}
					for _, role := range tspec.Roles(member.Component) {
						pair := tspec.Ports[role]
						expected[pair.GRPC], expected[pair.Membership] = true, true
						if role == tspec.Frontend {
							expected[tspec.FrontendHTTPPort] = true
						}
					}
					if member.Component == tspec.UI {
						expected[tspec.UIPort] = true
					}
					if len(runtime.Ports) != len(expected) {
						t.Fatalf("ports: %+v", runtime.Ports)
					}
					for _, port := range runtime.Ports {
						if !expected[port.Port] || port.Protocol != "tcp" || port.HTTP != (member.Component == tspec.UI && port.Port == tspec.UIPort) {
							t.Fatalf("unexpected port: %+v", port)
						}
					}
					manifest, err := zerops.RenderYAML(member.Name, platform.Layout(), "karakoram", runtime)
					if err != nil {
						t.Fatal(err)
					}
					for _, text := range []string{"base: ubuntu@24.04", "readinessCheck:", "KARAKORAM_ENGINE: temporal", fmt.Sprintf("port: %d", runtime.Ports[0].Port)} {
						if !strings.Contains(manifest, text) {
							t.Fatalf("manifest missing %q: %s", text, manifest)
						}
					}
				}
				services, err := backend.ListServices(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				for _, service := range services {
					if service.Kind != infra.KindRuntime {
						t.Fatal("Temporal unexpectedly allocated durable storage")
					}
				}
			})
		}
	}
}

func TestTemporalIPv4LegacyConfiguration(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	layout := rc.Backend.Layout()
	layout.PrivateAddressFamily = infra.AddressFamilyIPv4
	rc.Backend = &testBackend{infrafake.New(infrafake.Options{Layout: layout})}
	converge(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range state.Members {
		config, err := p.MemberConfig(t.Context(), rc, member.Name)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(config.EngineSpec, &wire); err != nil {
			t.Fatal(err)
		}
		if _, present := wire["ipv6"]; present {
			t.Fatal("IPv4 payload changed the legacy wire schema")
		}
	}
}
