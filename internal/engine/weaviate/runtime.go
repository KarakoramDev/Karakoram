// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func runtimeResources(r domain.Resources) domain.Resources { r.MinDisk = 1; r.MaxDisk = 3; return r }
func (p *Provider) runtimeSpec(rc *engine.Context, m *member, version string) (infra.RuntimeSpec, error) {
	prepare, e := PrepareCommands(version, rc.Backend.Layout())
	if e != nil {
		return infra.RuntimeSpec{}, e
	}
	ports := []infra.Port{{Port: 8080, Protocol: "tcp"}, {Port: 50051, Protocol: "tcp"}, {Port: 7946, Protocol: "tcp"}, {Port: 7946, Protocol: "udp"}, {Port: 7947, Protocol: "tcp"}, {Port: 8300, Protocol: "tcp"}, {Port: 8301, Protocol: "tcp"}}
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	return (engine.AgentRuntime{Cluster: rc.Cluster.Name, Member: m.Name, Engine: "weaviate", Volume: m.Volume, AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM, Ports: ports, Prepare: prepare}).Spec(rc.Backend.Layout()), nil
}
