// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// RuntimeInput contains non-secret deployment inputs for one member. AgentURL
// is already resolved for its namespace; agent tokens are imported separately.
type RuntimeInput struct {
	Hostname      string
	Cluster       string
	Component     string // agentspec component, not the spec component name.
	Version       string
	Volume        string // Empty for stateless stack components.
	AgentURL      string
	InternalCAPEM string
}

// RuntimeSpec describes a member without depending on a backend manifest.
// Data and master nodes share the Elasticsearch product runtime.
func RuntimeSpec(layout infra.Layout, in RuntimeInput) (infra.RuntimeSpec, error) {
	if !ValidHostname(in.Hostname) || !ValidClusterName(in.Cluster) {
		return infra.RuntimeSpec{}, errors.New("deployment requires valid cluster and service names")
	}
	if in.Volume != "" && !ValidHostname(in.Volume) {
		return infra.RuntimeSpec{}, errors.New("deployment requires a valid volume hostname")
	}
	prepare := PrepareCommands(in.Component, in.Version, layout.User)
	if len(prepare) == 0 {
		return infra.RuntimeSpec{}, errors.New("deployment requires a valid Elastic component and exact version")
	}
	var ports []infra.Port
	addPort := func(port int, http bool) {
		ports = append(ports, infra.Port{Port: port, Protocol: "tcp", HTTP: http})
	}
	var timeout time.Duration
	switch in.Component {
	case agentspec.ComponentElasticsearch:
		addPort(agentspec.PortHTTP, false)
		addPort(agentspec.PortTransport, false)
		timeout = 600 * time.Second
	case agentspec.ComponentKibana:
		addPort(agentspec.PortKibana, true)
	case agentspec.ComponentLogstash:
		addPort(agentspec.PortLogstashBeats, false)
		addPort(agentspec.PortLogstashHTTP, true)
	case agentspec.ComponentAPM:
		addPort(agentspec.PortAPM, true)
	}
	addPort(9443, false)
	return (engine.AgentRuntime{
		Cluster: in.Cluster, Member: in.Hostname, Engine: agentspec.Engine,
		Volume: in.Volume, AgentURL: in.AgentURL, InternalCAPEM: in.InternalCAPEM,
		Ports: ports, Prepare: prepare, ReadinessTimeout: timeout,
		ReadinessArgs: []string{"--engine=" + agentspec.Engine, "--data-dir=" + layout.DataDir(in.Volume), in.Component},
	}).Spec(layout), nil
}
