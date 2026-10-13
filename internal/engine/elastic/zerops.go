// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"gopkg.in/yaml.v3"
)

// YAMLInput contains non-secret deployment inputs for one service. AgentURL is
// already resolved for the cluster's namespace. Agent tokens are imported via
// envSecrets, never included in generated YAML.
type YAMLInput struct {
	Hostname      string
	Cluster       string
	Component     string // agentspec component, not the spec component name.
	Version       string
	Volume        string // Empty for stateless stack components.
	AgentURL      string
	InternalCAPEM string
}

// ZeropsYAML renders the per-service run/deploy configuration. ES data/master
// nodes share the elasticsearch product component and differ only in agent spec.
func ZeropsYAML(in YAMLInput) (string, error) {
	if !ValidHostname(in.Hostname) || !ValidClusterName(in.Cluster) {
		return "", errors.New("deployment requires valid cluster and service names")
	}
	if in.Volume != "" && !ValidHostname(in.Volume) {
		return "", errors.New("deployment requires a valid volume hostname")
	}
	prepare := prepareCommands(in.Component, in.Version)
	if len(prepare) == 0 {
		return "", errors.New("deployment requires a valid Elastic component and exact version")
	}
	ports := []map[string]any{}
	addPort := func(port int, http bool) {
		p := map[string]any{"port": port, "protocol": "tcp"}
		if http {
			p["httpSupport"] = true
		}
		ports = append(ports, p)
	}
	timeout := "300s"
	switch in.Component {
	case agentspec.ComponentElasticsearch:
		addPort(agentspec.PortHTTP, false)
		addPort(agentspec.PortTransport, false)
		timeout = "600s"
	case agentspec.ComponentKibana:
		addPort(agentspec.PortKibana, true)
	case agentspec.ComponentLogstash:
		addPort(agentspec.PortLogstashBeats, false)
		addPort(agentspec.PortLogstashHTTP, true)
	case agentspec.ComponentAPM:
		addPort(agentspec.PortAPM, true)
	}
	addPort(9443, false)
	dir := "/var/www/karakoram-data"
	if in.Volume != "" {
		dir = "/mnt/" + in.Volume
	}
	run := map[string]any{
		"base": "ubuntu@24.04", "ports": ports, "prepareCommands": prepare,
		"envVariables": map[string]string{
			"KARAKORAM_URL": in.AgentURL, "KARAKORAM_ENGINE": agentspec.Engine,
			"KARAKORAM_CLUSTER": in.Cluster, "KARAKORAM_MEMBER": in.Hostname,
			"KARAKORAM_INTERNAL_CA": in.InternalCAPEM, "KARAKORAM_DATA_DIR": dir,
		},
		"start": "./karakoram agent",
	}
	if in.Volume != "" {
		run["volume"] = map[string]string{"hostname": in.Volume}
	}
	doc := map[string]any{"zerops": []map[string]any{{
		"setup": in.Hostname, "run": run,
		"deploy": map[string]any{
			"temporaryShutdown": true,
			"readinessCheck": map[string]any{
				"exec":           map[string]string{"command": fmt.Sprintf("/var/www/karakoram agent readiness --engine=%s %s", agentspec.Engine, in.Component)},
				"failureTimeout": timeout, "retryPeriod": "10s",
			},
		},
	}}}
	b, err := yaml.Marshal(doc)
	return string(b), err
}
