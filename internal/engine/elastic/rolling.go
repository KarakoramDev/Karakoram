// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func (p *Provider) rollingStart(rc *engine.Context, s *state, reason, version, operationID string) {
	if s.Rolling != nil {
		return
	}
	s.Rolling = &rolling{Reason: reason, Version: version, OperationID: operationID, StartedAt: now(rc)}
	s.Phase = "rolling"
}

func rollingProgress(s *state) (string, string) {
	r := s.Rolling
	if r == nil {
		return "", ""
	}
	order := "Elasticsearch nodes (elected master last), then Kibana, Logstash and APM"
	if len(r.Order) == 0 {
		return "plan", "Planning rolling " + r.Reason + ": " + order
	}
	name, step := r.Current, r.Step
	if name == "" {
		for _, next := range r.Order {
			if !slices.Contains(r.Done, next) && !slices.Contains(r.Skipped, next) && s.find(next) != nil {
				name, step = next, "prepare"
				break
			}
		}
	}
	if name == "" {
		if len(r.Skipped) > 0 {
			return "complete", fmt.Sprintf("Rolling %s complete; disabled components skipped: %v", r.Reason, r.Skipped)
		}
		return "complete", "Rolling " + r.Reason + " complete: " + order
	}
	m := s.find(name)
	if m == nil {
		return name + "/" + step, "Rolling " + r.Reason + ": " + name + " — " + step
	}
	component := map[string]string{kindNode: "Elasticsearch node", kindMaster: "Elasticsearch master", kindKibana: "Kibana", kindLogstash: "Logstash", kindAPM: "APM"}[m.Kind]
	action := map[string]string{"prepare": "checking readiness before restart", "flush": "flushing indices with replica allocation paused", "apply": "applying version or configuration", "rejoin": "waiting for readiness and rejoin", "allocation": "restoring shard allocation", "recover": "waiting for shard recovery"}[step]
	if action == "" {
		action = step
	}
	return m.Kind + "/" + name + "/" + step, fmt.Sprintf("Rolling %s: %s %s — %s (%s)", r.Reason, component, name, action, step)
}
func (p *Provider) rollingStep(ctx context.Context, rc *engine.Context, s *state) (done bool, message string, err error) {
	before, _ := rollingProgress(s)
	defer func() {
		after, progress := rollingProgress(s)
		if progress != "" {
			if message != "" && before == after {
				progress += "; " + message
			}
			message = progress
		}
		if before != after && rc.Events != nil {
			eventErr := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: "RollingProgress", Message: message, CreatedAt: now(rc)})
			if err == nil {
				err = eventErr
			}
		}
	}()
	if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
		return false, "waiting for platform operation", err
	}
	// Observe accepted work, but never replay a deployment using a different
	// control-plane artifact. Compensation only restores the admitted member.
	if r := s.Rolling; agentTargetChanged(r) {
		if s.Retry != nil && (s.Retry.Action == "upgrade" || s.Retry.Action == "stop") {
			r.Step = "build-start"
			delete(s.PlatformRetries, retryKey(s.Retry))
			s.Retry = nil
		}
		if busy, _, err := p.retryPending(ctx, rc, s); busy || err != nil {
			return false, "waiting for platform recovery retry", err
		}
		return p.recoverAgentBuildChange(ctx, rc, s)
	}
	if busy, _, err := p.retryPending(ctx, rc, s); busy || err != nil {
		return false, "waiting for platform retry", err
	}
	if changed, err := p.revokeComponentExposure(ctx, rc, s); changed || err != nil {
		return false, "revoking component public access", err
	}
	r := s.Rolling
	if r == nil {
		return true, "", nil
	}
	// Skips are desired-state decisions, not successful restarts. Reconsider
	// them on every step, including the final step and after a process restart.
	r.Skipped = slices.DeleteFunc(r.Skipped, func(name string) bool {
		m := s.find(name)
		return m != nil && component(rc.Cluster.Spec, m).Replicas > 0
	})
	if wait := rollingWindowWait(rc, s); wait.RequeueAfter > 0 {
		return false, "waiting for maintenance window", nil
	}
	if len(r.Order) == 0 {
		c, err := p.es(ctx, rc, s)
		if err != nil {
			return false, "", err
		}
		master, err := c.MasterNode(ctx)
		c.Close()
		if err != nil {
			return false, "", err
		}
		nodes := s.esNodes()
		targetVersion := r.Version
		if targetVersion == "" {
			targetVersion = rc.Cluster.Spec.Version
		}
		changed := map[string]bool{}
		for _, m := range s.Members {
			include := !m.Removing && (isES(m) || component(rc.Cluster.Spec, m).Replicas > 0)
			if include && r.Reason == "upgrade" {
				include = m.Version != targetVersion
			}
			if include && r.Reason == "agents" {
				observed := domain.MemberStatus{Name: m.Name}
				for _, status := range rc.Cluster.Status.Members {
					if status.Name == m.Name {
						observed = status
						break
					}
				}
				include = !slices.Contains(r.Done, m.Name) && engine.MemberNeedsAgentUpgrade(observed, *r.AgentTarget)
			}
			if include && (r.Reason == "config" || r.Reason == "resources") {
				desired, err := p.desiredSpec(ctx, rc, s, m, targetVersion)
				if err != nil {
					return false, "", err
				}
				include = desired.RestartToken != m.ConfigHash
			}
			changed[m.Name] = include
		}
		sort.SliceStable(nodes, func(i, j int) bool {
			rank := func(m *member) int {
				if m.Name == master {
					return 2
				}
				if masterEligible(rc.Cluster.Spec, m) {
					return 1
				}
				return 0
			}
			a, b := rank(nodes[i]), rank(nodes[j])
			if a != b {
				return a < b
			}
			return nodes[i].Index < nodes[j].Index
		})
		for _, m := range nodes {
			if changed[m.Name] {
				r.Order = append(r.Order, m.Name)
			}
		}
		for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
			for _, m := range s.members(kind) {
				if changed[m.Name] {
					r.Order = append(r.Order, m.Name)
				}
			}
		}
		if len(r.Order) == 0 {
			s.Rolling = nil
			return true, "no rolling changes", nil
		}
		return false, "planned rolling order", nil
	}
	if r.Current == "" {
		for _, name := range r.Order {
			if !slices.Contains(r.Done, name) && !slices.Contains(r.Skipped, name) && s.find(name) != nil {
				r.Current = name
				r.Step = "prepare"
				break
			}
		}
		if r.Current == "" {
			s.Rolling = nil
			if len(r.Skipped) > 0 {
				return true, fmt.Sprintf("rolling pass complete; disabled components skipped: %v", r.Skipped), nil
			}
			return true, "rolling pass complete", nil
		}
	}
	m := s.find(r.Current)
	if m == nil {
		r.Done = append(r.Done, r.Current)
		r.Current = ""
		return false, "member removed", nil
	}
	version := r.Version
	if version == "" {
		version = rc.Cluster.Spec.Version
	}
	if r.Reason == "agents" {
		version = m.Version
	}
	if !isES(m) {
		if r.Reason == "restart" && component(rc.Cluster.Spec, m).Replicas == 0 {
			// The desired component no longer exists. Its readiness cannot be
			// required to finish a restart; ordinary Sync owns its deletion.
			r.Skipped = append(r.Skipped, m.Name)
			r.Current = ""
			return false, "component disabled; skipping restart", nil
		}
		switch r.Step {
		case "prepare":
			if r.Reason == "upgrade" || r.Reason == "agents" {
				err := p.issueAction(ctx, rc, s, &pending{Action: "stop", Member: m.Name})
				if err == nil || s.Retry != nil {
					r.Step = "apply"
				}
				return false, "stopping component for upgrade", err
			}
			r.Step = "apply"
			return false, "preparing component", nil
		case "apply":
			resources := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
			if r.Reason != "agents" && !reflect.DeepEqual(m.Resources, resources) {
				return false, "resizing component", p.issueAction(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: resources})
			}
			if r.Reason == "upgrade" || r.Reason == "agents" {
				if spec, err := p.deploymentSpec(rc, m, version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
					return false, "deployment retries exhausted; operator re-arm required", nil
				}
				err := p.deploy(ctx, rc, s, m, "upgrade", version)
				if err == nil || s.Retry != nil {
					r.Step = "rejoin"
				}
				return false, "upgrading component", err
			}
			if err := p.publishConfig(ctx, rc, s, m, version, rollingNonce(r)); err != nil {
				return false, "", err
			}
			r.Step = "rejoin"
			p.reloadMembers(ctx, rc, m.Name)
			return false, "restarting component", nil
		case "rejoin":
			desired, err := p.desiredSpec(ctx, rc, s, m, version)
			if err != nil {
				return false, "", err
			}
			if desired.RestartToken != m.ConfigHash {
				r.Step = "apply"
				return false, "component configuration changed; applying current configuration", nil
			}
			ready, readinessErr := p.componentReadyResult(ctx, rc, m)
			if _, configurationFailed := readinessErr.(*componentConfigurationError); configurationFailed && r.OperationID == "" {
				// Database operations do not own this failed stateless rollout.
				// Keep its applied config repairable without gating repositories.
				m.Stage, s.Phase, s.Rolling = stageDeployed, "components", nil
			}
			if readinessErr != nil || !ready {
				return false, "waiting for component readiness", readinessErr
			}
			if r.Reason == "agents" && !p.agentBuildReady(ctx, rc, m, r.AgentTarget.TargetBuild) {
				return false, "waiting for component agent target build", nil
			}
			m.Version = version
			m.Stage = stageJoined
			r.Done = append(r.Done, m.Name)
			r.Current = ""
			return false, "component ready", nil
		}
		return false, "", invalid("unknown rolling component step")
	}
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return false, "", err
	}
	defer c.Close()
	switch r.Step {
	case "prepare":
		if err := rollingMasterReady(ctx, rc, s, m, c, false); err != nil {
			return false, "waiting for all master-eligible nodes to join", err
		}
		ok, err := p.healthy(ctx, c, rc.Cluster.Spec.Components.Engine.Replicas)
		if err != nil || !ok {
			return false, "waiting for shard health", err
		}
		value := "primaries"
		if err = c.SetAllocationEnable(ctx, &value); err != nil {
			return false, "", err
		}
		r.Step = "flush"
		return false, "replica allocation paused", nil
	case "flush":
		if err := c.Flush(ctx); err != nil {
			return false, "waiting for index flush", err
		}
		r.Step = "apply"
		return false, "flushed indices", nil
	case "apply":
		if err := rollingMasterReady(ctx, rc, s, m, c, false); err != nil {
			return false, "waiting for all master-eligible nodes to join", err
		}
		resources := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
		if r.Reason != "agents" && !reflect.DeepEqual(m.Resources, resources) {
			return false, "resizing node", p.issueAction(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: resources})
		}
		if r.Reason == "upgrade" || r.Reason == "agents" {
			if spec, err := p.deploymentSpec(rc, m, version); err == nil && engine.DeploymentBlocked(rc, m.Name, spec) {
				return false, "deployment retries exhausted; operator re-arm required", nil
			}
			err := p.deploy(ctx, rc, s, m, "upgrade", version)
			if err == nil || s.Retry != nil {
				r.Step = "rejoin"
			}
			return false, "upgrading node", err
		}
		if err = p.publishConfig(ctx, rc, s, m, version, rollingNonce(r)); err != nil {
			return false, "", err
		}
		r.Step = "rejoin"
		p.reloadMembers(ctx, rc, m.Name)
		return false, "restarting node", nil
	case "rejoin":
		desired, err := p.desiredSpec(ctx, rc, s, m, version)
		if err != nil {
			return false, "", err
		}
		if desired.RestartToken != m.ConfigHash {
			// Repair only the already-disrupted member. Re-entering apply would
			// wait for this failed master to join before publishing its repair.
			// Keep the journal's version: correcting settings is not a downgrade.
			if err := p.publishConfig(ctx, rc, s, m, version, rollingNonce(r)); err != nil {
				return false, "", err
			}
			p.reloadMembers(ctx, rc, m.Name)
			return false, "node configuration changed; applying current configuration to the failed member", nil
		}
		nodes, err := c.Nodes(ctx)
		if err != nil {
			return false, "", err
		}
		joined := false
		for _, node := range nodes {
			joined = joined || node.Name == m.Name && node.Version == version
		}
		if !joined || !p.componentReady(ctx, rc, m) {
			return false, "waiting for node rejoin", nil
		}
		if r.Reason == "agents" && !p.agentBuildReady(ctx, rc, m, r.AgentTarget.TargetBuild) {
			return false, "waiting for node agent target build", nil
		}
		m.Version = version
		m.Stage = stageJoined
		r.Step = "allocation"
		return false, "node rejoined", nil
	case "allocation":
		if err = c.SetAllocationEnable(ctx, nil); err != nil {
			return false, "", err
		}
		r.Step = "recover"
		return false, "allocation restored", nil
	case "recover":
		ok, err := p.healthy(ctx, c, rc.Cluster.Spec.Components.Engine.Replicas)
		if err != nil || !ok {
			return false, "waiting for shard recovery", err
		}
		r.Done = append(r.Done, m.Name)
		r.Current = ""
		return false, "node recovered", nil
	default:
		return false, "", invalid("unknown rolling step")
	}
}

func agentTargetChanged(r *rolling) bool {
	return r != nil && r.Reason == "agents" && (r.AgentTarget == nil || r.AgentTarget.TargetBuild != engine.CurrentAgentBuild().Build)
}

func agentBuildChangedError() error {
	return fmt.Errorf("agent upgrade target build differs from the running control plane; re-run upgrade-agents")
}

// A changed artifact cannot fulfill the frozen target. Undo pre-deployment
// disruption, or finish readiness/recovery of an accepted deployment, without
// deploying any new artifact or admitting the next member. The operation fails
// only after the durable journal no longer owns cluster disruption.
func (p *Provider) recoverAgentBuildChange(ctx context.Context, rc *engine.Context, s *state) (bool, string, error) {
	r := s.Rolling
	finish := func() (bool, string, error) {
		s.Rolling = nil
		return true, "agent target changed; admitted member recovered", agentBuildChangedError()
	}
	if r.Current == "" || r.Step == "prepare" {
		return finish()
	}
	m := s.find(r.Current)
	if m == nil {
		// Allocation is cluster-wide and still needs restoring for a removed node.
		c, err := p.es(ctx, rc, s)
		if err != nil {
			return false, "", err
		}
		defer c.Close()
		if err := c.SetAllocationEnable(ctx, nil); err != nil {
			return false, "", err
		}
		return finish()
	}
	if r.Step == "build-start" {
		err := p.issueAction(ctx, rc, s, &pending{Action: "start", Member: m.Name})
		if err == nil || s.Retry != nil {
			r.Step = "rejoin"
		}
		return false, "starting admitted member after agent target change", err
	}
	if r.Step == "rejoin" {
		desired, err := p.desiredSpec(ctx, rc, s, m, m.Version)
		if err != nil {
			return false, "", err
		}
		if desired.RestartToken != m.ConfigHash {
			// Repair configuration on the admitted artifact only. Re-entering
			// apply would install the new control-plane build under the old target.
			if err := p.publishConfig(ctx, rc, s, m, m.Version, rollingNonce(r)); err != nil {
				return false, "", err
			}
			p.reloadMembers(ctx, rc, m.Name)
			return false, "applying corrected configuration after agent target change", nil
		}
	}
	if !isES(m) {
		if r.Step == "apply" {
			err := p.issueAction(ctx, rc, s, &pending{Action: "start", Member: m.Name})
			if err == nil || s.Retry != nil {
				r.Step = "rejoin"
			}
			return false, "restoring stopped component after agent target change", err
		}
		ready, err := p.componentReadyResult(ctx, rc, m)
		if err != nil || !ready {
			return false, "waiting for component recovery after agent target change", err
		}
		m.Stage = stageJoined
		return finish()
	}
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return false, "", err
	}
	defer c.Close()
	if r.Step == "rejoin" {
		nodes, err := c.Nodes(ctx)
		if err != nil {
			return false, "", err
		}
		if !slices.ContainsFunc(nodes, func(n esapi.Node) bool { return n.Name == m.Name && n.Version == m.Version }) || !p.componentReady(ctx, rc, m) {
			return false, "waiting for node recovery after agent target change", nil
		}
		m.Stage = stageJoined
	}
	if r.Step != "recover" {
		if err := c.SetAllocationEnable(ctx, nil); err != nil {
			return false, "", err
		}
		r.Step = "recover"
		return false, "allocation restored after agent target change", nil
	}
	healthy, err := p.healthy(ctx, c, rc.Cluster.Spec.Components.Engine.Replicas)
	if err != nil || !healthy {
		return false, "waiting for shard recovery after agent target change", err
	}
	return finish()
}

func rollingMasterReady(ctx context.Context, rc *engine.Context, s *state, m *member, c *esapi.Client, recovering bool) error {
	if !masterEligible(rc.Cluster.Spec, m) {
		return nil
	}
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return err
	}
	masterCount := 0
	for _, planned := range s.esNodes() {
		if !masterEligible(rc.Cluster.Spec, planned) {
			continue
		}
		masterCount++
		// A failed deployment may already have taken its own target offline.
		// Recovering that same member must not wait for it to recover itself;
		// all other planned masters and the committed quorum remain required.
		if recovering && planned.Name == m.Name {
			continue
		}
		if !slices.ContainsFunc(nodes, func(n esapi.Node) bool { return n.Name == planned.Name }) {
			return fmt.Errorf("%w: master-eligible node %s has not joined", errWaiting, planned.Name)
		}
	}
	voters, err := c.CommittedVoters(ctx)
	if err != nil {
		return err
	}
	if len(voters) == 0 {
		return fmt.Errorf("%w: committed voting configuration unavailable", errWaiting)
	}
	remaining := 0
	for _, id := range voters {
		if slices.ContainsFunc(nodes, func(n esapi.Node) bool { return n.ID == id && n.Name != m.Name }) {
			remaining++
		}
	}
	// One- and two-master topologies have no restart fault tolerance. With
	// every planned master present their temporary coordination outage is
	// unavoidable; never extend that exception to a larger committed config.
	if masterCount <= 2 && len(voters) <= 2 {
		return nil
	}
	if remaining < len(voters)/2+1 {
		return fmt.Errorf("%w: restarting %s would lose the committed voting quorum", errWaiting, m.Name)
	}
	return nil
}
func rollingNonce(r *rolling) string {
	if r.Reason == "restart" {
		return r.OperationID + "/" + strconv.FormatInt(r.StartedAt.UnixNano(), 10)
	}
	return ""
}

// Once allocation is restricted (or a component is stopped), finish recovery
// regardless of the window; only a fresh prepare step may wait.
func rollingWindowWait(rc *engine.Context, s *state) engine.Result {
	if s.Rolling != nil && (s.Rolling.Current == "" || s.Rolling.Step == "prepare") {
		return rc.WaitForWindow()
	}
	return engine.Result{}
}

func (p *Provider) agentBuildReady(ctx context.Context, rc *engine.Context, m *member, build string) bool {
	if !isES(m) {
		ready, _ := engine.ComponentInstancesHealthy(ctx, rc, m.Name, m.Containers, m.ConfigVersion, engine.ComponentProbe{Ready: func(a *agentapi.Client) (bool, error) {
			status, err := a.Status(ctx)
			return err == nil && status.Agent.Build == build, err
		}})
		return ready
	}
	a, err := p.agentClient(ctx, rc, m.Name)
	if err != nil {
		return false
	}
	defer a.Close()
	status, err := a.Status(ctx)
	return err == nil && status.Agent.Build == build
}
