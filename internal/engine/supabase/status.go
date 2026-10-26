// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

func (p *Provider) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	// Database, component and final gateway observations are independent.
	// Start them together so a hung native endpoint cannot consume later
	// members' share of the outer observation sweep.
	var out domain.ClusterStatus
	s, err := loadState(rc.Cluster)
	if err != nil {
		return out, err
	}
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return out, err
	}
	samples := make([]componentObservation, len(stackComponents))
	sampleErrors := make([]error, len(stackComponents))
	gatewayReady := true
	var probes sync.WaitGroup
	var coreErr error
	probes.Go(func() { out, coreErr = p.Core.Status(ctx, rc) })
	for i, def := range stackComponents {
		m := s.Members[def.Name]
		if m == nil {
			continue
		}
		probes.Go(func() { samples[i], sampleErrors[i] = componentStatus(ctx, rc, m) })
		if def.Name == "gateway" && rc.Cluster.Spec.Components.Extra[def.Name].Replicas > 0 {
			probes.Go(func() { gatewayReady = gatewayInstancesReady(ctx, rc, m) })
		}
	}
	probes.Wait()
	if coreErr != nil {
		return out, coreErr
	}
	ready := s.Pending == nil
	componentReason := "ComponentsStarting"
	var issues []string
	for i, def := range stackComponents {
		m := s.Members[def.Name]
		desired := rc.Cluster.Spec.Components.Extra[def.Name]
		if m == nil {
			ready = ready && desired.Replicas == 0
			continue
		}
		if def.Name == "gateway" && m.Public != options.Gateway.PublicHTTPS {
			ready = false
		}
		status, err := samples[i], sampleErrors[i]
		if err != nil {
			return out, err
		}
		healthy := status.Ready
		pendingDeployment := s.Pending != nil && s.Pending.Component == def.Name && s.Pending.Action == "deploy"
		if pendingDeployment && componentReason == "ComponentsStarting" {
			componentReason = "ComponentDeploymentPending"
		}
		if !healthy && status.Reason == "StorageS3Unavailable" && componentReason != "ComponentDeploymentFailed" {
			componentReason = status.Reason
		}
		if m.DeploymentFailure != nil {
			healthy = false
			componentReason = "ComponentDeploymentFailed"
			status.Message = m.DeploymentFailure.message() + "; " + status.Message
		}
		if m.RuntimeFailure != nil {
			healthy = false
			componentReason = "ComponentRuntimeFailed"
			status.Message = m.RuntimeFailure.message() + "; " + status.Message
		}
		if def.Name == "gateway" && desired.Replicas > 0 && !gatewayReady {
			// Final upstream convergence must not revoke listener/bootstrap
			// health: later upstream services join after the gateway.
			ready = false
			if status.Message != "" {
				status.Message += "; "
			}
			status.Message += "Gateway upstream observation unavailable or incomplete; inspect every gateway instance's native status and upstream connectivity; restore all enabled upstreams before serving traffic"
		}
		member := domain.MemberStatus{
			Name: m.Name, Set: def.Name, Role: def.Name, State: string(m.Stage),
			ComponentVersion: m.Version, Health: healthy,
			Agent: status.agent,
		}
		member.Agent.Message = status.Message
		instances := make([]string, 0, len(status.processes))
		for instance := range status.processes {
			instances = append(instances, instance)
		}
		sort.Strings(instances)
		for _, instance := range instances {
			out.Conditions = append(out.Conditions, engine.ApplyProcessDiagnostics(&member, status.processes[instance], instance, clockNow(rc))...)
		}
		if pendingDeployment {
			message := s.Pending.deploymentMessage(rc)
			if member.Agent.Message != "" {
				message += "; " + member.Agent.Message
			}
			member.Agent.Message = message
		}
		containers, err := rc.Backend.Containers(ctx, m.Name)
		if err != nil {
			ready = false
			member.Health = false
			message := fmt.Sprintf("container inventory is unavailable: %v", err)
			if member.Agent.Message != "" {
				member.Agent.Message += "; " + message
			} else {
				member.Agent.Message = message
			}
		}
		for _, container := range containers {
			if container.PlacementGroup != "" {
				member.PlacementGroup = container.PlacementGroup
				out.Placement.Groups[container.PlacementGroup] = append(out.Placement.Groups[container.PlacementGroup], m.Name)
			}
		}
		// These members deliberately leave Version empty: the reconciler aggregates
		// that field as PostgreSQL's observed version across the entire cluster.
		out.Members = append(out.Members, member)
		if member.Agent.Message != "" {
			issues = append(issues, m.Name+": "+member.Agent.Message)
		}
		ready = ready && member.Health && m.Stage == engine.ComponentJoined && m.Replicas == desired.Replicas && desired.Replicas > 0
	}
	if !ready && out.Phase == domain.PhaseReady {
		out.Phase = domain.PhaseUpdating
		if componentReason == "StorageS3Unavailable" || componentReason == "ComponentDeploymentFailed" {
			out.Phase = domain.PhaseDegraded
		}
		if rc.Cluster.Status.ObservedGeneration == 0 && (componentReason == "ComponentsStarting" || componentReason == "ComponentDeploymentPending") {
			out.Phase = domain.PhaseCreating
		}
		out.ObservedGeneration = rc.Cluster.Status.ObservedGeneration
	}
	if !ready {
		// Core health can remain positive after a deployment guard has already
		// degraded the phase. Compose readiness independently of that overlay,
		// without replacing a genuine core failure or unknown observation.
		for i := range out.Conditions {
			if out.Conditions[i].Type == "Ready" && out.Conditions[i].Status == "True" {
				out.Conditions[i] = domain.Condition{Type: "Ready", Status: "False", Reason: componentReason, Message: strings.Join(issues, "; "), LastTransitionTime: clockNow(rc)}
			}
		}
	}
	componentCondition := domain.Condition{Type: "ComponentsReady", Status: "True", Reason: "ComponentsReady", LastTransitionTime: clockNow(rc)}
	if !ready {
		componentCondition.Status, componentCondition.Reason = "False", componentReason
		componentCondition.Message = strings.Join(issues, "; ")
		if componentCondition.Message != "" {
			if out.Message != "" {
				out.Message += "; "
			}
			out.Message += componentCondition.Message
		}
	}
	out.Conditions = append(out.Conditions, componentCondition)
	if ready {
		api, err := p.apiURL(ctx, rc)
		if err != nil {
			return out, err
		}
		if api != "" {
			out.Connection.Endpoints = append(out.Connection.Endpoints, domain.Endpoint{Name: "supabase", URL: api})
		}
		if rc.Cluster.Spec.Components.Extra["studio"].Replicas > 0 {
			out.Connection.Endpoints = append(out.Connection.Endpoints, domain.Endpoint{Name: "studio", URL: api})
		}
	}
	return out, nil
}

// Final readiness requires local observations from every live gateway. Bootstrap
// readiness remains listener-only so sequential upstream creation can proceed.
func gatewayInstancesReady(ctx context.Context, rc *engine.Context, m *componentMember) bool {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	containers, err := rc.Backend.Containers(ctx, m.Name)
	all, err := engine.ComponentObservedInstancesHealthy(ctx, rc, m.Name, m.Replicas, m.ConfigVersion, engine.InstanceInventory{Containers: containers, Err: err}, engine.ComponentProbe{Ready: func(client *agentapi.Client) (bool, error) {
		var status sbspec.Status
		if err := client.Engine(ctx, "supabase", "status", struct{}{}, &status); err != nil {
			return false, err
		}
		if !status.Ready {
			return false, nil
		}
		for _, upstream := range sbspec.GatewayUpstreams {
			if rc.Cluster.Spec.Components.Extra[upstream].Replicas > 0 && status.GatewayUpstreamHosts[upstream] < 1 {
				return false, nil
			}
		}
		return true, nil
	}})
	return err == nil && all
}
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind == domain.OperationRestart {
		return p.restartStack(ctx, rc, op)
	}
	if string(op.Kind) != "rotate-jwt-secret" {
		return p.Core.RunOperation(ctx, rc, op)
	}
	credentials, err := loadCredentials(ctx, rc)
	if err != nil {
		return engine.OpResult{}, err
	}
	// The operation identity is sealed with the new keys before rolling starts.
	// Retrying a lost operation checkpoint cannot generate a second secret.
	if credentials.RotationID != op.ID {
		keys, err := sbkeys.Generate(rand.Reader, clockNow(rc))
		if err != nil {
			return engine.OpResult{}, err
		}
		credentials.Keys.JWTSecret = keys.JWTSecret
		credentials.Keys.AnonKey = keys.AnonKey
		credentials.Keys.ServiceRoleKey = keys.ServiceRoleKey
		credentials.Keys.PublishableKey = keys.PublishableKey
		credentials.Keys.SecretKey = keys.SecretKey
		credentials.Keys.AnonJWT = keys.AnonJWT
		credentials.Keys.ServiceRoleJWT = keys.ServiceRoleJWT
		credentials.Keys.JWTKeys = keys.JWTKeys
		credentials.Keys.JWKS = keys.JWKS
		credentials.RotationID = op.ID
		if err = storeCredentials(ctx, rc, credentials); err != nil {
			return engine.OpResult{}, err
		}
	}
	busy, err := p.syncComponents(ctx, rc)
	if err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: !busy, Step: "rotate-jwt-secret", Message: "Applying rotated API signing keys", RequeueAfter: 2 * time.Second}, nil
}
