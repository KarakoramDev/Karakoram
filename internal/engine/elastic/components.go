// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"reflect"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// stackComponents is ordered after Elasticsearch and before node scaling.
var stackComponents = func() engine.ComponentSet {
	var defs engine.ComponentSet
	for _, item := range []struct {
		name, display string
		max           int
		public        bool
	}{
		{kindKibana, "Kibana", 3, true}, {kindLogstash, "Logstash", 5, false}, {kindAPM, "APM", 3, true},
	} {
		name := item.name
		resources, _ := DefaultResources(name)
		defs = append(defs, engine.ComponentDef{Name: name, DisplayName: item.display, Defaults: domain.ComponentSpec{Resources: resources}, MinReplicas: 1, MaxReplicas: item.max, PublicHTTPS: item.public, ValidateResources: func(r domain.Resources) error { return ValidateMinimumRAM(name, r.MinRAM) }})
	}
	return defs
}()

func (p *Provider) reconcileComponent(ctx context.Context, rc *engine.Context, s *state, def engine.ComponentDef) (bool, error) {
	ms := s.members(def.Name)
	desired := rc.Cluster.Spec.Components.Extra[def.Name].Replicas
	if len(ms) > 0 && (desired < ms[0].Containers || desired == 0) && ms[0].Stage != "delete-volume" && ms[0].Stage != "removed" {
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			return true, nil
		}
	}
	var m *member
	current := engine.ComponentRuntime{HorizontalScaling: rc.Backend.Capabilities().HorizontalScaling}
	if len(ms) > 0 {
		m = ms[0]
		if rc.Cluster.Spec.Components.Extra[def.Name].Replicas > 0 && len(rc.Cluster.Status.DeploymentFailures) > 0 {
			spec, err := p.deploymentSpec(rc, m, rc.Cluster.Spec.Version)
			if err != nil {
				return false, err
			}
			if engine.DeploymentBlocked(rc, m.Name, spec) {
				return false, nil
			}
		}
		// A disabled component may be re-enabled after its runtime was deleted.
		// Finish removing the old identity before allocating a replacement.
		if m.Stage == "delete-volume" || m.Stage == "removed" {
			return true, p.deleteComponent(ctx, rc, s, m)
		}
		current.Exists, current.Stage, current.Replicas = true, engine.ComponentStage(m.Stage), m.Containers
	}
	return engine.ReconcileStatelessComponent(ctx, def, current, rc.Cluster.Spec.Components.Extra[def.Name], engine.ComponentCallbacks{
		Import: func(ctx context.Context) error {
			if m == nil {
				var err error
				m, err = p.allocate(rc, s, def.Name)
				if err != nil {
					return err
				}
				if err = p.ensureSecrets(ctx, rc, s); err != nil {
					return err
				}
			}
			return p.importMember(ctx, rc, s, m)
		},
		Deploy: func(ctx context.Context) error { return p.deploy(ctx, rc, s, m, "deploy", rc.Cluster.Spec.Version) },
		AwaitReady: func(ctx context.Context) error {
			// Initial application readiness must not block a corrective resize.
			// Apply the runtime limit before publishing a heap derived from it.
			resources := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
			if !reflect.DeepEqual(m.Resources, resources) {
				return p.issueAction(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: resources})
			}
			// This member has not joined: desiredSpec derives its corrective
			// heap from desired RAM without changing the activated fields.
			desired, err := p.desiredSpec(ctx, rc, s, m, m.Version)
			if err != nil {
				return err
			}
			if desired.RestartToken != m.ConfigHash {
				if err := p.publishConfig(ctx, rc, s, m, m.Version, ""); err != nil {
					return err
				}
				p.reloadMembers(ctx, rc, m.Name)
				return nil
			}
			ready, readinessErr := p.componentReadyResult(ctx, rc, m)
			if readinessErr != nil || !ready {
				s.Phase = "components"
				return readinessErr
			}
			m.Stage = stageJoined
			return nil
		},
		Scale: func(ctx context.Context, replicas int) error {
			return p.issueAction(ctx, rc, s, &pending{Action: "containers", Member: m.Name, Containers: replicas})
		},
		Delete: func(ctx context.Context) error { return p.deleteComponent(ctx, rc, s, m) },
	})
}
