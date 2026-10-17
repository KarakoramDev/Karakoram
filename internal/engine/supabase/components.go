// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type componentState struct {
	Members map[string]*componentMember `json:"members"`
	Pending *componentPending           `json:"pending,omitempty"`
}
type componentMember struct {
	Name          string                `json:"name"`
	Component     string                `json:"component"`
	ServiceID     string                `json:"serviceId,omitempty"`
	Stage         engine.ComponentStage `json:"stage"`
	Version       string                `json:"version"`
	ConfigVersion int64                 `json:"configVersion"`
	ConfigHash    string                `json:"configHash"`
	Replicas      int                   `json:"replicas"`
	Resources     domain.Resources      `json:"resources"`
	Public        bool                  `json:"public,omitempty"`
	RuntimePort   int                   `json:"runtimePort,omitempty"`
}
type componentPending struct {
	Op          infra.Op         `json:"op"`
	Component   string           `json:"component"`
	Action      string           `json:"action"`
	Replicas    int              `json:"replicas,omitempty"`
	Resources   domain.Resources `json:"resources"`
	Public      bool             `json:"public,omitempty"`
	RuntimePort int              `json:"runtimePort,omitempty"`
}

func loadState(c *domain.Cluster) (*componentState, error) {
	s := &componentState{Members: map[string]*componentMember{}}
	if len(c.Internal) == 0 {
		return s, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(c.Internal, &root); err != nil {
		return nil, err
	}
	if raw := root["supabase"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, s); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func saveState(c *domain.Cluster, s *componentState) error {
	root := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	root["supabase"] = raw
	c.Internal, err = json.Marshal(root)
	return err
}
func verifyComponent(ctx context.Context, rc *engine.Context, m *componentMember) (*infra.Service, error) {
	service, err := rc.Backend.GetService(ctx, m.Name)
	if errors.Is(err, infra.ErrNotFound) {
		return nil, nil
	}
	if err != nil || service == nil {
		return service, err
	}
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, engine.UnverifiedOwnership(service)
	}
	if marker := service.Ownership[engine.OwnerVariable]; marker != "" && marker != engine.OwnerMarker(token.Token, rc.Cluster.Name, m.Name, service.Hostname) {
		return nil, engine.UnverifiedOwnership(service)
	}
	if service.ID != m.ServiceID && !engine.HasOwnership(service, token.Token, rc.Cluster.Name, m.Name) {
		return nil, engine.UnverifiedOwnership(service)
	}
	if err = engine.EnsureOwnerMarker(ctx, rc.Backend, service, token.Token, rc.Cluster.Name, m.Name); err != nil {
		return nil, err
	}
	m.ServiceID = service.ID
	return service, nil
}
func observeComponent(ctx context.Context, rc *engine.Context, s *componentState) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	status, err := rc.Backend.OpStatus(ctx, q.Op)
	if err != nil {
		return true, err
	}
	if !status.Done && !status.Failed {
		return true, nil
	}
	s.Pending = nil
	if status.Failed {
		return true, fmt.Errorf("infrastructure operation %s failed for Supabase component", q.Action)
	}
	m := s.Members[q.Component]
	if m == nil {
		return true, engine.ErrNotFound
	}
	switch q.Action {
	case "import":
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return true, err
		}
		if id := engine.ImportedServiceID(q.Op, status, service); id != "" {
			m.ServiceID = id
		}
		service, err = verifyComponent(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if service == nil {
			return true, engine.ErrUnavailable
		}
		m.Stage = engine.ComponentImported
	case "deploy":
		m.Stage = engine.ComponentDeployed
		m.RuntimePort = q.RuntimePort
	case "scale":
		m.Replicas = q.Replicas
		m.Stage = engine.ComponentDeployed
	case "resize":
		m.Resources = q.Resources
		m.Stage = engine.ComponentDeployed
	case "public":
		m.Public = q.Public
	case "delete":
		service, err := verifyComponent(ctx, rc, m)
		if err != nil {
			return true, err
		}
		if service != nil {
			return true, engine.ErrUnavailable
		}
		for _, key := range []string{"supabase/config/" + m.Name, "supabase/agent-cert/" + m.Name} {
			if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return true, err
			}
		}
		if err = rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
			return true, err
		}
		delete(s.Members, q.Component)
	}
	return true, nil
}
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	// Restrict exposure even when core reconciliation fails or a database
	// replica is unhealthy. New exposure remains behind the readiness gate.
	busy, componentErr := p.syncGatewayRestrictions(ctx, rc)
	observed := rc.Cluster.Status.ObservedGeneration
	result, err := p.Core.Sync(ctx, rc)
	// The core's applied generation does not include the Supabase services.
	rc.Cluster.Status.ObservedGeneration = observed
	if busy || componentErr != nil {
		// Optional infrastructure work serializes only other optional work,
		// never database provisioning, replacement, or recovery.
		result.RequeueAfter = 2 * time.Second
		return result, errors.Join(err, componentErr)
	}
	if err != nil {
		return result, err
	}
	status, err := p.Core.Status(ctx, rc)
	if err != nil {
		return result, err
	}
	if status.Phase != domain.PhaseReady {
		return result, nil
	}
	busy, err = p.syncComponents(ctx, rc)
	if err == nil && !busy {
		status, err = p.Status(ctx, rc)
		if err == nil && status.Phase == domain.PhaseReady {
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
		} else if err == nil {
			// Final gateway DNS convergence needs another observation even when
			// every sequential component lifecycle has already joined.
			busy = true
		}
	}
	if busy || err != nil {
		result.RequeueAfter = 2 * time.Second
	}
	return result, err
}

func (p *Provider) syncGatewayRestrictions(ctx context.Context, rc *engine.Context) (busy bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if busy, e := observeComponent(ctx, rc, s); busy || e != nil {
		return true, e
	}
	m := s.Members["gateway"]
	if m == nil || !m.Public {
		return false, nil
	}
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return false, err
	}
	if o.Gateway.PublicHTTPS && rc.Cluster.Spec.Components.Extra["gateway"].Replicas > 0 {
		return false, nil
	}
	service, err := verifyComponent(ctx, rc, m)
	if err != nil {
		return true, err
	}
	if service == nil {
		m.Public = false
		return true, nil
	}
	access, err := engine.PublicHTTPS(rc.Backend)
	if err != nil {
		return true, err
	}
	op, err := access.SetSubdomainAccess(ctx, m.Name, false)
	if err != nil {
		return true, err
	}
	s.Pending = &componentPending{Op: op, Component: "gateway", Action: "public", Public: false}
	return true, nil
}
func (p *Provider) syncComponents(ctx context.Context, rc *engine.Context) (handled bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if busy, e := observeComponent(ctx, rc, s); busy || e != nil {
		return true, e
	}
	for _, def := range stackComponents {
		m := s.Members[def.Name]
		desired := rc.Cluster.Spec.Components.Extra[def.Name]
		current := engine.ComponentRuntime{HorizontalScaling: rc.Backend.Capabilities().HorizontalScaling}
		if m != nil {
			service, e := verifyComponent(ctx, rc, m)
			if e != nil {
				return true, e
			}
			if service == nil {
				m.Stage = engine.ComponentPlanned
				m.ServiceID = ""
				m.Public = false
			}
			current.Exists = true
			current.Stage = m.Stage
			current.Replicas = m.Replicas
			current.Resources = m.Resources
		}
		if m != nil && desired.Replicas > 0 && m.Stage == engine.ComponentJoined {
			o, e := ParseOptions(rc.Cluster.Spec.EngineOptions)
			if e != nil {
				return true, e
			}
			if def.Name == "gateway" && m.Public != o.Gateway.PublicHTTPS {
				access, e := engine.PublicHTTPS(rc.Backend)
				if e != nil {
					return true, e
				}
				op, e := access.SetSubdomainAccess(ctx, m.Name, o.Gateway.PublicHTTPS)
				if e != nil {
					return true, e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "public", Public: o.Gateway.PublicHTTPS}
				return true, nil
			}
			// Previous Studio runtimes exposed raw port 3000. A config reload
			// cannot update backend port declarations, so redeploy them too.
			if def.Name == "studio" && m.RuntimePort != ports[def.Name] {
				m.Stage = engine.ComponentImported
				return true, nil
			}
			sp, e := p.desiredSpec(ctx, rc, m)
			if e != nil {
				return true, e
			}
			renew, e := componentCertificateNeedsRenewal(ctx, rc, m)
			if e != nil {
				return true, e
			}
			if sp.RestartToken != m.ConfigHash || renew {
				if sp.Version != m.Version {
					m.Stage = engine.ComponentImported
					return true, nil
				}
				if e = p.publishConfig(ctx, rc, m, sp); e != nil {
					return true, e
				}
				m.Stage = engine.ComponentDeployed
				p.reloadComponent(ctx, rc, m)
				return true, nil
			}
		}
		handled, e := engine.ReconcileStatelessComponent(ctx, def, current, desired, engine.ComponentCallbacks{
			Import: func(ctx context.Context) error {
				if m == nil {
					name := rc.Cluster.Name + codes[def.Name]
					token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), name)
					if e != nil && !missing(e) {
						return e
					}
					if token == nil || missing(e) {
						value, e := secrets.Token()
						if e != nil {
							return e
						}
						token = &domain.AgentToken{
							Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
							Member: name, Token: value, Hash: secrets.Hash(value),
						}
						if e = rc.Secrets.SetAgentToken(ctx, token); e != nil {
							return e
						}
					}
					m = &componentMember{
						Name: name, Component: def.Name, Stage: engine.ComponentPlanned,
						Replicas: desired.Replicas, Resources: desired.Resources,
					}
					s.Members[def.Name] = m
					// Persist the identity before any infrastructure side effect. A
					// restart after import must never replace the ownership token.
					return nil
				}
				service, e := verifyComponent(ctx, rc, m)
				if e != nil {
					return e
				}
				if service != nil {
					m.Stage = engine.ComponentImported
					return nil
				}
				token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
				if e != nil {
					return e
				}
				op, e := rc.Backend.ImportServices(ctx, []infra.ServiceSpec{engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Resources: desired.Resources, Containers: desired.Replicas})})
				if e != nil {
					return e
				}
				m.Replicas, m.Resources = desired.Replicas, desired.Resources
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "import"}
				return nil
			},
			Deploy: func(ctx context.Context) error {
				if _, e := verifyComponent(ctx, rc, m); e != nil {
					return e
				}
				sp, e := p.desiredSpec(ctx, rc, m)
				if e != nil {
					return e
				}
				if e = p.publishConfig(ctx, rc, m, sp); e != nil {
					return e
				}
				runtime, e := p.runtimeSpec(rc, m)
				if e != nil {
					return e
				}
				op, e := engine.Deploy(ctx, rc, m.Name, runtime)
				if e != nil {
					return e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "deploy", RuntimePort: ports[def.Name]}
				return nil
			},
			AwaitReady: func(ctx context.Context) error {
				status, e := componentStatus(ctx, rc, m)
				if e != nil {
					return e
				}
				// Bootstrap readiness must not wait for gateway DNS: later
				// upstream services have not been created in this sequential loop.
				if status.Ready {
					m.Stage = engine.ComponentJoined
				} else {
					p.reloadComponent(ctx, rc, m)
				}
				return nil
			},
			Scale: func(ctx context.Context, replicas int) error {
				if _, e := verifyComponent(ctx, rc, m); e != nil {
					return e
				}
				op, e := rc.Backend.SetContainerCount(ctx, m.Name, replicas, replicas)
				if e != nil {
					return e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "scale", Replicas: replicas}
				return nil
			},
			Resize: func(ctx context.Context, resources domain.Resources) error {
				if _, e := verifyComponent(ctx, rc, m); e != nil {
					return e
				}
				op, e := rc.Backend.SetResources(ctx, m.Name, resources)
				if e != nil {
					return e
				}
				s.Pending = &componentPending{Op: op, Component: def.Name, Action: "resize", Resources: resources}
				return nil
			},
			Delete: func(ctx context.Context) error { return deleteComponent(ctx, rc, s, m) },
		})
		if handled || e != nil {
			return handled, e
		}
	}
	return false, nil
}
func deleteComponent(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember) error {
	service, err := verifyComponent(ctx, rc, m)
	if err != nil {
		return err
	}
	var op infra.Op
	if service != nil {
		if m.Public {
			access, err := engine.PublicHTTPS(rc.Backend)
			if err != nil {
				return err
			}
			op, err = access.SetSubdomainAccess(ctx, m.Name, false)
			if err != nil {
				return err
			}
			s.Pending = &componentPending{Op: op, Component: m.Component, Action: "public"}
			return nil
		}
		op, err = rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
		if err != nil {
			return err
		}
	}
	s.Pending = &componentPending{Op: op, Component: m.Component, Action: "delete"}
	return nil
}
func componentStatus(ctx context.Context, rc *engine.Context, m *componentMember) (sbspec.Status, error) {
	if rc.Agents == nil {
		return sbspec.Status{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return sbspec.Status{}, nil
	}
	defer client.Close()
	status, err := client.Status(ctx)
	if err != nil || status.ConfigVersion < m.ConfigVersion {
		return sbspec.Status{}, nil
	}
	var ready sbspec.Status
	if err = client.Engine(ctx, "supabase", "status", struct{}{}, &ready); err != nil {
		return sbspec.Status{}, nil
	}
	return ready, nil
}
func (p *Provider) reloadComponent(ctx context.Context, rc *engine.Context, m *componentMember) {
	if rc.Agents == nil {
		return
	}
	client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err == nil {
		defer client.Close()
		_ = client.Reload(ctx)
	}
}
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	// Deletion supersedes a deployment's application-readiness wait.
	if s.Pending != nil && s.Pending.Action == "deploy" {
		s.Pending = nil
	}
	if busy, e := observeComponent(ctx, rc, s); busy || e != nil {
		return false, e
	}
	for i := len(stackComponents) - 1; i >= 0; i-- {
		if m := s.Members[stackComponents[i].Name]; m != nil {
			return false, deleteComponent(ctx, rc, s, m)
		}
	}
	done, err = p.Core.Cleanup(ctx, rc, opts)
	if err != nil || !done || opts.KeepVolumes {
		return done, err
	}
	for _, key := range []string{credentialsKey, functionsKey, "supabase/smtp-password", "supabase/bootstrap-digest"} {
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return false, err
		}
	}
	return true, nil
}
