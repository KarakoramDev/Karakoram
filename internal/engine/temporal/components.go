// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func verifyComponent(ctx context.Context, rc *engine.Context, m *componentMember) (*infra.Service, error) {
	svc, e := rc.Backend.GetService(ctx, m.Name)
	if errors.Is(e, infra.ErrNotFound) {
		return nil, nil
	}
	if e != nil || svc == nil {
		return svc, e
	}
	token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return nil, e
	}
	if token == nil {
		return nil, engine.UnverifiedOwnership(svc)
	}
	if marker := svc.Ownership[engine.OwnerVariable]; marker != "" &&
		marker != engine.OwnerMarker(token.Token, rc.Cluster.Name, m.Name, svc.Hostname) {
		return nil, engine.UnverifiedOwnership(svc)
	}
	if svc.ID != m.ServiceID && !engine.HasOwnership(svc, token.Token, rc.Cluster.Name, m.Name) {
		return nil, engine.UnverifiedOwnership(svc)
	}
	if e = engine.EnsureOwnerMarker(ctx, rc.Backend, svc, token.Token, rc.Cluster.Name, m.Name); e != nil {
		return nil, e
	}
	m.ServiceID = svc.ID
	return svc, nil
}

func importComponent(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember, d domain.ComponentSpec) error {
	token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return e
	}
	if token == nil {
		return engine.ErrNotFound
	}
	if m.ImportBatch == nil {
		svc, e := verifyComponent(ctx, rc, m)
		if e != nil {
			return e
		}
		if svc != nil {
			if svc.Importing {
				return engine.Invalid("incomplete component import has no frozen journal")
			}
			m.Stage = engine.ComponentImported
			return nil
		}
		spec := engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{
			Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token,
			Resources: d.Resources, Containers: d.Replicas,
		})
		m.ImportBatch = engine.NewImportPlan([]infra.ServiceSpec{spec})
		m.Resources = d.Resources
		m.Replicas = d.Replicas
		return nil
	}
	op, e := m.ImportBatch.Submit(ctx, rc, m.Name, token.Token)
	if e != nil {
		return e
	}
	s.Pending = &componentPending{Component: m.Component, Action: "import", Op: op}
	return nil
}

func observePending(ctx context.Context, rc *engine.Context, s *componentState) (bool, error) {
	q := s.Pending
	if q == nil {
		return false, nil
	}
	m := s.Members[q.Component]
	if m == nil {
		return true, engine.ErrNotFound
	}
	if q.Action == "import" && m.ImportBatch != nil {
		token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return true, e
		}
		if token == nil {
			return true, engine.ErrNotFound
		}
		op, e := engine.ResumeImportPlan(ctx, rc, m.ImportBatch, m.Name, token.Token)
		if e != nil {
			return true, e
		}
		if op != nil {
			q.Op = *op
			return true, nil
		}
	}
	status, e := rc.Backend.OpStatus(ctx, q.Op)
	if e != nil {
		return true, e
	}
	if !status.Done {
		return true, nil
	}
	if status.Failed {
		return true, fmt.Errorf("temporal component %s %s failed (%s)", q.Component, q.Action, status.Status)
	}
	switch q.Action {
	case "import":
		svc, e := verifyComponent(ctx, rc, m)
		if e != nil {
			return true, e
		}
		if svc == nil || svc.Importing {
			return true, nil
		}
		m.ImportBatch = nil
		m.Stage = engine.ComponentImported
	case "deploy":
		m.Stage = engine.ComponentDeployed
		m.RuntimeDeployed = true
	case "scale":
		m.Replicas = q.Replicas
		m.Stage = engine.ComponentDeployed
	case "resize":
		m.Resources = q.Resources
		m.Stage = engine.ComponentDeployed
	case "public":
		m.Public = q.Public
	case "stop-public":
		m.Stage = engine.ComponentImported
	case "delete":
		svc, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil && !errors.Is(e, infra.ErrNotFound) {
			return true, e
		}
		if svc != nil {
			return true, nil
		}
		delete(s.Members, q.Component)
	}
	s.Pending = nil
	return true, nil
}

func componentStatus(ctx context.Context, rc *engine.Context, m *componentMember) (tspec.Status, error) {
	out := tspec.Status{Component: m.Component, Version: m.Version, RestartToken: m.ConfigHash}
	if rc.Agents == nil {
		return out, nil
	}
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationSweepTimeout)
	defer cancel()
	ready, e := engine.ComponentInstancesHealthy(ctx, rc, m.Name, max(1, m.Replicas), m.ConfigVersion, engine.ComponentProbe{Ready: func(client *agentapi.Client) (bool, error) {
		var observed tspec.Status
		if e := client.Engine(ctx, tspec.Engine, tspec.ActionStatus, struct{}{}, &observed); e != nil {
			return false, e
		}
		out.SchemaVersion = observed.SchemaVersion
		out.VisibilitySchemaVersion = observed.VisibilitySchemaVersion
		return observed.Ready && observed.RestartToken == m.ConfigHash && observed.Version == m.Version && observed.Component == m.Component, nil
	}})
	out.Ready = ready
	// Health failures are observations, never secret-bearing remote diagnostics.
	if e != nil {
		out.Detail = "waiting for all instances to apply the desired configuration and become Ready"
	}
	return out, nil
}

func reloadComponent(ctx context.Context, rc *engine.Context, m *componentMember) {
	if rc.Agents == nil {
		return
	}
	client, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if e == nil {
		defer client.Close()
		_ = client.Reload(ctx)
	}
}

func deleteComponent(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember) error {
	svc, e := verifyComponent(ctx, rc, m)
	if e != nil {
		return e
	}
	var op infra.Op
	if svc != nil {
		if m.Public {
			access, e := engine.PublicHTTPS(rc.Backend)
			if e != nil {
				return e
			}
			op, e = access.SetSubdomainAccess(ctx, m.Name, false)
			if e != nil {
				return e
			}
			s.Pending = &componentPending{Component: m.Component, Action: "public", Op: op}
			return nil
		}
		op, e = rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
		if e != nil {
			return e
		}
	}
	s.Pending = &componentPending{Component: m.Component, Action: "delete", Op: op}
	return nil
}

// syncUIExposure serializes route changes before configuration publication.
// An anonymous private UI is stopped before enabling its public callback origin.
func syncUIExposure(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember) (bool, error) {
	if m.Component != tspec.UI || m.Stage == engine.ComponentPlanned {
		return false, nil
	}
	if busy, err := observePending(ctx, rc, s); busy || err != nil {
		return true, err
	}
	o, err := optionsFor(rc.Cluster)
	if err != nil {
		return true, err
	}
	if o.UI.PublicHTTPS == m.Public {
		return false, nil
	}
	if o.UI.PublicHTTPS {
		secret, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), UIOIDCClientSecretKey)
		if err != nil && !missing(err) {
			return true, err
		}
		if len(secret) == 0 || o.UI.OIDC == nil {
			return true, engine.Invalid("ui.publicHttps requires OIDC and a sealed client secret")
		}
		if m.Stage == engine.ComponentJoined || m.Stage == engine.ComponentDeployed {
			m.RuntimeDeployed = true
		}
		if !m.RuntimeDeployed {
			// Register the runtime's HTTP port through a private deployment
			// before discovering a public origin. The next pass stops this
			// private UI before enabling the route and publishing OIDC.
			return false, nil
		}
		if m.Stage != engine.ComponentImported {
			op, err := rc.Backend.Stop(ctx, m.Name)
			if err != nil {
				return true, err
			}
			s.Pending = &componentPending{Component: m.Component, Action: "stop-public", Op: op}
			return true, nil
		}
	}
	access, err := engine.PublicHTTPS(rc.Backend)
	if err != nil {
		return true, err
	}
	op, err := access.SetSubdomainAccess(ctx, m.Name, o.UI.PublicHTTPS)
	if err != nil {
		return true, err
	}
	s.Pending = &componentPending{
		Component: m.Component, Action: "public", Op: op, Public: o.UI.PublicHTTPS,
	}
	return true, nil
}

func (p *Provider) syncComponents(ctx context.Context, rc *engine.Context, s *componentState, conn pgbinding.Connection, ca *pki.CA) (bool, error) {
	if busy, e := observePending(ctx, rc, s); busy || e != nil {
		return true, e
	}

	// Upgrade admission and settle checkpoints precede role reconciliation.
	if s.Version != "" && s.Version != rc.Cluster.Spec.Version && s.Upgrade == nil {
		// No schema migration starts until every current component is healthy.
		for _, name := range tspec.RollOrder {
			m := s.Members[name]
			if m == nil {
				continue
			}
			status, e := componentStatus(ctx, rc, m)
			if e != nil {
				return true, e
			}
			m.Observed = status
			if !status.Ready {
				return true, nil
			}
		}
		s.Upgrade = &upgradeState{From: s.Version, To: rc.Cluster.Spec.Version}
		return true, nil
	}
	if s.Upgrade != nil && !s.Upgrade.SettleUntil.IsZero() {
		if clockNow(rc).Before(s.Upgrade.SettleUntil) {
			return true, nil
		}
		s.Upgrade.SettleUntil = time.Time{}
	}

	// Components converge in RollOrder; UI additionally requires a Ready frontend.
	for _, def := range componentSet(rc.Cluster.Spec.Topology.Type) {
		m := s.Members[def.Name]
		d := rc.Cluster.Spec.Components.Extra[def.Name]
		if def.Name == tspec.UI && d.Replicas > 0 {
			f := s.Members[frontendComponent(rc.Cluster)]
			if f == nil {
				return true, nil
			}
			status, e := componentStatus(ctx, rc, f)
			if e != nil {
				return true, e
			}
			f.Observed = status
			if !status.Ready {
				return true, nil
			}
		}
		if m != nil && m.ImportBatch != nil {
			return true, importComponent(ctx, rc, s, m, d)
		}
		current := engine.ComponentRuntime{HorizontalScaling: rc.Backend.Capabilities().HorizontalScaling}
		if m != nil {
			svc, e := verifyComponent(ctx, rc, m)
			if e != nil {
				return true, e
			}
			if svc == nil {
				m.Stage = engine.ComponentPlanned
				m.ServiceID = ""
				m.Public = false
				m.RuntimeDeployed = false
			}
			current.Exists = true
			current.Stage = m.Stage
			current.Replicas = m.Replicas
			current.Resources = m.Resources
		}
		// Exposure transitions precede every UI configuration publication.
		if m != nil && d.Replicas > 0 {
			if busy, e := syncUIExposure(ctx, rc, s, m); busy || e != nil {
				return true, e
			}
		}
		if m != nil && d.Replicas > 0 && m.Stage == engine.ComponentJoined {
			sp, e := p.desiredSpec(ctx, rc, m, conn, ca)
			if e != nil {
				return true, e
			}
			if sp.RestartToken != m.ConfigHash {
				if s.Upgrade != nil {
					s.Upgrade.Component = m.Component
				}
				if sp.Version != m.Version {
					m.Stage = engine.ComponentImported
					return true, nil
				}
				if e = p.publishConfig(ctx, rc, m, sp, ca); e != nil {
					return true, e
				}
				m.Stage = engine.ComponentDeployed
				reloadComponent(ctx, rc, m)
				return true, nil
			}
			// Transport certificates renew independently of application material.
			cert, e := componentCertificate(ctx, rc, m, "agent", ca)
			if e != nil {
				return true, e
			}
			config, e := p.MemberConfig(ctx, rc, m.Name)
			if e != nil {
				return true, e
			}
			if cert.CertPEM != config.AgentTLS.CertPEM {
				if e = p.publishConfig(ctx, rc, m, sp, ca); e != nil {
					return true, e
				}
				m.Stage = engine.ComponentDeployed
				reloadComponent(ctx, rc, m)
				return true, nil
			}
		}
		handled, e := engine.ReconcileStatelessComponent(ctx, def, current, d, engine.ComponentCallbacks{
			Import: func(ctx context.Context) error {
				if m == nil {
					name := memberName(rc.Cluster, def.Name)
					token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), name)
					if e != nil && !missing(e) {
						return e
					}
					if token == nil {
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
					s.Members[def.Name] = &componentMember{
						Name: name, Component: def.Name, Stage: engine.ComponentPlanned,
						Replicas: d.Replicas, Resources: d.Resources,
					}
					return nil
				}
				return importComponent(ctx, rc, s, m, d)
			},
			Deploy: func(ctx context.Context) error {
				sp, e := p.desiredSpec(ctx, rc, m, conn, ca)
				if e != nil {
					return e
				}
				if e = p.publishConfig(ctx, rc, m, sp, ca); e != nil {
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
				s.Pending = &componentPending{Component: m.Component, Action: "deploy", Op: op}
				return nil
			},
			AwaitReady: func(ctx context.Context) error {
				status, e := componentStatus(ctx, rc, m)
				if e != nil {
					return e
				}
				m.Observed = status
				if status.Ready {
					m.Stage = engine.ComponentJoined
					m.NativeReadiness = nil
					if s.Upgrade != nil && s.Upgrade.Component == m.Component {
						s.Upgrade.Component = ""
						if m.Component == tspec.History {
							s.Upgrade.SettleUntil = clockNow(rc).Add(60 * time.Second)
						}
					}
				} else {
					reloadComponent(ctx, rc, m)
				}
				return nil
			},
			Scale: func(ctx context.Context, n int) error {
				op, e := rc.Backend.SetContainerCount(ctx, m.Name, n, n)
				if e == nil {
					s.Pending = &componentPending{Component: m.Component, Action: "scale", Op: op, Replicas: n}
				}
				return e
			},
			Resize: func(ctx context.Context, r domain.Resources) error {
				op, e := rc.Backend.SetResources(ctx, m.Name, r)
				if e == nil {
					s.Pending = &componentPending{Component: m.Component, Action: "resize", Op: op, Resources: r}
				}
				return e
			},
			Delete: func(ctx context.Context) error { return deleteComponent(ctx, rc, s, m) },
		})
		if handled || e != nil {
			return handled, e
		}
	}
	s.Version = rc.Cluster.Spec.Version
	s.Upgrade = nil
	s.CredentialGeneration = conn.CredentialGeneration
	return false, nil
}
