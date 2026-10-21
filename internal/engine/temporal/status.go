// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

// Sync converges bindings, runtime recovery, ordered components and namespaces.
func (p *Provider) Sync(ctx context.Context, rc *engine.Context) (result engine.Result, err error) {
	result.RequeueAfter = 2 * time.Second
	s, err := loadState(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if p.options.Bindings == nil {
		return result, engine.Invalid("PostgreSQL bindings are unavailable")
	}
	o, err := optionsFor(rc.Cluster)
	if err != nil {
		return result, err
	}
	var conn pgbinding.Connection
	if s.BindingID == "" {
		conn, err = p.options.Bindings.Ensure(ctx, bindingRequest(rc.Cluster, o))
		if err == nil {
			s.BindingID = conn.BindingID
		}
	}
	if err == nil {
		conn, err = p.options.Bindings.Resolve(ctx, s.BindingID)
	}
	if errors.Is(err, pgbinding.ErrSourceUnavailable) {
		s.WaitingSource = true
		rc.Cluster.Status.Message = "Waiting for bound PostgreSQL source"
		return result, nil
	}
	if err != nil {
		return result, engine.ErrUnavailable
	}
	s.WaitingSource = false
	s.Binding = bindingStatus{
		SourceNamespace: o.Persistence.Postgres.Namespace,
		SourceCluster:   o.Persistence.Postgres.Cluster,
		Shared:          conn.Shared, ConsumerCount: conn.ConsumerCount,
	}
	ca, err := ensureCA(ctx, rc)
	if err != nil {
		return result, err
	}
	if s.RestoreRecovery != nil {
		done, e := p.recoverRestore(ctx, rc, s, ca)
		if e != nil {
			return result, e
		}
		if done {
			s.RestoreRecovery = nil
		}
		return result, nil
	}
	if busy, e := journalStoppedComponents(ctx, rc, s); busy || e != nil {
		return result, e
	}
	busy, err := p.syncComponents(ctx, rc, s, conn, ca)
	if err != nil || busy {
		return result, err
	}
	for _, m := range s.Members {
		observed, e := componentStatus(ctx, rc, m)
		if e != nil {
			return result, e
		}
		m.Observed = observed
		if !observed.Ready {
			return result, nil
		}
	}
	if err = p.syncNamespaces(ctx, rc, s, o); err != nil {
		return result, err
	}
	rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
	result.RequeueAfter = 30 * time.Second
	return result, nil
}

func (p *Provider) syncNamespaces(ctx context.Context, rc *engine.Context, s *componentState, o EngineOptions) error {
	m := s.Members[frontendComponent(rc.Cluster)]
	if m == nil {
		return engine.ErrUnavailable
	}
	observed, e := componentStatus(ctx, rc, m)
	if e != nil {
		return e
	}
	m.Observed = observed
	if !observed.Ready {
		return engine.ErrUnavailable
	}
	client, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return engine.ErrUnavailable
	}
	defer client.Close()
	desired, e := namespaceRequest(o)
	if e != nil {
		return e
	}
	var namespaces []tspec.NamespaceStatus
	if e = client.Engine(ctx, tspec.Engine, tspec.ActionEnsureNamespaces, desired, &namespaces); e != nil {
		return engine.ErrUnavailable
	}
	s.Namespaces = namespaces
	s.NamespaceHash = configDigest(desired)
	return nil
}

// Status observes component health and publishes password-free binding metadata.
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	out := rc.Cluster.Status
	out.Phase = domain.PhaseUpdating
	out.Members = nil
	out.Conditions = nil
	out.Version = ""
	s, e := loadState(rc.Cluster)
	if e != nil {
		return out, e
	}
	out.Version = s.Version
	out.ObservedAt = clockNow(rc)
	if s.RestoreRecovery != nil {
		message := "Temporal restore recovery owns stopped runtimes until every component is Ready"
		if s.RestoreRecovery.Failure != "" {
			message = s.RestoreRecovery.Failure + "; " + message
		}
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "TemporalRestoreRecovery", Status: "False", Reason: "RecoveringRuntimes", Message: message,
		})
	}
	if s.WaitingSource {
		out.Message = "Waiting for bound PostgreSQL source"
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "PostgreSQLBinding", Status: "False",
			Reason: "SourceUnavailable", Message: out.Message,
		})
		return out, nil
	}
	binding, _ := json.Marshal(s.Binding)
	out.Conditions = append(out.Conditions, domain.Condition{
		Type: "PostgreSQLBinding", Status: "True", Reason: "Bound", Message: string(binding),
	})
	namespaces, _ := json.Marshal(s.Namespaces)
	out.Conditions = append(out.Conditions, domain.Condition{
		Type: "TemporalNamespaces", Status: "True", Reason: "Observed", Message: string(namespaces),
	})
	ready := s.BindingID != "" && s.Pending == nil && s.Upgrade == nil &&
		s.RestoreRecovery == nil && s.Version == rc.Cluster.Spec.Version && s.NamespaceHash != ""
	frontend := memberName(rc.Cluster, frontendComponent(rc.Cluster)) + ":7233"
	out.Connection = domain.Connection{Hosts: []string{frontend}, URI: "temporal://" + frontend}
	out.Connection.Endpoints = []domain.Endpoint{{Name: "frontend", URL: "temporal://" + frontend}}
	if ui := s.Members[tspec.UI]; ui != nil && ui.Public {
		access, e := engine.PublicHTTPS(rc.Backend)
		if e != nil {
			return out, e
		}
		urls, e := access.SubdomainURLs(ctx, ui.Name)
		if e != nil {
			return out, e
		}
		for _, url := range urls {
			out.Connection.Endpoints = append(out.Connection.Endpoints, domain.Endpoint{Name: "ui", URL: url, Public: true})
		}
	}
	for _, name := range tspec.RollOrder {
		if rc.Cluster.Spec.Components.Extra[name].Replicas == 0 {
			continue
		}
		m := s.Members[name]
		if m == nil {
			ready = false
			continue
		}
		observed, e := componentStatus(ctx, rc, m)
		if e != nil {
			return out, e
		}
		member := domain.MemberStatus{Name: m.Name, Set: name, Role: name, Version: observed.Version, State: "Waiting", Health: observed.Ready}
		if observed.Ready {
			member.State = "Ready"
		} else {
			ready = false
		}
		out.Members = append(out.Members, member)
		schemas, _ := json.Marshal(struct {
			Component, Persistence, Visibility string
		}{name, observed.SchemaVersion, observed.VisibilitySchemaVersion})
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "TemporalSchema", Key: name, Status: "True", Reason: "Observed", Message: string(schemas),
		})
	}
	if s.RestoreRecovery != nil {
		out.Message = "Recovering Temporal runtimes after restore; maintenance remains fenced"
	} else if s.Upgrade != nil {
		raw, _ := json.Marshal(s.Upgrade)
		out.Message = "Upgrading Temporal: " + string(raw)
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "TemporalUpgrade", Status: "False", Reason: "Rolling", Message: string(raw),
		})
	} else if ready {
		out.Phase = domain.PhaseReady
		out.Message = "Temporal is Ready"
	} else {
		out.Message = "Waiting for Temporal components and namespaces"
	}
	return out, nil
}

// Cleanup removes runtimes before releasing bound databases and sealed credentials.
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
	// Infrastructure deletion supersedes application readiness, not an unfinished import.
	if s.Pending != nil && s.Pending.Action == "deploy" {
		s.Pending = nil
	}
	if busy, e := observePending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	for i := len(tspec.RollOrder) - 1; i >= 0; i-- {
		m := s.Members[tspec.RollOrder[i]]
		if m == nil {
			continue
		}
		if m.ImportBatch != nil {
			return false, importComponent(ctx, rc, s, m, domain.ComponentSpec{Replicas: m.Replicas, Resources: m.Resources})
		}
		return false, deleteComponent(ctx, rc, s, m)
	}
	if p.options.Bindings == nil {
		return false, engine.Invalid("PostgreSQL bindings are unavailable")
	}
	o, err := optionsFor(rc.Cluster)
	if err != nil {
		return false, err
	}
	// Ensure can reserve source ownership before returning its first response.
	// Always release the deterministic identity, even without a saved receipt.
	bindingID := pgbinding.BindingID(bindingRequest(rc.Cluster, o))
	if err = p.options.Bindings.Release(ctx, bindingID, opts.KeepVolumes); err != nil {
		return false, engine.ErrUnavailable
	}
	if s.BindingID != "" && s.BindingID != bindingID {
		if err = p.options.Bindings.Release(ctx, s.BindingID, opts.KeepVolumes); err != nil {
			return false, engine.ErrUnavailable
		}
	}
	s.BindingID = ""
	if !opts.KeepVolumes {
		keys := []string{"tls/ca.crt", "tls/ca.key", UIOIDCClientSecretKey, certificateJournalKey, rotationStageKey, rotationTrustKey}
		for _, component := range tspec.RollOrder {
			name := memberName(rc.Cluster, component)
			keys = append(keys,
				ConfigSecretPrefix+name, "temporal/agent-cert/"+name,
				"temporal/internode-cert/"+name, "temporal/frontend-cert/"+name,
			)
			if e := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, key := range keys {
			if e := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return false, e
			}
		}
	}
	s.Version = ""
	s.NamespaceHash = ""
	s.Namespaces = nil
	s.Upgrade = nil
	return true, nil
}
