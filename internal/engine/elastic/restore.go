// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func componentProcess(m *member) string {
	if m.Kind == kindAPM {
		return "apm-server"
	}
	return m.Kind
}

func restoreVersionCompatible(source, target string) bool {
	a, ok := parseVersion(source)
	if !ok {
		return false
	}
	b, ok := parseVersion(target)
	if !ok || b[0] > a[0]+1 {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return true
}

func (p *Provider) runRestore(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, name string) (result engine.OpResult, runErr error) {
	if name == "" {
		return operationResult(cp, true), operationInvalid("restore name is required")
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return operationResult(cp, false), err
	}
	defer func() {
		if runErr == nil {
			return
		}
		r.Error = runErr.Error()
		if result.Done {
			stamp := operationNow(rc)
			r.Phase, r.CompletedAt = domain.PhaseFailed, &stamp
		}
		runErr = errors.Join(runErr, rc.Store.UpdateRestore(ctx, r))
	}()
	source := domain.ClusterRef{Namespace: r.SourceNamespace, Name: r.Source.SourceCluster}
	if source.Namespace == "" {
		source.Namespace = rc.Cluster.Ref().Namespace
	}
	if source.Name == "" {
		source.Name = rc.Cluster.Name
	}
	b, err := rc.Store.GetBackup(ctx, source, r.Source.Backup)
	if err != nil {
		return operationResult(cp, false), err
	}
	if cp.Stage == "" && (r.Source.PITRTime != nil || b.Type != "snapshot" || b.Provider != "elasticsearch" || b.Phase != domain.PhaseSucceeded || !restoreVersionCompatible(b.SourceVersion, rc.Cluster.Spec.Version)) {
		stamp := operationNow(rc)
		r.Phase = domain.PhaseFailed
		r.CompletedAt = &stamp
		r.Error = "restore requires a succeeded compatible Elasticsearch snapshot"
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "done"
		cp.Failure = r.Error
		return operationResult(cp, true), operationInvalid(r.Error)
	}
	storage, err := rc.Store.GetBackupStorage(ctx, source.Namespace, b.Storage)
	if err != nil {
		return operationResult(cp, false), err
	}
	base, _, snapshot, err := recordedSnapshot(storage, b)
	if err != nil {
		return operationResult(cp, false), err
	}
	readOnly := source != rc.Cluster.Ref() || base != clusterBasePath(storage, rc.Cluster, s)
	repo, err := p.operationRepository(ctx, rc, s, storage, readOnly, base)
	if errors.Is(err, errWaiting) {
		return operationResult(cp, false), nil
	}
	if err != nil {
		return operationResult(cp, false), err
	}
	client, err := p.es(ctx, rc, s)
	if err != nil {
		return operationResult(cp, false), err
	}
	defer client.Close()
	switch cp.Stage {
	case "":
		info, e := client.GetSnapshot(ctx, repo, snapshot)
		if e != nil {
			return operationResult(cp, false), e
		}
		if info.State != "SUCCESS" {
			return operationResult(cp, true), operationInvalid("source snapshot is not successful")
		}
		cp.Names = nil
		for _, m := range s.componentMembers() {
			cp.Names = append(cp.Names, m.Name)
		}
		s.RestoreActive = true
		s.Phase = "restore"
		r.Phase = domain.PhaseRunning
		stamp := operationNow(rc)
		if r.StartedAt == nil {
			r.StartedAt = &stamp
		}
		r.Error = ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return operationResult(cp, false), err
		}
		if err = operationEvent(ctx, rc, op, "RestoreStarted", "Elasticsearch snapshot restore started"); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "stop"
	case "stop":
		if cp.Index < len(cp.Names) {
			m := s.find(cp.Names[cp.Index])
			if m != nil {
				agent, e := p.agentClient(ctx, rc, m.Name)
				if e != nil {
					return operationResult(cp, false), e
				}
				defer agent.Close()
				if e = agent.Process(ctx, agentapi.ProcessAction{Process: componentProcess(m), Action: "stop"}); e != nil {
					return operationResult(cp, false), e
				}
			}
			cp.Index++
		} else {
			cp.Index = 0
			cp.Stage = "stopped"
		}
	case "stopped":
		for _, name := range cp.Names {
			m := s.find(name)
			if m == nil {
				continue
			}
			agent, e := p.agentClient(ctx, rc, name)
			if e != nil {
				return operationResult(cp, false), e
			}
			defer agent.Close()
			status, e := agent.Status(ctx)
			if e != nil {
				return operationResult(cp, false), e
			}
			if proc := status.Processes[componentProcess(m)]; proc.PID != 0 || proc.State == "running" {
				return operationResult(cp, false), nil
			}
		}
		cp.Stage = "destructive"
	case "destructive":
		if err = client.PutClusterSettings(ctx, nil, map[string]any{"action.destructive_requires_name": false}); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "streams"
	case "streams":
		if err = client.DeleteDataStreams(ctx, "*"); err != nil && !esapi.IsNotFound(err) {
			return operationResult(cp, false), err
		}
		cp.Stage = "indices"
	case "indices":
		indices, e := client.ListIndices(ctx, false)
		if e != nil {
			return operationResult(cp, false), e
		}
		// A non-hidden listing can still contain dot-prefixed indices. Never delete
		// system/security indices: snapshot feature-state restoration owns them.
		indices = slices.DeleteFunc(indices, func(name string) bool { return strings.HasPrefix(name, ".") })
		if err = client.DeleteIndices(ctx, indices); err != nil && !esapi.IsNotFound(err) {
			return operationResult(cp, false), err
		}
		cp.Before, err = client.ListIndices(ctx, true)
		if err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "submit"
	case "submit":
		// Newly recreated snapshot indices (including data-stream backing
		// indices) are the receipt if POST succeeded before checkpoint storage.
		// Existing feature indices or unrelated shard recovery are not receipts.
		indices, e := client.ListIndices(ctx, true)
		if e != nil {
			return operationResult(cp, false), e
		}
		info, e := client.GetSnapshot(ctx, repo, snapshot)
		if e != nil {
			return operationResult(cp, false), e
		}
		restored := false
		for _, index := range indices {
			if slices.Contains(info.Indices, index) && !slices.Contains(cp.Before, index) {
				restored = true
			}
		}
		if !restored {
			if err = client.RestoreSnapshot(ctx, repo, snapshot, esapi.RestoreRequest{Indices: "*", IncludeGlobalState: true, IncludeAliases: true}); err != nil {
				return operationResult(cp, false), err
			}
		}
		cp.Stage = "recover"
	case "recover":
		recovering, e := client.RecoveryInProgress(ctx)
		if e != nil {
			return operationResult(cp, false), e
		}
		if recovering {
			return operationResult(cp, false), nil
		}
		healthy, e := p.healthy(ctx, client)
		if e != nil {
			return operationResult(cp, false), e
		}
		if !healthy {
			return operationResult(cp, false), nil
		}
		cp.Stage = "reset"
	case "reset":
		if err = client.PutClusterSettings(ctx, nil, map[string]any{"action.destructive_requires_name": nil}); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "security"
	case "security":
		if err = p.ensureSecurity(ctx, rc, s, true); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "config"
	case "config":
		p.bumpConfig(s)
		cp.ConfigVersion = s.ConfigVersion
		p.reloadMembers(ctx, rc, cp.Names...)
		cp.Stage = "configured"
	case "configured":
		for _, name := range cp.Names {
			m := s.find(name)
			if m == nil {
				continue
			}
			agent, e := p.agentClient(ctx, rc, name)
			if e != nil {
				return operationResult(cp, false), e
			}
			defer agent.Close()
			status, e := agent.Status(ctx)
			if e != nil {
				return operationResult(cp, false), e
			}
			if status.ConfigVersion < cp.ConfigVersion {
				p.reloadMembers(ctx, rc, name)
				return operationResult(cp, false), nil
			}
		}
		cp.Index = 0
		cp.Stage = "start"
		s.RestoreActive = false
	case "start":
		if cp.Index < len(cp.Names) {
			m := s.find(cp.Names[cp.Index])
			if m != nil {
				agent, e := p.agentClient(ctx, rc, m.Name)
				if e != nil {
					return operationResult(cp, false), e
				}
				defer agent.Close()
				if e = agent.Process(ctx, agentapi.ProcessAction{Process: componentProcess(m), Action: "start"}); e != nil {
					return operationResult(cp, false), e
				}
			}
			cp.Index++
		} else {
			cp.Stage = "running"
		}
	case "running":
		for _, name := range cp.Names {
			m := s.find(name)
			if m == nil {
				continue
			}
			agent, e := p.agentClient(ctx, rc, name)
			if e != nil {
				return operationResult(cp, false), e
			}
			defer agent.Close()
			status, e := agent.Status(ctx)
			if e != nil {
				return operationResult(cp, false), e
			}
			if status.Processes[componentProcess(m)].State != "running" {
				return operationResult(cp, false), nil
			}
		}
		stamp := operationNow(rc)
		r.Phase = domain.PhaseSucceeded
		r.CompletedAt = &stamp
		r.Error = ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return operationResult(cp, false), err
		}
		if err = operationEvent(ctx, rc, op, "RestoreCompleted", "Elasticsearch snapshot restore completed"); err != nil {
			return operationResult(cp, false), err
		}
		s.RestoreActive = false
		s.Phase = "ready"
		if name == "initial-restore" {
			s.InitialRestore = "done"
		}
		cp.Stage = "done"
		return operationResult(cp, true), nil
	default:
		return operationResult(cp, false), operationInvalid("unknown restore checkpoint")
	}
	return operationResult(cp, false), nil
}
