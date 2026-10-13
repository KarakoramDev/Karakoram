// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"slices"

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
		p.bumpConfig(s)
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
		done, e := p.restoreComponentAction(ctx, rc, s, &cp, "stop")
		if e != nil {
			return operationResult(cp, false), e
		}
		if done {
			cp.Index = 0
			cp.Stage = "destructive"
		}
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
		indices, e := client.ListIndices(ctx, true)
		if e != nil {
			return operationResult(cp, false), e
		}
		system, e := client.SystemIndices(ctx)
		if e != nil {
			return operationResult(cp, false), e
		}
		// Feature-state restoration owns system indices, including security.
		indices = slices.DeleteFunc(indices, func(name string) bool { return system[name] })
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
		// Platform-stopped services cannot acknowledge configuration. Publish
		// the released intent before Start; fresh agents fetch it at startup.
		s.RestoreActive = false
		p.bumpConfig(s)
		cp.ConfigVersion = s.ConfigVersion
		cp.Index = 0
		cp.Stage = "start"
	case "start":
		done, e := p.restoreComponentAction(ctx, rc, s, &cp, "start")
		if e != nil {
			return operationResult(cp, false), e
		}
		if done {
			cp.Stage = "running"
		}
	case "running":
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

// restoreComponentAction checkpoints each service-wide stop/start receipt and
// waits for platform completion before advancing to the next component.
func (p *Provider) restoreComponentAction(ctx context.Context, rc *engine.Context, s *state, cp *operationState, action string) (bool, error) {
	if cp.Pending != nil {
		if s.Pending == nil && s.Retry == nil {
			s.Pending = cp.Pending
		}
		_, err := p.observePending(ctx, rc, s)
		if err == nil && s.Retry != nil {
			_, _, err = p.retryPending(ctx, rc, s)
		}
		if s.Pending != nil {
			cp.Pending = s.Pending
		} else if s.Retry != nil {
			cp.Pending = s.Retry
		} else {
			cp.Pending = nil
			cp.Index++
		}
		return false, err
	}
	if cp.Index >= len(cp.Names) {
		return true, nil
	}
	m := s.find(cp.Names[cp.Index])
	if m == nil {
		cp.Index++
		return false, nil
	}
	err := p.issueAction(ctx, rc, s, &pending{Action: action, Member: m.Name})
	cp.Pending = s.Pending
	if cp.Pending == nil {
		cp.Pending = s.Retry
	}
	return false, err
}
