// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

// InitialDataSourceApplied is generation-independent: process health and an
// applied spec generation are not evidence that the initial snapshot restored.
func (*Provider) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	if err != nil {
		return false, err
	}
	if s.InitialRestore == "failed" || s.InitialRestore == "abandoned" {
		return false, fmt.Errorf("initial snapshot restore did not complete: %s", s.InitialRestoreError)
	}
	return s.InitialRestore == "done", nil
}

// observeInitialRestore preserves terminal failure across control-plane restarts
// and generation changes. Only the durable successful restore record can repair
// the gap between native completion and saving the provider completion marker.
func observeInitialRestore(ctx context.Context, rc *engine.Context, s *state) error {
	if rc.Cluster.Spec.DataSource == nil || engine.InitialDataSourceUnproven(rc.Cluster) || s.Suspended || s.InitialRestore == "" || s.InitialRestore == "done" || s.InitialRestore == "abandoned" {
		return nil
	}
	if s.InitialRestore == "failed" {
		return fmt.Errorf("%w: initial snapshot restore did not complete: %s", engine.ErrInvalid, s.InitialRestoreError)
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
	if err != nil {
		return err
	}
	switch r.Phase {
	case domain.PhaseSucceeded:
		// Old versions could mark the restore record succeeded after observing
		// health alone. Preserve existing done markers, but never manufacture
		// a missing marker from that record or an accepted-only receipt.
		raw, e := secret(ctx, rc, "operations/initial-restore-"+s.UID+"/restore-submission")
		if e != nil {
			return e
		}
		var receipt struct {
			Repository string `json:"repository"`
			Snapshot   string `json:"snapshot"`
			Completed  bool   `json:"completed"`
		}
		if json.Unmarshal(raw, &receipt) != nil || !receipt.Completed || receipt.Repository == "" || receipt.Snapshot == "" {
			return errors.New("initial snapshot restore lacks durable native completion evidence; reconcile the restored data before proceeding")
		}
		s.InitialRestore = "done"
	case domain.PhaseFailed, domain.PhaseCancelled:
		s.InitialRestore = "failed"
		s.InitialRestoreError = fmt.Sprintf("restore initial-restore ended %s: %s", r.Phase, r.Error)
		return fmt.Errorf("%w: %s", engine.ErrInvalid, s.InitialRestoreError)
	}
	return nil
}

func componentProcess(m *member) string {
	if m.Kind == kindAPM {
		return "apm-server"
	}
	return m.Kind
}

func restoreVersionCompatible(source, target string) bool {
	a, ok := parseVersion(snapshotReleaseVersion(source))
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
	if r.Phase == domain.PhaseFailed || r.Phase == domain.PhaseCancelled {
		return operationResult(cp, true), operationInvalid("restore " + name + " ended " + string(r.Phase) + ": " + r.Error)
	}
	defer func() {
		if runErr == nil {
			return
		}
		// Error is the latest retry diagnostic, not an abort marker. Only a
		// terminal result changes Phase/CompletedAt; successful completion below
		// clears diagnostics after transient native/API failures.
		r.Error = runErr.Error()
		if result.Done {
			stamp := operationNow(rc)
			r.Phase, r.CompletedAt = domain.PhaseFailed, &stamp
		}
		runErr = errors.Join(runErr, rc.Store.UpdateRestore(ctx, r))
	}()
	switch cp.Stage {
	case "recover", "reset", "security", "config", "start", "running":
		raw, e := secret(ctx, rc, "operations/"+op.ID+"/restore-submission")
		if e != nil {
			return operationResult(cp, false), e
		}
		var receipt struct {
			Completed bool `json:"completed"`
		}
		if json.Unmarshal(raw, &receipt) != nil || !receipt.Completed {
			return operationResult(cp, false), errors.New("restore lacks durable native completion evidence; reconcile restored data before proceeding; managed components remain stopped")
		}
	}
	if r.Source.PITRTime != nil {
		cp.Stage, cp.Failure = "done", "Elasticsearch does not support point-in-time restore"
		return operationResult(cp, true), operationInvalid(cp.Failure)
	}
	if r.Source.Backup == "" {
		cp.Stage, cp.Failure = "done", "snapshot backup name is required"
		return operationResult(cp, true), operationInvalid(cp.Failure)
	}
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
	if cp.Stage == "" && (b.Type != "snapshot" || b.Provider != "elasticsearch" || b.Phase != domain.PhaseSucceeded || !restoreVersionCompatible(b.SourceVersion, rc.Cluster.Spec.Version)) {
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
		if esapi.IsNotFound(e) {
			return operationResult(cp, false), engine.InvalidField("source.backup", domain.ReasonNotFound, "Source snapshot is no longer available in the repository; select another successful backup and retry the restore")
		}
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
		cp.Stage = "submit-receipted"
	case "submit":
		return operationResult(cp, false), errors.New("restore submission predates durable receipts; reconcile the native restore and data before starting a new restore; managed components remain stopped")
	case "submit-receipted":
		if err = submitRestoreOnce(ctx, rc, client, op.ID, repo, snapshot); err != nil {
			return operationResult(cp, errors.Is(err, engine.ErrInvalid)), err
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
		healthy, e := p.healthy(ctx, client, rc.Cluster.Spec.Components.Engine.Replicas)
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

// Elasticsearch has no restore idempotency token. Persist intent before POST,
// and accept only a durable successful response, never index names or health.
// An intent without a response is deliberately not replayed: even a snapshot
// containing only feature state can otherwise overwrite data a second time.
func submitRestoreOnce(ctx context.Context, rc *engine.Context, client *esapi.Client, operationID, repo, snapshot string) error {
	if operationID == "" {
		return operationInvalid("restore submission requires an operation identity")
	}
	key := "operations/" + operationID + "/restore-submission"
	type receipt struct {
		Repository  string `json:"repository"`
		Snapshot    string `json:"snapshot"`
		Completed   bool   `json:"completed"`
		NativeError string `json:"nativeError,omitempty"`
	}
	value := receipt{Repository: repo, Snapshot: snapshot}
	raw, err := secret(ctx, rc, key)
	if err == nil {
		var prior receipt
		if json.Unmarshal(raw, &prior) != nil || prior.Repository != repo || prior.Snapshot != snapshot {
			return errors.New("restore submission receipt does not match the source; reconcile the native restore before proceeding")
		}
		if prior.Completed {
			return nil
		}
		return restoreUnknownOutcome(prior.NativeError)
	}
	if !missing(err) {
		return err
	}
	raw, err = json.Marshal(value)
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, key, raw); err != nil {
		return err
	}
	if err = client.RestoreSnapshot(ctx, repo, snapshot, esapi.RestoreRequest{Indices: "*", IncludeGlobalState: true, IncludeAliases: true}); err != nil {
		if errors.Is(err, esapi.ErrRestoreFailed) {
			return operationInvalid("native snapshot restore failed; target data is incomplete")
		}
		// A final validation-looking response cannot exclude an earlier native
		// execution: TransportMasterNodeAction can retry internally after a
		// master disconnect. Preserve evidence without claiming no restore ran.
		value.NativeError = err.Error()
		raw, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return marshalErr
		}
		if persistErr := putSecret(ctx, rc, key, raw); persistErr != nil {
			return persistErr
		}
		return restoreUnknownOutcome(value.NativeError)
	}
	value.Completed = true
	raw, err = json.Marshal(value)
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, key, raw)
}

func restoreUnknownOutcome(nativeError string) error {
	message := "restore submission outcome is unknown"
	if nativeError != "" {
		message += "; native response: " + nativeError
	}
	return operationInvalid(message + "; preserve restore receipts and holds; fence external writers, reconcile GET /_recovery?active_only=false&detailed=true with the source snapshot and acknowledged data; then suspend with keepVolumes: true and resume before requesting a fresh explicit restore; do not delete the receipt or retry blindly; see docs/elasticsearch/snapshots.md; managed components remain stopped")
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
