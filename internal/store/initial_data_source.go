// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type initialAmendmentKey struct{}

// initialDataSourceOperation reads the shared initial-restore fence. Mutation
// admission/claim callers use their writer transaction; HTTP preflight may use
// the reader, without replacing authoritative transactional admission.
func initialDataSourceOperation(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.ClusterRef) (*domain.Cluster, *domain.Operation, error) {
	c, err := scanCluster(query.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name))
	if err != nil {
		return nil, nil, err
	}
	if c.Spec.DataSource == nil {
		return c, nil, nil
	}
	// A genuine abort remains incomplete even if late native completion appears.
	// Running compensation is resumed separately without reopening admission.
	if state := c.Status.InitialDataSource; state == nil || state.AbortMessage == "" {
		var done bool
		if err = query.QueryRowContext(ctx, `SELECT COALESCE(json_extract(internal,'$.postgresBackup.initialApplied'),0) OR COALESCE(json_extract(internal,'$.elasticsearch.initialRestore')='done',0) OR (COALESCE(json_extract(internal,'$.initialRestore.completed'),0) AND COALESCE(json_extract(internal,'$.initialRestore.nativeName'),'')<>'' AND COALESCE(json_extract(internal,'$.initialRestore.failure'),'')='') OR COALESCE(json_extract(internal,'$.cloneImported'),0) FROM clusters WHERE namespace=? AND name=?`, c.Namespace, c.Name).Scan(&done); err != nil {
			return nil, nil, err
		}
		if done {
			return c, nil, nil
		}
		var completed int
		if err = query.QueryRowContext(ctx, `SELECT count(*) FROM records WHERE kind='operation' AND scope=? AND phase='Succeeded' AND json_extract(data,'$.clusterIncarnation')=? AND (json_extract(data,'$.kind')='initial-restore' OR (json_extract(data,'$.kind')='restore' AND json_extract(data,'$.target.name')='initial-restore'))`, ref.Key(), clusterIncarnation(c.CreatedAt.UnixNano())).Scan(&completed); err != nil {
			return nil, nil, err
		}
		if completed != 0 {
			return c, nil, nil
		}
	}
	var raw []byte
	err = query.QueryRowContext(ctx, `SELECT data FROM records WHERE kind='operation' AND scope=? AND json_extract(data,'$.kind')='create' AND json_extract(data,'$.clusterIncarnation')=? ORDER BY created DESC,key DESC LIMIT 1`, ref.Key(), clusterIncarnation(c.CreatedAt.UnixNano())).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return c, &domain.Operation{ID: ref.Key() + " initial restore"}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var op domain.Operation
	if err = json.Unmarshal(raw, &op); err != nil {
		return nil, nil, err
	}
	return c, &op, nil
}

// CheckInitialDataSourceMutation reports the owning operation before native
// capability/state checks can obscure the shared mutation fence. It is read-only;
// queued/spec writes must still use their existing transactional admission.
func (s *Store) CheckInitialDataSourceMutation(ctx context.Context, ref domain.ClusterRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	_, initial, err := initialDataSourceOperation(ctx, s.reader, ref)
	if err != nil {
		return err
	}
	if initial != nil {
		return initialRestoreConflict(initial)
	}
	return nil
}

func initialRestoreConflict(op *domain.Operation) error {
	return domain.Conflict("operation", op.ID, domain.ReasonInvalidState, ErrConflict, "Initial restore operation %s must complete before this mutation; only pre-claim placement/resource/replica amendments are allowed", op.ID)
}

func initialClaimed(ctx context.Context, tx *sql.Tx, c *domain.Cluster) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM records WHERE kind='initial-data-source' AND scope=? AND key=?) + (SELECT count(*) FROM records WHERE kind='operation' AND scope=? AND json_extract(data,'$.clusterIncarnation')=? AND json_extract(data,'$.kind') IN ('restore','initial-restore') AND (phase='Running' OR json_extract(data,'$.startedAt') IS NOT NULL))`, c.Ref().Key(), clusterIncarnation(c.CreatedAt.UnixNano()), c.Ref().Key(), clusterIncarnation(c.CreatedAt.UnixNano())).Scan(&n)
	return n != 0, err
}

func admitInitialSpecAmendment(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef, before, after domain.ClusterSpec, credentials bool) (context.Context, error) {
	c, op, err := initialDataSourceOperation(ctx, tx, ref)
	if err != nil || op == nil {
		return ctx, err
	}
	// Audit capture is optional; admission must always compare the persisted
	// desired state read under this writer transaction.
	before = c.Spec
	claimed, err := initialClaimed(ctx, tx, c)
	if err != nil {
		return ctx, err
	}
	requested, _ := ctx.Value(operationContextKey{}).(*domain.Operation)
	allowedKind := requested == nil || requested.Kind == domain.OperationUpdate
	if before.DataSource != nil && after.DataSource != nil {
		oldSource, newSource := *before.DataSource, *after.DataSource
		if oldSource.Namespace == "" {
			oldSource.Namespace = domain.NormalizedNamespace(ref.Namespace)
		}
		if newSource.Namespace == "" {
			newSource.Namespace = domain.NormalizedNamespace(ref.Namespace)
		}
		before.DataSource, after.DataSource = &oldSource, &newSource
	}
	// Components contain only replica counts and resource bounds. Everything
	// else, including source, version and topology, must remain byte-for-byte semantic equal.
	after.Components = before.Components
	if claimed || credentials || !allowedKind || op.Terminal() || op.ClusterIncarnation == "" || !reflect.DeepEqual(before, after) {
		return ctx, initialRestoreConflict(op)
	}
	return context.WithValue(ctx, initialAmendmentKey{}, op), nil
}

// ClaimInitialDataSource durably closes the amendment window before native work.
// Its writer transaction serializes against every desired-state admission.
func (s *Store) ClaimInitialDataSource(ctx context.Context, ref domain.ClusterRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = claimInitialDataSource(ctx, tx, ref); err != nil {
		return err
	}
	return tx.Commit()
}

// GetInitialDataSourceClaim returns only the current cluster incarnation's
// durable native-work receipt. Absence is ErrNotFound, never an inferred claim.
func (s *Store) GetInitialDataSourceClaim(ctx context.Context, ref domain.ClusterRef) (*domain.InitialDataSourceClaim, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return getInitialDataSourceClaim(ctx, s.reader, ref)
}

func getInitialDataSourceClaim(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.ClusterRef) (*domain.InitialDataSourceClaim, error) {
	var raw []byte
	var incarnation string
	err := query.QueryRowContext(ctx, `SELECT r.data,r.key FROM clusters c JOIN records r ON r.kind='initial-data-source' AND r.scope=? AND r.key=CAST(c.created AS TEXT) WHERE c.namespace=? AND c.name=?`, ref.Key(), domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&raw, &incarnation)
	if err != nil {
		return nil, typed(err)
	}
	var claim domain.InitialDataSourceClaim
	if err := json.Unmarshal(raw, &claim); err != nil {
		return nil, err
	}
	// Older receipts predate the explicit field; their durable key already
	// carries exactly this identity. Never rewrite their original claim time.
	if claim.ClusterIncarnation != "" && claim.ClusterIncarnation != incarnation {
		return nil, domain.Conflict("cluster", ref.Name, domain.ReasonInvalidState, ErrConflict, "Initial restore claim for cluster %s has inconsistent incarnation evidence", ref.Name)
	}
	claim.ClusterIncarnation = incarnation
	return &claim, nil
}

func claimInitialDataSource(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) error {
	c, op, err := initialDataSourceOperation(ctx, tx, ref)
	if err != nil {
		return err
	}
	if err = checkClusterSnapshot(ctx, tx, c); err != nil {
		return err
	}
	if op == nil {
		return nil
	}
	claim, err := admitInitialNativeContinuation(ctx, tx, c, op)
	if err != nil {
		return err
	}
	if claim != nil {
		return nil
	}
	claim = &domain.InitialDataSourceClaim{Operation: op.ID, ClaimedAt: time.Now().UTC(), ClusterIncarnation: clusterIncarnation(c.CreatedAt.UnixNano())}
	return put(ctx, tx, "initial-data-source", ref.Key(), claim.ClusterIncarnation, claim, "", "", "", false)
}

// A budget failure may stop observation, but cannot revoke native work that
// was durably claimed before that failure. It never authorizes a new claim.
func admitInitialNativeContinuation(ctx context.Context, tx *sql.Tx, c *domain.Cluster, create *domain.Operation) (*domain.InitialDataSourceClaim, error) {
	if c.Deleting || c.Status.CleanupStarted || create.CancelRequested || (c.Status.InitialDataSource != nil && c.Status.InitialDataSource.AbortMessage != "") {
		return nil, initialRestoreConflict(create)
	}
	claim, err := getInitialDataSourceClaim(ctx, tx, c.Ref())
	if errors.Is(err, ErrNotFound) {
		claim, err = nil, nil
	}
	if err != nil {
		return nil, err
	}
	if claim != nil && (claim.Operation != create.ID || claim.ClaimedAt.IsZero()) {
		return nil, initialRestoreConflict(create)
	}
	if !create.Terminal() {
		return claim, nil
	}
	if initialClaimPrecedesTrackerFailure(c, create, claim) {
		return claim, nil
	}
	// Preserve the narrowly validated historical supersession recovery path.
	pending, err := legacyInitialRestorePending(ctx, tx, c, create)
	if err != nil {
		return nil, err
	}
	if !pending {
		return nil, initialRestoreConflict(create)
	}
	return claim, nil
}

func initialClaimPrecedesTrackerFailure(c *domain.Cluster, create *domain.Operation, claim *domain.InitialDataSourceClaim) bool {
	if create.Phase != domain.PhaseFailed || claim == nil || create.CompletedAt == nil || claim.Operation != create.ID || claim.ClaimedAt.IsZero() || claim.ClaimedAt.After(*create.CompletedAt) {
		return false
	}
	budget := create.Step == domain.InitialRestoreTrackerDeadline || create.Step == domain.InitialRestoreTrackerRetryBudget
	validated := c.Status.InitialDataSource != nil && c.Status.InitialDataSource.TrackingFailure == create.ID
	return budget || validated
}

func legacyInitialRestorePending(ctx context.Context, tx *sql.Tx, c *domain.Cluster, create *domain.Operation) (bool, error) {
	if create.Phase != domain.PhaseCancelled || !strings.HasPrefix(create.Message, "Superseded by ") {
		return false, nil
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT data FROM records WHERE kind='restore' AND scope=? AND key='initial-restore' AND phase IN ('Pending','Running')`, c.Ref().Key()).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var restore domain.Restore
	if err = json.Unmarshal(raw, &restore); err != nil {
		return false, err
	}
	if !matchesInitialRestore(c, &restore) || restore.OperationID == "" {
		return false, nil
	}
	err = tx.QueryRowContext(ctx, `SELECT data FROM records WHERE kind='operation' AND scope=? AND key=? AND phase IN ('Pending','Running')`, c.Ref().Key(), restore.OperationID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var native domain.Operation
	if err = json.Unmarshal(raw, &native); err != nil {
		return false, err
	}
	return (native.Kind == domain.OperationRestore || native.Kind == "initial-restore") &&
		native.ClusterIncarnation == create.ClusterIncarnation && !native.CancelRequested, nil
}

func matchesInitialRestore(c *domain.Cluster, restore *domain.Restore) bool {
	if c.Spec.DataSource == nil || restore.Name != "initial-restore" {
		return false
	}
	expected := *c.Spec.DataSource
	actual := domain.DataSource{Namespace: restore.SourceNamespace, Cluster: restore.Source.SourceCluster, Backup: restore.Source.Backup, PITRTime: restore.Source.PITRTime}
	if expected.Namespace == "" {
		expected.Namespace = c.Namespace
	}
	if actual.Namespace == "" {
		actual.Namespace = c.Namespace
	}
	if expected.Cluster == "" {
		expected.Cluster = c.Name
	}
	if actual.Cluster == "" {
		actual.Cluster = c.Name
	}
	return reflect.DeepEqual(expected, actual)
}
