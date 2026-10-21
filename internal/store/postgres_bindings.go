// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

const bindingColumns = "id,incarnation,source_namespace,source_cluster,source_uid,consumer_kind,consumer_namespace,consumer_name,databases,extensions,role_name,min_major,max_major,state,credential_generation,created,updated"

// RestoreInProgressError asks a new consumer to retry after the source restore.
type RestoreInProgressError struct {
	Restore string
}

func (e *RestoreInProgressError) Error() string {
	return fmt.Sprintf("PostgreSQL source restore %s is pending or running; retry binding admission after it completes", e.Restore)
}

func (e *RestoreInProgressError) Unwrap() error { return ErrConflict }

// BoundConsumersError reports the consumers requiring restore acknowledgement.
type BoundConsumersError struct {
	Consumers []domain.PostgresBinding
}

func (e *BoundConsumersError) Error() string {
	return "in-place restore affects bound consumers; set acknowledgeBoundConsumers to proceed"
}

func (e *BoundConsumersError) Unwrap() error { return ErrConflict }

type restoreAcknowledgementKey struct{}

type restoreBindingKey struct{}

// WithRestoreBinding acknowledges the requesting consumer's own binding.
// Other consumers still require an explicit restore acknowledgement.
func WithRestoreBinding(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, restoreBindingKey{}, id)
}

// WithRestoreAcknowledgement explicitly permits restoring a bound source.
// EnqueueRestore checks the current bindings in its writer transaction.
func WithRestoreAcknowledgement(ctx context.Context) context.Context {
	return context.WithValue(ctx, restoreAcknowledgementKey{}, true)
}

func guardRestoreBindings(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) error {
	if acknowledged, _ := ctx.Value(restoreAcknowledgementKey{}).(bool); acknowledged {
		return nil
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+bindingColumns+" FROM postgres_bindings WHERE source_namespace=? AND source_cluster=? AND source_uid=CAST((SELECT created FROM clusters WHERE namespace=? AND name=?) AS TEXT) ORDER BY consumer_namespace,consumer_name,id", domain.NormalizedNamespace(ref.Namespace), ref.Name, domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err != nil {
		return err
	}
	defer rows.Close()
	var consumers []domain.PostgresBinding
	for rows.Next() {
		binding, err := scanBinding(rows)
		if err != nil {
			return err
		}
		if own, _ := ctx.Value(restoreBindingKey{}).(string); own != "" && binding.ID == own {
			continue
		}
		consumers = append(consumers, *binding)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(consumers) > 0 {
		return &BoundConsumersError{Consumers: consumers}
	}
	return nil
}

func scanBinding(row scanner) (*domain.PostgresBinding, error) {
	var b domain.PostgresBinding
	var databases, extensions []byte
	var created, updated int64
	if err := row.Scan(&b.ID, &b.Incarnation, &b.SourceNamespace, &b.SourceCluster, &b.SourceUID, &b.ConsumerKind, &b.ConsumerNamespace, &b.ConsumerName, &databases, &extensions, &b.RoleName, &b.MinMajor, &b.MaxMajor, &b.State, &b.CredentialGeneration, &created, &updated); err != nil {
		return nil, typed(err)
	}
	if err := json.Unmarshal(databases, &b.Databases); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(extensions, &b.Extensions); err != nil {
		return nil, err
	}
	b.CreatedAt = time.Unix(0, created).UTC()
	b.UpdatedAt = time.Unix(0, updated).UTC()
	return &b, nil
}

// EnsurePostgresBinding serializes dependency admission with deletion intent.
// The immutable created timestamp identifies the authoritative cluster incarnation.
func (s *Store) EnsurePostgresBinding(ctx context.Context, b *domain.PostgresBinding) (*domain.PostgresBinding, error) {
	b.SourceNamespace = domain.NormalizedNamespace(b.SourceNamespace)
	b.ConsumerNamespace = domain.NormalizedNamespace(b.ConsumerNamespace)
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var spec []byte
	var deleting bool
	var created int64
	if err = tx.QueryRowContext(ctx, "SELECT spec,deleting,created FROM clusters WHERE namespace=? AND name=?", b.SourceNamespace, b.SourceCluster).Scan(&spec, &deleting, &created); err != nil {
		return nil, typed(err)
	}
	var source domain.ClusterSpec
	if err = json.Unmarshal(spec, &source); err != nil {
		return nil, err
	}
	major, _ := strconv.Atoi(strings.Split(source.Version, ".")[0])
	if deleting || source.Provider != "postgres" || (b.MinMajor > 0 && major < b.MinMajor) || (b.MaxMajor > 0 && major > b.MaxMajor) {
		return nil, domain.Conflict("cluster", b.SourceCluster, domain.ReasonInvalidState, ErrConflict, "PostgreSQL binding source is deleting, incompatible, or outside supported major range")
	}
	b.SourceUID = strconv.FormatInt(created, 10)
	existing, err := scanBinding(tx.QueryRowContext(ctx, "SELECT "+bindingColumns+" FROM postgres_bindings WHERE consumer_kind=? AND consumer_namespace=? AND consumer_name=? AND source_namespace=? AND source_cluster=? AND source_uid=?", b.ConsumerKind, b.ConsumerNamespace, b.ConsumerName, b.SourceNamespace, b.SourceCluster, b.SourceUID))
	if err == nil {
		if strings.HasPrefix(existing.State, "releasing") {
			return nil, ErrConflict
		}
		databaseCount, extensionCount := len(existing.Databases), len(existing.Extensions)
		rangeChanged := existing.MinMajor != b.MinMajor || existing.MaxMajor != b.MaxMajor
		existing.Databases = bindingUnion(existing.Databases, b.Databases)
		existing.Extensions = bindingUnion(existing.Extensions, b.Extensions)
		if len(existing.Databases) == databaseCount && len(existing.Extensions) == extensionCount && !rangeChanged {
			return existing, nil
		}
		if existing.State == "ready" {
			existing.State = "pending"
		}
		existing.MinMajor, existing.MaxMajor = b.MinMajor, b.MaxMajor
		databases, err := json.Marshal(existing.Databases)
		if err != nil {
			return nil, err
		}
		extensions, err := json.Marshal(existing.Extensions)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE postgres_bindings SET databases=?,extensions=?,min_major=?,max_major=?,state=?,updated=? WHERE id=?", databases, extensions, b.MinMajor, b.MaxMajor, existing.State, time.Now().UTC().UnixNano(), existing.ID); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if err != ErrNotFound {
		return nil, err
	}
	var restoreName string
	err = tx.QueryRowContext(ctx, "SELECT key FROM records WHERE kind='restore' AND scope=? AND phase IN ('Pending','Running') ORDER BY key LIMIT 1", domain.ClusterRef{Namespace: b.SourceNamespace, Name: b.SourceCluster}.Key()).Scan(&restoreName)
	if err == nil {
		return nil, &RestoreInProgressError{Restore: restoreName}
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	if b.ID == "" {
		b.ID, err = secrets.Token()
		if err != nil {
			return nil, err
		}
	}
	// Always mint ownership at insertion, even if the caller reuses a row value.
	b.Incarnation, err = secrets.Token()
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(b.ID + "\x00" + b.Incarnation))
	b.RoleName = "kb_" + hex.EncodeToString(hash[:16])
	databases, err := json.Marshal(b.Databases)
	if err != nil {
		return nil, err
	}
	extensions, err := json.Marshal(b.Extensions)
	if err != nil {
		return nil, err
	}
	b.CreatedAt = time.Now().UTC()
	b.UpdatedAt = b.CreatedAt
	_, err = tx.ExecContext(ctx, "INSERT INTO postgres_bindings("+bindingColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", b.ID, b.Incarnation, b.SourceNamespace, b.SourceCluster, b.SourceUID, b.ConsumerKind, b.ConsumerNamespace, b.ConsumerName, databases, extensions, b.RoleName, b.MinMajor, b.MaxMajor, b.State, b.CredentialGeneration, b.CreatedAt.UnixNano(), b.UpdatedAt.UnixNano())
	if err != nil {
		return nil, typed(err)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Store) GetPostgresBinding(ctx context.Context, id string) (*domain.PostgresBinding, error) {
	return scanBinding(s.reader.QueryRowContext(ctx, "SELECT "+bindingColumns+" FROM postgres_bindings WHERE id=?", id))
}
func (s *Store) ListPostgresBindings(ctx context.Context, ref domain.ClusterRef) ([]domain.PostgresBinding, error) {
	rows, err := s.reader.QueryContext(ctx, "SELECT "+bindingColumns+" FROM postgres_bindings WHERE source_namespace=? AND source_cluster=? AND source_uid=CAST((SELECT created FROM clusters WHERE namespace=? AND name=?) AS TEXT) ORDER BY consumer_namespace,consumer_name,id", domain.NormalizedNamespace(ref.Namespace), ref.Name, domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.PostgresBinding{}
	for rows.Next() {
		b, err := scanBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}
func (s *Store) UpdatePostgresBindingState(ctx context.Context, id, state string, generation int64) error {
	r, err := s.writer.ExecContext(ctx, "UPDATE postgres_bindings SET state=?,credential_generation=?,updated=? WHERE id=?", state, generation, time.Now().UTC().UnixNano(), id)
	return affected(r, err)
}
func (s *Store) DeletePostgresBinding(ctx context.Context, id string) error {
	r, err := s.writer.ExecContext(ctx, "DELETE FROM postgres_bindings WHERE id=?", id)
	return affected(r, err)
}

func guardPostgresBindings(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) error {
	rows, err := tx.QueryContext(ctx, "SELECT consumer_kind,consumer_namespace,consumer_name FROM postgres_bindings WHERE source_namespace=? AND source_cluster=? AND source_uid=CAST((SELECT created FROM clusters WHERE namespace=? AND name=?) AS TEXT) ORDER BY consumer_namespace,consumer_name", domain.NormalizedNamespace(ref.Namespace), ref.Name, domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err != nil {
		return err
	}
	defer rows.Close()
	var consumers []string
	for rows.Next() {
		var kind, ns, name string
		if err = rows.Scan(&kind, &ns, &name); err != nil {
			return err
		}
		consumers = append(consumers, fmt.Sprintf("%s %s/%s", kind, ns, name))
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(consumers) > 0 {
		return domain.Conflict("cluster", ref.Name, domain.ReasonInvalidState, ErrConflict, "Cluster %s is required by PostgreSQL consumers: %s", ref.Name, strings.Join(consumers, ", "))
	}
	return nil
}

func bindingUnion(before, added []string) []string {
	for _, value := range added {
		if !slices.Contains(before, value) {
			before = append(before, value)
		}
	}
	return before
}

// guardBindingMajor shares the spec writer transaction with binding admission.
func guardBindingMajor(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef, spec domain.ClusterSpec) error {
	major, err := strconv.Atoi(strings.Split(spec.Version, ".")[0])
	if err != nil {
		return nil
	} // Provider validation owns version syntax.
	rows, err := tx.QueryContext(ctx, "SELECT consumer_kind,consumer_namespace,consumer_name FROM postgres_bindings WHERE source_namespace=? AND source_cluster=? AND source_uid=CAST((SELECT created FROM clusters WHERE namespace=? AND name=?) AS TEXT) AND ((min_major>0 AND min_major>?) OR (max_major>0 AND max_major<?))", domain.NormalizedNamespace(ref.Namespace), ref.Name, domain.NormalizedNamespace(ref.Namespace), ref.Name, major, major)
	if err != nil {
		return err
	}
	defer rows.Close()
	var consumers []string
	for rows.Next() {
		var kind, ns, name string
		if err := rows.Scan(&kind, &ns, &name); err != nil {
			return err
		}
		consumers = append(consumers, fmt.Sprintf("%s %s/%s", kind, ns, name))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(consumers) > 0 {
		return domain.Conflict("cluster", ref.Name, domain.ReasonInvalidState, ErrConflict, "PostgreSQL major %d is outside the supported range of bound consumers: %s", major, strings.Join(consumers, ", "))
	}
	return nil
}
