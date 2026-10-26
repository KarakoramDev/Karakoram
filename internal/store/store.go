// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	_ "modernc.org/sqlite"
)

// ErrNotFound indicates that the requested resource does not exist.
var ErrNotFound = errors.New("resource not found")

// ErrConflict indicates a duplicate identity or conflicting state transition.
var ErrConflict = errors.New("resource conflict")

// ErrInvalid indicates an invalid persistence identity.
var ErrInvalid = errors.New("invalid resource identity")

func storageAAD(namespace, name string) string {
	if namespace == "" || namespace == domain.DefaultNamespace {
		return "storage/" + name
	}
	return "storage-ns/" + namespace + "/" + name
}

//go:embed migrations/*.sql
var migrations embed.FS

// Store serializes writes through one SQLite connection. Readers use a separate
// pool in WAL mode; both pools apply connection-local pragmas in their DSN.
type Store struct {
	writer *sql.DB
	reader *sql.DB
	sealer *secrets.Sealer
}

// Open opens and migrates a SQLite database with encrypted secret persistence.
func Open(path string, sealer *secrets.Sealer) (_ *Store, openErr error) {
	defer func() {
		if openErr != nil {
			openErr = fmt.Errorf("control-plane state unavailable: %w; preserve the database, WAL and master key; restore free space and write access, or restore verified control-plane state if corrupt; never recreate the database to bypass this error", openErr)
		}
	}()
	if sealer == nil {
		return nil, errors.New("secret sealer is required")
	}
	if path != ":memory:" {
		// Check an existing file before SQLite opens it: its read-only fallback
		// can otherwise create equally read-only WAL sidecars which remain
		// unwritable even after the database permissions are repaired.
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if os.IsNotExist(err) {
			return initializeStore(path, sealer)
		}
		if file != nil {
			info, statErr := file.Stat()
			closeErr := file.Close()
			if err := errors.Join(statErr, closeErr); err != nil {
				return nil, err
			}
			if info.Size() == 0 {
				return nil, errors.New("existing control-plane database is empty; refusing to initialize over potentially truncated authoritative state")
			}
		}
	}
	return openSQLite(path, sealer)
}

func openSQLite(path string, sealer *secrets.Sealer) (*Store, error) {
	dsn := "file:" + url.PathEscape(path) + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	if path == ":memory:" {
		token, err := secrets.RandomString(16)
		if err != nil {
			return nil, err
		}
		dsn = "file:" + token + "?mode=memory&cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	}
	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	s := &Store{writer: w, sealer: sealer}
	if _, err = w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		w.Close()
		return nil, err
	}
	if err = s.migrate(context.Background()); err != nil {
		w.Close()
		return nil, err
	}
	// SQLite can silently fall back to a read-only connection for an existing
	// database. Reject that at startup so retrying Open after permissions or a
	// read-only mount are repaired creates a genuinely writable connection.
	if _, err = w.Exec("UPDATE schema_migrations SET name=name WHERE 0"); err != nil {
		w.Close()
		return nil, err
	}
	r, err := sql.Open("sqlite", dsn)
	if err != nil {
		w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(4)
	r.SetMaxIdleConns(4)
	s.reader = r
	return s, nil
}

// initializeStore publishes only a completely migrated, checkpointed database.
// A crash before publication leaves an untrusted staging file, never an empty
// authoritative path. Retrying starts a new isolated stage; old stages contain
// no accepted control-plane work and are never adopted as authoritative state.
func initializeStore(path string, sealer *secrets.Sealer) (*Store, error) {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".init-*")
	if err != nil {
		return nil, err
	}
	stage := file.Name()
	defer func() {
		_ = os.Remove(stage)
		_ = os.Remove(stage + "-wal")
		_ = os.Remove(stage + "-shm")
	}()
	if err = file.Close(); err != nil {
		return nil, err
	}
	staged, err := openSQLite(stage, sealer)
	if err != nil {
		return nil, err
	}
	// A published main file must not depend on stage-named WAL sidecars.
	_, checkpointErr := staged.writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if checkpointErr == nil {
		_, checkpointErr = staged.writer.Exec("PRAGMA journal_mode=DELETE")
	}
	if err = errors.Join(checkpointErr, staged.Close()); err != nil {
		return nil, err
	}
	file, err = os.OpenFile(stage, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = errors.Join(file.Sync(), file.Close()); err != nil {
		return nil, err
	}
	// Link is an atomic no-replace publication. Concurrent initializers may
	// open the winner, but must never overwrite it or an independently
	// created authoritative file.
	if err = os.Link(stage, path); err != nil {
		if os.IsExist(err) {
			return Open(path, sealer)
		}
		return nil, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err = errors.Join(directory.Sync(), directory.Close()); err != nil {
		return nil, err
	}
	return openSQLite(path, sealer)
}

// Close releases pooled SQLite connections.
func (s *Store) Close() error { return errors.Join(s.reader.Close(), s.writer.Close()) }

// execWriter identifies pool admission separately from SQLite execution for the
// background metadata/retention paths that can otherwise stall inspection.
func (s *Store) execWriter(ctx context.Context, operation, query string, args ...any) (sql.Result, error) {
	started := time.Now()
	conn, err := s.writer.Conn(ctx)
	logWriterWait(slog.Default(), operation, time.Since(started))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.ExecContext(ctx, query, args...)
}

func logWriterWait(logger *slog.Logger, operation string, elapsed time.Duration) {
	if elapsed > 2*time.Second {
		logger.Warn("SQLite writer wait", "operation", operation, "duration", elapsed)
	}
}
func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.writer.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY)"); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		var n int
		err = s.writer.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE name=?", entry.Name()).Scan(&n)
		if err != nil {
			return err
		}
		if n != 0 {
			continue
		}
		if entry.Name() == "012_sanitize_diagnostics.sql" {
			if err := s.sanitizeLegacyDiagnostics(ctx); err != nil {
				return fmt.Errorf("migration %s: %w", entry.Name(), err)
			}
		}
		script, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		tx, err := s.writer.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(script)); err == nil {
			_, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(name) VALUES(?)", entry.Name())
		}
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %s: %w", entry.Name(), err)
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
func typed(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil && (strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "FOREIGN KEY constraint failed")) {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	return err
}

func resourceError(kind, name string, err error) error {
	resource := kind
	switch kind {
	case "backup", "restore":
	case "storage":
		resource = "backupStorage"
	default:
		return err
	}
	if errors.Is(err, ErrNotFound) {
		return domain.NotFound(resource, name, err)
	}
	if errors.Is(err, ErrConflict) {
		return domain.Conflict(resource, name, domain.ReasonAlreadyExists, err, "%s %q already exists", resource, name)
	}
	return err
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func put(ctx context.Context, db executor, kind, scope, key string, value any, secret, hash, phase string, create bool) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	now := time.Now().UTC().UnixNano()
	q := "INSERT INTO records(kind,scope,key,data,secret,hash,phase,created,updated) VALUES(?,?,?,?,?,?,?,?,?)"
	if !create {
		q += " ON CONFLICT(kind,scope,key) DO UPDATE SET data=excluded.data,secret=excluded.secret,hash=excluded.hash,phase=excluded.phase,updated=excluded.updated"
	}
	_, err = db.ExecContext(ctx, q, kind, scope, key, data, secret, hash, phase, now, now)
	if err == nil && create && kind == "operation" {
		if commit, ok := ctx.Value(idempotentOperationKey{}).(idempotentOperationCommit); ok {
			op, ok := value.(*domain.Operation)
			if !ok {
				return ErrInvalid
			}
			if err = commit(ctx, db, op); err != nil {
				return err
			}
		}
	}
	return resourceError(kind, key, typed(err))
}
func (s *Store) get(ctx context.Context, kind, scope, key string, out any) (string, string, error) {
	var data []byte
	var secret, hash string
	err := s.reader.QueryRowContext(ctx, "SELECT data,secret,hash FROM records WHERE kind=? AND scope=? AND key=?", kind, scope, key).Scan(&data, &secret, &hash)
	if err != nil {
		return "", "", resourceError(kind, key, typed(err))
	}
	return secret, hash, json.Unmarshal(redact.DiagnosticsJSON(data), out)
}
func list[T any](ctx context.Context, s *Store, kind, scope string) ([]T, error) {
	q := "SELECT data FROM records WHERE kind=?"
	args := []any{kind}
	if scope != "*" {
		q += " AND scope=?"
		args = append(args, scope)
	}
	return listQuery[T](ctx, s, q+" ORDER BY created,key", args...)
}

func listQuery[T any](ctx context.Context, s *Store, q string, args ...any) ([]T, error) {
	rows, err := s.reader.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]T, 0)
	for rows.Next() {
		var data []byte
		var v T
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(redact.DiagnosticsJSON(data), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func namespaceScope(namespace string) string {
	if namespace == "" || namespace == domain.DefaultNamespace {
		return ""
	}
	return namespace
}

func clusterRecordFilter(ref domain.ClusterRef) (string, []any) {
	if ref.Name != "" {
		return " AND scope=?", []any{ref.Key()}
	}
	if ref.Namespace == "" {
		return "", nil
	}
	if ref.Namespace == domain.DefaultNamespace {
		return " AND instr(scope, ':')=0", nil
	}
	return " AND substr(scope,1,?)=?", []any{len(ref.Namespace) + 1, ref.Namespace + ":"}
}

func listClusterRecords[T any](ctx context.Context, s *Store, kind string, ref domain.ClusterRef) ([]T, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	filter, args := clusterRecordFilter(ref)
	return listQuery[T](ctx, s, "SELECT data FROM records WHERE kind=?"+filter+" ORDER BY created,key", append([]any{kind}, args...)...)
}
func (s *Store) remove(ctx context.Context, kind, scope, key string) error {
	result, err := s.writer.ExecContext(ctx, "DELETE FROM records WHERE kind=? AND scope=? AND key=?", kind, scope, key)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return resourceError(kind, key, ErrNotFound)
	}
	return err
}
func (s *Store) update(ctx context.Context, kind, scope, key string, value any, secret, hash, phase string) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	r, err := s.writer.ExecContext(ctx, "UPDATE records SET data=?,secret=?,hash=?,phase=?,updated=? WHERE kind=? AND scope=? AND key=?", data, secret, hash, phase, time.Now().UTC().UnixNano(), kind, scope, key)
	if err != nil {
		return typed(err)
	}
	n, err := r.RowsAffected()
	if err == nil && n == 0 {
		return resourceError(kind, key, ErrNotFound)
	}
	return err
}
func (s *Store) seal(identity string, value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return s.sealer.SealFor(identity, b)
}
func (s *Store) unseal(identity, value string, out any) error {
	b, err := s.sealer.OpenFor(identity, value)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

// ClusterRepository defines desired and observed cluster persistence.
type ClusterRepository interface {
	GetCluster(context.Context, domain.ClusterRef) (*domain.Cluster, error)
	ListClusters(context.Context, string) ([]domain.Cluster, error)
	CreateCluster(context.Context, *domain.Cluster) error
	UpdateClusterSpec(context.Context, domain.ClusterRef, domain.ClusterSpec) error
	UpdateClusterState(context.Context, *domain.Cluster) error
	MarkClusterDeleting(context.Context, domain.ClusterRef, domain.DeleteOptions) error
	DeleteCluster(context.Context, domain.ClusterRef) error
}

// OperationRepository defines durable operation enqueueing and resumption.
type OperationRepository interface {
	EnqueueOperation(context.Context, *domain.Operation) error
	GetOperation(context.Context, string) (*domain.Operation, error)
	ListOperations(context.Context, domain.ClusterRef, ...domain.Phase) ([]domain.Operation, error)
	UpdateOperation(context.Context, *domain.Operation) error
	UpdateOperationAndClusterState(context.Context, *domain.Operation, *domain.Cluster) error
	ClaimOperation(context.Context, domain.ClusterRef) (*domain.Operation, error)
	ClaimRunningOperation(context.Context, domain.ClusterRef) (*domain.Operation, error)
}

// EventRepository defines append-only durable event persistence.
type EventRepository interface {
	AppendEvent(context.Context, *domain.Event) error
	ListEvents(context.Context, EventFilter) ([]domain.Event, error)
}
