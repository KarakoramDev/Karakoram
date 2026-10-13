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
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	_ "modernc.org/sqlite"
)

// ErrNotFound indicates that the requested resource does not exist.
var ErrNotFound = errors.New("resource not found")

// ErrConflict indicates a duplicate identity or conflicting state transition.
var ErrConflict = errors.New("resource conflict")

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
func Open(path string, sealer *secrets.Sealer) (*Store, error) {
	if sealer == nil {
		return nil, errors.New("secret sealer is required")
	}
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

// Close releases pooled SQLite connections.
func (s *Store) Close() error { return errors.Join(s.reader.Close(), s.writer.Close()) }
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
	return typed(err)
}
func (s *Store) get(ctx context.Context, kind, scope, key string, out any) (string, string, error) {
	var data []byte
	var secret, hash string
	err := s.reader.QueryRowContext(ctx, "SELECT data,secret,hash FROM records WHERE kind=? AND scope=? AND key=?", kind, scope, key).Scan(&data, &secret, &hash)
	if err != nil {
		return "", "", typed(err)
	}
	return secret, hash, json.Unmarshal(data, out)
}
func list[T any](ctx context.Context, s *Store, kind, scope string) ([]T, error) {
	q := "SELECT data FROM records WHERE kind=?"
	args := []any{kind}
	if scope != "*" {
		q += " AND scope=?"
		args = append(args, scope)
	}
	q += " ORDER BY created,key"
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
		if err = json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) remove(ctx context.Context, kind, scope, key string) error {
	result, err := s.writer.ExecContext(ctx, "DELETE FROM records WHERE kind=? AND scope=? AND key=?", kind, scope, key)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return ErrNotFound
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
		return ErrNotFound
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
	GetCluster(context.Context, string) (*domain.Cluster, error)
	ListClusters(context.Context) ([]domain.Cluster, error)
	CreateCluster(context.Context, *domain.Cluster) error
	UpdateClusterSpec(context.Context, string, domain.ClusterSpec) error
	UpdateClusterState(context.Context, *domain.Cluster) error
	MarkClusterDeleting(context.Context, string, domain.DeleteOptions) error
	DeleteCluster(context.Context, string) error
}

// OperationRepository defines durable operation enqueueing and resumption.
type OperationRepository interface {
	EnqueueOperation(context.Context, *domain.Operation) error
	GetOperation(context.Context, string) (*domain.Operation, error)
	ListOperations(context.Context, string, ...domain.Phase) ([]domain.Operation, error)
	UpdateOperation(context.Context, *domain.Operation) error
	UpdateOperationAndClusterState(context.Context, *domain.Operation, *domain.Cluster) error
	ClaimOperation(context.Context, string) (*domain.Operation, error)
	ClaimRunningOperation(context.Context, string) (*domain.Operation, error)
}

// EventRepository defines append-only durable event persistence.
type EventRepository interface {
	AppendEvent(context.Context, *domain.Event) error
	ListEvents(context.Context, EventFilter) ([]domain.Event, error)
}
