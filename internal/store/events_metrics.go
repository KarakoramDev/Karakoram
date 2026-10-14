// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// EventFilter selects a durable event page. Pages are ascending from SinceID
// by default; Descending pages run newest-first below BeforeID (0 = newest).
type EventFilter struct {
	Namespace  string
	Cluster    string
	Type       string
	Actor      string
	Resource   string
	Limit      int
	SinceID    int64
	Descending bool
	BeforeID   int64
}

// AppendEvent appends an event and assigns its monotonic durable ID.
func (s *Store) AppendEvent(ctx context.Context, event *domain.Event) error {
	return appendEvent(ctx, s.writer, event)
}

func appendEvent(ctx context.Context, db executor, event *domain.Event) error {
	if event.Cluster != "" {
		event.Namespace = domain.NormalizedNamespace(event.Namespace)
	}
	if event.Actor == "" {
		event.Actor = Actor(ctx)
	}
	if event.Cluster != "" {
		if event.Resource == "" {
			event.Resource = "clusters"
		}
		if event.Target == "" {
			event.Target = event.Cluster
		}
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	r, err := db.ExecContext(ctx, "INSERT INTO events(namespace,cluster,type,actor,resource,data,created) VALUES(?,?,?,?,?,?,?)", event.Namespace, event.Cluster, event.Type, event.Actor, event.Resource, data, event.CreatedAt.UnixNano())
	if err != nil {
		return err
	}
	event.ID, err = r.LastInsertId()
	return err
}

// ListEvents returns one ordered event page (see EventFilter).
func (s *Store) ListEvents(ctx context.Context, filter EventFilter) ([]domain.Event, error) {
	q := "SELECT id,namespace,data FROM events WHERE id>?"
	args := []any{filter.SinceID}
	if filter.Descending && filter.BeforeID > 0 {
		q += " AND id<?"
		args = append(args, filter.BeforeID)
	}
	if filter.Namespace != "" {
		q += " AND namespace=?"
		args = append(args, filter.Namespace)
	}
	if filter.Cluster != "" {
		q += " AND cluster=?"
		args = append(args, filter.Cluster)
	}
	if filter.Type != "" {
		q += " AND type=?"
		args = append(args, filter.Type)
	}
	if filter.Actor != "" {
		q += " AND actor=?"
		args = append(args, filter.Actor)
	}
	if filter.Resource != "" {
		q += " AND resource=?"
		args = append(args, filter.Resource)
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	if filter.Descending {
		q += " ORDER BY id DESC LIMIT ?"
	} else {
		q += " ORDER BY id LIMIT ?"
	}
	args = append(args, limit)
	rows, err := s.reader.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Event, 0)
	for rows.Next() {
		var id int64
		var data []byte
		var namespace string
		var v domain.Event
		if err = rows.Scan(&id, &namespace, &data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		v.ID = id
		v.Namespace = namespace
		out = append(out, v)
	}
	return out, rows.Err()
}

// MetricQuery selects member metric samples in an inclusive time range.
type MetricQuery struct {
	Namespace string
	Cluster   string
	Member    string
	Metric    string
	Start     time.Time
	End       time.Time
}

// InsertMetricSamples inserts a transactional batch of metric samples.
func (s *Store) InsertMetricSamples(ctx context.Context, samples []domain.MetricSample) error {
	if len(samples) == 0 {
		return nil
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO metrics(namespace,cluster,member,metric,timestamp,data) VALUES(?,?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, v := range samples {
		v.Namespace = domain.NormalizedNamespace(v.Namespace)
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err = stmt.ExecContext(ctx, v.Namespace, v.Cluster, v.Member, v.Metric, v.Timestamp.UnixNano(), data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// QueryMetricSamples reads filtered metric samples.
func (s *Store) QueryMetricSamples(ctx context.Context, q MetricQuery) ([]domain.MetricSample, error) {
	sql := "SELECT data FROM metrics WHERE timestamp>=?"
	args := []any{q.Start.UnixNano()}
	if q.Namespace != "" {
		sql += " AND namespace=?"
		args = append(args, q.Namespace)
	}
	if q.Cluster != "" {
		sql += " AND cluster=?"
		args = append(args, q.Cluster)
	}
	if !q.End.IsZero() {
		sql += " AND timestamp<=?"
		args = append(args, q.End.UnixNano())
	}
	if q.Member != "" {
		sql += " AND member=?"
		args = append(args, q.Member)
	}
	if q.Metric != "" {
		sql += " AND metric=?"
		args = append(args, q.Metric)
	}
	sql += " ORDER BY timestamp,id"
	rows, err := s.reader.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.MetricSample, 0)
	for rows.Next() {
		var data []byte
		var v domain.MetricSample
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

// PruneMetricSamples removes expired metric samples.
func (s *Store) PruneMetricSamples(ctx context.Context, before time.Time) (int64, error) {
	r, err := s.writer.ExecContext(ctx, "DELETE FROM metrics WHERE timestamp<?", before.UnixNano())
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}
