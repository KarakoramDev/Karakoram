// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// IdempotencyTTL bounds both replay retention and abandoned request reservations.
const IdempotencyTTL = 24 * time.Hour

// IdempotentResponse is the final wire response. The entire value is sealed at rest,
// including headers and bodies that may contain one-time credentials.
type IdempotentResponse struct {
	Status int         `json:"status"`
	Body   []byte      `json:"body"`
	Header http.Header `json:"header"`
}

// IdempotencyRecord describes a reservation or a completed successful request.
type IdempotencyRecord struct {
	BodyHash string
	Owner    string
	Response *IdempotentResponse
}

// BeginIdempotency atomically reserves identity or returns its existing record.
// acquired distinguishes the owner from concurrent duplicates, including across
// separate Store instances. Expired reservations can be replaced immediately.
func (s *Store) BeginIdempotency(ctx context.Context, identity, bodyHash string, now time.Time) (record IdempotencyRecord, acquired bool, err error) {
	owner, err := secrets.RandomString(32)
	if err != nil {
		return record, false, err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return record, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DELETE FROM idempotency_requests WHERE identity=? AND expires<=?", identity, now.UnixNano()); err != nil {
		return record, false, err
	}
	result, err := tx.ExecContext(ctx, "INSERT INTO idempotency_requests(identity,body_hash,owner,expires) VALUES(?,?,?,?) ON CONFLICT(identity) DO NOTHING", identity, bodyHash, owner, now.Add(IdempotencyTTL).UnixNano())
	if err != nil {
		return record, false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return record, false, err
	}
	var sealed string
	if err = tx.QueryRowContext(ctx, "SELECT body_hash,owner,response FROM idempotency_requests WHERE identity=?", identity).Scan(&record.BodyHash, &record.Owner, &sealed); err != nil {
		return record, false, err
	}
	if err = tx.Commit(); err != nil {
		return record, false, err
	}
	if sealed != "" {
		record.Response = new(IdempotentResponse)
		if err = s.unseal("idempotency/"+identity, sealed, record.Response); err != nil {
			return record, false, err
		}
	}
	return record, n == 1, nil
}

// CompleteIdempotency persists only successful responses owned by this reservation.
func (s *Store) CompleteIdempotency(ctx context.Context, identity, owner string, response IdempotentResponse, now time.Time) error {
	if response.Status < 200 || response.Status >= 300 {
		return ErrInvalid
	}
	sealed, err := s.seal("idempotency/"+identity, response)
	if err != nil {
		return err
	}
	r, err := s.writer.ExecContext(ctx, "UPDATE idempotency_requests SET response=?,expires=? WHERE identity=? AND owner=? AND response=''", sealed, now.Add(IdempotencyTTL).UnixNano(), identity, owner)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n == 0 {
		return ErrConflict
	}
	return err
}

// ReleaseIdempotency drops a failed reservation without touching completed replays
// or a newer owner that replaced an expired reservation.
func (s *Store) ReleaseIdempotency(ctx context.Context, identity, owner string) error {
	_, err := s.writer.ExecContext(ctx, "DELETE FROM idempotency_requests WHERE identity=? AND owner=? AND response=''", identity, owner)
	return err
}

// PurgeIdempotency removes expired encrypted responses and abandoned reservations.
func (s *Store) PurgeIdempotency(ctx context.Context, now time.Time) error {
	_, err := s.writer.ExecContext(ctx, "DELETE FROM idempotency_requests WHERE expires<=?", now.UnixNano())
	return err
}
