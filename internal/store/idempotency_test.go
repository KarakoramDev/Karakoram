// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func TestIdempotencyEncryptedReplayAndExpiry(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	now := time.Now()
	record, acquired, err := s.BeginIdempotency(ctx, "request", "body-hash", now)
	if err != nil || !acquired || record.Response != nil {
		t.Fatal(record, acquired, err)
	}
	secret, err := secrets.Token()
	if err != nil {
		t.Fatal(err)
	}
	wire := IdempotentResponse{Status: 201, Body: []byte(`{"secret":"` + secret + `"}`), Header: http.Header{"Cache-Control": {"no-store"}, "Karakoram-Operation-Id": {"op-123"}}}
	if err := s.CompleteIdempotency(ctx, "request", record.Owner, wire, now); err != nil {
		t.Fatal(err)
	}
	var identity, bodyHash, owner, ciphertext string
	var expires int64
	if err := s.reader.QueryRowContext(ctx, "SELECT identity,body_hash,owner,response,expires FROM idempotency_requests").Scan(&identity, &bodyHash, &owner, &ciphertext, &expires); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(identity+bodyHash+owner+ciphertext, secret) || !strings.HasPrefix(ciphertext, "kk:v1:") {
		t.Fatal("response credential is not sealed at rest")
	}
	replayed, acquired, err := s.BeginIdempotency(ctx, "request", "body-hash", now.Add(time.Hour))
	if err != nil || acquired || replayed.Response == nil || !reflect.DeepEqual(*replayed.Response, wire) {
		t.Fatal("wire response changed", acquired, err)
	}
	// The same ciphertext cannot be substituted under a different identity.
	if _, err := s.writer.ExecContext(ctx, "INSERT INTO idempotency_requests(identity,body_hash,owner,response,expires) VALUES(?,?,?,?,?)", "other", "body-hash", "owner", ciphertext, expires); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.BeginIdempotency(ctx, "other", "body-hash", now); err == nil {
		t.Fatal("ciphertext was not bound to storage identity")
	}
	if err := s.PurgeIdempotency(ctx, now.Add(IdempotencyTTL)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.reader.QueryRowContext(ctx, "SELECT count(*) FROM idempotency_requests").Scan(&count); err != nil || count != 0 {
		t.Fatal("expired ciphertext retained", count, err)
	}
	if _, acquired, err := s.BeginIdempotency(ctx, "request", "new-body", now.Add(IdempotencyTTL)); err != nil || !acquired {
		t.Fatal("expired key not reusable", acquired, err)
	}
}

func TestIdempotencyConcurrentReservationAcrossStores(t *testing.T) {
	s, path := testStore(t)
	sealer, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	other, err := Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var wg sync.WaitGroup
	results := make(chan bool, 20)
	failures := make(chan error, 20)
	now := time.Now()
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := s
			if i%2 == 0 {
				st = other
			}
			_, acquired, err := st.BeginIdempotency(context.Background(), "shared", "hash", now)
			results <- acquired
			failures <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(failures)
	owners := 0
	for acquired := range results {
		if acquired {
			owners++
		}
	}
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if owners != 1 {
		t.Fatal("multiple reservation owners", owners)
	}
}

func TestIdempotencyFailuresAndExpiredOwners(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	now := time.Now()
	first, _, err := s.BeginIdempotency(ctx, "request", "hash", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteIdempotency(ctx, "request", first.Owner, IdempotentResponse{Status: 500}, now); !errors.Is(err, ErrInvalid) {
		t.Fatal("failure response accepted", err)
	}
	if err := s.ReleaseIdempotency(ctx, "request", first.Owner); err != nil {
		t.Fatal(err)
	}
	second, acquired, err := s.BeginIdempotency(ctx, "request", "corrected", now)
	if err != nil || !acquired {
		t.Fatal("failed key not reusable", acquired, err)
	}
	third, acquired, err := s.BeginIdempotency(ctx, "request", "new", now.Add(IdempotencyTTL))
	if err != nil || !acquired {
		t.Fatal("abandoned reservation not expired", acquired, err)
	}
	if err := s.ReleaseIdempotency(ctx, "request", second.Owner); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteIdempotency(ctx, "request", second.Owner, IdempotentResponse{Status: 201}, now); !errors.Is(err, ErrConflict) {
		t.Fatal("old owner overwrote reservation", err)
	}
	wire := IdempotentResponse{Status: 202, Body: []byte(`{"operationId":"accepted"}`)}
	if err := s.CompleteIdempotency(ctx, "request", third.Owner, wire, now); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseIdempotency(ctx, "request", third.Owner); err != nil {
		t.Fatal(err)
	}
	got, acquired, err := s.BeginIdempotency(ctx, "request", "new", now)
	if err != nil || acquired || got.Response == nil || !bytes.Equal(got.Response.Body, wire.Body) {
		t.Fatal("release removed completed response", acquired, err)
	}
}
