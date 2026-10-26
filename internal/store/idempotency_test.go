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
	third, acquired, err := s.BeginIdempotency(ctx, "request", "new", now.Add(IdempotencyLease))
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

func TestIdempotencyLeaseRenewal(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	first, acquired, err := s.BeginIdempotency(ctx, "leased", "hash", clock())
	if err != nil || !acquired {
		t.Fatal(first, acquired, err)
	}
	if err := s.RenewIdempotency(ctx, "leased", "wrong-owner", clock()); !errors.Is(err, ErrConflict) {
		t.Fatal("another owner renewed reservation", err)
	}
	// Renew well beyond the original expiry without allowing another owner.
	for range 10 {
		now = now.Add(time.Minute)
		if err := s.RenewIdempotency(ctx, "leased", first.Owner, clock()); err != nil {
			t.Fatal("active lease lost", err)
		}
		got, acquired, err := s.BeginIdempotency(ctx, "leased", "hash", clock())
		if err != nil || acquired || got.Owner != first.Owner {
			t.Fatal("active reservation reclaimed", got, acquired, err)
		}
	}
	now = now.Add(IdempotencyLease)
	if err := s.RenewIdempotency(ctx, "leased", first.Owner, clock()); !errors.Is(err, ErrConflict) {
		t.Fatal("expired owner revived reservation", err)
	}
	second, acquired, err := s.BeginIdempotency(ctx, "leased", "hash", clock())
	if err != nil || !acquired || second.Owner == first.Owner {
		t.Fatal("abandoned reservation not reclaimed", second, acquired, err)
	}
	if err := s.RenewIdempotency(ctx, "leased", first.Owner, clock()); !errors.Is(err, ErrConflict) {
		t.Fatal("stale owner renewed replacement", err)
	}
	wire := IdempotentResponse{Status: 202, Body: []byte(`{"operationId":"accepted"}`)}
	if err := s.CompleteIdempotency(ctx, "leased", second.Owner, wire, clock()); err != nil {
		t.Fatal(err)
	}
	completedAt := clock()
	now = now.Add(IdempotencyLease)
	replay, acquired, err := s.BeginIdempotency(ctx, "leased", "hash", clock())
	if err != nil || acquired || replay.Response == nil {
		t.Fatal("lease expiry shortened replay retention", replay, acquired, err)
	}
	now = completedAt.Add(IdempotencyTTL)
	if _, acquired, err := s.BeginIdempotency(ctx, "leased", "hash", clock()); err != nil || !acquired {
		t.Fatal("completed replay did not expire", acquired, err)
	}
}

func TestIdempotencyLeaseMigration(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	now := time.Now()
	replayExpiry := now.Add(IdempotencyTTL).UnixNano()
	shortExpiry := now.Add(time.Minute).UnixNano()
	for _, record := range []struct {
		identity, response string
		expires            int64
	}{
		{"legacy", "", replayExpiry},
		{"short", "", shortExpiry},
		{"completed", "sealed-response", replayExpiry},
	} {
		if _, err := s.writer.ExecContext(ctx, "INSERT INTO idempotency_requests(identity,body_hash,owner,response,expires) VALUES(?,?,?,?,?)", record.identity, "hash", "owner", record.response, record.expires); err != nil {
			t.Fatal(err)
		}
	}
	script, err := migrations.ReadFile("migrations/007_idempotency_lease.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, string(script)); err != nil {
		t.Fatal(err)
	}
	migrated := time.Now()
	for _, identity := range []string{"legacy", "short", "completed"} {
		var expiry int64
		if err := s.reader.QueryRowContext(ctx, "SELECT expires FROM idempotency_requests WHERE identity=?", identity).Scan(&expiry); err != nil {
			t.Fatal(err)
		}
		switch identity {
		case "legacy":
			if expiry > migrated.Add(IdempotencyLease).UnixNano() || expiry <= now.UnixNano() {
				t.Fatal("legacy reservation not bounded by lease", expiry)
			}
		case "short":
			if expiry != shortExpiry {
				t.Fatal("migration extended an existing shorter lease", expiry)
			}
		case "completed":
			if expiry != replayExpiry {
				t.Fatal("migration shortened a successful replay", expiry)
			}
		}
	}
}

func TestIdempotencyHashUpgradePurgesLegacyRowsOnce(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	if _, err := s.writer.ExecContext(ctx, "DELETE FROM schema_migrations WHERE name='008_idempotency_hmac.sql'"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, response := range []string{"", "legacy-sealed-response"} {
		if _, err := s.writer.ExecContext(ctx, "INSERT INTO idempotency_requests(identity,body_hash,owner,response,expires) VALUES(?,?,?,?,?)", secrets.Hash(response), secrets.Hash("password-body"), "owner", response, now.Add(IdempotencyTTL).UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.reader.QueryRowContext(ctx, "SELECT count(*) FROM idempotency_requests").Scan(&count); err != nil || count != 0 {
		t.Fatal("legacy request digests retained after upgrade", count, err)
	}
	record, acquired, err := s.BeginIdempotency(ctx, "new-request", "keyed-hash", now)
	if err != nil || !acquired {
		t.Fatal("upgrade prevented a fresh reservation", record, acquired, err)
	}
	if err := s.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	retry, acquired, err := s.BeginIdempotency(ctx, "new-request", "keyed-hash", now)
	if err != nil || acquired || retry.Owner != record.Owner {
		t.Fatal("restart purged a new reservation", retry, acquired, err)
	}
}
