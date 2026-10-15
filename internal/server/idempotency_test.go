// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func idempotentCall(s *Server, method, path, body, token, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	kind := "application/json"
	if method == http.MethodPatch {
		kind = "application/merge-patch+json"
	}
	r.Header.Set("Content-Type", kind)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	testHandler(s).ServeHTTP(w, r)
	return w
}

func assertReplay(t *testing.T, first, second *httptest.ResponseRecorder, status int) {
	t.Helper()
	if first.Code != status || second.Code != status || first.Body.String() != second.Body.String() {
		t.Fatalf("responses differ: %d %s / %d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if first.Header().Get("Idempotent-Replayed") != "" || second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("missing replay marker", second.Header())
	}
	if first.Header().Get("Karakoram-Operation-Id") != second.Header().Get("Karakoram-Operation-Id") {
		t.Fatal("replay changed operation tracking", first.Header(), second.Header())
	}
}

func assertIdempotencyConflict(t *testing.T, w *httptest.ResponseRecorder, reason string) {
	t.Helper()
	var body api.Error
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 409 || body.Code != "conflict" || body.Details == nil || (*body.Details)["field"] != "Idempotency-Key" || (*body.Details)["reason"] != reason {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestIdempotentTokenCreationReplaysEncryptedSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, _, token := testServerWithDatabase(t, path)
	first := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"retry-token"}`, token, "token-request")
	second := idempotentCall(s, "POST", "/api/v1/api-tokens", " { \"name\" : \"retry-token\" } ", token, "token-request")
	assertReplay(t, first, second, 201)
	var created api.CreatedAPIToken
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil || created.Secret == nil || *created.Secret == "" {
		t.Fatal("missing one-time secret", err)
	}
	if second.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("replayed secret may be cached")
	}
	tokens, err := s.Store.ListApiTokens(t.Context())
	if err != nil || len(tokens) != 2 {
		t.Fatal("retry created another token", len(tokens), err)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var identity, bodyHash, owner, sealed string
	if err := db.QueryRow("SELECT identity,body_hash,owner,response FROM idempotency_requests").Scan(&identity, &bodyHash, &owner, &sealed); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sealed, "kk:v1:") || strings.Contains(identity+bodyHash+owner+sealed, *created.Secret) || strings.Contains(sealed, "retry-token") {
		t.Fatal("token response persisted in plaintext")
	}
	conflict := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"another-token"}`, token, "token-request")
	assertIdempotencyConflict(t, conflict, domain.ReasonConflict)
	// Separate bearer identities do not share cached credentials, even when
	// they belong to the same account and use the same request key.
	otherToken, _, err := s.Auth.CreateToken(t.Context(), "admin", "other", nil)
	if err != nil {
		t.Fatal(err)
	}
	isolated := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"retry-token"}`, otherToken, "token-request")
	if isolated.Code != 201 || isolated.Header().Get("Idempotent-Replayed") != "" || isolated.Body.String() == first.Body.String() {
		t.Fatal("bearer identities shared response", isolated.Code)
	}
	if err := s.Store.PurgeIdempotency(t.Context(), time.Now().Add(store.IdempotencyTTL)); err != nil {
		t.Fatal(err)
	}
	afterExpiry := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"retry-token"}`, token, "token-request")
	if afterExpiry.Code != 201 || afterExpiry.Header().Get("Idempotent-Replayed") != "" || afterExpiry.Body.String() == first.Body.String() {
		t.Fatal("expired response replayed", afterExpiry.Code)
	}
}

func TestIdempotencyPasswordBodyHashIsKeyed(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body, different string
		status                              int
	}{
		{"account", "POST", "/api/v1/accounts", `{"name":"reader","password":"chosen-test-password"}`, `{"name":"reader","password":"different-test-password"}`, 201},
		{"database user create", "POST", "/api/v1/clusters/prod/database-users", `{"username":"app","database":"app","roles":[],"password":"chosen-test-password"}`, `{"username":"app","database":"app","roles":[],"password":"different-test-password"}`, 201},
		{"database user replace", "PUT", "/api/v1/clusters/prod/database-users/app/app", `{"roles":[],"password":"chosen-test-password"}`, `{"roles":[],"password":"different-test-password"}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, _, token := testServerWithDatabase(t, path)
			first := idempotentCall(s, tc.method, tc.path, tc.body, token, "password-request")
			second := idempotentCall(s, tc.method, tc.path, tc.body, token, "password-request")
			assertReplay(t, first, second, tc.status)
			assertIdempotencyConflict(t, idempotentCall(s, tc.method, tc.path, tc.different, token, "password-request"), domain.ReasonConflict)
			db, err := sql.Open("sqlite", "file:"+path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var bodyHash string
			if err := db.QueryRow("SELECT body_hash FROM idempotency_requests").Scan(&bodyHash); err != nil {
				t.Fatal(err)
			}
			canonical, err := canonicalIdempotencyBody([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			if bodyHash == secrets.Hash(string(canonical)) {
				t.Fatal("password-bearing request stored as unkeyed SHA-256")
			}
		})
	}
}

func TestIdempotentConcurrentTokenCreation(t *testing.T) {
	s, _, token := testServer(t)
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 20)
	start := make(chan struct{})
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"concurrent-token"}`, token, "concurrent")
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	originals := 0
	var successfulBody string
	for w := range results {
		switch w.Code {
		case 201:
			if successfulBody != "" && w.Body.String() != successfulBody {
				t.Fatal("successful duplicates returned different secrets")
			}
			successfulBody = w.Body.String()
			if w.Header().Get("Idempotent-Replayed") == "" {
				originals++
			}
		case 409:
			assertIdempotencyConflict(t, w, domain.ReasonInvalidState)
		default:
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if originals != 1 {
		t.Fatal("concurrent original responses", originals)
	}
	tokens, err := s.Store.ListApiTokens(t.Context())
	if err != nil || len(tokens) != 2 {
		t.Fatal("concurrent duplicate token creation", len(tokens), err)
	}
}

func TestIdempotentClusterCreatePutPatchAndPathScope(t *testing.T) {
	s, _, token := testServer(t)
	create := `{"name":"retrycluster","spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":3,"resources":{"maxCpu":2,"minCpu":1,"minRam":2,"maxRam":2,"minDisk":10,"maxDisk":10}}}}}`
	first := idempotentCall(s, "POST", "/api/v1/clusters", create, token, "cluster-request")
	second := idempotentCall(s, "POST", "/api/v1/clusters?namespace=default", create, token, "cluster-request")
	assertReplay(t, first, second, 201)
	if first.Header().Get("Karakoram-Operation-Id") == "" {
		t.Fatal("cluster acceptance lacks an operation tracking header")
	}
	patch := `{"spec":{"engineConfig":"changed"}}`
	first = idempotentCall(s, "PATCH", "/api/v1/clusters/prod", patch, token, "cluster-request")
	second = idempotentCall(s, "PATCH", "/api/v1/clusters/prod", patch, token, "cluster-request")
	assertReplay(t, first, second, 200)
	if c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"}); err != nil || c.Generation != 2 {
		t.Fatal("PATCH replay changed generation", c, err)
	}
	other := idempotentCall(s, "PATCH", "/api/v1/clusters/dev", patch, token, "cluster-request")
	if other.Code != 200 || other.Header().Get("Idempotent-Replayed") != "" {
		t.Fatal("path scopes shared response", other.Code, other.Body.String())
	}
	var requestBody map[string]json.RawMessage
	if err := json.Unmarshal([]byte(create), &requestBody); err != nil {
		t.Fatal(err)
	}
	put := `{"spec":` + string(requestBody["spec"]) + `}`
	first = idempotentCall(s, "PUT", "/api/v1/clusters/retrycluster", put, token, "cluster-request")
	second = idempotentCall(s, "PUT", "/api/v1/clusters/retrycluster", put, token, "cluster-request")
	assertReplay(t, first, second, 200)
	triggers := s.Trigger.(*triggerSpy)
	triggers.mu.Lock()
	defer triggers.mu.Unlock()
	if len(triggers.names) != 4 {
		t.Fatal("replays triggered reconciliation", triggers.names)
	}
}

func TestIdempotentClusterActions(t *testing.T) {
	for _, action := range []struct {
		path, body, different string
	}{
		{"/restart", "", ""},
		{"/raise-fcv", `{"version":"8.0"}`, `{"version":"7.0"}`},
	} {
		t.Run(action.path, func(t *testing.T) {
			s, _, token := testServer(t)
			path := "/api/v1/clusters/prod" + action.path
			first := idempotentCall(s, "POST", path, action.body, token, "action-request")
			second := idempotentCall(s, "POST", path+"?namespace=default", action.body, token, "action-request")
			assertReplay(t, first, second, 202)
			if action.different != "" {
				assertIdempotencyConflict(t, idempotentCall(s, "POST", path, action.different, token, "action-request"), domain.ReasonConflict)
			}
			ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil || len(ops) != 1 {
				t.Fatal("action retry queued another operation", len(ops), err)
			}
		})
	}
}

func TestIdempotencyContractCoverage(t *testing.T) {
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	covered := map[string]bool{}
	for _, path := range doc.Paths.Map() {
		for _, operation := range path.Operations() {
			declared := false
			for _, parameter := range operation.Parameters {
				if parameter.Value.In == "header" && parameter.Value.Name == "Idempotency-Key" {
					declared = true
				}
			}
			if declared != idempotencyCovered(operation.OperationID) {
				t.Errorf("%s: contract coverage %t differs from server", operation.OperationID, declared)
			}
			if declared {
				covered[operation.OperationID] = true
			}
		}
	}
	for _, id := range []string{
		"restartCluster", "rotateClusterTLS", "resumeCluster", "raiseClusterFCV",
		"replaceClusterMember", "rotateSystemUser", "replaceDatabaseUser",
		"replaceDatabaseRole", "replaceBackupStorage",
	} {
		if !covered[id] {
			t.Errorf("keyed UI operation %s is not covered", id)
		}
	}
	if len(covered) != 32 {
		t.Fatalf("covered operations = %d, want 32", len(covered))
	}
}

func TestIdempotencyLeaseHeartbeat(t *testing.T) {
	s, _, _ := testServer(t)
	now := time.Now()
	record, acquired, err := s.Store.BeginIdempotency(t.Context(), "long-handler", "hash", now)
	if err != nil || !acquired {
		t.Fatal(record, acquired, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticks := make(chan time.Time)
	done := make(chan error, 1)
	go func() {
		done <- s.renewIdempotencyLease(ctx, "long-handler", record.Owner, now.Add(store.IdempotencyLease), ticks, s.Store.RenewIdempotency)
	}()
	// Inject minute ticks across two original lease periods.
	for minute := 1; minute <= 10; minute++ {
		select {
		case ticks <- now.Add(time.Duration(minute) * time.Minute):
		case err := <-done:
			t.Fatal("heartbeat stopped before long handler finished", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// A heartbeat after the renewed lease expires must stop rather than revive it.
	select {
	case ticks <- now.Add(10*time.Minute + store.IdempotencyLease):
	case err := <-done:
		t.Fatal("heartbeat stopped early", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		if !errors.Is(err, store.ErrConflict) {
			t.Fatal("lost lease was not reported", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestIdempotencyLeaseTransientRenewalFailure(t *testing.T) {
	s, _, _ := testServer(t)
	var logs bytes.Buffer
	s.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	ticks := make(chan time.Time)
	calls := make(chan time.Time, 1)
	done := make(chan error, 1)
	transient := errors.New("temporary database failure")
	go func() {
		done <- s.renewIdempotencyLease(ctx, "request", "owner", start.Add(store.IdempotencyLease), ticks,
			func(ctx context.Context, identity, owner string, now time.Time) error {
				calls <- now
				if now.Equal(start.Add(time.Minute)) {
					return transient
				}
				return nil
			})
	}()
	// A failed first renewal must not cancel the handler. Successful later
	// renewals must extend protection past the original lease deadline.
	for _, minute := range []int{1, 2, 6} {
		now := start.Add(time.Duration(minute) * time.Minute)
		select {
		case ticks <- now:
		case err := <-done:
			t.Fatalf("heartbeat stopped while lease remained valid: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case got := <-calls:
			if !got.Equal(now) {
				t.Fatal("unexpected renewal time", got)
			}
		case err := <-done:
			t.Fatalf("heartbeat stopped while lease remained valid: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal("transient failure survived a recovered lease", err)
	}
	if !strings.Contains(logs.String(), "Failed to renew idempotency lease") {
		t.Fatal("transient renewal failure was not logged")
	}
}

func TestIdempotencyLeaseRenewalStopsAtExpiryOrOwnershipLoss(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		ticks []time.Duration
		calls int
	}{
		{"lease lost", store.ErrConflict, []time.Duration{time.Minute}, 1},
		{"transient until expiry", errors.New("temporary database failure"), []time.Duration{time.Minute, 4 * time.Minute, store.IdempotencyLease}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := testServer(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			start := time.Now()
			ticks := make(chan time.Time, len(tc.ticks))
			for _, tick := range tc.ticks {
				ticks <- start.Add(tick)
			}
			calls := 0
			err := s.renewIdempotencyLease(ctx, "request", "owner", start.Add(store.IdempotencyLease), ticks,
				func(context.Context, string, string, time.Time) error {
					calls++
					return tc.err
				})
			if !errors.Is(err, store.ErrConflict) {
				t.Fatal("expired or lost lease was not reported", err)
			}
			if calls != tc.calls {
				t.Fatalf("renewal calls = %d, want %d", calls, tc.calls)
			}
		})
	}
}

func TestIdempotencyLeaseExpiryBoundsBlockedRenewal(t *testing.T) {
	s, _, _ := testServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ticks := make(chan time.Time, 1)
	now := time.Now()
	ticks <- now
	err := s.renewIdempotencyLease(ctx, "request", "owner", now.Add(20*time.Millisecond), ticks,
		func(ctx context.Context, identity, owner string, now time.Time) error {
			<-ctx.Done()
			return ctx.Err()
		})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatal("blocked renewal outlived the lease", err)
	}
}

func TestIdempotencyLeaseExpiryWithoutHeartbeat(t *testing.T) {
	s, _, _ := testServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := s.renewIdempotencyLease(ctx, "request", "owner", time.Now().Add(20*time.Millisecond), nil,
		func(context.Context, string, string, time.Time) error {
			t.Error("renewed without a heartbeat")
			return nil
		})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatal("lease expiry did not stop the handler", err)
	}
}

func TestIdempotencyValidationAndUncachedFailures(t *testing.T) {
	s, _, token := testServer(t)
	for _, key := range []string{"", "with space", "non-ascii-é", strings.Repeat("x", 256)} {
		w := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"invalid-key"}`, token, key)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "Idempotency-Key") {
			t.Fatal("invalid key accepted", w.Code, w.Body.String())
		}
	}
	bad := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"retry","expiresAt":"2020-01-01T00:00:00Z"}`, token, "fix-input")
	if bad.Code != 400 {
		t.Fatal(bad.Code, bad.Body.String())
	}
	fixed := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"retry"}`, token, "fix-input")
	if fixed.Code != 201 || fixed.Header().Get("Idempotent-Replayed") != "" {
		t.Fatal("4xx was cached", fixed.Code, fixed.Body.String())
	}
	// Failed response capture releases reservations; operation headers and exact
	// response bytes are retained on successful asynchronous responses.
	for attempt, status := range []int{500, 202, 202} {
		r := httptest.NewRequest("POST", "/api/v1/api-tokens", strings.NewReader(`{"name":"wire"}`))
		r.Header.Set("Idempotency-Key", "wire")
		r.Header.Set("Authorization", "Bearer "+token)
		r.Pattern = "POST /api/v1/api-tokens"
		w := httptest.NewRecorder()
		r, _ = s.captureIdempotencyBody(w, r)
		called := false
		next := func(ctx context.Context, w http.ResponseWriter, r *http.Request, v any) (any, error) {
			called = true
			w.Header().Set("Karakoram-Operation-Id", "op-456")
			return response{status: status, body: map[string]string{"operationId": "op-456"}}, nil
		}
		out, err := s.middleware(next, "CreateAPIToken")(r.Context(), w, r, api.CreateAPITokenRequestObject{})
		if err != nil {
			t.Fatal(err)
		}
		if err := out.(response).write(w); err != nil {
			t.Fatal(err)
		}
		if w.Code != status || (status == 202 && w.Header().Get("Karakoram-Operation-Id") != "op-456") {
			t.Fatal("wire response not preserved", w.Code, w.Header())
		}
		if called != (attempt < 2) {
			t.Fatal("handler ran on replay or failure was retained", attempt, called)
		}
		if attempt == 2 && w.Header().Get("Idempotent-Replayed") != "true" {
			t.Fatal("missing replay header")
		}
	}
}

func TestIdempotencyReplayRechecksAuthentication(t *testing.T) {
	s, _, token := testServer(t)
	first := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"protected-secret"}`, token, "protected")
	if first.Code != 201 {
		t.Fatal(first.Code, first.Body.String())
	}
	// Revoke the bearer used for the first request, not the newly created token.
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	p, err := s.Auth.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DeleteApiToken(t.Context(), p.TokenID); err != nil {
		t.Fatal(err)
	}
	second := idempotentCall(s, "POST", "/api/v1/api-tokens", `{"name":"protected-secret"}`, token, "protected")
	if second.Code != 401 || second.Body.String() == first.Body.String() {
		t.Fatal("revoked token obtained cached secret", second.Code)
	}
}

func TestIdempotencyRecreatedAccountCannotReplaySecrets(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := t.Context()
	name := "retry-owner"
	create := func() {
		t.Helper()
		if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: name, Roles: []string{"role:admin"}}, "retry-owner-password"); err != nil {
			t.Fatal(err)
		}
	}
	login := func() *httptest.ResponseRecorder {
		t.Helper()
		w := call(s, "POST", "/api/v1/auth/login", `{"username":"retry-owner","password":"retry-owner-password"}`, "")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		return w
	}
	send := func(session *httptest.ResponseRecorder) *httptest.ResponseRecorder {
		t.Helper()
		var authenticated api.Session
		if err := json.Unmarshal(session.Body.Bytes(), &authenticated); err != nil || authenticated.CsrfToken == nil {
			t.Fatal("login lacks CSRF token", err)
		}
		r := httptest.NewRequest("POST", "/api/v1/api-tokens", strings.NewReader(`{"name":"one-time-secret"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "same-account-key")
		r.Header.Set("X-Karakoram-CSRF", *authenticated.CsrfToken)
		for _, cookie := range session.Result().Cookies() {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		testHandler(s).ServeHTTP(w, r)
		return w
	}
	create()
	firstSession := login()
	first := send(firstSession)
	assertReplay(t, first, send(firstSession), 201)
	// Fresh sessions for the same existing account still share its replay scope.
	assertReplay(t, first, send(login()), 201)
	if err := s.Auth.DeleteAccount(ctx, name); err != nil {
		t.Fatal(err)
	}
	create()
	second := send(login())
	if second.Code != 201 || second.Header().Get("Idempotent-Replayed") != "" || second.Body.String() == first.Body.String() {
		t.Fatal("recreated account replayed deleted account's secret", second.Code, second.Body.String())
	}
	tokens, err := s.Store.ListApiTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owned := 0
	for _, token := range tokens {
		if token.Owner == name {
			owned++
		}
	}
	if owned != 1 {
		t.Fatal("recreated account did not create its own token", owned)
	}
}

func TestIdempotentBackupsRestoresAndGeneratedPassword(t *testing.T) {
	s, _, token := testServer(t)
	if err := s.Store.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://storage.example", Region: "region", Bucket: "backups", VerifyTLS: true}); err != nil {
		t.Fatal(err)
	}
	backupBody := `{"name":"retrybackup","type":"logical","storage":"archive"}`
	first := idempotentCall(s, "POST", "/api/v1/clusters/prod/backups", backupBody, token, "backup-request")
	second := idempotentCall(s, "POST", "/api/v1/clusters/prod/backups", backupBody, token, "backup-request")
	assertReplay(t, first, second, 202)
	backups, err := s.Store.ListBackups(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(backups) != 1 {
		t.Fatal("backup retry created another record", len(backups), err)
	}
	ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(ops) != 1 {
		t.Fatal("backup retry queued another operation", len(ops), err)
	}
	// Supply a completed source without running an external database operation.
	source := domain.Backup{Name: "sourcebackup", Cluster: "dev", Provider: "fake", Storage: "archive", Type: "logical", Phase: domain.PhaseSucceeded, SourceVersion: "8.0", SourceTopology: domain.Topology{Type: "replicaSet"}}
	if err := s.Store.CreateBackup(t.Context(), &source); err != nil {
		t.Fatal(err)
	}
	restoreBody := `{"name":"retryrestore","source":{"backup":"sourcebackup","sourceCluster":"dev"}}`
	first = idempotentCall(s, "POST", "/api/v1/clusters/dev/restores", restoreBody, token, "restore-request")
	second = idempotentCall(s, "POST", "/api/v1/clusters/dev/restores", restoreBody, token, "restore-request")
	assertReplay(t, first, second, 202)
	restores, err := s.Store.ListRestores(t.Context(), domain.ClusterRef{Name: "dev"})
	if err != nil || len(restores) != 1 {
		t.Fatal("restore retry created another record", len(restores), err)
	}
	ops, err = s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "dev"})
	if err != nil || len(ops) != 1 {
		t.Fatal("restore retry queued another operation", len(ops), err)
	}
	userBody := `{"username":"appuser","database":"appdb","roles":[]}`
	first = idempotentCall(s, "POST", "/api/v1/clusters/prod/database-users", userBody, token, "user-request")
	second = idempotentCall(s, "POST", "/api/v1/clusters/prod/database-users", userBody, token, "user-request")
	assertReplay(t, first, second, 201)
	var created api.CreatedDatabaseUser
	if err := json.Unmarshal(second.Body.Bytes(), &created); err != nil || created.GeneratedPassword == nil || *created.GeneratedPassword == "" || second.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("generated password replay missing or cacheable", err)
	}
}

func TestIdempotencyCanonicalBody(t *testing.T) {
	a, err := canonicalIdempotencyBody([]byte(`{"b":[1,2],"a":{"value":"same"}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalIdempotencyBody([]byte(" { \"a\": {\"value\": \"same\"}, \"b\": [1, 2] } "))
	if err != nil || string(a) != string(b) {
		t.Fatal("object order or whitespace changed identity", err)
	}
	for _, body := range []string{`{}`, `{"a":null}`, `{"a":{"value":"same"},"b":[2,1]}`} {
		other, err := canonicalIdempotencyBody([]byte(body))
		if err != nil || string(other) == string(a) {
			t.Fatal("different values conflated", err)
		}
	}
	for _, body := range []string{"", " \n\t "} {
		if canonical, err := canonicalIdempotencyBody([]byte(body)); err != nil || len(canonical) != 0 {
			t.Fatal("empty action body rejected", err)
		}
	}
	for _, body := range []string{`{} {}`, `{} garbage`} {
		if _, err := canonicalIdempotencyBody([]byte(body)); err == nil {
			t.Fatal("malformed body accepted")
		}
	}
}
