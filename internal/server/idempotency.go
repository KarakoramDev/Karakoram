// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type idempotencyBodyKey struct{}

// captureIdempotencyBody retains the original JSON before strict decoding. It
// runs after the normal body-size limit and does not change header-free requests.
func (s *Server) captureIdempotencyBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if _, present := r.Header["Idempotency-Key"]; !present {
		return r, true
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		status := 400
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			status = 413
		}
		_ = fail(status, "invalid_request", "Request body could not be read or exceeds the size limit").write(w)
		return r, false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return r.WithContext(context.WithValue(r.Context(), idempotencyBodyKey{}, body)), true
}

func idempotencyCovered(id string) bool {
	switch id {
	case "createAPIToken", "createCluster", "replaceCluster", "patchCluster", "createBackup", "createRestore", "createBackupStorage", "replaceBackupStorage", "createAccount", "createNamespace", "createDatabaseUser", "replaceDatabaseUser", "createDatabaseRole", "replaceDatabaseRole",
		"createRabbitMQVhost", "replaceRabbitMQVhost", "createRabbitMQUser", "replaceRabbitMQUser",
		"createRabbitMQPermission", "replaceRabbitMQPermission", "createRabbitMQTopicPermission", "replaceRabbitMQTopicPermission",
		"createRabbitMQPolicy", "replaceRabbitMQPolicy", "createRabbitMQOperatorPolicy", "replaceRabbitMQOperatorPolicy",
		"restartCluster", "rotateClusterTLS", "resumeCluster", "raiseClusterFCV", "replaceClusterMember", "rotateSystemUser":
		return true
	}
	return false
}

func validIdempotencyKey(values []string) bool {
	if len(values) != 1 || len(values[0]) < 1 || len(values[0]) > 255 {
		return false
	}
	for _, b := range []byte(values[0]) {
		if b < 0x21 || b > 0x7e {
			return false
		}
	}
	return true
}

func canonicalIdempotencyBody(body []byte) ([]byte, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("request body must contain exactly one JSON value")
	}
	return json.Marshal(value)
}

// idempotent runs inside authentication and authorization, so cached secrets are
// never accessible to a revoked token or a principal whose permissions changed.
func (s *Server) idempotent(next api.StrictHandlerFunc, id string, ctx context.Context, w http.ResponseWriter, r *http.Request, v any) (any, error) {
	values, present := r.Header["Idempotency-Key"]
	if !present || !idempotencyCovered(id) {
		return next(ctx, w, r, v)
	}
	if !validIdempotencyKey(values) {
		return invalidField("Idempotency-Key", domain.ReasonInvalid, "Idempotency-Key must contain 1–255 visible ASCII characters, without spaces"), nil
	}
	p := principal(ctx)
	if p == nil {
		return fail(401, "unauthorized", "Authentication required"), nil
	}
	body, _ := ctx.Value(idempotencyBodyKey{}).([]byte)
	canonical, err := canonicalIdempotencyBody(body)
	if err != nil {
		return invalidField("body", domain.ReasonInvalid, "body must contain exactly one valid JSON value"), nil
	}
	namespace := ""
	switch id {
	case "createCluster", "replaceCluster", "patchCluster", "createBackup", "createRestore", "createBackupStorage", "replaceBackupStorage", "createDatabaseUser", "replaceDatabaseUser", "createDatabaseRole", "replaceDatabaseRole",
		"createRabbitMQVhost", "replaceRabbitMQVhost", "createRabbitMQUser", "replaceRabbitMQUser",
		"createRabbitMQPermission", "replaceRabbitMQPermission", "createRabbitMQTopicPermission", "replaceRabbitMQTopicPermission",
		"createRabbitMQPolicy", "replaceRabbitMQPolicy", "createRabbitMQOperatorPolicy", "replaceRabbitMQOperatorPolicy",
		"restartCluster", "rotateClusterTLS", "resumeCluster", "raiseClusterFCV", "replaceClusterMember", "rotateSystemUser":
		namespace = domain.NormalizedNamespace(r.URL.Query().Get("namespace"))
	}
	// A JSON tuple avoids delimiter collisions. The actual path adds every path
	// parameter to the method and route-template scope without storing credentials.
	scope, err := json.Marshal([]string{p.Account, p.AccountCreatedAt.UTC().Format(time.RFC3339Nano), p.OIDCIssuer, p.TokenID, r.Method, r.Pattern, r.URL.Path, namespace, values[0]})
	if err != nil {
		return s.failure(err), nil
	}
	identity := s.Store.IdempotencyIdentityHash(scope)
	bodyHash := s.Store.IdempotencyBodyHash(canonical)
	reservedAt := time.Now()
	record, acquired, err := s.Store.BeginIdempotency(ctx, identity, bodyHash, reservedAt)
	if err != nil {
		return s.failure(err), nil
	}
	if !acquired {
		if record.BodyHash != bodyHash {
			return idempotencyConflict(domain.ReasonConflict, "Idempotency-Key was already used with a different request body"), nil
		}
		if record.Response == nil {
			return idempotencyConflict(domain.ReasonInvalidState, "Idempotency-Key request is still in progress; retry the same request after it finishes"), nil
		}
		return replayResponse(*record.Response, true), nil
	}
	// Persistence must survive a disconnected caller. Failed requests release the
	// reservation; crashes leave it protected until its bounded expiry.
	completed := false
	defer func() {
		if !completed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := s.Store.ReleaseIdempotency(cleanup, identity, record.Owner); err != nil {
				s.Logger.Error("Failed to release idempotency reservation")
			}
		}
	}()
	// Keep long-running handlers protected, independently of caller cancellation.
	// Lease loss or expiry cancels the handler; transient errors do not. Do not
	// release an ambiguous mutation once lease ownership is no longer certain.
	handlerCtx, cancelHandler := context.WithCancelCause(ctx)
	leaseCtx, cancelLease := context.WithCancel(context.WithoutCancel(ctx))
	leaseDone := make(chan struct{})
	var leaseErr error
	go func() {
		defer close(leaseDone)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		leaseErr = s.renewIdempotencyLease(leaseCtx, identity, record.Owner, reservedAt.Add(store.IdempotencyLease), ticker.C, s.Store.RenewIdempotency)
		if leaseErr != nil {
			cancelHandler(leaseErr)
		}
	}()
	stopLease := func() {
		cancelLease()
		<-leaseDone
		cancelHandler(nil)
	}
	defer stopLease()
	capture := &idempotencyWriter{header: w.Header().Clone()}
	result, err := next(handlerCtx, capture, r.WithContext(handlerCtx), v)
	stopLease()
	if leaseErr != nil {
		completed = true
		return s.failure(leaseErr), nil
	}
	if err != nil {
		return result, err
	}
	resp, yes := result.(response)
	if !yes {
		return nil, errors.New("unsupported idempotent response")
	}
	if err := resp.write(capture); err != nil {
		return nil, err
	}
	wire := store.IdempotentResponse{Status: capture.status, Body: capture.body.Bytes(), Header: make(http.Header)}
	for _, header := range []string{"Content-Type", "Cache-Control", "Location", "ETag", "Karakoram-Operation-Id"} {
		if values := capture.header.Values(header); len(values) > 0 {
			wire.Header[http.CanonicalHeaderKey(header)] = append([]string(nil), values...)
		}
	}
	if wire.Status >= 200 && wire.Status < 300 {
		persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := s.Store.CompleteIdempotency(persist, identity, record.Owner, wire, time.Now()); err != nil {
			// The mutation may already have committed. Keep its reservation rather
			// than allow a retry to execute it again after a persistence failure.
			completed = true
			return s.failure(err), nil
		}
		completed = true
	}
	return replayResponse(wire, false), nil
}

func (s *Server) renewIdempotencyLease(ctx context.Context, identity, owner string, expires time.Time, ticks <-chan time.Time, renewLease func(context.Context, string, string, time.Time) error) error {
	expiry := time.NewTimer(time.Until(expires))
	defer expiry.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-expiry.C:
			if ctx.Err() != nil {
				return nil
			}
			return store.ErrConflict
		case now := <-ticks:
			if ctx.Err() != nil {
				return nil
			}
			if !now.Before(expires) || !time.Now().Before(expires) {
				return store.ErrConflict
			}
			// A blocked database call must not outlive the lease either.
			deadline := time.Now().Add(5 * time.Second)
			if expires.Before(deadline) {
				deadline = expires
			}
			renew, cancel := context.WithDeadline(ctx, deadline)
			err := renewLease(renew, identity, owner, now)
			cancel()
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, store.ErrConflict) {
				return err
			}
			if err != nil {
				if !time.Now().Before(expires) {
					return store.ErrConflict
				}
				// Keep the last confirmed expiry; a failed write cannot extend it.
				s.Logger.Warn("Failed to renew idempotency lease; retrying while lease remains valid")
				continue
			}
			expires = now.Add(store.IdempotencyLease)
			if !time.Now().Before(expires) {
				return store.ErrConflict
			}
			expiry.Reset(time.Until(expires))
		}
	}
}

func idempotencyConflict(reason, message string) response {
	return failDetails(409, "conflict", message, map[string]any{"field": "Idempotency-Key", "reason": reason})
}

func replayResponse(wire store.IdempotentResponse, replayed bool) response {
	return response{serve: func(w http.ResponseWriter) error {
		for name, values := range wire.Header {
			w.Header()[name] = append([]string(nil), values...)
		}
		if replayed {
			w.Header().Set("Idempotent-Replayed", "true")
		}
		w.WriteHeader(wire.Status)
		_, err := w.Write(wire.Body)
		return err
	}}
}

type idempotencyWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *idempotencyWriter) Header() http.Header { return w.header }
func (w *idempotencyWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *idempotencyWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(body)
}

// runIdempotencyPurge also purges on startup; periodic purging bounds retention
// even when no further idempotent requests arrive.
func (s *Server) runIdempotencyPurge(ctx context.Context) error {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		if err := s.Store.PurgeIdempotency(ctx, time.Now()); err != nil && ctx.Err() == nil {
			s.Logger.Error("Failed to purge expired idempotency records")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
