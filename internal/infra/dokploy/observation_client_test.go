// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestObservationAdmissionCancellationAndLifecyclePriority(t *testing.T) {
	entered := make(chan struct{}, 8)
	queued := make(chan struct{}, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/blocked":
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		case "/api/queued":
			queued <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := newClient(server.URL, "test")
	ctx, cancel := context.WithTimeout(infra.ObservationContext(t.Context()), 3*time.Second)
	defer cancel()
	done := make(chan error, 8)
	for range 8 {
		go func() { done <- c.get(ctx, "blocked", nil, nil) }()
	}
	for range 8 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("observation admission did not overlap")
		}
	}
	queueCtx, stopQueue := context.WithCancel(ctx)
	queueDone := make(chan error, 1)
	go func() { queueDone <- c.get(queueCtx, "queued", nil, nil) }()
	select {
	case <-queued:
		t.Error("ninth observation reached HTTP before admission")
	case <-time.After(40 * time.Millisecond):
	}
	stopQueue()
	if err := <-queueDone; !errors.Is(err, context.Canceled) {
		t.Error("queued admission lost cancellation", err)
	}
	lifecycle, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
	if err := c.get(lifecycle, "lifecycle", nil, nil); err != nil {
		t.Error("lifecycle read starved behind observations", err)
	}
	stop()
	close(release)
	for range 8 {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func TestObservationRetryBackoffReleasesAdmission(t *testing.T) {
	first := make(chan struct{}, 8)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/retry" && calls.Add(1) <= 8 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{}`))
			first <- struct{}{}
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := newClient(server.URL, "test")
	ctx, cancel := context.WithTimeout(infra.ObservationContext(t.Context()), 3*time.Second)
	defer cancel()
	done := make(chan error, 8)
	for range 8 {
		go func() { done <- c.get(ctx, "retry", nil, nil) }()
	}
	for range 8 {
		select {
		case <-first:
		case <-ctx.Done():
			t.Fatal("retry fixture did not receive the first wave")
		}
	}
	prompt, stop := context.WithTimeout(ctx, 60*time.Millisecond)
	if err := c.get(prompt, "other", nil, nil); err != nil {
		t.Error("retry sleepers retained every observation slot", err)
	}
	stop()
	for range 8 {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}
