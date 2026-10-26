// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
		case "/v1/blocked":
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		case "/v1/queued":
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
	c := newClient(server.URL, "", "tenant", "test", nil)
	ctx, cancel := context.WithTimeout(infra.ObservationContext(t.Context()), 3*time.Second)
	defer cancel()
	done := make(chan error, 8)
	for range 8 {
		go func() { _, err := c.get(ctx, "/v1/blocked", nil, nil); done <- err }()
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
	go func() { _, err := c.get(queueCtx, "/v1/queued", nil, nil); queueDone <- err }()
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
	if _, err := c.get(lifecycle, "/v1/lifecycle", nil, nil); err != nil {
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
