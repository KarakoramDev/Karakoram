// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestrictedTokenSSEReloadsOverlayPerEvent(t *testing.T) {
	s, _, _ := testServer(t)
	token, record, err := s.Auth.CreateToken(t.Context(), "admin", "stream", nil, &domain.TokenRestrictions{Clusters: []string{"default/prod"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := streamContext(t, s, token)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	initial := domain.Event{Cluster: "prod", Type: "Normal", Message: "initial-visible"}
	if err = s.Store.AppendEvent(ctx, &initial); err != nil {
		t.Fatal(err)
	}
	hidden := domain.Event{Cluster: "dev", Type: "Normal", Message: "initial-hidden"}
	if err = s.Store.AppendEvent(ctx, &hidden); err != nil {
		t.Fatal(err)
	}
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	changed := false
	w.flushed = func() {
		body := w.Body.String()
		if !changed && strings.Contains(body, `"message":"initial-visible"`) {
			changed = true
			// The public API keeps restrictions immutable; simulate a persisted policy
			// replacement to ensure the stream does not retain its authentication snapshot.
			stored, err := s.Store.GetApiToken(ctx, record.ID)
			if err != nil {
				t.Fatal(err)
			}
			stored.Restrictions.Clusters = []string{"default/dev"}
			if err = s.Store.UpdateApiToken(ctx, stored); err != nil {
				t.Fatal(err)
			}
			for _, e := range []domain.Event{{Cluster: "prod", Type: "Normal", Message: "later-hidden"}, {Cluster: "dev", Type: "Normal", Message: "later-visible"}} {
				if err = s.Store.AppendEvent(ctx, &e); err != nil {
					t.Fatal(err)
				}
				s.Hub.Publish(e)
			}
		}
		if strings.Contains(body, `"message":"later-visible"`) {
			cancel()
		}
	}
	err = testStream(s, ctx, w)
	// Stream cancellation can return nil from its select loop or propagate
	// context.Canceled from an in-flight read. Delivery and the canceled context,
	// rather than that scheduling-dependent return value, prove completion.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal("unexpected stream failure", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) || !changed || !strings.Contains(w.Body.String(), `"message":"later-visible"`) || strings.Contains(w.Body.String(), `"message":"later-hidden"`) {
		t.Fatal("stream restrictions not reloaded", err, w.Body.String())
	}
}
