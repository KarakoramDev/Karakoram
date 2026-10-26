// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Zerops queues user-data processes behind an accepted deployment. The first
// PostgreSQL deployment can need its peers before passing native readiness.
func TestCreateDeployReturnsBeforeQueuedReceiptProcess(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	imported, err := plain.ImportServices(t.Context(), []infra.ServiceSpec{
		{Hostname: "first", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404},
		{Hostname: "second", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404},
	})
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	var queuedWrites atomic.Int32
	transport := receiptTransport(func(r *http.Request) (*http.Response, error) {
		if accepted.Load() == 1 && r.Method != http.MethodGet && strings.Contains(r.URL.Path, "/user-data/") {
			queuedWrites.Add(1)
		}
		response, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return response, err
		}
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy") {
			accepted.Add(1)
		}
		// Until the second deployment is submitted, the first deployment and
		// user-data processes queued behind it cannot finish.
		if accepted.Load() == 1 && (strings.HasPrefix(r.URL.Path, "/process/") || strings.HasPrefix(r.URL.Path, "/app-version/") && r.Method == http.MethodGet) {
			response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(`{"status":"DEPLOYING"}`))
		}
		return response, nil
	})
	b := hardeningBackend(t, server, transport)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	first, err := b.Deploy(ctx, "first", hardeningDeployment())
	if err != nil || ctx.Err() != nil || queuedWrites.Load() != 0 {
		t.Fatalf("accepted deployment waited for a receipt queued behind quorum readiness: op=%+v err=%v context=%v queuedWrites=%d", first, err, ctx.Err(), queuedWrites.Load())
	}
	if first.ID == "" || first.ServiceID != imported.Services["first"] {
		t.Fatalf("deployment lost its durable identity: %+v", first)
	}
	recovered := hardeningBackend(t, server, transport)
	state, err := recovered.OpStatus(t.Context(), first)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("first deployment incorrectly completed before its peer: %+v %v", state, err)
	}
	retry, err := recovered.Deploy(t.Context(), "first", hardeningDeployment())
	if err != nil || retry.ID != first.ID || accepted.Load() != 1 {
		t.Fatalf("reconstructed backend duplicated pending deployment: %+v %v accepted=%d", retry, err, accepted.Load())
	}
	if _, err = plain.Deploy(t.Context(), "second", hardeningDeployment()); err != nil {
		t.Fatal(err)
	}
	accepted.Add(1)
	state, err = recovered.OpStatus(t.Context(), first)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("first deployment did not complete after peer submission: %+v %v", state, err)
	}
	receipt, err := recovered.receipt(t.Context(), first.ServiceID)
	if err != nil || receipt.Action != "" {
		t.Fatalf("completed version retained receipt: %+v %v", receipt, err)
	}
	versions, err := getPages[AppVersion](t.Context(), recovered.client, "/service-stack/"+first.ServiceID+"/app-version")
	if err != nil || len(versions) != 1 {
		t.Fatalf("unexpected first-member versions: %d %v", len(versions), err)
	}
}

func TestCreateReceiptExclusionIsServiceScopedAndCancelable(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	if _, err := plain.ImportServices(t.Context(), []infra.ServiceSpec{
		{Hostname: "first", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404},
		{Hostname: "second", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404},
	}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	first := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/upload") {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	}))
	firstResult := make(chan error, 1)
	go func() {
		_, err := first.Deploy(t.Context(), "first", hardeningDeployment())
		firstResult <- err
	}()
	<-entered
	other := hardeningBackend(t, server, nil)
	unrelated := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		_, err := other.Deploy(ctx, "second", hardeningDeployment())
		unrelated <- err
	}()
	select {
	case err := <-unrelated:
		if err != nil {
			t.Errorf("unrelated service deployment was blocked: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("unrelated service deployment trapped behind another service's upload")
	}
	// All entry points that share a service's receipt must honor cancellation
	// without submitting a second mutation or detaching the active upload.
	for _, action := range []string{"deploy", "stop", "observe"} {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result := make(chan error, 1)
		go func() {
			var err error
			switch action {
			case "deploy":
				_, err = other.Deploy(ctx, "first", hardeningDeployment())
			case "stop":
				_, err = other.Stop(ctx, "first")
			case "observe":
				service, e := plain.GetService(t.Context(), "first")
				if e != nil {
					result <- e
					return
				}
				_, err = other.OpStatus(ctx, infra.Op{ID: "pending", Kind: "deploy", ServiceID: service.ID})
			}
			result <- err
		}()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s did not honor cancellation: %v", action, err)
			}
		case <-time.After(time.Second):
			t.Errorf("%s trapped a canceled caller behind the active upload", action)
		}
	}
	unblock()
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
}
