// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func nomadObservationAttempt(t *testing.T) context.Context {
	t.Helper()
	return infra.WithObservationCache(infra.ObservationContext(t.Context()), &sync.Map{})
}

func TestObservationMemoReuseAndBypasses(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	paths := []string{"/v1/var/" + recordPrefix + "runtime", "/v1/job/runtime", "/v1/job/runtime/allocations"}
	ctx := nomadObservationAttempt(t)
	before := len(f.requests())
	for range 2 {
		for _, path := range paths {
			var out any
			if _, err := b.c.get(ctx, path, nil, &out); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := len(f.requests()) - before; got != len(paths) {
		t.Fatalf("attempt made %d GETs, want %d", got, len(paths))
	}
	for name, fresh := range map[string]context.Context{
		"new attempt":      nomadObservationAttempt(t),
		"lifecycle":        infra.WithoutObservation(ctx),
		"unmarked context": infra.WithObservationCache(t.Context(), infra.ObservationCache(ctx)),
	} {
		t.Run(name, func(t *testing.T) {
			before := len(f.requests())
			for _, path := range paths {
				if _, err := b.c.get(fresh, path, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			if got := len(f.requests()) - before; got != len(paths) {
				t.Fatalf("fresh reads made %d GETs, want %d", got, len(paths))
			}
		})
	}
	before = len(f.requests())
	fresh := infra.WithFreshTaskInventory(ctx)
	for range 2 {
		for _, path := range paths {
			if _, err := b.c.get(fresh, path, nil, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if calls := f.requests()[before:]; len(calls) != 2 || calls[0].Path != paths[2] || calls[1].Path != paths[2] {
		t.Fatalf("pinned authorization did not refresh inventory: %+v", calls)
	}
}

func TestObservationMemoSingleFlightAndIndependentDecoding(t *testing.T) {
	var requests atomic.Int32
	entered, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"Meta":{"owner":"original"}}`))
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	c := newClient(server.URL, "", "default", "test", nil)
	ctx := nomadObservationAttempt(t)
	results := make(chan map[string]map[string]string, 8)
	errors := make(chan error, 8)
	for range 8 {
		go func() {
			var out map[string]map[string]string
			_, err := c.get(ctx, "/v1/job/runtime", nil, &out)
			results <- out
			errors <- err
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	close(release)
	for range 8 {
		out := <-results
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
		if out["Meta"]["owner"] != "original" {
			t.Fatal("callers shared mutable decoded state")
		}
		out["Meta"]["owner"] = "changed"
	}
	if requests.Load() != 1 {
		t.Fatalf("single flight made %d requests", requests.Load())
	}
}

func TestObservationMemoEvictsErrorsAndMutations(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		switch requests.Add(1) {
		case 1:
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		case 2:
			_, _ = w.Write([]byte(`invalid`))
		default:
			_, _ = w.Write([]byte(`{"ID":"runtime"}`))
		}
	}))
	defer server.Close()
	c := newClient(server.URL, "", "default", "test", nil)
	ctx := nomadObservationAttempt(t)
	for range 2 {
		var out map[string]string
		if _, err := c.get(ctx, "/v1/job/runtime", nil, &out); err == nil {
			t.Fatal("expected failed read")
		}
	}
	for range 2 {
		var out map[string]string
		if _, err := c.get(ctx, "/v1/job/runtime", nil, &out); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 3 {
		t.Fatalf("failed reads were memoized, GETs=%d", requests.Load())
	}
	if err := c.put(infra.WithoutObservation(ctx), "/v1/var/karakoram/test", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.get(ctx, "/v1/job/runtime", nil, nil); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 4 {
		t.Fatal("memo survived a lifecycle mutation")
	}
}

func TestObservationMemoCanceledLeaderDoesNotPoisonAttempt(t *testing.T) {
	var requests atomic.Int32
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := newClient(server.URL, "", "default", "test", nil)
	ctx := nomadObservationAttempt(t)
	leader, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.get(leader, "/v1/job/runtime", nil, nil); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled read succeeded")
	}
	if _, err := c.get(ctx, "/v1/job/runtime", nil, nil); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatal("canceled leader poisoned the attempt")
	}
}

func TestObservationMemoSteadyCannotServeFreshInventory(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	// Warm the platform snapshot, then populate the pass memo from it.
	steady := infra.WithSteadyObservation(nomadObservationAttempt(t))
	if _, hit := b.PlatformSnapshot().Lookup(steady, "listing-baseline"); hit {
		t.Fatal("listing warm-up unexpectedly served an entry")
	}
	if _, err := b.Containers(steadyNomadContext(t), "runtime"); err != nil {
		t.Fatal(err)
	}
	before := len(f.requests())
	if _, err := b.Containers(steady, "runtime"); err != nil {
		t.Fatal(err)
	}
	if len(f.requests()) != before {
		t.Fatal("fixture did not populate memo from the steady snapshot")
	}
	f.mu.Lock()
	delete(f.variables, fakeKey("default", recordPrefix+"runtime"))
	f.mu.Unlock()
	if _, err := b.Containers(infra.WithFreshTaskInventory(steady), "runtime"); err == nil {
		t.Fatal("fresh authorization consumed the revoked steady service record")
	}
	calls := f.requests()[before:]
	if len(calls) != 1 || calls[0].Path != "/v1/var/"+recordPrefix+"runtime" {
		t.Fatalf("authorization did not reload the service record: %+v", calls)
	}
}

func TestObservationMemoFreshDoesNotJoinSteadyLeader(t *testing.T) {
	for _, path := range []string{"/v1/var/karakoram/services/runtime", "/v1/job/runtime"} {
		t.Run(path, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
					_, _ = w.Write([]byte(`{"value":"steady"}`))
					return
				}
				_, _ = w.Write([]byte(`{"value":"fresh"}`))
			}))
			defer server.Close()
			defer close(release)
			c := newClient(server.URL, "", "default", "test", nil)
			ctx, cancel := context.WithTimeout(nomadObservationAttempt(t), 5*time.Second)
			defer cancel()
			steady := infra.WithSteadyObservation(ctx)
			done := make(chan error, 1)
			go func() { _, err := c.get(steady, path, nil, nil); done <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var out map[string]string
			if _, err := c.get(infra.WithFreshTaskInventory(steady), path, nil, &out); err != nil {
				t.Fatal(err)
			}
			if out["value"] != "fresh" {
				t.Fatal("fresh reader joined the steady leader")
			}
			if _, err := c.get(steady, path, nil, &out); err != nil || out["value"] != "fresh" || requests.Load() != 2 {
				t.Fatal("fresh memo did not serve both classes", out, err, requests.Load())
			}
			// Cancel releases the outstanding leader without changing memo class.
			cancel()
			<-done
		})
	}
}
