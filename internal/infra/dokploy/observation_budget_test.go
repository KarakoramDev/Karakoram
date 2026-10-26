// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func observationFailure(w http.ResponseWriter, at time.Time) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusBadRequest
	if !at.IsZero() {
		status = http.StatusTooManyRequests
		w.Header().Set("Retry-After", at.Format(http.TimeFormat))
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"message":"observation failed"}`))
}

func TestObservationJoinsRuntimeRetryConstraints(t *testing.T) {
	for _, two := range []bool{false, true} {
		t.Run(fmt.Sprint(two), func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			latest := time.Now().UTC().Truncate(time.Second).Add(2 * time.Hour)
			first := time.Time{}
			if two {
				first = latest.Add(-time.Hour)
			}
			f.mu.Lock()
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				switch r.URL.Path {
				case "/api/docker.getConfig":
					time.Sleep(40 * time.Millisecond)
					observationFailure(w, first)
					return true, nil
				case "/api/deployment.all":
					observationFailure(w, latest)
					return true, nil
				}
				return false, nil
			}
			f.mu.Unlock()
			_, err := b.GetService(t.Context(), "runtime")
			if !infra.RetryAt(err).Equal(latest) || (!two && !errors.Is(err, infra.ErrRejected)) {
				t.Fatal("lost a concurrently observed constraint", err, infra.RetryAt(err), latest)
			}
		})
	}
}

func TestObservationJoinsListRetryConstraints(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime-a", ""), runtimeSpec("runtime-b", ""))
	deployFake(t, b, "runtime-a", "")
	deployFake(t, b, "runtime-b", "")
	latest := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	f.mu.Lock()
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.URL.Path != "/api/application.one" {
			return false, nil
		}
		switch r.URL.Query().Get("applicationId") {
		case imported.Services["runtime-a"]:
			observationFailure(w, time.Time{})
			return true, nil
		case imported.Services["runtime-b"]:
			observationFailure(w, latest)
			return true, nil
		}
		return false, nil
	}
	f.mu.Unlock()
	_, err := b.ListServices(t.Context(), "runtime")
	if !errors.Is(err, infra.ErrRejected) || !infra.RetryAt(err).Equal(latest) {
		t.Fatal("list dropped a sibling read error", err, infra.RetryAt(err))
	}
}

func TestObservationHTTPBudgetSharedAcrossBackendInstances(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	names := []string{}
	for i := range 9 {
		name := fmt.Sprint("runtime", i)
		volume := fmt.Sprint("volume", i)
		importFake(t, b, volumeSpec(volume), runtimeSpec(name, volume))
		deployFake(t, b, name, volume)
		names = append(names, name)
	}
	second := fakeBackend(t, f, nil)
	ctx, cancel := context.WithTimeout(infra.ObservationContext(t.Context()), 10*time.Second)
	defer cancel()
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	entered := make(chan struct{}, 128)
	var active, maximum atomic.Int32
	f.mu.Lock()
	f.requestHook = func(_ http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.Method != http.MethodGet {
			return false, nil
		}
		n := active.Add(1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		finish := func() { active.Add(-1) }
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return false, finish
		case <-r.Context().Done():
			return true, finish
		}
	}
	f.mu.Unlock()
	done := make(chan engine.PlatformObservation, 2)
	for _, backend := range []*Backend{b, second} {
		go func() { done <- engine.ObservePlatform(ctx, backend, "runtime", names) }()
	}
	for range 8 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("observations did not overlap")
		}
	}
	exceeded := false
	select {
	case <-entered:
		exceeded = true
	case <-time.After(40 * time.Millisecond):
	}
	close(release)
	for range 2 {
		result := <-done
		if err := errors.Join(result.ProjectErr, result.ServicesErr); err != nil {
			t.Fatal(err)
		}
	}
	if exceeded || maximum.Load() != 8 {
		t.Fatal("nested/cross-client requests exceeded the observation budget", maximum.Load())
	}
}
