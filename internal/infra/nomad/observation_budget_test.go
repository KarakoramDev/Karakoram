// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
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
	_, _ = w.Write([]byte(`{"error":"observation failed"}`))
}

func TestObservationJoinsRetryConstraints(t *testing.T) {
	for _, layer := range []string{"records", "services", "hydration"} {
		t.Run(layer, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime-a", "")
			spec.MinContainers, spec.MaxContainers = 2, 2
			importFake(t, b, spec, runtimeSpec("runtime-b", ""))
			deployFake(t, b, "runtime-a", "")
			deployFake(t, b, "runtime-b", "")
			ids := []string{}
			f.mu.Lock()
			for id, a := range f.allocs {
				if a.row["JobID"] == "runtime-a" {
					ids = append(ids, id)
				}
			}
			f.projectAllocations = true
			f.mu.Unlock()
			slices.Sort(ids)
			if len(ids) != 2 {
				t.Fatal("fixture allocation count", len(ids))
			}
			latest := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
			f.mu.Lock()
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				first, last := false, false
				switch layer {
				case "records":
					first = strings.HasPrefix(r.URL.Path, "/v1/var/") && strings.HasSuffix(r.URL.Path, "runtime-a")
					last = strings.HasPrefix(r.URL.Path, "/v1/var/") && strings.HasSuffix(r.URL.Path, "runtime-b")
				case "services":
					first = r.URL.Path == "/v1/job/runtime-a"
					last = r.URL.Path == "/v1/job/runtime-b"
				case "hydration":
					first = r.URL.Path == "/v1/allocation/"+ids[0]
					last = r.URL.Path == "/v1/allocation/"+ids[1]
				}
				if first {
					time.Sleep(40 * time.Millisecond)
					observationFailure(w, time.Time{})
					return true, nil
				}
				if last {
					observationFailure(w, latest)
					return true, nil
				}
				return false, nil
			}
			f.mu.Unlock()
			var err error
			if layer == "hydration" {
				_, err = b.Containers(t.Context(), "runtime-a")
			} else {
				_, err = b.ListServices(t.Context(), "runtime")
			}
			if !errors.Is(err, infra.ErrRejected) || !infra.RetryAt(err).Equal(latest) {
				t.Fatal("lost a concurrent read constraint", err, infra.RetryAt(err))
			}
		})
	}
}

func TestObservationHTTPBudgetSharedAcrossBackendInstances(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	names := []string{}
	for i := range 3 {
		name := fmt.Sprint("runtime", i)
		spec := runtimeSpec(name, "")
		spec.MinContainers, spec.MaxContainers = 9, 9
		importFake(t, b, spec)
		deployFake(t, b, name, "")
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
	entered := make(chan struct{}, 256)
	var active, maximum atomic.Int32
	f.mu.Lock()
	f.projectAllocations = true
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
			t.Fatal("observation reads did not overlap")
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
		for _, member := range result.Members {
			if member.Err != nil || len(member.Containers) != 9 {
				t.Fatal("missing projected allocations", len(member.Containers), member.Err)
			}
		}
	}
	if exceeded || maximum.Load() != 8 {
		t.Fatal("nested/cross-client HTTP reads exceeded budget", maximum.Load())
	}
}
