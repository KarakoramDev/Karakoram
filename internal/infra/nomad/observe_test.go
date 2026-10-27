// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLogsSinceIncludesOngoingAndRecentRuns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		finished time.Time
		want     bool
	}{
		{"ongoing", time.Time{}, true},
		{"finished-recently", time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC), true},
		{"finished-at-since", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), true},
		{"finished-before-since", time.Date(2026, 1, 1, 23, 59, 0, 0, time.UTC), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			f.mu.Lock()
			for _, a := range f.allocs {
				state := fakeMap(fakeMap(a.row["TaskStates"])["member"])
				state["StartedAt"] = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				state["FinishedAt"] = tc.finished
				a.logs["member/stdout"] = "recent output\n"
				a.logs["member/stderr"] = ""
			}
			f.mu.Unlock()
			lines, err := b.Logs(t.Context(), "runtime", infra.LogQuery{Since: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)})
			if err != nil || (len(lines) == 1) != tc.want {
				t.Fatalf("Since logs = %+v, %v; want output %v", lines, err, tc.want)
			}
		})
	}
}

func TestLogsSkipUnreadableInactiveAllocations(t *testing.T) {
	for _, status := range []string{"complete", "failed", "lost", "unknown"} {
		for _, missingState := range []bool{false, true} {
			t.Run(status+"/missing-state-"+map[bool]string{false: "no", true: "yes"}[missingState], func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				importFake(t, b, runtimeSpec("runtime", ""))
				deployFake(t, b, "runtime", "")
				old := ""
				f.mu.Lock()
				for id := range f.allocs {
					old = id
				}
				f.mu.Unlock()
				op, err := b.Start(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
				f.setAutoTick(false)
				f.mu.Lock()
				f.allocs[old].row["ClientStatus"] = status
				if missingState {
					delete(f.allocs[old].row, "TaskStates")
				}
				f.mu.Unlock()
				transport := b.c.http.Transport
				b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/v1/client/fs/logs/"+old || r.URL.Path == "/v1/allocation/"+old {
						return &http.Response{StatusCode: 500, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("allocation logs unavailable"))}, nil
					}
					return transport.RoundTrip(r)
				})
				lines, err := b.Logs(t.Context(), "runtime", infra.LogQuery{})
				if err != nil || len(lines) == 0 {
					t.Fatalf("terminal allocation blocked live logs: %+v %v", lines, err)
				}
				for _, line := range lines {
					if line.Container == old {
						t.Fatal("unreadable terminal log returned")
					}
				}
			})
		}
	}
}

func TestLogsTruncateGrowingTail(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	transport := b.c.http.Transport
	b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasPrefix(r.URL.Path, "/v1/client/fs/logs/") {
			text := ""
			if r.URL.Query().Get("type") == "stdout" {
				text = strings.Repeat("old line\n", 600) + "newest line\n"
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(text))}, nil
		}
		return transport.RoundTrip(r)
	})
	lines, err := b.Logs(t.Context(), "runtime", infra.LogQuery{Limit: 1})
	if err != nil || len(lines) != 1 || lines[0].Message != "newest line" {
		t.Fatalf("growing tail = %+v, %v", lines, err)
	}
}

func TestLogsMissingStreamDoesNotHideScaledMember(t *testing.T) {
	for _, member := range []string{"demom3", "demom4", "demop3", "demopx"} {
		for _, missing := range []string{"stdout", "stderr", "both"} {
			t.Run(member+"/"+missing, func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				importFake(t, b, runtimeSpec(member, ""))
				deployFake(t, b, member, "")
				transport := b.c.http.Transport
				b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
					if strings.HasPrefix(r.URL.Path, "/v1/client/fs/logs/") {
						stream := r.URL.Query().Get("type")
						status, message := 200, "member diagnostic\n"
						if missing == "both" || stream == missing {
							status = 404
							message = fmt.Sprintf("log entry for task %q and log type %q not found", r.URL.Query().Get("task"), stream)
						}
						return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(message))}, nil
					}
					return transport.RoundTrip(r)
				})
				logs, err := b.Logs(t.Context(), member, infra.LogQuery{Limit: 100})
				want := 1
				if missing == "both" {
					want = 0
				}
				if err != nil || len(logs) != want {
					t.Fatalf("log tail = %+v, %v; want %d lines", logs, err, want)
				}
				if want == 1 && (logs[0].Message != "member diagnostic" || logs[0].Stream == missing) {
					t.Fatalf("readable sibling output lost: %+v", logs)
				}
			})
		}
	}
}

func TestLogsPreserveActiveAllocationErrors(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		status        int
		want          error
	}{
		{"unknown-allocation", "unknown allocation", 404, infra.ErrNotFound},
		{"task-not-started", `task "member" not started yet. No logs available`, 404, infra.ErrNotFound},
		{"wrong-task-stream", `log entry for task "other" and log type "stdout" not found`, 404, infra.ErrNotFound},
		{"forbidden", "permission denied", 403, infra.ErrUnauthorized},
		{"unavailable", "client unavailable", 503, infra.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("demom3", ""))
			deployFake(t, b, "demom3", "")
			transport := b.c.http.Transport
			b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
				if strings.HasPrefix(r.URL.Path, "/v1/client/fs/logs/") {
					return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.message))}, nil
				}
				return transport.RoundTrip(r)
			})
			if _, err := b.Logs(t.Context(), "demom3", infra.LogQuery{Limit: 100}); !errors.Is(err, tc.want) {
				t.Fatalf("active allocation error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestContainersPreferAllocationNetworkStatus(t *testing.T) {
	for _, address := range []string{"192.0.2.123", "", "invalid"} {
		t.Run("address-"+address, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			f.mu.Lock()
			for _, a := range f.allocs {
				a.row["NetworkStatus"] = fakeRow{"Address": address, "AddressIPv6": "2001:db8::123"}
			}
			f.mu.Unlock()
			containers, err := b.Containers(t.Context(), "runtime")
			want := address
			if address == "invalid" {
				want = ""
			}
			if err != nil || len(containers) != 1 || containers[0].IPv4 != want || containers[0].IPv6 != "2001:db8::123" {
				t.Fatalf("allocation address = %+v, %v; want IPv4 %q", containers, err, want)
			}
		})
	}
}

func TestContainersHydrateProjectedAllocationNetworkStatus(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	f.mu.Lock()
	for _, a := range f.allocs {
		a.row["NetworkStatus"] = fakeRow{"Address": "198.51.100.42", "AddressIPv6": ""}
	}
	f.mu.Unlock()
	transport := b.c.http.Transport
	detailReads := 0
	b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(r.URL.Path, "/v1/allocation/") {
			detailReads++
		}
		if r.URL.Path == "/v1/job/runtime/allocations" {
			defer response.Body.Close()
			var rows []map[string]any
			if err := json.NewDecoder(response.Body).Decode(&rows); err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				delete(row, "NetworkStatus")
			}
			data, err := json.Marshal(rows)
			if err != nil {
				t.Fatal(err)
			}
			response.Body = io.NopCloser(bytes.NewReader(data))
			response.ContentLength = int64(len(data))
		}
		return response, nil
	})
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 || containers[0].IPv4 != "198.51.100.42" || detailReads != 1 {
		t.Fatalf("projected allocation = %+v, %v; detail reads %d", containers, err, detailReads)
	}
}

func TestContainersNeverSubstituteHostAddressForCNI(t *testing.T) {
	for _, status := range []string{"pending", "running"} {
		t.Run(status, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			f.mu.Lock()
			for _, a := range f.allocs {
				a.row["ClientStatus"] = status
				delete(a.row, "NetworkStatus")
			}
			f.mu.Unlock()
			containers, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(containers) != 1 || containers[0].IPv4 != "" || containers[0].IPv6 != "" {
				t.Fatalf("missing CNI address became a host address: %+v %v", containers, err)
			}
		})
	}
}

func TestContainersSkipAllocationRemovedDuringHydration(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	f.mu.Lock()
	for _, a := range f.allocs {
		delete(a.row, "NetworkStatus")
	}
	f.mu.Unlock()
	transport := b.c.http.Transport
	b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasPrefix(r.URL.Path, "/v1/allocation/") {
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("allocation no longer exists"))}, nil
		}
		return transport.RoundTrip(r)
	})
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 0 {
		t.Fatalf("removed allocation failed container observation: %+v %v", containers, err)
	}
}
