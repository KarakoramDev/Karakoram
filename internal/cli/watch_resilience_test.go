// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAcceptedWaitRetriesTransientObservations(t *testing.T) {
	for _, status := range []int{408, 429, 502, 503, 504} {
		for _, observation := range []string{"operation", "cluster"} {
			t.Run(fmt.Sprintf("%s/%d", observation, status), func(t *testing.T) {
				mutations, failures := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method == http.MethodPatch {
						mutations++
						w.Header().Set("Karakoram-Operation-Id", "upgrade-one")
						w.WriteHeader(http.StatusAccepted)
						fmt.Fprint(w, `{"name":"demo","generation":4}`)
						return
					}
					isOperation := strings.Contains(r.URL.Path, "/operations/")
					if isOperation == (observation == "operation") && failures == 0 {
						failures++
						w.WriteHeader(status)
						return
					}
					if isOperation {
						fmt.Fprint(w, `{"id":"upgrade-one","phase":"Succeeded","message":"Upgrade complete"}`)
					} else {
						fmt.Fprint(w, `{"name":"demo","generation":4,"status":{"phase":"Ready","observedGeneration":4}}`)
					}
				}))
				defer server.Close()
				code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "upgrade", "demo", "--version", "8.0", "--wait", "--timeout", "2s")
				if code != 0 || mutations != 1 || failures != 1 || !strings.Contains(out, "Ready") || !strings.Contains(stderr, "Observation temporarily unavailable; retrying") {
					t.Fatalf("code=%d mutations=%d failures=%d stdout=%q stderr=%q", code, mutations, failures, out, stderr)
				}
			})
		}
	}
}

func TestAcceptedWaitObservationFailureGuidance(t *testing.T) {
	for _, tc := range []struct{ status, code int }{{502, 7}, {401, 5}, {403, 5}, {404, 3}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			mutations, observations := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPatch {
					mutations++
					w.Header().Set("Karakoram-Operation-Id", "upgrade-one")
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"name":"demo","generation":4}`)
					return
				}
				observations++
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			code, _, stderr := runCLI(t, server.URL, "", false, "clusters", "upgrade", "demo", "--version", "8.0", "--wait", "--timeout", "350ms")
			if code != tc.code || mutations != 1 {
				t.Fatalf("code=%d mutations=%d stderr=%q", code, mutations, stderr)
			}
			for _, text := range []string{"operation upgrade-one was accepted", "Resume waiting without resubmitting", `operations wait "upgrade-one"`} {
				if !strings.Contains(stderr, text) {
					t.Errorf("missing %q: %q", text, stderr)
				}
			}
			if tc.status == 502 {
				if observations < 2 || !strings.Contains(stderr, "retrying") {
					t.Errorf("no retries: observations=%d stderr=%q", observations, stderr)
				}
			} else if observations != 1 {
				t.Errorf("non-transient response retried %d times", observations)
			}
		})
	}
}

func TestOperationWaitRetriesConnectionFailure(t *testing.T) {
	observations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observations++
		if observations == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"op-one","phase":"Succeeded"}`)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "operations", "wait", "op-one", "--timeout", "2s")
	if code != 0 || observations < 2 || !strings.Contains(out, "Succeeded") || !strings.Contains(stderr, "retrying") {
		t.Fatalf("code=%d observations=%d stdout=%q stderr=%q", code, observations, out, stderr)
	}
}

func TestTransientObservationErrorClassification(t *testing.T) {
	for _, err := range []error{&net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}, context.DeadlineExceeded, io.EOF, io.ErrUnexpectedEOF} {
		if !transientObservationError(err) {
			t.Errorf("transient error not retried: %v", err)
		}
	}
	for _, err := range []error{apiFailure(400, "invalid"), apiFailure(401, "unauthorized"), apiFailure(403, "forbidden"), apiFailure(404, "missing"), fmt.Errorf("invalid JSON")} {
		if transientObservationError(err) {
			t.Errorf("permanent error retried: %v", err)
		}
	}
}

func TestSuspensionWaitUsesDesiredGeneration(t *testing.T) {
	for _, target := range []string{`,"targetGeneration":4`, ""} {
		t.Run(target, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodDelete:
					w.Header().Set("Karakoram-Operation-Id", "suspend-one")
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprintf(w, `{"id":"suspend-one","type":"delete","namespace":"default","cluster":"demo","phase":"Pending"%s}`, target)
				case strings.Contains(r.URL.Path, "/operations/"):
					fmt.Fprint(w, `{"id":"suspend-one","type":"delete","phase":"Succeeded","targetGeneration":4,"message":"Cluster runtime suspended; volumes retained"}`)
				default:
					fmt.Fprint(w, `{"name":"demo","namespace":"default","generation":4,"spec":{"provider":"psmdb","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":3}}},"status":{"phase":"Suspended","observedGeneration":4,"message":"Runtime suspended; retained volumes are available for resume"}}`)
				}
			}))
			defer server.Close()
			code, _, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "demo", "--keep-volumes", "--yes", "--wait", "--timeout", "2s")
			if code != 0 || !strings.Contains(stderr, "Suspended; generation 4/4") || strings.Contains(stderr, "generation 4/0") {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
		})
	}
}

func TestCompletedOperationDoesNotWrapClusterFailures(t *testing.T) {
	for _, degraded := range []bool{false, true} {
		t.Run(fmt.Sprint(degraded), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPatch:
					w.Header().Set("Karakoram-Operation-Id", "upgrade-one")
					fmt.Fprint(w, `{"name":"demo","generation":4}`)
				case strings.Contains(r.URL.Path, "/operations/"):
					fmt.Fprint(w, `{"id":"upgrade-one","phase":"Succeeded"}`)
				case degraded:
					fmt.Fprint(w, `{"name":"demo","generation":4,"status":{"phase":"Degraded","observedGeneration":4,"message":"A member is unavailable"}}`)
				default:
					w.WriteHeader(http.StatusForbidden)
				}
			}))
			defer server.Close()
			code, _, stderr := runCLI(t, server.URL, "", false, "clusters", "upgrade", "demo", "--version", "8.0", "--wait", "--timeout", "1s")
			if degraded {
				if code != 1 || !strings.Contains(stderr, "cluster demo is Degraded: A member is unavailable") || strings.Contains(stderr, "work continues server-side") || strings.Contains(stderr, "Resume waiting") {
					t.Fatalf("code=%d stderr=%q", code, stderr)
				}
			} else if code != 5 || strings.Count(stderr, "Resume waiting") != 1 || !strings.Contains(stderr, `clusters wait "demo"`) {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			if strings.Contains(stderr, `operations wait "upgrade-one"`) {
				t.Errorf("completed operation offered as resume target: %q", stderr)
			}
		})
	}
}

func TestObservationRejectsPermanentNetworkErrors(t *testing.T) {
	for _, cause := range []error{
		x509.UnknownAuthorityError{},
		&net.DNSError{Err: "no such host", Name: "missing.example.test", IsNotFound: true},
		fmt.Errorf("unsupported protocol scheme"),
	} {
		err := &url.Error{Op: "Get", URL: "https://example.test", Err: cause}
		if transientObservationError(err) {
			t.Errorf("permanent network error retried: %v", err)
		}
	}
	for _, cause := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, context.DeadlineExceeded} {
		err := &url.Error{Op: "Get", URL: "https://example.test", Err: &net.OpError{Op: "read", Net: "tcp", Err: cause}}
		if !transientObservationError(err) {
			t.Errorf("transient network error not retried: %v", err)
		}
	}
}

func TestRetryAfterParsingAndDeadline(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"2", 2 * time.Second},
		{now.Add(5 * time.Second).Format(http.TimeFormat), 5 * time.Second},
		{now.Add(-time.Second).Format(http.TimeFormat), 0},
		{"-1", 0}, {"invalid", 0}, {"", 0},
	} {
		if got := retryAfterDelay(tc.value, now); got != tc.want {
			t.Errorf("Retry-After %q = %s, want %s", tc.value, got, tc.want)
		}
	}
	for _, status := range []int{429, 503} {
		for _, body := range []string{"Service unavailable", `{"code":"busy","message":"Try later"}`} {
			recorder := httptest.NewRecorder()
			recorder.Header().Set("Retry-After", "2")
			recorder.WriteHeader(status)
			fmt.Fprint(recorder, body)
			_, err := decodeResponse(recorder.Result())
			var response *commandError
			if !errors.As(err, &response) || response.retryAfter != 2*time.Second {
				t.Fatalf("status=%d error=%v", status, err)
			}
			if got := observationRetryDelay(context.Background(), err, 100*time.Millisecond); got != 2*time.Second {
				t.Errorf("Retry-After ignored: %s", got)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			delay := observationRetryDelay(ctx, err, 100*time.Millisecond)
			cancel()
			if delay <= 0 || delay > time.Second {
				t.Errorf("delay exceeds remaining deadline: %s", delay)
			}
		}
	}
}
