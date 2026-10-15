// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
