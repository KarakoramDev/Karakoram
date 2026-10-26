// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestClusterDeleteAcceptedOperation(t *testing.T) {
	const id = "7MwcYfLarA0InKIY76ZjRIBu"
	for _, format := range []string{"table", "json"} {
		t.Run(format, func(t *testing.T) {
			operation := map[string]any{
				"id": id, "namespace": "default", "cluster": "t4n19ot1",
				"target": map[string]any{"kind": "cluster", "name": "t4n19ot1"},
				"type":   "delete", "phase": "Pending", "targetGeneration": float64(2),
				"createdAt": "2026-10-21T20:27:32.332805971Z",
				// Unknown fields and large JSON strings must not reject an accepted operation.
				"futureField": strings.Repeat("safe diagnostic text ", 1<<16),
			}
			deletes, observations := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != "/api/v1/clusters/t4n19ot1" || r.URL.Query().Get("namespace") != "default" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				switch r.Method {
				case http.MethodGet:
					observations++
					fmt.Fprint(w, `{"name":"t4n19ot1","status":{"phase":"Degraded","conditions":[{"type":"RestoreConsistency","status":"False","reason":"OperatorRecoveryRequired"}]}}`)
				case http.MethodDelete:
					deletes++
					w.Header().Set("Karakoram-Operation-Id", id)
					w.WriteHeader(http.StatusAccepted)
					if err := json.NewEncoder(w).Encode(operation); err != nil {
						t.Error(err)
					}
				default:
					t.Errorf("unexpected method: %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "t4n19ot1", "--yes", "-n", "default", "-o", format)
			if code != 0 || deletes != 1 || observations != 1 || !strings.Contains(out, id) {
				t.Fatalf("code=%d deletes=%d observations=%d containsID=%t stderr=%q", code, deletes, observations, strings.Contains(out, id), stderr)
			}
			if format == "json" {
				var got map[string]any
				if err := json.Unmarshal([]byte(out), &got); err != nil {
					t.Fatal(err)
				}
				want := make(map[string]any, len(operation)+1)
				for key, value := range operation {
					want[key] = value
				}
				want["operationId"] = id // Correlation header is preserved by the decoder.
				if !reflect.DeepEqual(got, want) {
					t.Fatal("JSON output did not preserve the full accepted operation")
				}
			}
		})
	}
}

func TestClusterDeleteWaitReportsTerminalFailure(t *testing.T) {
	const id = "7MwcYfLarA0InKIY76ZjRIBu"
	deletes, operationReads := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/clusters/t4n19ot1":
			deletes++
			w.Header().Set("Karakoram-Operation-Id", id)
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, `{"id":%q,"cluster":"t4n19ot1","type":"delete","phase":"Pending","targetGeneration":2}`, id)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/operations/"+id:
			operationReads++
			fmt.Fprintf(w, `{"id":%q,"phase":"Failed","step":"Deleting","message":"AuthenticationFailed while deleting backup artifacts"}`, id)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/clusters/t4n19ot1":
			fmt.Fprint(w, `{"name":"t4n19ot1","status":{"phase":"Degraded"}}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "t4n19ot1", "--yes", "--wait", "--timeout", "30m", "-n", "default", "-o", "json")
	if code != 1 || out != "" || deletes != 1 || operationReads != 1 {
		t.Fatalf("code=%d stdout=%q deletes=%d operationReads=%d stderr=%q", code, out, deletes, operationReads, stderr)
	}
	for _, text := range []string{"Accepted deletion for cluster t4n19ot1; operation " + id, "Waiting for operation " + id, "karactl: operation " + id + " Failed: AuthenticationFailed"} {
		if !strings.Contains(stderr, text) {
			t.Errorf("missing %q in stderr %q", text, stderr)
		}
	}
}

func TestClusterDeleteHTTPFailureExitCodes(t *testing.T) {
	for _, tc := range []struct{ status, code int }{{400, 6}, {401, 5}, {403, 5}, {404, 3}, {409, 4}, {500, 1}, {504, 7}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			deletes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					// A failed optional preflight must not prevent delete-only access.
					w.WriteHeader(http.StatusForbidden)
					return
				}
				deletes++
				w.WriteHeader(tc.status)
				fmt.Fprint(w, `{"code":"delete_rejected","message":"delete was not accepted"}`)
			}))
			defer server.Close()
			code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "t4n19ot1", "--yes", "-o", "json")
			if code != tc.code || out != "" || deletes != 1 || !strings.Contains(stderr, "delete was not accepted") {
				t.Fatalf("code=%d want=%d stdout=%q deletes=%d stderr=%q", code, tc.code, out, deletes, stderr)
			}
		})
	}
}
