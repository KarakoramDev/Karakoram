// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMoveMemberCommand(t *testing.T) {
	moved, waited := false, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/clusters/demo/members/m0/move":
			moved = true
			if r.Method != http.MethodPost || r.URL.Query().Get("ignoreMaintenanceWindow") != "true" {
				t.Errorf("move request: %s %s", r.Method, r.URL)
			}
			w.Header().Set("Karakoram-Operation-Id", "move-op")
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"id":"move-op","type":"replace-member","cluster":"demo","phase":"Pending","targetGeneration":1}`)
		case "/api/v1/operations/move-op":
			waited = true
			fmt.Fprint(w, `{"id":"move-op","type":"replace-member","cluster":"demo","phase":"Succeeded","targetGeneration":1}`)
		case "/api/v1/clusters/demo":
			fmt.Fprint(w, `{"name":"demo","generation":1,"status":{"phase":"Ready","observedGeneration":1}}`)
		default:
			t.Errorf("unexpected request: %s", r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	code, _, stderr := runCLI(t, server.URL, "", false, "clusters", "members", "move", "demo", "m0", "--ignore-maintenance-window", "--wait", "--timeout", "1s", "--yes")
	if code != 0 || !moved || !waited {
		t.Fatal(code, stderr, moved, waited)
	}
	root := NewCommand(Options{})
	command, _, err := root.Find([]string{"clusters", "members", "move"})
	if err != nil || command.Name() != "move" || !strings.Contains(command.Annotations[operationAnnotation], "moveClusterMember") {
		t.Fatal(command, err)
	}
}
