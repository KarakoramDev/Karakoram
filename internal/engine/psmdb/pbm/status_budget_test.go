// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func assertStatusBudget(t *testing.T, start time.Time, err error, member string) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
		t.Errorf("status read took %s", elapsed)
	}
	if err == nil {
		t.Fatal("expected status observation diagnostic")
	}
	for _, text := range []string{"generic status read exceeded 4s budget", "inspect member " + member + " responsiveness/connectivity"} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("missing %q in %v", text, err)
		}
	}
}

func TestGenericStatusBudget(t *testing.T) {
	for _, path := range []string{"healthy-member", "restore-fence", "memory-observation"} {
		t.Run(path, func(t *testing.T) {
			env, _, _, _ := setup(t)
			env.Nodes = env.Nodes[:1]
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/status" {
					t.Errorf("unexpected request %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			env.RC.Agents = testDialer{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			start := time.Now()
			switch path {
			case "healthy-member":
				_, err := healthyMember(ctx, env)
				assertStatusBudget(t, start, err, env.Nodes[0].Member)
			case "restore-fence":
				err := restoreFenceSupported(ctx, env)
				assertStatusBudget(t, start, err, env.Nodes[0].Member)
			case "memory-observation":
				if observeGoMemoryBudget(ctx, env) {
					t.Fatal("accepted absent memory evidence")
				}
				if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
					t.Fatalf("status read took %s", elapsed)
				}
			}
		})
	}
}
