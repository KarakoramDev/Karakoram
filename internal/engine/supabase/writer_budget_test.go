// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestBootstrapWriterDiscoveryBudget(t *testing.T) {
	for _, hang := range []bool{true, false} {
		name := "bootstrap_retains_parent"
		if hang {
			name = "hung_status"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			convergeProvider(t, p, rc)
			if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest", []byte("obsolete")); err != nil {
				t.Fatal(err)
			}
			var mutations atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				switch r.URL.Path {
				case "/v1/postgres/status":
					if hang {
						<-r.Context().Done()
						return
					}
					_ = json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
				case "/v1/postgres/" + pgspec.ActionSQLBootstrap:
					// SQL bootstrap may legitimately outlive an observation budget.
					timer := time.NewTimer(engine.ObservationTimeout + 250*time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-r.Context().Done():
						return
					}
					mutations.Add(1)
					_ = json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			start := time.Now()
			err := (supabaseFlavour{core: p.Core}).PostReady(ctx, rc)
			elapsed := time.Since(start)
			if hang {
				if elapsed > engine.ObservationTimeout+time.Second {
					t.Errorf("writer discovery blocked for %s", elapsed)
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("PostReady = %v; want deadline failure", err)
				}
				for _, text := range []string{"supabase bootstrap writer-discovery status read", "4s budget", "inspect member demop0 responsiveness/connectivity"} {
					if !strings.Contains(err.Error(), text) {
						t.Errorf("missing %q in %v", text, err)
					}
				}
				if mutations.Load() != 0 {
					t.Fatal("bootstrapped without writer status")
				}
				digest, e := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "supabase/bootstrap-digest")
				if e != nil || string(digest) != "obsolete" {
					t.Fatalf("digest advanced without bootstrap: %q %v", digest, e)
				}
			} else if err != nil || mutations.Load() != 1 {
				t.Fatalf("bootstrap with original context: %v, mutations=%d", err, mutations.Load())
			}
		})
	}
}
