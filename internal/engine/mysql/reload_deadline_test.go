// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestConfigurationReloadOutlivesObservationBudget(t *testing.T) {
	for _, check := range []struct {
		name string
		run  func(context.Context, *engine.Context, *member) (bool, error)
	}{{"ready", ready}, {"configApplied", configApplied}} {
		t.Run(check.name, func(t *testing.T) {
			_, rc, b := setup(t, "single")
			m := &member{Name: "demom0", ConfigVersion: 1}
			b.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
				switch r.URL.Path {
				case "/v1/status":
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 0})
					return true
				case "/v1/reload":
					select {
					case <-time.After(engine.ObservationTimeout + 100*time.Millisecond):
						json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					case <-r.Context().Done():
					}
					return true
				}
				return false
			}
			ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationTimeout+2*time.Second)
			defer cancel()
			if applied, err := check.run(ctx, rc, m); err != nil || applied {
				t.Fatalf("reload shortened to observation budget: applied=%v err=%v", applied, err)
			}
			// A caller cancellation must still terminate the same engine action.
			ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			if _, err := check.run(ctx, rc, m); err == nil {
				t.Fatal("reload ignored caller cancellation")
			}
		})
	}
}
