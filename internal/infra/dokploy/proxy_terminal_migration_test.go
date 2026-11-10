// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestOrdinaryProxyOldFormatFailedReceipt(t *testing.T) {
	for _, action := range []string{"scale", "resources"} {
		for _, state := range []string{"paused", "rollback_started", "rollback_paused", "rollback_completed"} {
			for _, lookupFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/lookup=%t", action, state, lookupFirst), func(t *testing.T) {
					f, b, a := markedProxyFake(t, 2)
					var op infra.Op
					f.mu.Lock()
					f.services[fakeString(a.row, "appName")]["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ForceUpdate"] = 1
					f.mu.Unlock()
					var err error
					if action == "scale" {
						op, err = b.SetContainerCount(t.Context(), "proxy", 3, 3)
					} else {
						op, err = b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2})
					}
					if err != nil {
						t.Fatal(err)
					}
					original, _ := decodeOp(op)
					if original.ForceUpdate != 2 {
						t.Fatalf("migration fixture expected ForceUpdate=2, got %d", original.ForceUpdate)
					}
					m := fakeMeta(t, f, "proxy")
					// This is precisely the old persisted shape: authoritative Failed and
					// admission authority, but no typed failure field or target diagnostic.
					m.Pending.Failed, m.Pending.Uncertain, m.Pending.ProxyRolloutFailure = true, false, nil
					m.ProxyAdmissionFailure = &proxyAdmissionFailure{Issued: original.Issued, ForceUpdate: original.ForceUpdate}
					oldReceipt, err := json.Marshal(m.Pending)
					if err != nil || strings.Contains(string(oldReceipt), "proxyRolloutFailure") {
						t.Fatalf("fixture is not old format: %s %v", oldReceipt, err)
					}
					f.mu.Lock()
					a.row["description"] = m.description()
					a.holdTasks = true
					service := f.services[fakeString(a.row, "appName")]
					service["UpdateStatus"] = map[string]any{"State": state}
					reloads := f.calls["application.reload"]
					f.mu.Unlock()
					b = fakeBackend(t, f, nil)
					if lookupFirst {
						service, err := b.GetService(t.Context(), "proxy")
						var terminal *infra.ProxyRolloutFailureError
						if service == nil || service.Status != infra.StatusFailed || !errors.As(err, &terminal) {
							t.Fatalf("old retained receipt degraded to availability error: %+v %v", service, err)
						}
					}
					b = fakeBackend(t, f, nil)
					_, err = engine.ObserveProxyRollout(t.Context(), &engine.Context{Backend: b}, op, "proxy", 1, true)
					var terminal *infra.ProxyRolloutFailureError
					if !errors.As(err, &terminal) || terminal.ForceUpdate != original.ForceUpdate || !terminal.Issued.Equal(original.Issued) {
						t.Fatalf("old receipt did not gain exact terminal classification: %v", err)
					}
					persisted := fakeMeta(t, f, "proxy")
					if persisted.Pending.ProxyRolloutFailure == nil || persisted.ProxyAdmissionFailure == nil || !sameOperation(persisted.Pending, original) || fakeCalls(f, "application.reload") != reloads {
						t.Fatal("upgrade replayed or lost old failed receipt")
					}
				})
			}
		}
	}
}
