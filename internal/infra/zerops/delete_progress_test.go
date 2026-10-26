// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestDeleteSupersedesDeploymentReceiptAndReportsPlatformQueue(t *testing.T) {
	deletes, receiptReads := 0, 0
	created := time.Now().Add(-44 * time.Minute).UTC().Format(time.RFC3339Nano)
	started := time.Now().Add(-68 * time.Minute).UTC().Format(time.RFC3339Nano)
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/service-stack/owned":
			deletes++
			_ = json.NewEncoder(w).Encode(Process{ID: "delete-process", ServiceStackID: "owned", Status: "PENDING"})
		case r.URL.Path == "/process/delete-process":
			_, _ = w.Write([]byte(`{"id":"delete-process","serviceStackId":"owned","status":"PENDING","actionName":"stack.delete","created":"` + created + `"}`))
		case r.URL.Path == "/service-stack/owned/process":
			_, _ = w.Write([]byte(`{"list":[{"id":"deploy-process","serviceStackId":"owned","projectId":"project","status":"RUNNING","actionName":"stack.deploy","started":"` + started + `"},{"id":"foreign-process","serviceStackId":"foreign","status":"RUNNING","actionName":"stack.deploy"}],"total":2}`))
		case r.Method == http.MethodGet && r.URL.Path == "/service-stack/owned":
			_ = json.NewEncoder(w).Encode(ServiceStack{ID: "owned", Name: "member", ProjectID: "project"})
		case strings.HasSuffix(r.URL.Path, "/service-stack"):
			_ = json.NewEncoder(w).Encode(page[ServiceStack]{List: []ServiceStack{{ID: "owned", Name: "member", ProjectID: "project"}}})
		default:
			receiptReads++
			t.Errorf("delete inspected deployment receipt or submitted unrelated work: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	b.client.project = "project"
	// Service-scoped deploy exclusion must not prevent deletion from
	// superseding that deployment; a pending delete must never be resubmitted.
	lock := b.operationLock("owned")
	if err := lock.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	op, err := b.DeleteServiceByID(ctx, "member", "owned")
	if err != nil || op.ID != "delete-process" {
		t.Fatalf("delete waited for the deployment fence: op=%+v err=%v", op, err)
	}
	for range 2 {
		state, err := b.OpStatus(ctx, op)
		if err != nil || state.Done || state.Failed {
			t.Fatalf("pending deletion lost its handle: state=%+v err=%v", state, err)
		}
		for _, want := range []string{"delete-process", "owned", "PENDING", "44m", "deploy-process", "stack.deploy", "RUNNING", "1h8m"} {
			if !strings.Contains(state.Message, want) {
				t.Errorf("missing %q in deletion progress %q", want, state.Message)
			}
		}
		if strings.Contains(state.Message, "foreign-process") {
			t.Fatal("foreign service process reported as deletion blocker")
		}
	}
	if deletes != 1 || receiptReads != 0 {
		t.Fatalf("pending deletion was replayed or inspected deploy receipt: deletes=%d receiptReads=%d", deletes, receiptReads)
	}
}

func TestDeleteProgressDoesNotInventTerminalOutcome(t *testing.T) {
	for _, status := range []string{"RUNNING", "PENDING"} {
		t.Run(status, func(t *testing.T) {
			inventoryCalls := 0
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/process/delete-process" {
					_ = json.NewEncoder(w).Encode(Process{ID: "delete-process", ServiceStackID: "owned", Status: status, ActionName: "stack.delete"})
					return
				}
				inventoryCalls++
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			b.client.retries = 2
			b.client.retryBase = time.Millisecond
			state, err := b.OpStatus(t.Context(), infra.Op{ID: "delete-process", ServiceID: "owned", Kind: "delete"})
			if state.Done || state.Failed || !strings.Contains(state.Message, "delete-process") {
				t.Fatalf("missing blocker evidence changed accepted delete outcome: %+v %v", state, err)
			}
			if status == "PENDING" && !errors.Is(err, infra.ErrUnavailable) || status == "RUNNING" && err != nil {
				t.Fatalf("diagnostic observation error not preserved: status=%s err=%v", status, err)
			}
			if status == "PENDING" && (inventoryCalls != 3 || !strings.Contains(state.Message, "inventory unavailable")) {
				t.Fatalf("diagnostic retries were not bounded: calls=%d state=%+v", inventoryCalls, state)
			}
			if status == "RUNNING" && inventoryCalls != 0 {
				t.Fatal("running deletion unnecessarily inspected queue blockers")
			}
		})
	}
}

func TestDeleteProgressPropagatesInventoryCooldown(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			requests := 0
			before := time.Now().Add(time.Hour)
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				switch r.URL.Path {
				case "/process/delete-process":
					_ = json.NewEncoder(w).Encode(Process{ID: "delete-process", ServiceStackID: "owned", Status: "PENDING"})
				case "/service-stack/owned/process":
					w.Header().Set("Retry-After", "3600")
					w.WriteHeader(code)
				default:
					t.Errorf("unexpected request after queue cooldown: %s %s", r.Method, r.URL.Path)
				}
			})
			b.client.retries = 2
			state, err := b.OpStatus(t.Context(), infra.Op{ID: "delete-process", ServiceID: "owned", Kind: "delete"})
			if !errors.Is(err, infra.ErrUnavailable) || infra.RetryAt(err).Before(before) {
				t.Fatalf("queue inventory cooldown swallowed: state=%+v err=%v retry=%v", state, err, infra.RetryAt(err))
			}
			if !strings.Contains(err.Error(), "retaining the accepted deletion") {
				t.Fatalf("observation error lost accepted-delete diagnostic: %v", err)
			}
			if state.Done || state.Failed || state.Status != "PENDING" || !strings.Contains(state.Message, "retaining the accepted deletion") {
				t.Fatalf("cooldown retired accepted delete or lost diagnostic: %+v", state)
			}
			if requests != 2 {
				t.Fatalf("requests continued after Retry-After: got %d want process + inventory", requests)
			}
		})
	}
}
