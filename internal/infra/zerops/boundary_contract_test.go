// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Exercise the public backend, not just the client's no-replay switch: observing
// the committed configuration must not turn a lost response into a second PUT.
func TestBoundaryConfigurationLostResponseThroughBackend(t *testing.T) {
	for _, action := range []string{"environment", "resources"} {
		for _, status := range []int{0, http.StatusInternalServerError} {
			name := action + "/reset"
			if status != 0 {
				name = action + "/500"
			}
			t.Run(name, func(t *testing.T) {
				server := newFakeAPI(t)
				plain := hardeningBackend(t, server, nil)
				id := importHardeningRuntime(t, plain)
				if err := plain.SetEnvVars(t.Context(), "member", map[string]string{"EXAMPLE": "old"}); err != nil {
					t.Fatal(err)
				}
				fault := &faultTransport{base: http.DefaultTransport, status: status, match: func(r *http.Request) bool {
					return r.Method == http.MethodPut && (strings.HasPrefix(r.URL.Path, "/user-data/") || strings.HasSuffix(r.URL.Path, "/autoscaling"))
				}}
				b := hardeningBackend(t, server, fault)
				var err error
				if action == "environment" {
					err = b.SetEnvVars(t.Context(), "member", map[string]string{"EXAMPLE": "new"})
				} else {
					_, err = b.SetResources(t.Context(), "member", infra.Resources{MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1})
				}
				if !fault.fired || fault.calls != 1 || !errors.Is(err, infra.ErrUnavailable) {
					t.Fatalf("lost committed configuration replayed or hidden: calls=%d fired=%v err=%v", fault.calls, fault.fired, err)
				}
				if action == "environment" {
					entries, err := plain.client.Env(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					found := false
					for _, entry := range entries {
						found = found || entry.Key == "EXAMPLE" && entry.Content == "new"
					}
					if !found {
						t.Fatal("fault did not follow the committed environment update")
					}
				}
			})
		}
	}
}

// The original import/lifecycle regressions cover reset-after-commit. Retain the
// separate HTTP 500 boundary because it traverses the HTTP retry classifier.
func TestBoundaryImportAndLifecycleLost500(t *testing.T) {
	for _, action := range []string{"import", "restart", "start", "stop"} {
		t.Run(action, func(t *testing.T) {
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			if action != "import" {
				importHardeningRuntime(t, plain)
				if action == "start" {
					op, err := plain.Stop(t.Context(), "member")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := plain.OpStatus(t.Context(), op); err != nil {
						t.Fatal(err)
					}
				}
			}
			fault := &faultTransport{base: http.DefaultTransport, status: http.StatusInternalServerError, match: func(r *http.Request) bool {
				return r.Method != http.MethodGet && strings.HasSuffix(r.URL.Path, "/"+action)
			}}
			b := hardeningBackend(t, server, fault)
			invoke := func(b *Backend) (infra.Op, error) {
				if action == "import" {
					return b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Vars: map[string]string{"KARAKORAM_OWNER": "boundary-owner"}}})
				}
				return b.lifecycle(t.Context(), "member", action)
			}
			op, err := invoke(b)
			if action == "start" || action == "stop" {
				receipt, receiptErr := b.receipt(t.Context(), op.ServiceID)
				if err != nil || receiptErr != nil || receipt.Action != "" || !fault.fired || fault.calls != 1 {
					t.Fatalf("committed desired state not recovered: op=%+v err=%v receipt=%+v receiptErr=%v calls=%d", op, err, receipt, receiptErr, fault.calls)
				}
				return
			}
			recovered := hardeningBackend(t, server, fault)
			retry, retryErr := invoke(recovered)
			if !fault.fired || fault.calls != 1 {
				t.Fatalf("expected exactly one committed mutation: fired=%v calls=%d", fault.fired, fault.calls)
			}
			if action == "restart" {
				if !errors.Is(err, infra.ErrConflict) || !errors.Is(retryErr, infra.ErrConflict) {
					t.Fatalf("ambiguous restart lost durable fence: %v / %v", err, retryErr)
				}
				return
			}
			if err != nil || retryErr != nil {
				t.Fatalf("committed desired state not recovered: %v / %v", err, retryErr)
			}
			if action == "import" && (op.Services["member"] == "" || retry.Services["member"] != op.Services["member"]) {
				t.Fatalf("import changed immutable identity: %+v / %+v", op, retry)
			}
		})
	}
}

// Assert the policy on the actual deployment request as well as the renderer's
// goldens: stateless frontends can overlap, durable writers remain stop-first.
func TestBoundaryDeployCarriesWriterFence(t *testing.T) {
	for _, volume := range []string{"", "memberv"} {
		t.Run("volume="+volume, func(t *testing.T) {
			server := newFakeAPI(t)
			deploys := 0
			b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy") {
					deploys++
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatal(err)
					}
					r.Body.Close()
					var body map[string]string
					if err := json.Unmarshal(data, &body); err != nil {
						t.Fatal(err)
					}
					want := "temporaryShutdown: false"
					if volume != "" {
						want = "temporaryShutdown: true"
					}
					if !strings.Contains(body["zeropsYaml"], want) || !strings.Contains(body["zeropsYaml"], "readinessCheck:") {
						t.Fatalf("deployment lost writer/readiness policy: %s", body["zeropsYaml"])
					}
					r.Body = io.NopCloser(strings.NewReader(string(data)))
				}
				return http.DefaultTransport.RoundTrip(r)
			}))
			importHardeningRuntime(t, b)
			d := hardeningDeployment()
			d.Runtime.Volume = volume
			d.Runtime.StopBeforeStart = true
			d.Runtime.Readiness = &infra.Readiness{Args: []string{"agent", "readiness"}, FailureTimeout: time.Minute, RetryPeriod: time.Second}
			if _, err := b.Deploy(t.Context(), "member", d); err != nil {
				t.Fatal(err)
			}
			if deploys != 1 {
				t.Fatalf("expected one deployment request, got %d", deploys)
			}
		})
	}
}
