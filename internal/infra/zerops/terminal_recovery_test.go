// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package zerops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func terminalFailureBackend(t *testing.T, failures int) (*Backend, func() *Backend, *int, *int) {
	t.Helper()
	server := newFakeAPI(t)
	deploys, uploads := 0, 0
	failedProcesses := map[string]bool{}
	transport := receiptTransport(func(r *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return response, err
		}
		replace := func(value any) {
			response.Body.Close()
			data, _ := json.Marshal(value)
			response.Body = io.NopCloser(strings.NewReader(string(data)))
		}
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/upload") {
			uploads++
		}
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy") {
			deploys++
			var p Process
			_ = json.NewDecoder(response.Body).Decode(&p)
			if deploys <= failures {
				failedProcesses[p.ID] = true
				p.Status = "FAILED"
			}
			replace(p)
		}
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/app-version/") {
			var v AppVersion
			_ = json.NewDecoder(response.Body).Decode(&v)
			if deploys > 0 && deploys <= failures {
				v.Status = "DEPLOY_FAILED"
			}
			replace(v)
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/process") {
			var page page[Process]
			_ = json.NewDecoder(response.Body).Decode(&page)
			for i := range page.List {
				if failedProcesses[page.List[i].ID] {
					page.List[i].Status = "FAILED"
					page.List[i].Error = json.RawMessage(`{"code":"500","message":"Internal Server Error"}`)
				}
			}
			replace(page)
		}
		return response, nil
	})
	fresh := func() *Backend { return hardeningBackend(t, server, transport) }
	b := fresh()
	importHardeningRuntime(t, b)
	return b, fresh, &deploys, &uploads
}

func expireDeploymentRetry(t *testing.T, b *Backend, id string) {
	t.Helper()
	entries, err := b.client.Env(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Key != operationReceiptKey || entry.Content == "" {
			continue
		}
		var value map[string]any
		if err := json.Unmarshal([]byte(entry.Content), &value); err != nil {
			t.Fatal(err)
		}
		value["deployRetryAfter"] = time.Now().Add(-time.Second).UnixNano()
		encoded, _ := json.Marshal(value)
		p, err := b.client.SetUserData(t.Context(), id, entry.ID, operationReceiptKey, string(encoded), false)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.waitOp(t.Context(), processOp(&p, "receipt", id)); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("terminal deployment discarded its exact-version receipt")
}

func TestTerminalDeploymentRetriesSameUploadedVersionWithDurableBackoff(t *testing.T) {
	b, fresh, deploys, uploads := terminalFailureBackend(t, 1)
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed {
		t.Fatalf("fixture did not fail: %+v %v", state, err)
	}
	_, err = fresh().Deploy(t.Context(), "member", hardeningDeployment())
	if !errors.Is(err, infra.ErrConflict) || infra.RetryAt(err).IsZero() || *deploys != 1 {
		t.Fatalf("terminal retry has no durable backoff: %v deploys=%d", err, *deploys)
	}
	expireDeploymentRetry(t, b, op.ServiceID)
	retry, err := fresh().Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil || retry.ID != op.ID {
		t.Fatalf("retry replaced uploaded version: %+v %v", retry, err)
	}
	state, err = fresh().OpStatus(t.Context(), retry)
	if err != nil || !state.Done || state.Failed || *deploys != 2 || *uploads != 1 {
		t.Fatalf("exact retry did not recover: %+v %v deploys=%d uploads=%d", state, err, *deploys, *uploads)
	}
	versions, err := b.allocationVersions(t.Context(), op.ServiceID)
	if err != nil || len(versions) != 1 {
		t.Fatalf("terminal retry allocated more versions: %d %v", len(versions), err)
	}
}

func TestTerminalDeploymentRetryCapSurvivesBackendRestarts(t *testing.T) {
	b, fresh, deploys, uploads := terminalFailureBackend(t, 100)
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	for budget := range 2 {
		d := func() infra.Deployment {
			d := hardeningDeployment()
			d.RetryEpoch = int64(budget)
			return d
		}
		for attempt := range 3 {
			before := time.Now()
			_, err := fresh().Deploy(t.Context(), "member", d())
			deadline := infra.RetryAt(err)
			delay := (15 * time.Second) << attempt
			if deadline.Before(before.Add(delay)) || deadline.After(time.Now().Add(delay)) {
				t.Fatalf("retry %d has incorrect durable backoff: %v", attempt+1, err)
			}
			if !strings.Contains(infra.SafeDetail(err), "Internal Server Error") {
				t.Fatalf("missing sanitized platform diagnostic: %v", err)
			}
			expireDeploymentRetry(t, b, op.ServiceID)
			if retry, err := fresh().Deploy(t.Context(), "member", d()); err != nil || retry.ID != op.ID {
				t.Fatalf("retry replaced uploaded version: %+v %v", retry, err)
			}
		}
		_, err = fresh().Deploy(t.Context(), "member", d())
		if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || !infra.RetryAt(err).IsZero() {
			t.Fatalf("budget did not become non-retryable: %v", err)
		}
		var exhausted *infra.DeploymentExhaustedError
		if !errors.As(err, &exhausted) || exhausted.Service == "" || exhausted.Version != strings.TrimPrefix(op.ID, "version:") || exhausted.Attempts != 1+3*(budget+1) || !strings.Contains(exhausted.LastError, "Internal Server Error") {
			t.Fatalf("exhaustion lost structured attempt diagnosis: %+v", exhausted)
		}
		requests := 0
		for range 3 {
			recovered := fresh()
			transport := recovered.client.http.Transport
			recovered.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet {
					requests++
				}
				return transport.RoundTrip(r)
			})
			unchanged := d()
			if budget > 0 {
				unchanged.RetryEpoch = 0 // stale callers cannot re-arm again.
			}
			_, err = recovered.Deploy(t.Context(), "member", unchanged)
			if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
				t.Fatal("terminal state lost on reconstruction", err)
			}
		}
		if requests != 0 || *deploys != 1+3*(budget+1) || *uploads != 1 {
			t.Fatalf("terminal state mutated platform: writes=%d deploys=%d uploads=%d", requests, *deploys, *uploads)
		}
		state, err := fresh().OpStatus(t.Context(), op)
		if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || !state.Failed || state.Status != "DeploymentRetriesExhausted" {
			t.Fatalf("terminal observation lost: %+v %v", state, err)
		}
	}
}

func TestTerminalDeploymentRetryCrashAndLostResponseKeepExactAttempt(t *testing.T) {
	for _, boundary := range []string{"pre-send", "lost-response"} {
		t.Run(boundary, func(t *testing.T) {
			b, fresh, deploys, uploads := terminalFailureBackend(t, 1)
			op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.OpStatus(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if _, err = fresh().Deploy(t.Context(), "member", hardeningDeployment()); infra.RetryAt(err).IsZero() {
				t.Fatal("missing retry fence/backoff", err)
			}
			expireDeploymentRetry(t, b, op.ServiceID)
			interrupted := fresh()
			transport := interrupted.client.http.Transport
			interrupted.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err != nil {
					return response, err
				}
				interrupt := boundary == "lost-response" && r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy")
				if boundary == "pre-send" && r.Method != http.MethodGet && strings.Contains(r.URL.Path, "user-data") {
					body, e := r.GetBody()
					if e != nil {
						return response, e
					}
					var entry EnvItem
					_ = json.NewDecoder(body).Decode(&entry)
					body.Close()
					var receipt operationReceipt
					interrupt = json.Unmarshal([]byte(entry.Content), &receipt) == nil && receipt.Stage == "deploying" && receipt.DeployRetries > 0
				}
				if interrupt {
					response.Body.Close()
					if boundary == "pre-send" {
						panic("terminal retry pre-send crash")
					}
					return nil, infra.ErrUnavailable
				}
				return response, nil
			})
			if boundary == "pre-send" {
				func() {
					defer func() {
						if recover() == nil {
							t.Error("pre-send crash boundary not reached")
						}
					}()
					_, _ = interrupted.Deploy(t.Context(), "member", hardeningDeployment())
				}()
				if *deploys != 1 {
					t.Fatal("pre-send crash submitted a retry")
				}
				_, err = fresh().Deploy(t.Context(), "member", hardeningDeployment())
				if !errors.Is(err, infra.ErrConflict) {
					t.Fatalf("unknown retry was replayed before settle evidence: %v", err)
				}
				changed := hardeningDeployment()
				changed.Runtime.Args = []string{"agent", "--changed-during-unknown-retry"}
				_, err = fresh().Deploy(t.Context(), "member", changed)
				if !errors.Is(err, infra.ErrConflict) || *deploys != 1 {
					t.Fatalf("changed intent bypassed the uncertain retry fence: %v deploys=%d", err, *deploys)
				}
				receipt, err := b.receipt(t.Context(), op.ServiceID)
				if err != nil {
					t.Fatal(err)
				}
				receipt.RequestSettleAfter = time.Now().Add(-time.Second).UnixNano()
				receipt.RequestEmptySince = time.Now().Add(-time.Second).UnixNano()
				if err = b.saveReceipt(t.Context(), op.ServiceID, receipt); err != nil {
					t.Fatal(err)
				}
			} else {
				retry, err := interrupted.Deploy(t.Context(), "member", hardeningDeployment())
				if err != nil || retry.ID != op.ID {
					t.Fatalf("lost accepted response discarded version: %+v %v", retry, err)
				}
			}
			retry, err := fresh().Deploy(t.Context(), "member", hardeningDeployment())
			if err != nil || retry.ID != op.ID {
				t.Fatalf("retry reconstruction lost version: %+v %v", retry, err)
			}
			state, err := fresh().OpStatus(t.Context(), retry)
			if err != nil || !state.Done || state.Failed || *deploys != 2 || *uploads != 1 {
				t.Fatalf("ambiguous retry replayed or stuck: %+v %v deploys=%d uploads=%d", state, err, *deploys, *uploads)
			}
		})
	}
}

func TestTerminalDeploymentChangedIntentRequiresTerminalProcessProof(t *testing.T) {
	b, fresh, deploys, _ := terminalFailureBackend(t, 1)
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	changed := hardeningDeployment()
	changed.Runtime.Args = []string{"agent", "--new-runtime-intent"}
	retry, err := fresh().Deploy(t.Context(), "member", changed)
	if err != nil || retry.ID == op.ID || *deploys != 2 {
		t.Fatalf("proven terminal receipt prevented corrected runtime intent: %+v %v", retry, err)
	}
}

func TestTerminalDeploymentMismatchedVersionCannotReleaseReceipt(t *testing.T) {
	b, _, deploys, uploads := terminalFailureBackend(t, 1)
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	transport := b.client.http.Transport
	b.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/app-version/") {
			var v AppVersion
			_ = json.NewDecoder(response.Body).Decode(&v)
			response.Body.Close()
			v.ID = "another-terminal-version"
			data, _ := json.Marshal(v)
			response.Body = io.NopCloser(strings.NewReader(string(data)))
		}
		return response, err
	})
	state, err := b.OpStatus(t.Context(), op)
	if !errors.Is(err, infra.ErrConflict) || state.Done {
		t.Fatalf("foreign terminal outcome released selected deployment: %+v %v", state, err)
	}
	receipt, err := b.receipt(t.Context(), op.ServiceID)
	if err != nil || "version:"+receipt.Version != op.ID || *deploys != 1 || *uploads != 1 {
		t.Fatalf("inconsistent exact-version read consumed the request fence: %+v %v", receipt, err)
	}
}

func TestTerminalDeploymentRecoveryRequiresExactTerminalEvidence(t *testing.T) {
	for _, evidence := range []string{"running", "unknown", "unrelated-running", "missing-version", "wrong-version", "duplicate-process", "listing-unavailable"} {
		t.Run(evidence, func(t *testing.T) {
			b, fresh, deploys, uploads := terminalFailureBackend(t, 1)
			op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
			if err != nil {
				t.Fatal(err)
			}
			transport := b.client.http.Transport
			b.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err != nil || r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/process") {
					return response, err
				}
				var processes page[Process]
				_ = json.NewDecoder(response.Body).Decode(&processes)
				response.Body.Close()
				for i := range processes.List {
					p := &processes.List[i]
					if p.AppVersion == nil || p.AppVersion.ID != strings.TrimPrefix(op.ID, "version:") {
						continue
					}
					switch evidence {
					case "running":
						p.Status = "RUNNING"
					case "unknown":
						p.Status = "UNKNOWN"
					case "missing-version":
						p.AppVersion = nil
					case "wrong-version":
						p.AppVersion.ID = "another-version"
					case "duplicate-process":
						processes.List = append(processes.List, *p)
					}
					break
				}
				if evidence == "unrelated-running" {
					processes.List = append(processes.List, Process{ID: "another-service-process", ServiceStackID: op.ServiceID, Status: "RUNNING", ActionName: "restart"})
				}
				if evidence == "listing-unavailable" {
					response.StatusCode = http.StatusServiceUnavailable
					response.Body = io.NopCloser(strings.NewReader(`{}`))
				} else {
					data, _ := json.Marshal(processes)
					response.Body = io.NopCloser(strings.NewReader(string(data)))
				}
				return response, nil
			})
			_, err = b.Deploy(t.Context(), "member", hardeningDeployment())
			if err == nil || *deploys != 1 || *uploads != 1 {
				t.Fatalf("insufficient terminal proof authorized retry: %v deploys=%d uploads=%d", err, *deploys, *uploads)
			}
			receipt, err := fresh().receipt(t.Context(), op.ServiceID)
			if err != nil || receipt.Version != strings.TrimPrefix(op.ID, "version:") || receipt.DeployRetries != 0 || receipt.Stage != "deploying" {
				t.Fatalf("insufficient evidence changed the durable attempt: %+v %v", receipt, err)
			}
		})
	}
}

func TestTerminalDeploymentLastRetryDoesNotReplayUncertainAttempt(t *testing.T) {
	for _, processStatus := range []string{"RUNNING", "UNKNOWN"} {
		t.Run(processStatus, func(t *testing.T) {
			b, fresh, deploys, uploads := terminalFailureBackend(t, 100)
			op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, err = fresh().Deploy(t.Context(), "member", hardeningDeployment()); infra.RetryAt(err).IsZero() {
					t.Fatal("missing terminal retry backoff", err)
				}
				expireDeploymentRetry(t, b, op.ServiceID)
				if _, err = fresh().Deploy(t.Context(), "member", hardeningDeployment()); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := b.receipt(t.Context(), op.ServiceID)
			if err != nil || receipt.DeployRetries != 3 {
				t.Fatalf("last permitted attempt was not reached: %+v %v", receipt, err)
			}
			// Even arbitrarily old request/deadline values do not authorize
			// replay while the current exact-version process is non-terminal.
			receipt.RequestSettleAfter = time.Now().Add(-time.Hour).UnixNano()
			receipt.RequestEmptySince = time.Now().Add(-time.Hour).UnixNano()
			if err = b.saveReceipt(t.Context(), op.ServiceID, receipt); err != nil {
				t.Fatal(err)
			}
			baseline, err := requestBaseline(receipt)
			if err != nil {
				t.Fatal(err)
			}
			transport := b.client.http.Transport
			uncertain := receiptTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err != nil || r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/process") {
					return response, err
				}
				var processes page[Process]
				_ = json.NewDecoder(response.Body).Decode(&processes)
				response.Body.Close()
				for i := range processes.List {
					p := &processes.List[i]
					if p.AppVersion != nil && p.AppVersion.ID == receipt.Version && !baseline[p.ID] {
						p.Status = processStatus
					}
				}
				data, _ := json.Marshal(processes)
				response.Body = io.NopCloser(strings.NewReader(string(data)))
				return response, nil
			})
			for range 3 {
				recovered := fresh()
				recovered.client.http.Transport = uncertain
				state, err := recovered.OpStatus(t.Context(), op)
				if err != nil || state.Done || state.Failed {
					t.Fatalf("old failed version invented a terminal current attempt: %+v %v", state, err)
				}
				retry, err := recovered.Deploy(t.Context(), "member", hardeningDeployment())
				if err != nil || retry.ID != op.ID || *deploys != 4 || *uploads != 1 {
					t.Fatalf("uncertain last attempt was replayed: %+v %v deploys=%d uploads=%d", retry, err, *deploys, *uploads)
				}
			}
		})
	}
}

func TestTerminalDeploymentRetryClockSteps(t *testing.T) {
	for _, failures := range []int{1, 3} {
		for _, step := range []struct {
			name string
			by   time.Duration
			wait bool
		}{
			{"unchanged", 0, true},
			{"small-backward", -time.Second, true},
			{"large-backward", -time.Hour, false},
			{"forward", time.Hour, false},
		} {
			t.Run(fmt.Sprintf("retry-%d/%s", failures, step.name), func(t *testing.T) {
				b, fresh, deploys, uploads := terminalFailureBackend(t, failures)
				op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
				if err != nil {
					t.Fatal(err)
				}
				for range failures - 1 {
					if _, err = fresh().Deploy(t.Context(), "member", hardeningDeployment()); infra.RetryAt(err).IsZero() {
						t.Fatal("missing fast retry deadline", err)
					}
					expireDeploymentRetry(t, b, op.ServiceID)
					if _, err = fresh().Deploy(t.Context(), "member", hardeningDeployment()); err != nil {
						t.Fatal(err)
					}
				}
				delay := (15 * time.Second) << (failures - 1)
				_, err = b.Deploy(t.Context(), "member", hardeningDeployment())
				deadline := infra.RetryAt(err)
				if deadline.IsZero() {
					t.Fatal("missing durable retry deadline", err)
				}
				// Reconstruct the backend and move only its cadence clock.
				// HTTP request deadlines and process evidence stay unchanged.
				recovered := fresh()
				recovered.retryNow = func() time.Time { return deadline.Add(-delay).Add(step.by) }
				retry, err := recovered.Deploy(t.Context(), "member", hardeningDeployment())
				if step.wait {
					if infra.RetryAt(err) != deadline || *deploys != failures {
						t.Fatalf("ordinary persisted backoff bypassed: %v deploys=%d", err, *deploys)
					}
				} else if err != nil || retry.ID != op.ID || *deploys != failures+1 {
					t.Fatalf("clock step stalled exact-version recovery: %+v %v deploys=%d", retry, err, *deploys)
				}
				if *uploads != 1 {
					t.Fatal("clock recovery uploaded a replacement artifact")
				}
			})
		}
	}
}

func TestTerminalDeploymentClockClampKeepsProcessFence(t *testing.T) {
	for _, processStatus := range []string{"RUNNING", "UNKNOWN"} {
		t.Run(processStatus, func(t *testing.T) {
			b, fresh, deploys, uploads := terminalFailureBackend(t, 1)
			if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); err != nil {
				t.Fatal(err)
			}
			_, err := b.Deploy(t.Context(), "member", hardeningDeployment())
			deadline := infra.RetryAt(err)
			if deadline.IsZero() {
				t.Fatal("missing retry deadline", err)
			}
			recovered := fresh()
			recovered.retryNow = func() time.Time { return deadline.Add(-time.Hour) }
			transport := recovered.client.http.Transport
			recovered.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err != nil || r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/process") {
					return response, err
				}
				var processes page[Process]
				_ = json.NewDecoder(response.Body).Decode(&processes)
				response.Body.Close()
				for i := range processes.List {
					if strings.Contains(strings.ToLower(processes.List[i].ActionName), "deploy") {
						processes.List[i].Status = processStatus
					}
				}
				data, _ := json.Marshal(processes)
				response.Body = io.NopCloser(strings.NewReader(string(data)))
				return response, nil
			})
			if _, err := recovered.Deploy(t.Context(), "member", hardeningDeployment()); err == nil || *deploys != 1 || *uploads != 1 {
				t.Fatalf("clock clamp bypassed %s process fence: %v deploys=%d uploads=%d", processStatus, err, *deploys, *uploads)
			}
		})
	}
}

func TestTerminalDeploymentPersistentRetriesKeepWriterFence(t *testing.T) {
	b, fresh, deploys, uploads := terminalFailureBackend(t, 100)
	deployment := func() infra.Deployment {
		d := hardeningDeployment()
		d.Runtime.Volume = "memberdata"
		d.Runtime.StopBeforeStart = true
		return d
	}
	expected, err := RenderYAML("member", b.Layout(), "karakoram", deployment().Runtime)
	if err != nil || !strings.Contains(expected, "temporaryShutdown: true") {
		t.Fatalf("fixture lacks stop-before-start: %v", err)
	}
	transport := b.client.http.Transport
	submissions := 0
	capture := receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy") {
			var payload map[string]string
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			r.Body = io.NopCloser(strings.NewReader(string(data)))
			if err := json.Unmarshal(data, &payload); err != nil {
				t.Fatal(err)
			}
			if payload["zeropsYaml"] != expected {
				t.Fatal("retry changed persistent volume identity or stop-before-start policy")
			}
			submissions++
		}
		return transport.RoundTrip(r)
	})
	b.client.http.Transport = capture
	op, err := b.Deploy(t.Context(), "member", deployment())
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		recovered := fresh()
		recovered.client.http.Transport = capture
		if _, err := recovered.Deploy(t.Context(), "member", deployment()); infra.RetryAt(err).IsZero() {
			t.Fatal("permanent failure did not schedule recovery", err)
		}
		expireDeploymentRetry(t, recovered, op.ServiceID)
		if retry, err := recovered.Deploy(t.Context(), "member", deployment()); err != nil || retry.ID != op.ID {
			t.Fatalf("persistent retry replaced exact version: %+v %v", retry, err)
		}
	}
	if submissions != 4 || *deploys != 4 || *uploads != 1 {
		t.Fatalf("permanent failure lost same-artifact behavior: submissions=%d deploys=%d uploads=%d", submissions, *deploys, *uploads)
	}
}

func TestTerminalDeploymentLegacyOverBudgetReceiptStopsBeforeSubmission(t *testing.T) {
	b, fresh, deploys, uploads := terminalFailureBackend(t, 100)
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); infra.RetryAt(err).IsZero() {
		t.Fatal("fixture did not schedule retry", err)
	}
	receipt, err := b.receipt(t.Context(), op.ServiceID)
	if err != nil {
		t.Fatal(err)
	}
	receipt.DeployRetries = 4
	receipt.DeployRetryAfter = time.Now().Add(-time.Hour).UnixNano()
	if err := b.saveReceipt(t.Context(), op.ServiceID, receipt); err != nil {
		t.Fatal(err)
	}
	_, err = fresh().Deploy(t.Context(), "member", hardeningDeployment())
	if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || *deploys != 1 || *uploads != 1 {
		t.Fatalf("legacy over-budget retry was submitted: %v deploys=%d uploads=%d", err, *deploys, *uploads)
	}
	var exhausted *infra.DeploymentExhaustedError
	if !errors.As(err, &exhausted) || exhausted.Attempts != 4 {
		t.Fatalf("unsent legacy retry counted as an attempt: %+v", exhausted)
	}
}
