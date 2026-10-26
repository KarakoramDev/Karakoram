// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestMissingAppVersionClassification(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		absent bool
	}{
		{400, "appVersionNotFound", true}, {404, "appVersionNotFound", true},
		{403, "appVersionNotFound", false}, {500, "appVersionNotFound", false},
		{400, "invalidRequest", false}, {400, "serviceStackNotFound", true},
	} {
		err := &APIError{StatusCode: tc.status, Code: tc.code}
		if got := notFound(err); got != tc.absent {
			t.Errorf("notFound(%+v)=%v want %v", err, got, tc.absent)
		}
		if got := errors.Is(err, infra.ErrNotFound); got != tc.absent {
			t.Errorf("ErrNotFound(%+v)=%v want %v", err, got, tc.absent)
		}
		if tc.absent && errors.Is(err, infra.ErrRejected) {
			t.Errorf("absence also classified as rejection: %v", err)
		}
	}
}

func TestMissingReceiptVersionWaitsForTerminalEvidence(t *testing.T) {
	for _, stage := range []string{"upload-ready", "uploading", "deploy-ready", "deploying", "submitted", "exhausted"} {
		for _, evidence := range []string{"running", "unknown", "unreadable", "truncated"} {
			t.Run(stage+"/"+evidence, func(t *testing.T) {
				server := newFakeAPI(t)
				mutations, reads := 0, 0
				armed := false
				b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
					if armed && r.Method != http.MethodGet {
						mutations++
					}
					if armed && strings.HasSuffix(r.URL.Path, "/process") {
						reads++
						status, body := 200, `{"list":[{"id":"prior","actionName":"stack.deploy","status":"RUNNING"}],"total":1}`
						switch evidence {
						case "unknown":
							body = `{"list":[{"id":"prior","status":"UNKNOWN"}],"total":1}`
						case "unreadable":
							status, body = 503, `{}`
						case "truncated":
							body = `{"list":[],"total":501}`
						}
						return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
					}
					return http.DefaultTransport.RoundTrip(r)
				}))
				id := importHardeningRuntime(t, b)
				receipt := operationReceipt{Action: "deploy", Stage: stage, Version: "missing", Fingerprint: "old-intent", Process: "expired-process"}
				if err := b.saveReceipt(t.Context(), id, receipt); err != nil {
					t.Fatal(err)
				}
				armed = true
				for _, opID := range []string{"version:missing", receipt.Process} {
					state, err := b.OpStatus(t.Context(), infra.Op{ID: opID, Kind: "deploy", ServiceID: id})
					var waiting *infra.DeploymentWaitError
					if state.Done || state.Failed || !errors.As(err, &waiting) || waiting.Reason != "PreviousDeployment" {
						t.Fatalf("incomplete evidence became terminal: state=%+v err=%v", state, err)
					}
				}
				deployment := hardeningDeployment()
				deployment.RetryEpoch = 1
				_, err := b.Deploy(t.Context(), "member", deployment)
				var waiting *infra.DeploymentWaitError
				if !errors.As(err, &waiting) || waiting.Reason != "PreviousDeployment" {
					t.Fatalf("incomplete evidence allowed retry: %v", err)
				}
				got, err := b.receipt(t.Context(), id)
				if err != nil || got != receipt || mutations != 0 || reads == 0 {
					t.Fatalf("wait changed receipt or platform: receipt=%+v err=%v mutations=%d reads=%d", got, err, mutations, reads)
				}
			})
		}
	}
}

func TestMissingReceiptVersionReallocatesWithinSharedBudget(t *testing.T) {
	for _, stage := range []string{"upload-ready", "uploading", "deploy-ready", "deploying", "submitted", "exhausted"} {
		t.Run(stage, func(t *testing.T) {
			server, platform := newFakeAPIWithState(t)
			b := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, b)
			receipt := operationReceipt{Action: "deploy", Stage: stage, Version: "missing", Fingerprint: "old-intent", Process: "expired-process"}
			if err := b.saveReceipt(t.Context(), id, receipt); err != nil {
				t.Fatal(err)
			}
			for retries := 1; retries <= terminalDeploymentRetries; retries++ {
				state, err := b.OpStatus(t.Context(), infra.Op{ID: "version:" + receipt.Version, Kind: "deploy", ServiceID: id})
				if err != nil || !state.Done || !state.Failed || state.Status != "MissingAppVersion" {
					t.Fatalf("positive absence not a failed attempt: %+v %v", state, err)
				}
				op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
				if err != nil || !strings.HasPrefix(op.ID, "version:") {
					t.Fatalf("terminal evidence did not permit replacement: %+v %v", op, err)
				}
				receipt, err = b.receipt(t.Context(), id)
				if err != nil || deploymentRetries(receipt) != retries || receipt.Version == "missing" {
					t.Fatalf("replacement lost retry budget: %+v %v", receipt, err)
				}
				platform.mu.Lock()
				delete(platform.versions, receipt.Version)
				delete(platform.versionStatus, receipt.Version)
				platform.mu.Unlock()
			}
			state, err := b.OpStatus(t.Context(), infra.Op{ID: "version:" + receipt.Version, Kind: "deploy", ServiceID: id})
			if !state.Done || !state.Failed || !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
				t.Fatalf("missing-version retry cap bypassed: %+v %v", state, err)
			}
			for range 2 {
				if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
					t.Fatalf("exhausted deployment replayed: %v", err)
				}
			}
			deployment := hardeningDeployment()
			deployment.RetryEpoch = 1
			op, err := b.Deploy(t.Context(), "member", deployment)
			if err != nil || op.ID == "version:"+receipt.Version {
				t.Fatalf("explicit re-arm did not allocate fresh version: %+v %v", op, err)
			}
			state, err = b.OpStatus(t.Context(), op)
			if err != nil || !state.Done || state.Failed {
				t.Fatalf("re-armed replacement did not converge: %+v %v", state, err)
			}
		})
	}
}

func TestMissingVersionSharesExactVersionRetryBudget(t *testing.T) {
	server, platform := newFakeAPIWithState(t)
	b := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, b)
	receipt := operationReceipt{Action: "deploy", Stage: "deploying", Version: "missing", Fingerprint: "old", DeployRetries: 2, RequestBaseline: `["terminal"]`, DeployRetryAfter: 1}
	if err := b.saveReceipt(t.Context(), id, receipt); err != nil {
		t.Fatal(err)
	}
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = b.receipt(t.Context(), id)
	if err != nil || deploymentRetries(receipt) != 3 {
		t.Fatalf("replacement reset prior exact-version retries: %+v %v", receipt, err)
	}
	platform.mu.Lock()
	delete(platform.versions, receipt.Version)
	delete(platform.versionStatus, receipt.Version)
	platform.mu.Unlock()
	if _, err = b.OpStatus(t.Context(), op); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatalf("mixed exact-version/replacement retries bypassed cap: %v", err)
	}
}

func TestMissingVersionAllocationRejectionPreservesBudget(t *testing.T) {
	server := newFakeAPI(t)
	b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/app-version") {
			return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"allocationRejected"}}`))}, nil
		}
		return http.DefaultTransport.RoundTrip(r)
	}))
	id := importHardeningRuntime(t, b)
	receipt := operationReceipt{Action: "deploy", Stage: "deploying", Version: "missing", PriorRetries: 2}
	if err := b.saveReceipt(t.Context(), id, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("allocation rejection not propagated: %v", err)
	}
	receipt, err := b.receipt(t.Context(), id)
	if err != nil || receipt.Stage != "creating" || deploymentRetries(receipt) != 3 {
		t.Fatalf("rejected allocation reset shared retry budget: %+v %v", receipt, err)
	}
}

func TestMissingVersionCompleteServiceHistoryAllowsReplacement(t *testing.T) {
	for _, history := range []string{"empty", "unrelated-terminal"} {
		for _, evidence := range []string{"complete", "unreadable", "truncated"} {
			t.Run(history+"/"+evidence, func(t *testing.T) {
				server := newFakeAPI(t)
				armed, mutations := false, 0
				b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
					if armed && r.Method != http.MethodGet {
						mutations++
					}
					if armed && strings.HasSuffix(r.URL.Path, "/process") {
						processes := []Process{}
						if history == "unrelated-terminal" {
							processes = append(processes, Process{ID: "unrelated", ActionName: "stack.deploy", Status: "FINISHED", AppVersion: &AppVersion{ID: "another-version"}})
						}
						total, status := len(processes), http.StatusOK
						if evidence == "truncated" {
							total = 501
						}
						if evidence == "unreadable" {
							status = http.StatusServiceUnavailable
						}
						data, _ := json.Marshal(page[Process]{List: processes, Total: total})
						return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(data)))}, nil
					}
					return http.DefaultTransport.RoundTrip(r)
				}))
				id := importHardeningRuntime(t, b)
				receipt := operationReceipt{Action: "deploy", Stage: "deploying", Version: "missing", Process: "expired-process"}
				if err := b.saveReceipt(t.Context(), id, receipt); err != nil {
					t.Fatal(err)
				}
				armed = true
				state, err := b.OpStatus(t.Context(), infra.Op{ID: "version:missing", Kind: "deploy", ServiceID: id})
				if evidence == "complete" {
					// Exact-version history can age out. Positive version absence
					// plus a complete service inventory without non-terminal work
					// authorizes a bounded replacement, not deployment success.
					if err != nil || !state.Done || !state.Failed {
						t.Fatalf("complete %s history did not retire absent version as failed: %+v %v", history, state, err)
					}
					op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
					if err != nil || !strings.HasPrefix(op.ID, "version:") || mutations == 0 {
						t.Fatalf("complete %s history did not allow bounded replacement: %+v %v mutations=%d", history, op, err, mutations)
					}
					got, err := b.receipt(t.Context(), id)
					if err != nil || deploymentRetries(got) != 1 {
						t.Fatalf("replacement bypassed shared budget: %+v %v", got, err)
					}
				} else {
					var waiting *infra.DeploymentWaitError
					if state.Done || state.Failed || !errors.As(err, &waiting) || waiting.Reason != "PreviousDeployment" {
						t.Fatalf("incomplete %s history did not wait: %+v %v", history, state, err)
					}
					_, err := b.Deploy(t.Context(), "member", hardeningDeployment())
					if !errors.As(err, &waiting) || waiting.Reason != "PreviousDeployment" || mutations != 0 {
						t.Fatalf("incomplete history allowed mutation: %v mutations=%d", err, mutations)
					}
				}
			})
		}
	}
}

func TestMissingVersionReplacementTerminalFailureSurvivesRestartAndRearm(t *testing.T) {
	server, platform := newFakeAPIWithState(t)
	b := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, b)
	receipt := operationReceipt{Action: "deploy", Stage: "deploying", Version: "missing"}
	if err := b.saveReceipt(t.Context(), id, receipt); err != nil {
		t.Fatal(err)
	}
	var op infra.Op
	for retry := 1; retry <= terminalDeploymentRetries; retry++ {
		var err error
		op, err = b.Deploy(t.Context(), "member", hardeningDeployment())
		if err != nil {
			t.Fatal(err)
		}
		receipt, err = b.receipt(t.Context(), id)
		if err != nil || receipt.PriorRetries != retry || receipt.DeployRetries != 0 {
			t.Fatalf("replacement did not retain shared count: %+v %v", receipt, err)
		}
		platform.mu.Lock()
		if retry < terminalDeploymentRetries {
			delete(platform.versions, receipt.Version)
			delete(platform.versionStatus, receipt.Version)
		} else {
			platform.versionStatus[receipt.Version] = "DEPLOY_FAILED"
			for processID, process := range platform.processes {
				if process.AppVersion != nil && process.AppVersion.ID == receipt.Version {
					process.Status = "FAILED"
					platform.processes[processID] = process
				}
			}
		}
		platform.mu.Unlock()
	}
	if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatalf("present replacement failure did not exhaust shared budget: %v", err)
	}
	for range 2 {
		receipt, err := b.receipt(t.Context(), id)
		if err != nil || receipt.Stage != "exhausted" || receipt.RequestBaseline == "" || receipt.DeployRetries != 0 || receipt.PriorRetries != 3 {
			t.Fatalf("exhausted replacement receipt unreadable or lost evidence: %+v %v", receipt, err)
		}
		b = hardeningBackend(t, server, nil)
		state, err := b.OpStatus(t.Context(), op)
		if !state.Done || !state.Failed || !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatalf("restart lost durable exhaustion: %+v %v", state, err)
		}
		if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatalf("repeated deployment lost durable exhaustion: %v", err)
		}
	}
	deployment := hardeningDeployment()
	deployment.RetryEpoch = 1
	if _, err := b.Deploy(t.Context(), "member", deployment); infra.RetryAt(err).IsZero() {
		t.Fatalf("re-arm did not schedule exact-version retry with durable backoff: %v", err)
	}
	expireDeploymentRetry(t, b, id)
	deployment = hardeningDeployment()
	deployment.RetryEpoch = 1
	retry, err := b.Deploy(t.Context(), "member", deployment)
	if err != nil || retry.ID != op.ID {
		t.Fatalf("re-arm failed to retry the present replacement: %+v %v", retry, err)
	}
	state, err := b.OpStatus(t.Context(), retry)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("re-armed present replacement did not converge: %+v %v", state, err)
	}
}

func TestMissingVersionFullClusterDeleteAndRerun(t *testing.T) {
	server := newFakeAPI(t)
	mutations := 0
	b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			mutations++
		}
		return http.DefaultTransport.RoundTrip(r)
	}))
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	stateStore, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stateStore.Close() })
	provider := rabbitmq.New(rabbitmq.Options{Store: stateStore})
	cluster := &domain.Cluster{Name: "demo"}
	provider.ApplyDefaults(&cluster.Spec)
	cluster.Internal = json.RawMessage(`{"uid":"original","phase":"creating","members":[{"name":"demom0","volume":"demov0","serviceId":"gone-runtime","volumeId":"gone-volume","stage":"created"}],"pending":{"action":"deploy","member":"demom0","op":{"id":"version:missing","kind":"deploy","serviceId":"gone-runtime"}}}`)
	if err := stateStore.CreateCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	rc := &engine.Context{Cluster: cluster, Store: stateStore, Secrets: stateStore, Backend: b, Clock: &receiptEngineClock{now: time.Now()}}
	// The same persisted pending version survived an earlier failed delete. No
	// runtime or volume remains, so deleting again must retire that checkpoint.
	done := false
	for range 8 {
		done, err = provider.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if done {
			break
		}
	}
	if !done || err != nil {
		t.Fatalf("full delete did not converge: done=%v err=%v state=%s", done, err, cluster.Internal)
	}
	done, err = provider.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if !done || err != nil || mutations != 0 {
		t.Fatalf("repeated delete not absent-only: done=%v err=%v mutations=%d", done, err, mutations)
	}
}
