// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type faultTransport struct {
	base   http.RoundTripper
	match  func(*http.Request) bool
	fired  bool
	calls  int
	status int
}

func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	matched := f.match(r)
	if matched {
		f.calls++
	}
	resp, err := f.base.RoundTrip(r)
	if err == nil && matched && !f.fired && resp.StatusCode < 300 {
		f.fired = true
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if f.status != 0 {
			resp.StatusCode = f.status
			resp.Body = io.NopCloser(strings.NewReader(`{"error":{"code":"internalServerError"}}`))
			return resp, nil
		}
		return nil, io.ErrUnexpectedEOF
	}
	return resp, err
}
func hardeningBackend(t *testing.T, server *httptest.Server, transport http.RoundTripper) *Backend {
	t.Helper()
	b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", HTTPClient: &http.Client{Transport: transport}, RetryBase: time.Millisecond, PollInterval: time.Millisecond, WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func importHardeningRuntime(t *testing.T, b *Backend) string {
	t.Helper()
	op, err := b.ImportServices(context.Background(), []infra.ServiceSpec{{Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}})
	if err != nil {
		t.Fatal(err)
	}
	return op.Services["member"]
}
func TestFencedDeleteStaleListing(t *testing.T) {
	for _, tc := range []struct {
		name, target, project string
		protected             bool
		want                  error
	}{
		{name: "live", target: "member", project: "project"},
		{name: "renamed", target: "other", project: "project", want: infra.ErrConflict},
		{name: "foreign", target: "member", project: "other", want: infra.ErrConflict},
		{name: "protected", target: "member", project: "project", protected: true, want: infra.ErrRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := false
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/service-stack") {
					_ = json.NewEncoder(w).Encode(page[ServiceStack]{})
					return
				}
				if r.Method == http.MethodDelete {
					deleted = true
					_ = json.NewEncoder(w).Encode(Process{ID: "delete"})
					return
				}
				_ = json.NewEncoder(w).Encode(ServiceStack{ID: "expected", Name: tc.target, ProjectID: tc.project, IsSystem: tc.protected})
			})
			b.client.project = "project"
			op, err := b.DeleteServiceByID(context.Background(), "member", "expected")
			if tc.want != nil {
				if !errors.Is(err, tc.want) || deleted {
					t.Fatalf("unsafe delete: %v deleted=%v", err, deleted)
				}
				return
			}
			if err != nil || !deleted || op.ID == "" {
				t.Fatalf("live target acknowledged without deletion: %+v %v deleted=%v", op, err, deleted)
			}
		})
	}
}
func TestCustomClientTimeoutBound(t *testing.T) {
	supplied := &http.Client{}
	c, err := NewClient(Options{Token: "token", ProjectID: "project", HTTPClient: supplied})
	if err != nil {
		t.Fatal(err)
	}
	if c.http.Timeout <= 0 || c.http.Timeout > 2*time.Minute {
		t.Fatalf("unbounded timeout: %v", c.http.Timeout)
	}
	if supplied.Timeout != 0 {
		t.Fatal("mutated caller client")
	}
}
func TestProcessProducingPUTNoReplayAfterCommit(t *testing.T) {
	for _, tc := range []struct {
		name, action string
		status       int
	}{{"userdata-reset", "userdata", 0}, {"autoscaling-reset", "autoscaling", 0}, {"userdata-500", "userdata", 500}, {"autoscaling-500", "autoscaling", 500}} {
		t.Run(tc.name, func(t *testing.T) {
			action := tc.action
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, plain)
			var entryID string
			if action == "userdata" {
				if err := plain.SetEnvVars(context.Background(), "member", map[string]string{"EXAMPLE": "old"}); err != nil {
					t.Fatal(err)
				}
				entries, err := plain.client.Env(context.Background(), id)
				if err != nil {
					t.Fatal(err)
				}
				entryID = entries[0].ID
			}
			transport := &faultTransport{base: http.DefaultTransport, status: tc.status, match: func(r *http.Request) bool {
				return r.Method == http.MethodPut && (strings.HasSuffix(r.URL.Path, "/autoscaling") || strings.HasPrefix(r.URL.Path, "/user-data/"))
			}}
			b := hardeningBackend(t, server, transport)
			var err error
			if action == "userdata" {
				_, err = b.client.SetUserData(context.Background(), id, entryID, "EXAMPLE", "new", false)
			} else {
				_, err = b.client.Autoscaling(context.Background(), id, map[string]json.RawMessage{})
			}
			if !errors.Is(err, infra.ErrUnavailable) || transport.calls != 1 {
				t.Fatalf("committed mutation replayed: calls=%d err=%v", transport.calls, err)
			}
		})
	}
}
func TestRateLimitCarriesResetWithoutReplay(t *testing.T) {
	for _, header := range []string{"3600", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)} {
		t.Run(header, func(t *testing.T) {
			calls := 0
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Retry-After", header)
				fakeError(w, 429, "rateLimited")
			})
			b.client.retries = 3
			before := time.Now()
			_, err := b.client.GetService(context.Background(), "member")
			if !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("wrong class: %v", err)
			}
			at := infra.RetryAt(err)
			if calls != 1 || at.Before(before.Add(3598*time.Second)) {
				t.Fatalf("lost retry schedule: calls=%d retry=%v err=%v", calls, at, err)
			}
		})
	}
}

func hardeningDeployment() infra.Deployment {
	return infra.Deployment{Artifact: infra.Artifact{Reader: strings.NewReader("archive"), Name: "example", Binary: "karakoram"}, Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}}}
}
func TestDeploymentLostResponseDoesNotDuplicateAfterReconstruction(t *testing.T) {
	for _, tc := range []struct {
		name, stage string
		status      int
	}{{"create-reset", "app-version", 0}, {"upload-reset", "upload", 0}, {"deploy-reset", "deploy", 0}, {"create-500", "app-version", 500}, {"upload-500", "upload", 500}, {"deploy-500", "deploy", 500}} {
		t.Run(tc.name, func(t *testing.T) {
			stage := tc.stage
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, plain)
			transport := &faultTransport{base: http.DefaultTransport, status: tc.status, match: func(r *http.Request) bool {
				return r.Method != http.MethodGet && strings.HasSuffix(r.URL.Path, "/"+stage)
			}}
			b := hardeningBackend(t, server, transport)
			op, err := b.Deploy(context.Background(), "member", hardeningDeployment())
			if !transport.fired {
				t.Fatal("fault was not applied after commit")
			}
			recovered := hardeningBackend(t, server, nil)
			retry, retryErr := recovered.Deploy(context.Background(), "member", hardeningDeployment())
			if stage == "app-version" {
				if !errors.Is(err, infra.ErrConflict) || retryErr != nil || retry.ID == "" {
					t.Fatalf("committed allocation was not adopted: %v / %+v %v", err, retry, retryErr)
				}
				state, e := recovered.OpStatus(context.Background(), retry)
				if e != nil || !state.Done || state.Failed {
					t.Fatalf("adopted allocation incomplete: %+v %v", state, e)
				}
			} else {
				if err != nil || retryErr != nil || op.ID == "" || retry.ID != op.ID {
					t.Fatalf("lost accepted operation: %+v %v / %+v %v", op, err, retry, retryErr)
				}
				state, e := recovered.OpStatus(context.Background(), retry)
				if e != nil || !state.Done || state.Failed {
					t.Fatalf("recovered deployment incomplete: %+v %v", state, e)
				}
			}
			versions, e := getPages[AppVersion](context.Background(), recovered.client, "/service-stack/"+id+"/app-version")
			if e != nil || len(versions) != 1 {
				t.Fatalf("duplicated artifact allocation: %d %v", len(versions), e)
			}
		})
	}
}
func TestRestartLostResponseIsDurablyFenced(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, plain)
	transport := &faultTransport{base: http.DefaultTransport, match: func(r *http.Request) bool {
		return r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/restart")
	}}
	b := hardeningBackend(t, server, transport)
	_, err := b.Restart(context.Background(), "member")
	if !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("unknown restart outcome not surfaced: %v", err)
	}
	recovered := hardeningBackend(t, server, transport)
	_, err = recovered.Restart(context.Background(), "member")
	if !errors.Is(err, infra.ErrConflict) || transport.calls != 1 {
		t.Fatalf("unknown restart replayed after reconstruction: calls=%d err=%v", transport.calls, err)
	}
	s, e := recovered.client.GetService(context.Background(), id)
	if e != nil || s.Status != infra.StatusActive {
		t.Fatalf("fixture did not commit restart: %+v %v", s, e)
	}
}

func TestImportLostResponseRecoversOwnedServices(t *testing.T) {
	server := newFakeAPI(t)
	transport := &faultTransport{base: http.DefaultTransport, match: func(r *http.Request) bool {
		return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/service-stack/import")
	}}
	b := hardeningBackend(t, server, transport)
	specs := []infra.ServiceSpec{{Hostname: "volume", Kind: infra.KindVolume, Vars: map[string]string{"KARAKORAM_OWNER": "example-owner"}}, {Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Vars: map[string]string{"KARAKORAM_OWNER": "example-owner"}}}
	op, err := b.ImportServices(context.Background(), specs)
	if err != nil || len(op.Services) != 2 || op.ID == "" {
		t.Fatalf("lost import provenance: %+v %v", op, err)
	}
	recovered := hardeningBackend(t, server, transport)
	retry, err := recovered.ImportServices(context.Background(), specs)
	if err != nil || retry.Services["volume"] != op.Services["volume"] || retry.Services["member"] != op.Services["member"] || transport.calls != 1 {
		t.Fatalf("import replayed or identities lost: %+v %v calls=%d", retry, err, transport.calls)
	}
	state, err := recovered.OpStatus(context.Background(), retry)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("recovered import incomplete: %+v %v", state, err)
	}
}

func TestRuntimeOnlyRestoreRetainsVolumeIdentity(t *testing.T) {
	server := newFakeAPI(t)
	b := hardeningBackend(t, server, nil)
	specs := []infra.ServiceSpec{{Hostname: "volume", Kind: infra.KindVolume}, {Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Volume: "volume"}}
	op, err := b.ImportServices(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	volumeID := op.Services["volume"]
	if _, err = b.client.DeleteService(context.Background(), op.Services["member"]); err != nil {
		t.Fatal(err)
	}
	b = hardeningBackend(t, server, nil)
	if service, e := b.GetService(context.Background(), "member"); e != nil || service != nil {
		t.Fatalf("missing runtime not confirmed: %+v %v", service, e)
	}
	replacement, err := b.ImportServices(context.Background(), specs[1:])
	if err != nil || replacement.Services["member"] == op.Services["member"] {
		t.Fatalf("runtime not replaced: %+v %v", replacement, err)
	}
	d := hardeningDeployment()
	d.Runtime.Volume = "volume"
	deployment, err := b.Deploy(context.Background(), "member", d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(context.Background(), deployment); err != nil {
		t.Fatal(err)
	}
	volume, err := b.GetService(context.Background(), "volume")
	list, listErr := b.ListServices(context.Background(), "")
	if err != nil || listErr != nil || volume.ID != volumeID || len(list) != 2 {
		t.Fatalf("retained volume recreated: %+v %v %v count=%d", volume, err, listErr, len(list))
	}
}

func TestStartStopLostResponseObservesCommittedState(t *testing.T) {
	for _, action := range []string{"start", "stop"} {
		t.Run(action, func(t *testing.T) {
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, plain)
			if action == "start" {
				if _, err := plain.client.Lifecycle(context.Background(), id, "stop"); err != nil {
					t.Fatal(err)
				}
			}
			transport := &faultTransport{base: http.DefaultTransport, match: func(r *http.Request) bool {
				return r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/"+action)
			}}
			b := hardeningBackend(t, server, transport)
			op, err := b.lifecycle(context.Background(), "member", action)
			if err != nil || op.ID != "" || transport.calls != 1 {
				t.Fatalf("desired state was not recovered: %+v %v calls=%d", op, err, transport.calls)
			}
			receipt, err := b.receipt(context.Background(), id)
			if err != nil || receipt.Action != "" {
				t.Fatalf("completed state retains fence: %+v %v", receipt, err)
			}
		})
	}
}

func TestDeploymentReceiptRejectsChangedIntent(t *testing.T) {
	server := newFakeAPI(t)
	b := hardeningBackend(t, server, nil)
	importHardeningRuntime(t, b)
	if _, err := b.Deploy(context.Background(), "member", hardeningDeployment()); err != nil {
		t.Fatal(err)
	}
	different := hardeningDeployment()
	different.Artifact.Reader = strings.NewReader("different artifact")
	if _, err := b.Deploy(context.Background(), "member", different); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("different intent adopted old operation: %v", err)
	}
}

func TestReceiptWriteLostResponseRecoversWithoutReplay(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	importHardeningRuntime(t, plain)
	transport := &faultTransport{base: http.DefaultTransport, match: func(r *http.Request) bool {
		return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/user-data")
	}}
	b := hardeningBackend(t, server, transport)
	op, err := b.Deploy(context.Background(), "member", hardeningDeployment())
	if err != nil || op.ID == "" || !transport.fired || transport.calls != 1 {
		t.Fatalf("receipt response loss blocked or replayed deployment: %+v %v calls=%d", op, err, transport.calls)
	}
}

type processExpiryTransport struct{ id string }

func (f processExpiryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/process/"+f.id {
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"processNotFound"}}`)), Request: r}, nil
	}
	return http.DefaultTransport.RoundTrip(r)
}
func TestExpiredReceiptsObserveWithoutDuplicateSubmission(t *testing.T) {
	for _, action := range []string{"deploy", "restart"} {
		t.Run(action, func(t *testing.T) {
			server := newFakeAPI(t)
			b := hardeningBackend(t, server, nil)
			importHardeningRuntime(t, b)
			var op infra.Op
			var err error
			if action == "deploy" {
				op, err = b.Deploy(context.Background(), "member", hardeningDeployment())
				// Upgrade/restart recovery must still drain a process handle
				// persisted by older controllers, even though new deployments
				// return app-version handles directly.
				if err == nil {
					receipt, e := b.receipt(t.Context(), op.ServiceID)
					if e != nil {
						t.Fatal(e)
					}
					op.ID = "expired-deploy"
					receipt.Stage, receipt.Process = "submitted", op.ID
					if e := b.saveReceipt(t.Context(), op.ServiceID, receipt); e != nil {
						t.Fatal(e)
					}
				}
			} else {
				op, err = b.Restart(context.Background(), "member")
			}
			if err != nil {
				t.Fatal(err)
			}
			recovered := hardeningBackend(t, server, processExpiryTransport{id: op.ID})
			state, err := recovered.OpStatus(context.Background(), op)
			if err != nil {
				t.Fatal(err)
			}
			if action == "deploy" {
				if !state.Done || state.Failed {
					t.Fatalf("known version lost after process expiry: %+v", state)
				}
			} else if state.Done || state.Status != "OUTCOME_UNKNOWN" || !strings.Contains(state.Message, "automatic replay refused") {
				t.Fatalf("expired restart permits unsafe replay: %+v", state)
			}
		})
	}
}

type rejectedUploadTransport struct{ rejected bool }

func (f *rejectedUploadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !f.rejected && strings.HasSuffix(r.URL.Path, "/upload") {
		f.rejected = true
		return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"invalidArtifact"}}`)), Request: r}, nil
	}
	return http.DefaultTransport.RoundTrip(r)
}
func TestRejectedUploadResumesOriginalVersion(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, plain)
	transport := &rejectedUploadTransport{}
	b := hardeningBackend(t, server, transport)
	if _, err := b.Deploy(context.Background(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrRejected) {
		t.Fatalf("explicit rejection misclassified: %v", err)
	}
	recovered := hardeningBackend(t, server, transport)
	if op, err := recovered.Deploy(context.Background(), "member", hardeningDeployment()); err != nil || op.ID == "" {
		t.Fatalf("rejected upload did not resume: %+v %v", op, err)
	}
	versions, err := getPages[AppVersion](context.Background(), recovered.client, "/service-stack/"+id+"/app-version")
	if err != nil || len(versions) != 1 {
		t.Fatalf("rejection allocated another version: %d %v", len(versions), err)
	}
}
