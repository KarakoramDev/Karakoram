// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type allocationFault struct {
	base     http.RoundTripper
	commit   bool
	calls    int
	evidence string
	late     bool
	timeout  bool
}

func (f *allocationFault) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/process") {
		if f.evidence == "running" {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"list":[{"id":"busy","status":"RUNNING"}],"total":1}`)), Header: make(http.Header)}, nil
		}
		if f.evidence == "process-error" {
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		}
	}
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/app-version") && f.evidence == "list-error" {
		return &http.Response{StatusCode: 429, Body: io.NopCloser(strings.NewReader(`{}`)), Header: http.Header{"Retry-After": []string{"8"}}}, nil
	}
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/app-version") {
		f.calls++
		if f.calls == 1 {
			if !f.commit {
				if f.timeout {
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				return nil, io.ErrUnexpectedEOF
			}
			response, err := f.base.RoundTrip(r)
			if err != nil {
				return response, err
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if f.timeout {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return nil, io.ErrUnexpectedEOF
		}
		if f.late {
			response, err := f.base.RoundTrip(r)
			if err != nil {
				return response, err
			}
			late := r.Clone(r.Context())
			late.Body, err = r.GetBody()
			if err != nil {
				response.Body.Close()
				return nil, err
			}
			duplicate, err := f.base.RoundTrip(late)
			if err != nil {
				response.Body.Close()
				return nil, err
			}
			_, _ = io.Copy(io.Discard, duplicate.Body)
			duplicate.Body.Close()
			return response, nil
		}
	}
	return f.base.RoundTrip(r)
}

func TestAllocationRecoveryAfterLostResponse(t *testing.T) {
	for _, tc := range []struct {
		name            string
		commit, timeout bool
	}{
		{name: "not-committed"},
		{name: "committed", commit: true},
		{name: "timeout-not-committed", timeout: true},
		{name: "timeout-committed", commit: true, timeout: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commit := tc.commit
			server := newFakeAPI(t)
			id := importHardeningRuntime(t, hardeningBackend(t, server, nil))
			fault := &allocationFault{base: http.DefaultTransport, commit: commit, timeout: tc.timeout}
			backend := func() *Backend {
				b := hardeningBackend(t, server, fault)
				b.client.http.Timeout = 100 * time.Millisecond
				b.client.retries = 0
				b.waitTimeout = 100 * time.Millisecond
				return b
			}
			b := backend()
			_, err := b.Deploy(context.Background(), "member", hardeningDeployment())
			if !errors.Is(err, infra.ErrConflict) {
				t.Fatalf("lost allocation not fenced: %v", err)
			}
			recovered := backend()
			op, err := recovered.Deploy(context.Background(), "member", hardeningDeployment())
			if !commit {
				if !errors.Is(err, infra.ErrConflict) || !strings.Contains(infra.SafeDetail(err), "settle") || infra.RetryAt(err).IsZero() {
					t.Fatalf("missing settle diagnostic/deadline: %v", err)
				}
				time.Sleep(350 * time.Millisecond)
				// A second reconstruction must retain the first negative observation.
				recovered = backend()
				op, err = recovered.Deploy(context.Background(), "member", hardeningDeployment())
			}
			if err != nil || op.ID == "" {
				t.Fatalf("allocation did not recover automatically: %+v %v", op, err)
			}
			state, err := recovered.OpStatus(context.Background(), op)
			if err != nil || !state.Done || state.Failed {
				t.Fatalf("recovered operation: %+v %v", state, err)
			}
			versions, err := getPages[AppVersion](context.Background(), recovered.client, "/service-stack/"+id+"/app-version")
			if err != nil || len(versions) != 1 {
				t.Fatalf("versions=%+v err=%v", versions, err)
			}
			want := 2
			if commit {
				want = 1
			}
			if fault.calls != want {
				t.Fatalf("allocations=%d want=%d", fault.calls, want)
			}
			receipt, err := recovered.receipt(context.Background(), id)
			if err != nil || receipt.Action != "" {
				t.Fatalf("receipt not consumed: %+v %v", receipt, err)
			}
		})
	}
}

func TestAllocationRecoveryInconclusiveEvidence(t *testing.T) {
	for _, evidence := range []string{"running", "list-error", "process-error"} {
		t.Run(evidence, func(t *testing.T) {
			server := newFakeAPI(t)
			importHardeningRuntime(t, hardeningBackend(t, server, nil))
			fault := &allocationFault{base: http.DefaultTransport}
			b := hardeningBackend(t, server, fault)
			b.client.retries = 0
			_, err := b.Deploy(context.Background(), "member", hardeningDeployment())
			if !errors.Is(err, infra.ErrConflict) {
				t.Fatal(err)
			}
			fault.evidence = evidence
			_, err = b.Deploy(context.Background(), "member", hardeningDeployment())
			diagnostic := "process evidence"
			if evidence == "running" {
				diagnostic = "non-terminal process"
			}
			if evidence == "list-error" {
				diagnostic = "app-version evidence"
			}
			if err == nil || !strings.Contains(infra.SafeDetail(err), diagnostic) || !strings.Contains(infra.SafeDetail(err), "automatic replay refused") || fault.calls != 1 {
				t.Fatalf("inconclusive evidence replayed or unexplained: calls=%d err=%v", fault.calls, err)
			}
			if evidence == "list-error" && infra.RetryAt(err).IsZero() {
				t.Fatal("lost evidence Retry-After")
			}
		})
	}
}

func TestAllocationRecoveryLegacyReceipt(t *testing.T) {
	server := newFakeAPI(t)
	b := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, b)
	// Old receipts have no allocation inventory or delivery deadline. They may
	// recover empty inventories, but cannot attribute an existing version.
	receipt := operationReceipt{Action: "deploy", Stage: "creating"}
	// Obtain the exact fingerprint from an ordinary interrupted request instead
	// of duplicating its artifact/runtime hashing in the fixture.
	fault := &allocationFault{base: http.DefaultTransport}
	b = hardeningBackend(t, server, fault)
	_, err := b.Deploy(context.Background(), "member", hardeningDeployment())
	if !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	current, err := b.receipt(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Fingerprint = current.Fingerprint
	if err = b.saveReceipt(context.Background(), id, receipt); err != nil {
		t.Fatal(err)
	}
	_, err = b.client.CreateAppVersion(context.Background(), id, "unattributed")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Deploy(context.Background(), "member", hardeningDeployment())
	if err == nil || !strings.Contains(err.Error(), "legacy allocation receipt") {
		t.Fatalf("legacy version adopted without identity: %v", err)
	}
}

func TestAllocationRecoveryLateOriginalStaysInert(t *testing.T) {
	server := newFakeAPI(t)
	id := importHardeningRuntime(t, hardeningBackend(t, server, nil))
	fault := &allocationFault{base: http.DefaultTransport, late: true}
	backend := func() *Backend {
		b := hardeningBackend(t, server, fault)
		b.client.http.Timeout = 100 * time.Millisecond
		b.waitTimeout = 100 * time.Millisecond
		return b
	}
	b := backend()
	_, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	_, err = b.Deploy(t.Context(), "member", hardeningDeployment())
	if !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	recovered := backend()
	_, err = recovered.Deploy(t.Context(), "member", hardeningDeployment())
	if err == nil || !strings.Contains(err.Error(), "waiting for previous platform deployment to finish") {
		t.Fatalf("unselected non-terminal version did not fence upload: %v", err)
	}
	selected, err := recovered.receipt(t.Context(), id)
	if err != nil || selected.Version == "" || selected.Stage != "upload-ready" {
		t.Fatalf("selected allocation lost while waiting: %+v %v", selected, err)
	}
	versions, err := getPages[AppVersion](t.Context(), recovered.client, "/service-stack/"+id+"/app-version")
	active, inert := 0, 0
	for _, v := range versions {
		if v.Status == "ACTIVE" {
			active++
		}
		if v.Status == "UPLOADING" {
			inert++
		}
	}
	if err != nil || active != 0 || inert != 2 || fault.calls != 2 {
		t.Fatalf("active=%d inert=%d allocations=%d err=%v", active, inert, fault.calls, err)
	}
}

func TestAllocationRecoveryUsesExactInventory(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "existing-version"
		if ambiguous {
			name = "multiple-candidates"
		}
		t.Run(name, func(t *testing.T) {
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, plain)
			old, err := plain.client.CreateAppVersion(t.Context(), id, "older")
			if err != nil {
				t.Fatal(err)
			}
			// The baseline is a terminal previous version, not an unfinished
			// upload that the admission guard must now leave untouched.
			if err = plain.client.Upload(t.Context(), old.ID, strings.NewReader("archive"), 7); err != nil {
				t.Fatal(err)
			}
			runtimeYAML, err := RenderYAML("member", plain.Layout(), "karakoram", hardeningDeployment().Runtime)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = plain.client.Deploy(t.Context(), old.ID, runtimeYAML, "member"); err != nil {
				t.Fatal(err)
			}
			fault := &allocationFault{base: http.DefaultTransport, commit: true}
			b := hardeningBackend(t, server, fault)
			_, err = b.Deploy(t.Context(), "member", hardeningDeployment())
			if !errors.Is(err, infra.ErrConflict) {
				t.Fatal(err)
			}
			if ambiguous {
				if _, err = plain.client.CreateAppVersion(t.Context(), id, "other-writer"); err != nil {
					t.Fatal(err)
				}
			}
			op, err := hardeningBackend(t, server, fault).Deploy(t.Context(), "member", hardeningDeployment())
			if ambiguous {
				if err == nil || !strings.Contains(err.Error(), "multiple unselected app versions") || fault.calls != 1 {
					t.Fatalf("ambiguous identity adopted/replayed: %+v %v", op, err)
				}
			} else if err != nil || op.ID == "version:"+old.ID || op.ID == "" || fault.calls != 1 {
				t.Fatalf("baseline version adopted or committed allocation lost: %+v %v", op, err)
			}
		})
	}
}

func TestAllocationRecoveryAdoptsVersionAppearingAfterNegativePoll(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, plain)
	fault := &allocationFault{base: http.DefaultTransport}
	b := hardeningBackend(t, server, fault)
	if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	late, err := plain.client.CreateAppVersion(t.Context(), id, "delayed-original")
	if err != nil {
		t.Fatal(err)
	}
	recovered := hardeningBackend(t, server, fault)
	op, err := recovered.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil || op.ID != "version:"+late.ID || fault.calls != 1 {
		t.Fatalf("late committed allocation was not adopted: %+v %v calls=%d", op, err, fault.calls)
	}
	state, err := recovered.OpStatus(t.Context(), op)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("late committed allocation did not finish: %+v %v", state, err)
	}
}

func TestAllocationRecoveryLegacyEmptyReceipt(t *testing.T) {
	server := newFakeAPI(t)
	id := importHardeningRuntime(t, hardeningBackend(t, server, nil))
	fault := &allocationFault{base: http.DefaultTransport}
	backend := func() *Backend {
		b := hardeningBackend(t, server, fault)
		b.client.http.Timeout = 100 * time.Millisecond
		return b
	}
	b := backend()
	if _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrConflict) {
		t.Fatal(err)
	}
	current, err := b.receipt(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	legacy := operationReceipt{Action: "deploy", Stage: "creating", Fingerprint: current.Fingerprint}
	if err = b.saveReceipt(t.Context(), id, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err = backend().Deploy(t.Context(), "member", hardeningDeployment()); !errors.Is(err, infra.ErrConflict) || infra.RetryAt(err).IsZero() {
		t.Fatalf("legacy empty allocation did not begin observation: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	recovered := backend()
	op, err := recovered.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil || op.ID == "" || fault.calls != 2 {
		t.Fatalf("legacy empty allocation did not recover: %+v %v calls=%d", op, err, fault.calls)
	}
}
