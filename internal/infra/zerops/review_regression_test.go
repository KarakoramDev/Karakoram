// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type receiptTransport func(*http.Request) (*http.Response, error)

func (f receiptTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConcurrentCompletionCannotConsumeSuccessorReceipt(t *testing.T) {
	server := newFakeAPI(t)
	first := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, first)
	old, err := first.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	first.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && r.URL.Path == "/app-version/"+strings.TrimPrefix(old.ID, "version:") {
			close(entered)
			<-release
		}
		return response, err
	})
	observed := make(chan error, 1)
	go func() {
		_, err := first.OpStatus(t.Context(), old)
		observed <- err
	}()
	<-entered
	lost := &faultTransport{base: http.DefaultTransport, match: func(r *http.Request) bool {
		return r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy")
	}}
	successor := hardeningBackend(t, server, lost)
	other := hardeningBackend(t, server, nil)
	if first.operationLock(id) != successor.operationLock(id) || first.operationLock(id) != other.operationLock(id) {
		t.Fatal("reconstructed backends do not share receipt exclusion")
	}
	secondObserved := make(chan error, 1)
	go func() {
		_, err := other.OpStatus(t.Context(), old)
		secondObserved <- err
	}()
	type submitted struct {
		op  infra.Op
		err error
	}
	newOperation := make(chan submitted, 1)
	go func() {
		op, err := successor.Deploy(t.Context(), "member", hardeningDeployment())
		newOperation <- submitted{op, err}
	}()
	select {
	case got := <-newOperation:
		t.Fatalf("successor bypassed completion exclusion: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}
	// The first completion must finish its conditional clear before either a
	// second observer or a successor can enter the receipt transition.
	unblock()
	if err := <-observed; err != nil {
		t.Fatal(err)
	}
	if err := <-secondObserved; err != nil {
		t.Fatal(err)
	}
	got := <-newOperation
	if got.err != nil || got.op.ID == "" || !lost.fired {
		t.Fatalf("lost successor submission not exercised: %+v", got)
	}
	// A delayed terminal observation of the predecessor must not retire the new
	// uncertain version receipt, even through another reconstructed backend.
	recovered := hardeningBackend(t, server, nil)
	if _, err := recovered.OpStatus(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	receipt, err := recovered.receipt(t.Context(), id)
	if err != nil || receipt.Action != "deploy" || receipt.Version == "" || receipt.Stage != "deploying" {
		t.Fatalf("predecessor consumed successor uncertainty: %+v %v", receipt, err)
	}
	retry, err := recovered.Deploy(context.Background(), "member", hardeningDeployment())
	if err != nil || retry.ID != got.op.ID {
		t.Fatalf("successor was duplicated instead of recovered: %+v %+v %v", got.op, retry, err)
	}
	versions, err := getPages[AppVersion](t.Context(), recovered.client, "/service-stack/"+id+"/app-version")
	if err != nil || len(versions) != 2 {
		t.Fatalf("unexpected app-version allocations: %d %v", len(versions), err)
	}
}

func TestReceiptConsumptionChecksFinalReadIdentity(t *testing.T) {
	server := newFakeAPI(t)
	b := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, b)
	old := operationReceipt{Action: "restart", Stage: "submitted", Process: "old"}
	next := operationReceipt{Action: "restart", Stage: "submitted", Process: "next"}
	if err := b.saveReceipt(t.Context(), id, next); err != nil {
		t.Fatal(err)
	}
	if err := b.replaceReceipt(t.Context(), id, operationReceipt{}, &old); err != nil {
		t.Fatal(err)
	}
	got, err := b.receipt(t.Context(), id)
	if err != nil || got != next {
		t.Fatalf("conditional receipt clear erased a different identity: %+v %v", got, err)
	}
}
