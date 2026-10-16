// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestAPIErrorNeutralClasses(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		class  error
	}{
		{401, "", infra.ErrUnauthorized}, {403, "", infra.ErrUnauthorized}, {400, "accessDenied", infra.ErrUnauthorized}, {0, "tokenExpired", infra.ErrUnauthorized},
		{404, "", infra.ErrNotFound}, {400, "serviceStackNotFound", infra.ErrNotFound}, {409, "", infra.ErrConflict},
		{429, "", infra.ErrUnavailable}, {500, "", infra.ErrUnavailable}, {503, "", infra.ErrUnavailable},
		{400, "lastCpuModeChangeTooRecent", infra.ErrResourceChangeCooldown}, {400, "invalidValue", infra.ErrRejected}, {422, "", infra.ErrRejected},
	} {
		t.Run(fmt.Sprintf("%d-%s", tc.status, tc.code), func(t *testing.T) {
			api := &APIError{StatusCode: tc.status, Code: tc.code, Message: "Request rejected"}
			wrapped := fmt.Errorf("operation: %w", api)
			if !errors.Is(wrapped, tc.class) {
				t.Fatalf("%v does not match %v", api, tc.class)
			}
			if tc.code == "lastCpuModeChangeTooRecent" && !errors.Is(wrapped, infra.ErrRejected) {
				t.Fatal("cooldown must also be rejected")
			}
			if infra.SafeDetail(wrapped) != api.Error() {
				t.Fatal("safe detail changed")
			}
		})
	}
}

func TestTransportNeutralClass(t *testing.T) {
	client, err := NewClient(Options{BaseURL: "http://api.example.com", Token: "secret", ProjectID: "project", HTTPClient: &http.Client{Transport: &brokenTransport{}}, MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ProjectInfo(t.Context())
	if !errors.Is(err, infra.ErrUnavailable) || infra.SafeDetail(err) != "network request or platform response failed" || err.Error() != "zerops: network request failed" {
		t.Fatalf("network error: %v (%s)", err, infra.SafeDetail(err))
	}
}

func TestLocalValidationNeutralClass(t *testing.T) {
	b, err := New(Options{BaseURL: "http://api.example.com", Token: "token", ProjectID: "project", MaxRetries: -1, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { _, err := b.GetService(t.Context(), "invalid-hostname"); return err },
		func() error { _, err := b.SetContainerCount(t.Context(), "member", 0, 0); return err },
		func() error {
			_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "member", Kind: infra.KindRuntime, Base: "unknown"}})
			return err
		},
		func() error {
			_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "member", Kind: "unknown"}})
			return err
		},
		func() error { _, err := b.Deploy(t.Context(), "member", infra.Deployment{}); return err },
	} {
		err := call()
		var rejected *infra.RejectedError
		if !errors.Is(err, infra.ErrRejected) || !errors.As(err, &rejected) {
			t.Fatalf("local rejection: %v", err)
		}
	}
}

func TestInvalidResponseNeutralClass(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "invalid JSON") })
	_, err := b.ProjectInfo(t.Context())
	if !errors.Is(err, infra.ErrUnavailable) || infra.SafeDetail(err) != "network request or platform response failed" || !strings.Contains(err.Error(), "invalid JSON response") {
		t.Fatalf("response error: %v", err)
	}
}

func TestClassifiedErrorSafeDetail(t *testing.T) {
	for _, class := range []error{infra.ErrNotFound, infra.ErrConflict, infra.ErrUnavailable} {
		t.Run(class.Error(), func(t *testing.T) {
			cause := errors.New("private https://api.example.com/secret")
			err := &classifiedError{cause: cause, class: class}
			want := class.Error()
			if class == infra.ErrUnavailable {
				want = "network request or platform response failed"
			}
			if !errors.Is(err, class) || !errors.Is(err, cause) || infra.SafeDetail(err) != want {
				t.Fatalf("classified error: %v (%s)", err, infra.SafeDetail(err))
			}
		})
	}
}
