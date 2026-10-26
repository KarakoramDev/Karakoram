// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"errors"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestPublicIPSupportUsesFreshProject(t *testing.T) {
	ipv4 := false
	unavailable := false
	calls := 0
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if unavailable {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if ipv4 {
			_, _ = w.Write([]byte(`{"id":"project","publicIpV4":"192.0.2.1","publicIpV6":"2001:db8::1"}`))
		} else {
			_, _ = w.Write([]byte(`{"id":"project","publicIpV4":null,"publicIpV6":"2001:db8::1"}`))
		}
	})
	if supported, err := b.SupportsPublicIP(t.Context(), "IPV4"); supported || err != nil {
		t.Fatal(supported, err)
	}
	if supported, err := b.SupportsPublicIP(t.Context(), "IPV6"); !supported || err != nil {
		t.Fatal(supported, err)
	}
	ipv4 = true
	if supported, err := b.SupportsPublicIP(t.Context(), "IPV4"); !supported || err != nil {
		t.Fatal("stale capability", supported, err)
	}
	if calls != 3 {
		t.Fatalf("project lookups=%d", calls)
	}
	ipv4 = false
	if supported, err := b.SupportsPublicIP(t.Context(), "IPV4"); supported || err != nil {
		t.Fatal("removed address retained stale positive", supported, err)
	}
	unavailable = true
	if _, err := b.SupportsPublicIP(t.Context(), "IPV4"); err == nil {
		t.Fatal("failed lookup reported definitive capability")
	}
	if !errors.Is(&APIError{StatusCode: 400, Code: "publicIpTypeNotSupported"}, infra.ErrPublicIPTypeUnsupported) {
		t.Fatal("platform refusal lost classification")
	}
}
