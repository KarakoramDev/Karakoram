// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRetirementFallbackPreservesOriginalAndProbeErrors(t *testing.T) {
	for _, probeFails := range []bool{false, true} {
		p, rc, b := setup(t, "ha")
		s := converge(t, p, rc)
		m := s.find("demop1")
		m.Retirement = "tags"
		if probeFails {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "status rejected", http.StatusConflict)
			}))
			t.Cleanup(server.Close)
			rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
		}
		cause := errors.New("original reload failure")
		ok, err := retirementRuntimeFallback(t.Context(), rc, s, m, cause)
		if ok || !errors.Is(err, cause) || m.Retirement != "tags" || m.RetirementOp != nil {
			t.Fatal("reachable fallback lost its cause or checkpoint", err)
		}
		if probeFails && !strings.Contains(err.Error(), "HTTP 409") {
			t.Fatal("application status failure discarded instead of joined", err)
		}
		for _, action := range b.actions {
			if strings.HasPrefix(action, "runtime-stop:") {
				t.Fatal("application failure selected backend fencing")
			}
		}
	}
}
