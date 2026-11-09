// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func proxyControlFixture(t *testing.T, engine, dir string) (*nodeAgent, *servingProxyEngine) {
	t.Helper()
	a, e := proxyServingLatchFixture(t, engine, dir)
	a.o.ProxyServingProbe = true
	a.o.ProxyServingAdmission = true
	a.o.TaskID = "new-task"
	a.o.Token = "private-test-member-credential"
	a.o.Member = "proxy"
	a.c.Member = "proxy"
	return a, e
}

func TestProxyControlAdmissionRequiresAuthenticatedExactProof(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			a, e := proxyControlFixture(t, engine, t.TempDir())
			listener, err := a.serveProxyServing()
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			handler := a.handler()
			unauthenticated := httptest.NewRecorder()
			handler.ServeHTTP(unauthenticated, httptest.NewRequest("POST", "/v1/proxy-serving", strings.NewReader(`{"taskId":"new-task","configVersion":4}`)))
			if unauthenticated.Code != http.StatusUnauthorized || a.proxyAdmitted.Load() {
				t.Fatal("unauthenticated caller admitted task", unauthenticated.Code)
			}
			for _, body := range []string{`{"taskId":"new-task","configVersion":4,"unknown":true}`, `{"taskId":"new-task","configVersion":4} {}`, strings.Repeat("x", 4097)} {
				request := httptest.NewRequest("POST", "/v1/proxy-serving", strings.NewReader(body))
				request.Header.Set("Authorization", "Bearer "+a.o.Token)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusBadRequest || a.proxyAdmitted.Load() {
					t.Fatal("malformed admission request authorized task", response.Code)
				}
			}
			server := httptest.NewTLSServer(handler)
			defer server.Close()
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: a.o.Token, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			proof, err := client.ProxyServing(t.Context())
			if err != nil || !proof.Ready || proof.Admitted || proof.TaskID != "new-task" || proof.Member != "proxy" || proof.ConfigVersion != 4 {
				t.Fatal("wrong native observation", proof, err)
			}
			if err := proxyServingReadiness(t.Context(), a.o.DataDir); err == nil {
				t.Fatal("local native success or CP GET admitted task before explicit authorization")
			}
			for _, request := range []agentapi.ProxyServingAdmission{{TaskID: "old-task", ConfigVersion: 4}, {TaskID: "new-task", ConfigVersion: 3}, {TaskID: "new-task", ConfigVersion: 0}} {
				if err := client.AdmitProxyServing(t.Context(), request); err == nil {
					t.Fatal("stale task/revision admitted", request)
				}
			}
			e.readinessErr = errors.New("no online writer")
			request := agentapi.ProxyServingAdmission{TaskID: "new-task", ConfigVersion: 4}
			if err := client.AdmitProxyServing(t.Context(), request); err == nil {
				t.Fatal("native failure between proof and admit was ignored")
			}
			e.readinessErr = nil
			if err := client.AdmitProxyServing(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := proxyServingReadiness(t.Context(), a.o.DataDir); err != nil {
				t.Fatal("CP authorization did not release health", err)
			}
			e.readinessErr = errors.New("writer later unavailable")
			if err := client.AdmitProxyServing(t.Context(), request); err != nil {
				t.Fatal("lost-reply replay must remain idempotent", err)
			}
			proof, err = client.ProxyServing(t.Context())
			if err != nil || !proof.Admitted || proof.Ready {
				t.Fatal("proof did not separate admission latch and current native health", proof, err)
			}
			server.Close()
			if err := a.applyMu.LockContext(t.Context()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			err = proxyServingReadiness(ctx, a.o.DataDir)
			a.applyMu.Unlock()
			if err != nil {
				t.Fatal("admitted health depends on CP/native/config lock", err)
			}
		})
	}
}

func TestProxyControlAdmissionSurvivesRestartWithoutControlPlane(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir()
			a, _ := proxyControlFixture(t, engine, dir)
			listener, err := a.serveProxyServing()
			if err != nil {
				t.Fatal(err)
			}
			if err := a.admitProxyServing(t.Context(), agentapi.ProxyServingAdmission{TaskID: a.o.TaskID, ConfigVersion: 4}); err != nil {
				t.Fatal(err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, e := proxyControlFixture(t, engine, dir)
			e.readinessErr = errors.New("writer and CP unavailable")
			restarted.c.ConfigVersion = 5
			restarted.acceptedVersion = 4
			listener, err = restarted.serveProxyServing()
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := proxyServingReadiness(t.Context(), dir); err != nil {
				t.Fatal("agent restart invalidated authenticated container admission", err)
			}
			if err := proxyServingShutdownError(restarted.o); err != nil {
				t.Fatal("admitted task cannot shut down gracefully", err)
			}
			if err := restarted.admitProxyServing(t.Context(), agentapi.ProxyServingAdmission{TaskID: a.o.TaskID, ConfigVersion: 4}); err == nil {
				t.Fatal("old revision replay accepted after configuration changed")
			}
		})
	}
}

func TestProxyControlAdmissionRejectsForgedOrCopiedRecord(t *testing.T) {
	for _, condition := range []string{"unsigned", "revision-tamper", "signature-tamper", "different-task", "different-key"} {
		t.Run(condition, func(t *testing.T) {
			a, _ := proxyControlFixture(t, "postgres", t.TempDir())
			record, err := proxyControlRecord(a.o, 4)
			if err != nil {
				t.Fatal(err)
			}
			switch condition {
			case "unsigned":
				record, err = proxyAdmissionForContainer(a.o.DataDir)
			case "revision-tamper":
				record.identity = strings.Replace(record.identity, "v2:4:", "v2:5:", 1)
			case "signature-tamper":
				record.identity = "v2:4:" + strings.Repeat("0", 64) + "\n"
			case "different-task":
				a.o.TaskID = "another-task"
			case "different-key":
				a.o.Token = "rotated-test-credential"
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := record.record(); err != nil {
				t.Fatal(err)
			}
			if admitted, err := proxyControlAdmitted(a.o); err != nil || admitted {
				t.Fatal("forged/copied record authorized task", admitted, err)
			}
			listener, err := a.serveProxyServing()
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := proxyServingReadiness(t.Context(), a.o.DataDir); err == nil {
				t.Fatal("forged/copied record released Docker health")
			}
		})
	}
}

func TestProxyControlAdmissionOldControlPlaneCompatibility(t *testing.T) {
	a, _ := proxyServingLatchFixture(t, "mysql", t.TempDir())
	a.o.ProxyServingProbe = true
	a.o.Member = "proxy"
	a.c.Member = "proxy"
	proof, err := a.proxyServingProof(t.Context())
	if err != nil || proof.Member != "proxy" || proof.TaskID != "" || !proof.Ready {
		t.Fatal("fixed CP cannot preflight a fixed agent launched by an old CP", proof, err)
	}
	if err := a.admitProxyServing(t.Context(), agentapi.ProxyServingAdmission{TaskID: "new-task", ConfigVersion: 4}); err == nil {
		t.Fatal("protocol capability read enabled admission without explicit task opt-in")
	}
	listener, err := a.serveProxyServing()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := proxyServingReadiness(t.Context(), a.o.DataDir); err != nil {
		t.Fatal("old CP without opt-in deadlocked fixed agent", err)
	}
	if err := proxyServingShutdownError(a.o); err != nil {
		t.Fatal("legacy admitted agent cannot exit normally", err)
	}
}

func TestProxyControlAdmissionMissingControlPlaneFailsClosed(t *testing.T) {
	a, _ := proxyControlFixture(t, "postgres", t.TempDir())
	listener, err := a.serveProxyServing()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	for range 3 {
		if err := proxyServingReadiness(context.Background(), a.o.DataDir); err == nil {
			t.Fatal("healthy native process bypassed unavailable CP")
		}
	}
	if err := proxyServingShutdownError(a.o); err == nil {
		t.Fatal("never-admitted task healthcheck termination would exit zero")
	}
}
