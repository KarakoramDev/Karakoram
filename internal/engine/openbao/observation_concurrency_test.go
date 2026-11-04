// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestObserveMembersOverlapsReadsAndWithdrawsFailedHealth(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "one-failed"}[failed], func(t *testing.T) {
			p, rc, _, agents := setup(t)
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
			s := state(t, rc)
			s.Phase = "ready"
			for range 3 {
				m := s.add(rc.Cluster.Name)
				m.Stage = "ready"
				m.Observed, m.Healthy, m.Active, m.Standby = true, true, true, true
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			arrived := make(chan string, 3)
			release := make(chan struct{})
			var releaseOnce sync.Once
			for i, m := range s.Members {
				name, primary, fail := m.Name, i == 0, failed && i == 2
				certificate, err := memberCertificate(t.Context(), rc, "tls/members/"+name+".pem")
				if err != nil {
					t.Fatal(err)
				}
				pair, err := tls.X509KeyPair([]byte(certificate.CertPEM), []byte(certificate.KeyPEM))
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch r.URL.Path {
					case "/v1/sys/seal-status":
						arrived <- name
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
						if fail {
							http.Error(w, "observation unavailable", http.StatusServiceUnavailable)
							return
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"initialized": true, "sealed": false, "version": "2.7.1"})
					case "/v1/sys/leader":
						_ = json.NewEncoder(w).Encode(map[string]any{"is_self": primary, "ha_enabled": true, "leader_address": "https://demom0:8200"})
					default:
						http.NotFound(w, r)
					}
				}))
				server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
				server.StartTLS()
				t.Cleanup(server.Close)
				agents.mu.Lock()
				agents.nativeServers[name] = server
				agents.mu.Unlock()
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				p.observeMembers(t.Context(), rc, s)
			}()
			defer func() {
				releaseOnce.Do(func() { close(release) })
				<-done
			}()
			deadline := time.After(2 * time.Second)
			for range 3 {
				select {
				case <-arrived:
				case <-deadline:
					t.Fatal("member seal observations were serialized rather than overlapping")
				}
			}
			releaseOnce.Do(func() { close(release) })
			<-done
			for i, m := range s.Members {
				if failed && i == 2 {
					if m.Observed || m.Healthy || m.Active || m.Standby {
						t.Fatal("failed member reused positive native health", m.Name)
					}
					continue
				}
				if !m.Observed || !m.Initialized || m.Sealed || m.Active != (i == 0) || m.Standby != (i != 0) || m.Version != "2.7.1" {
					t.Fatal("fresh native member observation was not reduced correctly", m.Name)
				}
			}
		})
	}
}
