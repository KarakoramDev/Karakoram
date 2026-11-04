// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"golang.org/x/net/dns/dnsmessage"
)

func TestCachedWeaviateLifecycleDuringDNSAndControlPlaneOutage(t *testing.T) {
	for _, stopIntent := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-start", true: "stop-without-dns"}[stopIntent], func(t *testing.T) {
			var recovered atomic.Bool
			var published atomic.Pointer[agentapi.MemberConfig]
			dns, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer dns.Close()
			dnsDone := make(chan struct{})
			go func() {
				defer close(dnsDone)
				buffer := make([]byte, 2048)
				for {
					n, from, err := dns.ReadFrom(buffer)
					if err != nil {
						return
					}
					var q dnsmessage.Message
					if q.Unpack(buffer[:n]) != nil {
						continue
					}
					answer := dnsmessage.Message{Header: dnsmessage.Header{ID: q.ID, Response: true, RecursionAvailable: true, RCode: dnsmessage.RCodeNameError}, Questions: q.Questions}
					if recovered.Load() {
						answer.Header.RCode = dnsmessage.RCodeSuccess
						for _, question := range q.Questions {
							if question.Type == dnsmessage.TypeAAAA {
								answer.Answers = append(answer.Answers, dnsmessage.Resource{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeAAAA, Class: dnsmessage.ClassINET, TTL: 0}, Body: &dnsmessage.AAAAResource{AAAA: [16]byte{0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12}}})
							}
						}
					}
					packet, err := answer.Pack()
					if err == nil {
						_, _ = dns.WriteTo(packet, from)
					}
				}
			}()
			oldResolver := net.DefaultResolver
			net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "udp", dns.LocalAddr().String())
			}}
			defer func() { net.DefaultResolver = oldResolver; dns.Close(); <-dnsDone }()
			cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if value := published.Load(); value != nil {
					_ = json.NewEncoder(w).Encode(value)
					return
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer cp.Close()
			root := t.TempDir()
			t.Setenv("KARAKORAM_WEAVIATE_INSTALL_ROOT", root)
			s := testSpec()
			s.BindHost = "::"
			s.TLS = false
			// Member identity is a native node label, not a URL. A single-label private
			// hostname avoids dependence on external resolver search configuration.
			s.NodeName = "dnsmember"
			c := config(s)
			c.Cluster = "test"
			c.Member = s.NodeName
			c.ConfigVersion = 7
			if stopIntent {
				c.DesiredProcesses = []string{}
			} else {
				executable := binary(s.Version)
				if err := os.MkdirAll(filepath.Dir(executable), 0700); err != nil {
					t.Fatal(err)
				}
				script := "#!/bin/sh\nprintf '%s' \"$CLUSTER_ADVERTISE_ADDR\" > advertised\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n"
				if err := os.WriteFile(executable, []byte(script), 0700); err != nil {
					t.Fatal(err)
				}
			}
			ca, err := pki.NewCA("agent lifecycle test", 0)
			if err != nil {
				t.Fatal(err)
			}
			cert, key, err := ca.Issue(pki.CertRequest{CommonName: s.NodeName, DNSNames: []string{s.NodeName}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true})
			if err != nil {
				t.Fatal(err)
			}
			c.AgentTLS = agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(key)}
			dataDir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dataDir, "karakoram"), 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(c)
			if err := os.WriteFile(filepath.Join(dataDir, "karakoram/config.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			listener.Close()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			cpCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cp.Certificate().Raw})
			startAgent := func() {
				go func() {
					done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: string(cpCA), Token: "token", Cluster: c.Cluster, Member: c.Member, DataDir: dataDir, Listen: address, Engine: "weaviate", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, New())
				}()
			}
			startAgent()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("agent shutdown stalled")
				}
			}()
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + address, Token: "token", CAPEM: ca.CertPEM, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			deadline := time.Now().Add(8 * time.Second)
			observedFailure := false
			for {
				status, err := client.Status(t.Context())
				if err == nil {
					if !stopIntent && !recovered.Load() {
						if status.ConfigVersion == c.ConfigVersion {
							t.Fatal("unresolved configuration acknowledged")
						}
						observedFailure = true
						recovered.Store(true)
					}
					if status.ConfigVersion == c.ConfigVersion {
						if stopIntent {
							if status.Processes["weaviate"].PID != 0 {
								t.Fatal("stop intent started database")
							}
							break
						}
						if status.Processes["weaviate"].PID != 0 {
							if !observedFailure {
								t.Fatal("test did not exercise failed cached apply")
							}
							break
						}
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("cached lifecycle failed to converge with CP unavailable", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !stopIntent {
				data, err := os.ReadFile(filepath.Join(dataDir, "advertised"))
				if err != nil || string(data) != "fd00::c" {
					t.Fatalf("native process advertisement=%q err=%v", data, err)
				}
				// Persist a real operator hold, then restart the agent while DNS
				// and the CP are unavailable. Cached apply must preserve the hold
				// and explicit start must prepare the process before releasing it.
				if err := client.Process(t.Context(), agentapi.ProcessAction{Process: "weaviate", Action: "stop"}); err != nil {
					t.Fatal(err)
				}
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("first agent failed to stop")
				}
				recovered.Store(false)
				ctx, cancel = context.WithCancel(t.Context())
				defer cancel()
				done = make(chan error, 1)
				startAgent()
				deadline = time.Now().Add(5 * time.Second)
				for {
					status, err := client.Status(t.Context())
					if err == nil && status.ConfigVersion == c.ConfigVersion {
						if status.Processes["weaviate"].PID != 0 {
							t.Fatal("cached apply released the operator stop hold")
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("restarted held agent did not acknowledge retained config", err)
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err := client.Process(t.Context(), agentapi.ProcessAction{Process: "weaviate", Action: "start"}); err == nil {
					t.Fatal("explicit start admitted without resolving private DNS")
				}
				recovered.Store(true)
				if err := client.Process(t.Context(), agentapi.ProcessAction{Process: "weaviate", Action: "start"}); err != nil {
					t.Fatal("operator stop -> agent restart -> explicit start failed", err)
				}
				status, err := client.Status(t.Context())
				if err != nil || status.Processes["weaviate"].PID == 0 {
					t.Fatal("explicit start did not launch the prepared process", err)
				}
				if err := client.Maintenance(t.Context(), agentapi.Maintenance{Enabled: true}); err != nil {
					t.Fatal(err)
				}
				recovered.Store(false)
				published.Store(&c)
				if err := client.Reload(t.Context()); err != nil {
					t.Fatal("maintenance-period config apply failed", err)
				}
				published.Store(nil)
				// A native exit is not an operator stop: desired-running intent
				// must remain intact while maintenance suppresses auto-restart.
				native, err := os.FindProcess(status.Processes["weaviate"].PID)
				if err != nil {
					t.Fatal(err)
				}
				if err := native.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
				deadline = time.Now().Add(5 * time.Second)
				for {
					observed, err := client.Status(t.Context())
					if err == nil && observed.Processes["weaviate"].PID == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("native process did not exit during maintenance", err)
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err := client.Maintenance(t.Context(), agentapi.Maintenance{Enabled: false}); err != nil {
					t.Fatal("maintenance release failed with CP unavailable", err)
				}
				status, err = client.Status(t.Context())
				if err != nil || status.Processes["weaviate"].PID == 0 {
					t.Fatal("maintenance release did not restart from the retained valid process spec", status.Processes["weaviate"], err)
				}
			}
		})
	}
}
