// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/relay"
)

type relayProxyAgents struct {
	engine.AgentDialer
	proxy *agentapi.Client
}

func (a relayProxyAgents) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if member == "demopx" {
		return a.proxy, nil
	}
	return a.AgentDialer.Dial(ctx, ref, member)
}

func TestProxyReadinessAndDrainThroughRealRelay(t *testing.T) {
	for _, mode := range []string{"single-ipv4", "single-ipv6", "single-task-dns", "two-delayed-ack"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			caPEM, caKey, err := relay.GenerateCA(time.Now())
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caPEM, caKey)
			if err != nil {
				t.Fatal(err)
			}
			certPEM, keyPEM, err := ca.Issue(pki.CertRequest{CommonName: "demopx", DNSNames: []string{"demopx"}, Server: true})
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			a := newProxyInstanceFixtureTLS(t, s.ConfigVersion, &tls.Config{Certificates: []tls.Certificate{leaf}})
			late := newProxyInstanceFixtureTLS(t, s.ConfigVersion, &tls.Config{Certificates: []tls.Certificate{leaf}})
			host := "192.0.2.11"
			if mode == "single-ipv6" {
				host = "2001:db8::11"
			}
			if mode == "single-task-dns" {
				host = "proxy.1.taskid"
			}
			inventory := &proxyInventoryBackend{fakeBackend: backend, instances: []infra.Container{{ID: "a", Hostname: host, Status: infra.ContainerRunning}}}
			if mode == "two-delayed-ack" {
				inventory.instances = append(inventory.instances, infra.Container{ID: "b", Hostname: "proxy.2.taskid", Status: infra.ContainerActive})
			}
			rc.Backend = inventory
			hubCert, hubKey, err := relay.IssueHubCertificate(caPEM, caKey, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			hubPair, err := tls.X509KeyPair(hubCert, hubKey)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(caPEM)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			hub := relay.NewHub(relay.HubOptions{Certificate: hubPair, ClientCAs: roots, Logger: logger,
				Authorize: func(_ context.Context, namespace string, _ *x509.Certificate) error {
					if namespace != "proj-test" {
						return errors.New("wrong namespace")
					}
					return nil
				},
				AuthorizeInstance: func(ctx context.Context, namespace, member, instance string) error {
					if namespace != "proj-test" || member != "demopx" {
						return errors.New("wrong namespace member")
					}
					containers, err := inventory.Containers(ctx, member)
					if err != nil {
						return err
					}
					for _, c := range containers {
						if c.Hostname == instance && (c.Status == infra.ContainerRunning || c.Status == infra.ContainerActive) {
							return nil
						}
					}
					return errors.New("not in member inventory")
				},
			})
			hub.SetPolicy("proj-test", p.RelayTargets(rc.Cluster)) // Actual engine policy contains logical names only.
			server := httptest.NewTLSServer(hub.Handler())
			clientCert, clientKey, err := relay.IssueClientCertificate(caPEM, caKey, "proj-test", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				done <- relay.Run(ctx, relay.Options{URL: server.URL, Namespace: "proj-test", Hostname: "relayhost", CertPEM: clientCert, KeyPEM: clientKey, CAPEM: caPEM, HTTPClient: server.Client(), ListenAgent: "127.0.0.1:0", Logger: logger,
					Resolver: func(_ context.Context, name string) ([]netip.Addr, error) {
						if name == "proxy.2.taskid" {
							return []netip.Addr{netip.MustParseAddr("192.0.2.12")}, nil
						}
						return []netip.Addr{netip.MustParseAddr("192.0.2.11")}, nil // Logical DNS always selects A.
					},
					DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
						selected := a
						if address == "192.0.2.12:9443" {
							selected = late
						}
						return (&net.Dialer{}).DialContext(ctx, network, selected.server.Listener.Addr().String())
					},
				})
			}()
			t.Cleanup(func() {
				cancel()
				hub.Close()
				server.Close()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			deadline := time.Now().Add(3 * time.Second)
			for !hub.Status("proj-test").Connected && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if !hub.Status("proj-test").Connected {
				t.Fatal("real relay did not connect")
			}
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://demopx:9443", Token: "demopx", CAPEM: caPEM,
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return hub.DialContext(ctx, "proj-test", network, addr)
				},
				DialInstanceContext: func(ctx context.Context, network, addr, instance string) (net.Conn, error) {
					return hub.DialInstanceContext(ctx, "proj-test", network, addr, instance)
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			rc.Agents = relayProxyAgents{AgentDialer: rc.Agents, proxy: client}
			if ok, err := ready(t.Context(), rc, s.find("demopx")); err != nil || !ok {
				t.Fatalf("relayed proxy cannot become ready: %v", err)
			}
			retiring := s.find("demop2")
			if _, err := p.retireMember(t.Context(), rc, s, retiring); err != nil {
				t.Fatal(err)
			}
			version := retiring.ProxyDrainVersion
			if mode == "two-delayed-ack" {
				a.applied(version)
			}
			// A control-plane restart must retain exclusion, not one logical ack.
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			retiring = s.find("demop2")
			p = New(p.options)
			for range 2 {
				if _, err := p.retireMember(t.Context(), rc, s, retiring); err != nil {
					t.Fatal(err)
				}
				if backend.services[retiring.Name] == nil {
					t.Fatal("relayed drain accepted a logical/reload acknowledgement")
				}
			}
			a.applied(version)
			late.applied(version)
			if _, err := p.retireMember(t.Context(), rc, s, retiring); err != nil {
				t.Fatal(err)
			}
			if backend.services[retiring.Name] != nil {
				t.Fatal("all relayed instance acknowledgements did not release retirement")
			}
		})
	}
}
