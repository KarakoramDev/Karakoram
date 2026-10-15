// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

const esStatsJSON = `{"nodes":{"local":{"jvm":{"mem":{"heap_used_in_bytes":123,"heap_max_in_bytes":512},"gc":{"collectors":{"young":{"collection_time_in_millis":1500},"old":{"collection_time_in_millis":250}}}},"indices":{"docs":{"count":11},"store":{"size_in_bytes":22},"indexing":{"index_total":33},"search":{"query_total":44}},"fs":{"total":{"available_in_bytes":55}},"thread_pool":{"write":{"rejected":6},"search":{"rejected":7}}}}}`
const esHealthJSON = `{"status":"yellow","number_of_nodes":3,"active_shards":8,"unassigned_shards":2,"relocating_shards":1}`

func TestMetricsCannedJSON(t *testing.T) {
	tests := []struct {
		component, body string
		status          int
		want            map[string]float64
		counter         []string
	}{
		{"elasticsearch", esStatsJSON, 200, map[string]float64{"cluster_status": 1, "cluster_nodes": 3, "active_shards": 8, "unassigned_shards": 2, "relocating_shards": 1, "jvm_heap_used_bytes": 123, "jvm_heap_max_bytes": 512, "docs": 11, "store_bytes": 22, "indexing_total": 33, "search_query_total": 44, "fs_available_bytes": 55}, []string{"indexing_total", "search_query_total", "gc_collection_seconds_total", "thread_pool_rejected_total"}},
		{"kibana", `{"status":{"overall":{"level":"degraded"}}}`, 503, map[string]float64{"status_level": 1}, nil},
		{"logstash", `{"events":{"in":23,"out":21},"jvm":{"mem":{"heap_used_in_bytes":1234}}}`, 200, map[string]float64{"events_in_total": 23, "events_out_total": 21, "jvm_heap_used_bytes": 1234}, []string{"events_in_total", "events_out_total"}},
		{"apm-server", `{"apm-server":{"server":{"request":{"count":13},"response":{"valid":{"accepted":3}}},"processor":{"stream":{"accepted":57}}}}`, 200, map[string]float64{"requests_total": 13, "events_accepted_total": 57}, []string{"requests_total", "events_accepted_total"}},
	}
	for _, tt := range tests {
		t.Run(tt.component, func(t *testing.T) {
			spec := esSpec()
			spec.Component = tt.component
			spec.Elasticsearch.Monitor = agentspec.Credentials{Username: "monitor", Password: "secret"}
			requests := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				body := tt.body
				if tt.component == "elasticsearch" {
					user, password, ok := req.BasicAuth()
					if !ok || user != "monitor" || password != "secret" {
						t.Fatal("monitor authentication missing")
					}
					if req.URL.Scheme != "https" || req.URL.Host != "127.0.0.1:9200" {
						t.Fatal(req.URL)
					}
					if req.URL.Path == "/_cluster/health" {
						body = esHealthJSON
						if req.URL.RawQuery != "local=true" {
							t.Fatal(req.URL)
						}
					} else if req.URL.Path != "/_nodes/_local/stats/jvm,indices,fs,thread_pool,process" {
						t.Fatal(req.URL)
					}
				} else if req.Header.Get("Authorization") != "" {
					t.Fatal("unexpected component authorization")
				}
				return &http.Response{StatusCode: tt.status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
			})}
			metrics, err := scrape(context.Background(), client, spec)
			if err != nil {
				t.Fatal(err)
			}
			prefix := metricPrefix(tt.component)
			got := map[string]Metric{}
			for _, m := range metrics {
				got[strings.TrimPrefix(m.Name, prefix)] = m
				if m.Help == "" {
					t.Fatal("undocumented metric")
				}
			}
			for name, value := range tt.want {
				m, ok := got[name]
				if !ok || m.Value != value {
					t.Errorf("%s=%v want %v", name, m.Value, value)
				}
			}
			for _, name := range tt.counter {
				if !got[name].Counter {
					t.Errorf("%s not counter", name)
				}
			}
			if tt.component == "elasticsearch" {
				if requests != 2 {
					t.Fatal(requests)
				}
				collectors, pools := 0, 0
				for _, m := range metrics {
					switch strings.TrimPrefix(m.Name, prefix) {
					case "gc_collection_seconds_total":
						collectors++
						if m.Labels["collector"] == "young" && m.Value != 1.5 {
							t.Fatal(m)
						}
					case "thread_pool_rejected_total":
						pools++
						if m.Labels["pool"] == "write" && m.Value != 6 {
							t.Fatal(m)
						}
					}
				}
				if collectors != 2 || pools != 2 {
					t.Fatal("missing labelled samples")
				}
			}
		})
	}
}

func TestMetricsFailures(t *testing.T) {
	for _, component := range New().Processes() {
		spec := esSpec()
		spec.Component = component
		for _, body := range []string{"invalid", "{}", "null", `{} {}`} {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
			})}
			if _, err := scrape(context.Background(), client, spec); err == nil {
				t.Errorf("%s accepted %s", component, body)
			}
		}
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unavailable") })}
		if _, err := scrape(context.Background(), client, spec); err == nil {
			t.Fatal("request error swallowed")
		}
	}
	// Invalid cluster CA must fail closed before making any HTTP request.
	metrics := New().Metrics(context.Background(), &fakeHost{}, esSpec())
	if len(metrics) != 1 || metrics[0].Name != "karakoram_elasticsearch_up" || metrics[0].Value != 0 {
		t.Fatal(metrics)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, component := range []string{"kibana", "logstash", "apm-server"} {
		metrics := New().Metrics(ctx, &fakeHost{}, agentspec.Spec{Component: component})
		if len(metrics) != 1 || metrics[0].Name != metricPrefix(component)+"up" || metrics[0].Value != 0 {
			t.Fatal(metrics)
		}
	}
}

func testCertificates(t *testing.T, node string) (string, agentspec.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: node}, DNSNames: []string{node, "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), agentspec.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))}
}

func TestElasticsearchMetricsTLS(t *testing.T) {
	ca, cert := testCertificates(t, "elastic-node")
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		user, password, ok := req.BasicAuth()
		if !ok || user != "monitor" || password != "secret" {
			w.WriteHeader(401)
			return
		}
		if req.URL.Path == "/_cluster/health" {
			io.WriteString(w, esHealthJSON)
		} else {
			io.WriteString(w, esStatsJSON)
		}
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "elastic-node", MinVersion: tls.VersionTLS12}, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	spec := esSpec()
	spec.CAPEM = ca
	spec.Elasticsearch.NodeName = "elastic-node"
	spec.Elasticsearch.Monitor = agentspec.Credentials{Username: "monitor", Password: "secret"}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	if _, err := scrape(context.Background(), client, spec); err != nil {
		t.Fatal(err)
	}
	transport.CloseIdleConnections()
	transport.TLSClientConfig.ServerName = "wrong-node"
	if _, err := scrape(context.Background(), client, spec); err == nil {
		t.Fatal("hostname mismatch accepted")
	}
}
