// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type testSamples struct {
	lookups, subscriptions   int
	instance, engine, action string
	request                  []byte
	body                     []byte
	at                       time.Time
	ok                       bool
	pushing                  bool
	bound                    string
}

func (s *testSamples) PushedSample(instance, engine, action string, request []byte) ([]byte, time.Time, bool) {
	s.lookups++
	s.instance, s.engine, s.action, s.request = instance, engine, action, append([]byte(nil), request...)
	return s.body, s.at, s.ok
}
func (s *testSamples) Pushing(instance string) bool            { return s.pushing }
func (s *testSamples) BindLogical(session string, _ time.Time) { s.bound = session }
func (s *testSamples) Subscribe(instance, engine, action string, request []byte) {
	s.subscriptions++
	s.instance, s.engine, s.action, s.request = instance, engine, action, append([]byte(nil), request...)
}

func TestPushedSamplesClient(t *testing.T) {
	calls := 0
	var sent []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		sent, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"configVersion":2}`))
	}))
	defer server.Close()
	samples := &testSamples{body: []byte(`{"configVersion":1}`), at: time.Unix(100, 0), ok: true}
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-bearer", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Samples: samples, SampleMember: "member"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	plain := context.Background()
	observation := infra.ObservationContext(plain)
	steady, evidence := infra.WithEvidence(infra.WithSteadyObservation(observation))
	for _, ctx := range []context.Context{plain, observation, infra.WithoutObservation(steady), infra.WithFreshTaskInventory(steady)} {
		status, err := client.Status(ctx)
		if err != nil || status.ConfigVersion != 2 {
			t.Fatal(status, err)
		}
	}
	if calls != 4 || samples.lookups != 0 || samples.subscriptions != 2 {
		t.Fatal(calls, samples)
	}
	status, err := client.Status(steady)
	if err != nil || status.ConfigVersion != 1 || calls != 4 {
		t.Fatal(status, err, calls)
	}
	if samples.engine != "" || samples.action != GenericStatusAction || len(samples.request) != 0 {
		t.Fatal(samples)
	}
	if at, ok := evidence.Oldest(); !ok || !at.Equal(samples.at) {
		t.Fatal(at, ok)
	}
	if at, ok := evidence.MemberOldest("member"); !ok || !at.Equal(samples.at) {
		t.Fatal(at, ok)
	}
	instance, err := client.ForInstance("svc.1.task123")
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	var out map[string]any
	if err := instance.Engine(steady, "mysql", "status", struct{}{}, &out); err != nil {
		t.Fatal(err)
	}
	if samples.instance != "svc.1.task123" || string(samples.request) != `{}` {
		t.Fatal(samples)
	}
	lookups, subscriptions := samples.lookups, samples.subscriptions
	if err := client.Engine(steady, "mysql", "start-group", nil, nil); err != nil {
		t.Fatal(err)
	}
	if samples.lookups != lookups || samples.subscriptions != subscriptions || len(sent) != 0 {
		t.Fatal("non-pushable action used samples")
	}
	samples.ok = false
	if err := client.Engine(steady, "mysql", "status", map[string]string{"key": "value"}, &out); err != nil {
		t.Fatal(err)
	}
	if string(sent) != string(samples.request) || string(sent) != `{"key":"value"}` {
		t.Fatal(string(sent), string(samples.request))
	}
	// Known pushers reject immediately instead of spending the steady budget on
	// a potentially black-holed pull. Legacy agents above still use the network.
	samples.pushing = true
	before := calls
	if _, err := client.Status(steady); err != ErrPushedSampleUnavailable {
		t.Fatal("status did not fail fast", err)
	}
	if err := instance.Engine(steady, "mysql", "status", nil, &out); err != ErrPushedSampleUnavailable {
		t.Fatal("engine did not fail fast", err)
	}
	if calls != before {
		t.Fatal("known pusher performed a steady pull")
	}
	if _, err := client.Status(infra.WithoutSteadyObservation(steady)); err != nil || calls != before+1 {
		t.Fatal("fresh fallback must still pull", err, calls)
	}
	samples.ok = true
	samples.body = []byte(`invalid`)
	if _, err := client.Status(steady); err == nil {
		t.Fatal("sample decode differs from HTTP decode")
	}
}

func TestLogicalBindingRequiresNewConnection(t *testing.T) {
	var session atomic.Value
	session.Store("A")
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if id := session.Load().(string); id != "" {
			w.Header().Set(HeartbeatSessionHeader, id)
		}
		_, _ = w.Write([]byte(`{"configVersion":1}`))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	samples := &testSamples{}
	client, err := NewClient(ClientOptions{
		BaseURL: server.URL, Token: "member-bearer",
		CAPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		Samples: samples, Timeout: 11 * time.Second, EngineTimeout: 17 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, pair := range [][2]*http.Client{{client.http, client.logicalProbe}, {client.engine, client.logicalEngineProbe}} {
		normal, probe := pair[0], pair[1]
		normalTransport := normal.Transport.(*http.Transport)
		probeTransport := probe.Transport.(*http.Transport)
		if probeTransport == normalTransport || !probeTransport.DisableKeepAlives || normalTransport.DisableKeepAlives {
			t.Fatal("logical probe did not isolate its no-reuse transport")
		}
		if normal.Timeout != probe.Timeout || normalTransport.ResponseHeaderTimeout != probeTransport.ResponseHeaderTimeout ||
			normalTransport.TLSHandshakeTimeout != probeTransport.TLSHandshakeTimeout ||
			normalTransport.TLSClientConfig.RootCAs != probeTransport.TLSClientConfig.RootCAs ||
			normalTransport.TLSClientConfig.MinVersion != probeTransport.TLSClientConfig.MinVersion {
			t.Fatal("logical probe changed request limits or TLS trust")
		}
		if err := probe.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
			t.Fatal("logical probe changed redirect policy", err)
		}
	}
	if _, err := client.Status(context.Background()); err != nil || samples.bound != "A" {
		t.Fatal("new connection did not establish binding", samples.bound, err)
	}
	session.Store("B")
	if _, err := client.Status(context.Background()); err != nil || samples.bound != "A" || connections.Load() != 1 {
		t.Fatal("reused connection rebound logical session", samples.bound, connections.Load(), err)
	}
	session.Store("")
	if _, err := client.Status(context.Background()); err != nil || samples.bound != "" || connections.Load() != 1 {
		t.Fatal("legacy response on reused connection did not clear binding", samples.bound, connections.Load(), err)
	}
	session.Store("B")
	steady := infra.WithSteadyObservation(infra.ObservationContext(context.Background()))
	for _, native := range []bool{false, true} {
		before := connections.Load()
		if native {
			var status Status
			err = client.Engine(steady, "mysql", "status", struct{}{}, &status)
		} else {
			_, err = client.Status(steady)
		}
		if err != nil || samples.bound != "B" || connections.Load() != before+1 {
			t.Fatal("steady logical probe did not establish a new connection", samples.bound, connections.Load(), err)
		}
	}
}
