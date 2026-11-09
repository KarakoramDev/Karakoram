// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type heartbeatEngine struct {
	fakeEngine
	name   string
	action string
	serve  http.HandlerFunc
}

func (e *heartbeatEngine) Name() string { return e.name }
func (e *heartbeatEngine) Routes(mux *http.ServeMux, _ *Node) {
	mux.HandleFunc("POST /v1/"+e.name+"/"+e.action, e.serve)
	mux.HandleFunc("POST /v1/"+e.name+"/mutate", e.serve)
}

func heartbeatTestNode(t *testing.T, cp http.Handler) *nodeAgent {
	t.Helper()
	server := httptest.NewTLSServer(cp)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "test-bearer", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return &nodeAgent{o: Options{Token: "test-bearer", TaskID: "runtime-task", DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, client: client, sup: newSupervisor(io.Discard), started: time.Now()}
}

func TestHeartbeatTokenRoutesAndOrdering(t *testing.T) {
	for _, route := range []struct{ engine, action string }{
		{"mysql", "status"}, {"mysql", "proxy-status"}, {"mysql", "galera-status"},
		{"postgres", "status"}, {"neo4j", "readiness"}, {"weaviate", "readiness"},
		{"elasticsearch", "readiness"}, {"rabbitmq", "observe"}, {"keycloak", "status"},
		{"supabase", "status"}, {"temporal", "status"}, {"openbao", "status"}, {"datamcp", "readiness"},
	} {
		t.Run(route.engine+"/"+route.action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls, waits atomic.Int32
			received := make(chan agentapi.HeartbeatRequest, 2)
			a := heartbeatTestNode(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != agentapi.HeartbeatPath || !agentapi.AuthenticateBearer(r, "test-bearer") {
					t.Error("incorrect heartbeat transport")
				}
				var req agentapi.HeartbeatRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				received <- req
				_ = json.NewEncoder(w).Encode(agentapi.HeartbeatResponse{Token: "issued-token", IntervalMillis: 1, Requests: []agentapi.HeartbeatAction{{Engine: route.engine, Action: route.action, Request: json.RawMessage(`{"probe":true}`)}, {Engine: route.engine, Action: "mutate"}}})
			}))
			a.engine = &heartbeatEngine{name: route.engine, action: route.action, serve: func(w http.ResponseWriter, r *http.Request) {
				if got := w.Header().Get(agentapi.HeartbeatSessionHeader); got == "" || got != a.sessionID() {
					t.Errorf("engine route session header=%q", got)
				}
				if waits.Load() == 0 {
					t.Error("sample ran before token reply was received")
				}
				if r.URL.Path != "/v1/"+route.engine+"/"+route.action || !agentapi.AuthenticateBearer(r, a.o.Token) {
					t.Error("non-pushable route executed or missing authentication")
				}
				body, _ := io.ReadAll(r.Body)
				if string(body) != `{"probe":true}` {
					t.Errorf("body = %s", body)
				}
				calls.Add(1)
				_, _ = w.Write([]byte(`{"healthy":true}`))
			}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				a.heartbeatLoop(ctx, a.handler(), func(ctx context.Context, interval time.Duration) bool {
					if interval != agentapi.HeartbeatMinInterval {
						t.Errorf("interval = %v", interval)
					}
					if waits.Add(1) == 1 {
						return true
					}
					<-ctx.Done()
					return false
				})
			}()
			var first, next agentapi.HeartbeatRequest
			for _, target := range []*agentapi.HeartbeatRequest{&first, &next} {
				select {
				case *target = <-received:
				case <-time.After(5 * time.Second):
					t.Fatal("heartbeat missing")
				}
			}
			if first.Token != "" || len(first.Samples) != 0 || first.Session == "" || first.Session != next.Session || first.Session != a.sessionID() || first.Instance.TaskID != "runtime-task" {
				t.Fatalf("invalid initial heartbeat: %+v", first)
			}
			if next.Token != "issued-token" || len(next.Samples) != 2 || calls.Load() != 1 {
				t.Fatalf("invalid echoed samples: %+v; calls=%d", next, calls.Load())
			}
			for _, sample := range next.Samples {
				if sample.Code != http.StatusOK || !json.Valid(sample.Response) {
					t.Fatalf("invalid sample: %+v", sample)
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown did not stop heartbeat loop")
			}
		})
	}
}

func TestHeartbeatIntervals(t *testing.T) {
	for _, test := range []struct {
		millis int64
		want   time.Duration
	}{{0, agentapi.HeartbeatInterval}, {-1, agentapi.HeartbeatInterval}, {1, agentapi.HeartbeatMinInterval}, {3000, 3 * time.Second}, {math.MaxInt64, agentapi.HeartbeatMaxInterval}} {
		if got := heartbeatInterval(test.millis); got != test.want {
			t.Errorf("interval(%d)=%v, want %v", test.millis, got, test.want)
		}
	}
}

func TestHeartbeatLegacyBackoffAndErrors(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int32
			a := heartbeatTestNode(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(code)
			}))
			waits := 0
			a.heartbeatLoop(context.Background(), a.handler(), func(_ context.Context, interval time.Duration) bool {
				want := agentapi.HeartbeatInterval
				if code == http.StatusNotFound {
					want = agentapi.HeartbeatLegacyBackoff
				}
				if interval != want {
					t.Errorf("backoff=%v, want %v", interval, want)
				}
				waits++
				return waits < 2
			})
			if calls.Load() != 2 {
				t.Fatalf("calls=%d", calls.Load())
			}
		})
	}
}

func TestHeartbeatDropsOversizedSamples(t *testing.T) {
	a := heartbeatTestNode(t, http.NotFoundHandler())
	a.engine = &heartbeatEngine{name: "mysql", action: "status", serve: func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", agentapi.HeartbeatSampleMaxBytes+1))
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	samples := <-a.heartbeatSamples(ctx, a.handler(), []agentapi.HeartbeatAction{{Engine: "mysql", Action: "status"}})
	if len(samples) != 1 || samples[0].Engine != "" {
		t.Fatalf("oversized sample retained: %+v", samples)
	}
}

func TestHeartbeatIdentity(t *testing.T) {
	t.Setenv("NOMAD_ALLOC_ID", "allocation")
	if got := heartbeatIdentity(Options{}); got.TaskID != "allocation" || got.Hostname == "" {
		t.Fatalf("identity = %+v", got)
	}
	if got := heartbeatIdentity(Options{TaskID: "explicit"}); got.TaskID != "explicit" {
		t.Fatalf("explicit identity = %+v", got)
	}
}

func TestHeartbeatSessionHeader(t *testing.T) {
	a := heartbeatTestNode(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	a.engine = &heartbeatEngine{name: "mysql", action: "status", serve: func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}}
	handler := a.handler()
	var session string
	for _, path := range []string{"/v1/status", "/v1/mysql/status"} {
		method := "GET"
		if path != "/v1/status" {
			method = "POST"
		}
		req := httptest.NewRequest(method, path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer "+a.o.Token)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		got := recorder.Header().Get(agentapi.HeartbeatSessionHeader)
		if got == "" || session != "" && got != session {
			t.Fatalf("route %s returned inconsistent session %q (previous %q)", path, got, session)
		}
		session = got
	}
	if session != a.sessionID() {
		t.Fatal("HTTP identity differs from heartbeat process session")
	}
}
