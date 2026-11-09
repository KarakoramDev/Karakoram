// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestAgentHeartbeatAuthenticationAndDecode(t *testing.T) {
	s, _, _ := testServer(t)
	s.AgentPush = agentpush.NewStore(nil)
	key := agentpush.MemberKey{Namespace: "production", Cluster: "demo", Member: "demom0"}
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: key.Namespace, Cluster: key.Cluster, Member: key.Member, Token: "member-bearer"}); err != nil {
		t.Fatal(err)
	}
	var ingested []agentpush.MemberKey
	s.AgentPush.OnIdentityChange(func(key agentpush.MemberKey) { ingested = append(ingested, key) })
	handler := s.InternalHandler()
	for _, tc := range []struct {
		name, method, bearer, body string
		want                       int
	}{
		{"missing bearer", "POST", "", `{"session":"missing","instance":{}}`, 401},
		{"wrong bearer", "POST", "wrong", `{"session":"wrong","instance":{}}`, 401},
		{"wrong method", "GET", "member-bearer", "", 405},
		{"malformed", "POST", "member-bearer", `{`, 400},
		{"null", "POST", "member-bearer", `null`, 400},
		{"unknown field", "POST", "member-bearer", `{"session":"unknown","instance":{},"cluster":"other"}`, 400},
		{"trailing value", "POST", "member-bearer", `{"session":"trailing","instance":{}} {}`, 400},
		{"oversized", "POST", "member-bearer", `{"session":"` + strings.Repeat("x", agentapi.HeartbeatMaxBytes) + `","instance":{}}`, 413},
		{"valid", "POST", "member-bearer", `{"session":"valid","instance":{"hostname":"runtime"}}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, agentapi.HeartbeatPath, strings.NewReader(tc.body))
			if tc.bearer != "" {
				r.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.want == 200 {
				var reply agentapi.HeartbeatResponse
				if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
					t.Fatal(err)
				}
				if reply.Token == "" || reply.IntervalMillis != agentapi.HeartbeatInterval.Milliseconds() {
					t.Fatalf("invalid heartbeat reply: %+v", reply)
				}
			}
		})
	}
	if len(ingested) != 1 || ingested[0] != key {
		t.Fatalf("ingested identities=%v, want only %v", ingested, key)
	}
	if err := s.Store.DeleteAgentToken(t.Context(), domain.ClusterRef{Namespace: key.Namespace, Name: key.Cluster}, key.Member); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, agentapi.HeartbeatPath, strings.NewReader(`{"session":"revoked","instance":{}}`))
	r.Header.Set("Authorization", "Bearer member-bearer")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 || len(ingested) != 1 {
		t.Fatalf("revoked bearer status=%d identities=%v", w.Code, ingested)
	}
}

func TestAgentHeartbeatDialerSamplesBoundToMember(t *testing.T) {
	s, _, _ := testServer(t)
	at := time.Now()
	push := agentpush.NewStore(func() time.Time { return at })
	ca, err := pki.NewCA("test agent CA", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	refs := []domain.ClusterRef{{Namespace: domain.DefaultNamespace, Name: "demo"}, {Namespace: "production", Name: "demo"}}
	for _, ref := range refs {
		if err := s.Store.SetClusterSecret(t.Context(), ref, "tls/ca.crt", ca.CertPEM); err != nil {
			t.Fatal(err)
		}
		for _, member := range []string{"demom0", "demom1"} {
			if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: ref.Namespace, Cluster: ref.Name, Member: member, Token: ref.Namespace + member}); err != nil {
				t.Fatal(err)
			}
			key := agentpush.MemberKey{Namespace: ref.Namespace, Cluster: ref.Name, Member: member}
			first := push.Ingest(key, agentapi.HeartbeatRequest{Session: "session"})
			body, err := json.Marshal(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{ref.Namespace + member: {}}})
			if err != nil {
				t.Fatal(err)
			}
			push.Ingest(key, agentapi.HeartbeatRequest{Session: "session", Token: first.Token, Samples: []agentapi.HeartbeatSample{{Action: agentapi.GenericStatusAction, Code: 200, Response: body}}})
			// Model the session proof obtained by a successful live logical pull.
			push.Source(key).BindLogical("session", at)
		}
	}
	d := NewAgentDialer(s.Store, nil, push)
	defer d.Close()
	ctx := infra.WithSteadyObservation(infra.ObservationContext(context.Background()))
	for _, ref := range refs {
		for _, member := range []string{"demom0", "demom1"} {
			client, err := d.Dial(ctx, ref, member)
			if err != nil {
				t.Fatal(err)
			}
			cached, err := d.Dial(ctx, ref, member)
			if err != nil || cached != client {
				t.Fatal("member cache missed", err)
			}
			evidenceCtx, evidence := infra.WithEvidence(ctx)
			status, err := client.Status(evidenceCtx)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := status.Processes[ref.Namespace+member]; !ok {
				t.Fatalf("sample crossed member/namespace boundary: %+v", status)
			}
			if sampledAt, ok := evidence.MemberOldest(member); !ok || !sampledAt.Equal(at) {
				t.Fatalf("sample member evidence=%v, %v", sampledAt, ok)
			}
		}
	}
	// Ordinary observation must still make a live request, never serve pushed state.
	transport := &namespaceAgentTransport{}
	direct := NewAgentDialer(s.Store, transport, push)
	defer direct.Close()
	client, err := direct.Dial(t.Context(), refs[1], "demom0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Status(infra.ObservationContext(t.Context())); err == nil {
		t.Fatal("ordinary observation served pushed sample")
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.addresses) != 1 || transport.addresses[0] != "demom0:9443" {
		t.Fatalf("live addresses=%v", transport.addresses)
	}
}
