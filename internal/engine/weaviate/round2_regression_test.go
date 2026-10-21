// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/dokploy"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/infra/nomad"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
)

func TestRound2BindHostBackendFamilies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		layout infra.Layout
		want   string
	}{
		{"zerops", (&zerops.Backend{}).Layout(), "::"},
		{"nomad", (&nomad.Backend{}).Layout(), "0.0.0.0"},
		{"dokploy", (&dokploy.Backend{}).Layout(), "0.0.0.0"},
		{"default", infra.Layout{}, "::"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			f.rc.Backend = fake.New(fake.Options{Layout: tc.layout})
			cfg, err := f.p.desiredConfig(context.Background(), f.rc, s, s.Members[0])
			if err != nil {
				t.Fatal(err)
			}
			var spec wvspec.MemberSpec
			if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.BindHost != tc.want {
				t.Fatalf("bind host %q, want %q", spec.BindHost, tc.want)
			}
		})
	}
}

func TestRound2MoveProgressionTransientErrorsAndLostReply(t *testing.T) {
	ctx := context.Background()
	source := NodeHostname("demo", 3)
	states := []wvapi.ReplicaState{wvapi.ReplicaRegistered, wvapi.ReplicaHydrating, wvapi.ReplicaHydrating, wvapi.ReplicaHydrating, wvapi.ReplicaFinalizing, wvapi.ReplicaDehydrating, wvapi.ReplicaReady}
	polls, posts := 0, 0
	replicas := []string{source}
	var move wvapi.Replication
	f := coreSetup(t, 4, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v1/schema":
			json.NewEncoder(w).Encode(map[string]any{"classes": []any{map[string]any{"class": "C", "replicationConfig": map[string]int{"factor": 1}}}})
		case "/v1/replication/sharding-state":
			json.NewEncoder(w).Encode(wvapi.ShardingStateResponse{ShardingState: wvapi.ShardingState{Shards: []wvapi.ShardReplicas{{Shard: "S", Replicas: replicas}}}})
		case "/v1/replication/replicate/list":
			ops := []wvapi.Replication{}
			if move.ID != "" {
				ops = append(ops, move)
			}
			json.NewEncoder(w).Encode(ops)
		case "/v1/replication/replicate":
			posts++
			var req wvapi.ReplicateRequest
			json.NewDecoder(r.Body).Decode(&req)
			move = wvapi.Replication{ID: "durable", Collection: req.Collection, Shard: req.Shard, SourceNode: req.SourceNode, TargetNode: req.TargetNode, Status: wvapi.ReplicationStatus{State: wvapi.ReplicaRegistered}}
			// The native mutation succeeds but its response is lost.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
			} else {
				conn.Close()
			}
		case "/v1/replication/replicate/durable":
			state := states[min(polls, len(states)-1)]
			polls++
			move.Status.State = state
			if state == wvapi.ReplicaHydrating {
				move.Status.Errors = []wvapi.ReplicationReplicateDetailsReplicaStatusError{{}}
			} else {
				move.Status.Errors = nil
			}
			if state == wvapi.ReplicaReady {
				replicas = []string{move.TargetNode}
			}
			json.NewEncoder(w).Encode(move)
		default:
			return false
		}
		return true
	})
	f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"replicationMinimumFactor":1}`)
	s := coreConverge(t, f)
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	for range 80 {
		_, err := f.p.Sync(ctx, f.rc)
		if err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if s.ScaleDown != nil {
			s.ScaleDown.RetryAt = time.Time{}
			if s.ScaleDown.Stage == "verify" {
				break
			}
			if err = s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
		}
	}
	if posts != 1 || polls != len(states) {
		t.Fatalf("posts=%d polls=%d", posts, polls)
	}
	if s.ScaleDown == nil || s.ScaleDown.Stage != "verify" || s.ScaleDown.Attempts != 1 {
		t.Fatalf("lost reply recovery or transient errors failed: %+v", s.ScaleDown)
	}
}

func TestRound2PlacementFenceRechecksTenantShardsAndRestarts(t *testing.T) {
	ctx := context.Background()
	source := NodeHostname("demo", 3)
	shards := []wvapi.ShardReplicas{}
	created, moves, removed := false, 0, false
	var f *coreFixture
	f = coreSetup(t, 4, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v1/schema":
			classes := []any{}
			if created {
				classes = append(classes, map[string]any{"class": "Tenants", "multiTenancyConfig": map[string]bool{"enabled": true}, "replicationConfig": map[string]int{"factor": 1}})
			}
			json.NewEncoder(w).Encode(map[string]any{"classes": classes})
		case "/v1/replication/sharding-state":
			json.NewEncoder(w).Encode(wvapi.ShardingStateResponse{ShardingState: wvapi.ShardingState{Shards: shards}})
		case "/v1/replication/replicate/list":
			json.NewEncoder(w).Encode([]wvapi.Replication{})
		case "/v1/replication/replicate":
			var req wvapi.ReplicateRequest
			json.NewDecoder(r.Body).Decode(&req)
			s, _ := load(f.rc.Cluster)
			if s.find(source).CleanupStep == "stopped" {
				t.Error("MOVE attempted before restart acknowledgement")
			}
			for i := range shards {
				if shards[i].Shard == req.Shard {
					shards[i].Replicas = []string{req.TargetNode}
				}
			}
			moves++
			json.NewEncoder(w).Encode(wvapi.ReplicateResponse{ID: fmt.Sprint(moves)})
		default:
			if strings.HasPrefix(r.URL.Path, "/v1/replication/replicate/") {
				json.NewEncoder(w).Encode(wvapi.Replication{Status: wvapi.ReplicationStatus{State: wvapi.ReplicaReady}})
			} else {
				return false
			}
		}
		return true
	})
	f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"replicationMinimumFactor":1}`)
	coreConverge(t, f)
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, sh := range shards {
			if slices.Contains(sh.Replicas, source) {
				t.Error("retired node still owns tenant shard")
			}
		}
		removed = true
	}))
	defer internal.Close()
	f.rc.Dialer = retirementDialer{f.rc.Dialer, strings.TrimPrefix(internal.URL, "http://")}
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	seen := map[string]bool{}
	for range 180 {
		if _, err := f.p.Sync(ctx, f.rc); err != nil {
			t.Fatal(err)
		}
		s, err := load(f.rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.ScaleDown != nil {
			stage := s.ScaleDown.Stage
			seen[stage] = true
			if stage == "stop-config" && !created {
				// A collection with HOT and COLD physical tenant shards is created
				// after the final empty observation but before the process stops.
				created = true
				shards = []wvapi.ShardReplicas{{Shard: "hot", Replicas: []string{source}}, {Shard: "cold", Replicas: []string{source}}}
			}
			if stage == "fence" || stage == "fenced-scan" || strings.HasPrefix(stage, "restart") {
				runtime, e := f.backend.GetService(ctx, source)
				if e != nil || runtime == nil {
					t.Fatal("fence deleted reversible runtime", e)
				}
				volume, e := f.backend.GetService(ctx, s.find(source).Volume)
				if e != nil || volume == nil {
					t.Fatal("fence deleted reversible volume", e)
				}
			}
		}
		// Recreate the controller after every checkpoint, including pending ops.
		if err = s.save(f.rc.Cluster); err != nil {
			t.Fatal(err)
		}
		f.p = New(Options{Store: f.rc.Store})
		if len(s.Members) == 3 && s.ScaleDown == nil {
			break
		}
	}
	s, _ := load(f.rc.Cluster)
	for _, stage := range []string{"fence", "fenced-scan", "restart-config", "restart", "restart-ack", "raft", "runtime", "volume", "forget"} {
		if !seen[stage] {
			t.Errorf("checkpoint %s not exercised", stage)
		}
	}
	if !removed || moves != 2 || len(s.Members) != 3 {
		t.Fatalf("retirement incomplete: removed=%v moves=%d members=%d", removed, moves, len(s.Members))
	}
}

func TestRound2ScaleBackUpWaitsForUncancellableMove(t *testing.T) {
	ctx := context.Background()
	ready := false
	polls := 0
	f := coreSetup(t, 4, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			w.WriteHeader(http.StatusConflict)
			return true
		}
		if r.URL.Path == "/v1/replication/replicate/late" {
			polls++
			state := wvapi.ReplicaDehydrating
			if ready {
				state = wvapi.ReplicaReady
			}
			json.NewEncoder(w).Encode(wvapi.Replication{Status: wvapi.ReplicationStatus{State: state}})
			return true
		}
		return false
	})
	s := coreConverge(t, f)
	m := s.Members[3]
	m.Removing = true
	s.ScaleDown = &scaleDown{Member: m.Name, Stage: "drain", MoveID: "late", Target: 3}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Sync(ctx, f.rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(f.rc.Cluster)
	if s.ScaleDown == nil || s.ScaleDown.MoveID != "late" {
		t.Fatal("forgot uncancellable MOVE")
	}
	ready = true
	if _, err := f.p.Sync(ctx, f.rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(f.rc.Cluster)
	if s.ScaleDown != nil || s.find(m.Name).Removing || polls != 2 {
		t.Fatal("late MOVE completion did not cancel drain")
	}
}

func TestRound2RollingStaleAcknowledgements(t *testing.T) {
	for _, status := range []bool{true, false} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx := context.Background()
			f := coreSetup(t, 3)
			coreConverge(t, f)
			f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"queryDefaultsLimit":42}`)
			for range 40 {
				if _, err := f.p.Sync(ctx, f.rc); err != nil {
					t.Fatal(err)
				}
				s, _ := load(f.rc.Cluster)
				if s.Rolling != nil && s.Rolling.Step == "ack" && s.Pending == nil {
					break
				}
			}
			if status {
				f.statusStale = true
			} else {
				f.readinessStale = true
			}
			for range 4 {
				if _, err := f.p.Sync(ctx, f.rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := load(f.rc.Cluster)
			if s.Rolling == nil || s.Rolling.Position != 0 || s.Rolling.Step != "ack" {
				t.Fatalf("stale acknowledgement advanced roll: %+v", s.Rolling)
			}
			f.statusStale = false
			f.readinessStale = false
			coreConverge(t, f)
		})
	}
}

// expiredPair signs a leaf in the real past under the fixture's trusted CA.
func expiredPair(t *testing.T, f *coreFixture, name string, serial int64) ([]byte, []byte) {
	t.Helper()
	ctx := context.Background()
	caPEM, err := secret(ctx, f.rc, secretCA)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := secret(ctx, f.rc, secretCAKey)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(caPEM)
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	block, _ = pem.Decode(keyPEM)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := f.p.MemberConfig(ctx, f.rc, name)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair([]byte(cfg.TLS.CertPEM), []byte(cfg.TLS.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	leaf.SerialNumber = big.NewInt(serial)
	leaf.NotBefore = time.Now().Add(-48 * time.Hour)
	leaf.NotAfter = time.Now().Add(-24 * time.Hour)
	// Keep the CA valid at the recovery verification time as well.
	ca.NotBefore = time.Now().Add(-72 * time.Hour)
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, key.(crypto.Signer).Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if err = putSecret(ctx, f.rc, secretCA, caPEM); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, leaf.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), []byte(cfg.TLS.KeyPEM)
}

func TestRound2ExpiredActiveLeafRollingRecovery(t *testing.T) {
	for _, pinned := range []bool{true, false} {
		t.Run(fmt.Sprint(pinned), func(t *testing.T) {
			ctx := context.Background()
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			name := s.Members[0].Name
			leaf, key := expiredPair(t, f, name, 700)
			cfg, err := f.p.MemberConfig(ctx, f.rc, name)
			if err != nil {
				t.Fatal(err)
			}
			cfg.TLS.CertPEM = string(leaf)
			cfg.TLS.KeyPEM = string(key)
			ca, err := secret(ctx, f.rc, secretCA)
			if err != nil {
				t.Fatal(err)
			}
			cfg.TLS.CAPEM = string(ca)
			if err = activateConfig(ctx, f.rc, s, s.Members[0], cfg); err != nil {
				t.Fatal(err)
			}
			if err = s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			served := leaf
			if !pinned {
				served, key = expiredPair(t, f, name, 701)
			}
			pair, err := tls.X509KeyPair(served, key)
			if err != nil {
				t.Fatal(err)
			}
			native := httptest.NewUnstartedServer(f.native.Config.Handler)
			native.TLS = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				active, err := f.p.MemberConfig(ctx, f.rc, name)
				if err != nil {
					return nil, err
				}
				if active.TLS.CertPEM == string(leaf) {
					return &pair, nil
				}
				next, err := tls.X509KeyPair([]byte(active.TLS.CertPEM), []byte(active.TLS.KeyPEM))
				return &next, err
			}}
			native.StartTLS()
			defer native.Close()
			f.rc.Dialer = coreDialer{native.Listener.Addr().String()}
			// Renew the desired secret; the process still serves the active expired leaf.
			if err = putSecret(ctx, f.rc, "tls/members/"+name+".crt", leaf); err != nil {
				t.Fatal(err)
			}
			if err = putSecret(ctx, f.rc, "tls/members/"+name+".key", key); err != nil {
				t.Fatal(err)
			}
			strict, err := f.p.native(ctx, f.rc, s)
			if err != nil {
				t.Fatal(err)
			}
			_, err = strict.Nodes(ctx)
			strict.Close()
			if err == nil {
				t.Fatal("ordinary native client accepted expired certificate")
			}
			if !pinned {
				expired, err := expiredActiveLeaves(ctx, f.rc, s)
				if err != nil {
					t.Fatal(err)
				}
				c, err := f.p.nativeClient(ctx, f.rc, s, expired)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				if _, err = c.Nodes(ctx); err == nil {
					t.Fatal("different expired leaf accepted")
				}
				return
			}
			coreConverge(t, f)
			active, err := f.p.MemberConfig(ctx, f.rc, name)
			if err != nil {
				t.Fatal(err)
			}
			if active.TLS.CertPEM == string(leaf) {
				t.Fatal("expired active leaf not rolled")
			}
		})
	}
}

func TestRound2PlacementFenceRequiresMemberlistAbsence(t *testing.T) {
	ctx := context.Background()
	source := NodeHostname("demo", 3)
	view := ""
	f := coreSetup(t, 4, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/v1/nodes" || view == "" {
			return false
		}
		nodes := []map[string]string{}
		if view != "absent" {
			nodes = append(nodes, map[string]string{"name": source, "status": view})
		}
		json.NewEncoder(w).Encode(map[string]any{"nodes": nodes})
		return true
	})
	s := coreConverge(t, f)
	m := s.find(source)
	m.Removing = true
	m.CleanupStep = "stopped"
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	s.ScaleDown = &scaleDown{Member: source, Stage: "fence", Target: 3}
	for _, status := range []string{"HEALTHY", "UNHEALTHY", "UNAVAILABLE", "TIMEOUT", "absent"} {
		view = status
		if err := s.save(f.rc.Cluster); err != nil {
			t.Fatal(err)
		}
		if _, err := f.p.advanceScaleDown(ctx, f.rc, s); err != nil {
			t.Fatal(err)
		}
		want := "fence"
		if status == "absent" {
			want = "fenced-scan"
		}
		if s.ScaleDown.Stage != want {
			t.Fatalf("%s: advanced to %s, want %s", status, s.ScaleDown.Stage, want)
		}
	}
}
