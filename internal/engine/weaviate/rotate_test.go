// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRootRotationTwoPhase(t *testing.T) {
	ctx := context.Background()
	verified := map[string]bool{}
	newKey := ""
	f := coreSetup(t, 3, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/v1/.well-known/ready" {
			if r.Header.Get("Authorization") != "Bearer "+newKey {
				t.Error("new credential not used")
			}
			verified[strings.Split(r.Host, ":")[0]] = true
			return true
		}
		return false
	})
	s := coreConverge(t, f)
	old, _ := secret(ctx, f.rc, secretAdmin)
	op := &domain.Operation{ID: "root-rotation", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"karakoram-admin"}`)}
	sawBoth := false
	sawOnlyNew := false
	for range 300 {
		result, e := f.p.RunOperation(ctx, f.rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = result.Checkpoint
		next, e := secret(ctx, f.rc, rotationNew)
		if e == nil {
			newKey = string(next)
		}
		s, _ = load(f.rc.Cluster)
		for _, m := range s.Members {
			cfg, e := f.p.MemberConfig(ctx, f.rc, m.Name)
			if e != nil {
				t.Fatal(e)
			}
			var spec wvspec.MemberSpec
			json.Unmarshal(cfg.EngineSpec, &spec)
			if len(spec.AllowedKeys) == 2 {
				if spec.AllowedKeys[0] != string(old) || spec.AllowedKeys[1] != newKey {
					t.Fatal("bad key pair")
				}
				sawBoth = true
			}
			if len(spec.AllowedKeys) == 1 && spec.AllowedKeys[0] == newKey {
				if len(verified) != 3 {
					t.Fatal("old key removed before every member verified")
				}
				sawOnlyNew = true
			}
		}
		if result.Done {
			break
		}
	}
	if !sawBoth || !sawOnlyNew || s.Rotation != nil {
		t.Fatalf("rotation incomplete: %+v", s.Rotation)
	}
	users, e := f.p.SystemUsers(ctx, f.rc)
	if e != nil {
		t.Fatal(e)
	}
	if users[0].LastRotatedAt == nil {
		t.Fatal("missing rotation timestamp")
	}
	final, _ := secret(ctx, f.rc, secretAdmin)
	if string(final) == string(old) {
		t.Fatal("credential unchanged")
	}
}

func TestClusterCredentialCoordinatedRestart(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	coreConverge(t, f)
	old, _ := secret(ctx, f.rc, secretClusterPassword)
	before := len(f.backend.Calls())
	op := &domain.Operation{ID: "cluster-rotation", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"karakoram-cluster"}`)}
	message := false
	done := false
	for range 300 {
		r, e := f.p.RunOperation(ctx, f.rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = r.Checkpoint
		message = message || strings.Contains(r.Message, "unavailability")
		if r.Done {
			done = true
			break
		}
	}
	if !done || !message {
		t.Fatal("coordinated restart incomplete or unavailability undisclosed")
	}
	stops := 0
	deploys := 0
	for _, call := range f.backend.Calls()[before:] {
		if call == "Stop" {
			stops++
		}
		if call == "Deploy" {
			if stops != 3 {
				t.Fatal("mixed credentials started before every member stopped")
			}
			deploys++
		}
	}
	if stops != 3 || deploys != 3 {
		t.Fatal(stops, deploys)
	}
	s, _ := load(f.rc.Cluster)
	next, _ := secret(ctx, f.rc, secretClusterPassword)
	if string(next) == string(old) || s.LastRotated[ClusterUser].IsZero() {
		t.Fatal("credential or timestamp unchanged")
	}
	for _, m := range s.Members {
		cfg, e := f.p.MemberConfig(ctx, f.rc, m.Name)
		if e != nil {
			t.Fatal(e)
		}
		var spec wvspec.MemberSpec
		json.Unmarshal(cfg.EngineSpec, &spec)
		if spec.ClusterPassword != string(next) {
			t.Fatal("member password mismatch")
		}
	}
}

func leafSerial(t *testing.T, crt []byte) string {
	t.Helper()
	b, _ := pem.Decode(crt)
	if b == nil {
		t.Fatal("missing certificate")
	}
	cert, e := x509.ParseCertificate(b.Bytes)
	if e != nil {
		t.Fatal(e)
	}
	return cert.SerialNumber.String()
}
func TestTLSRotationRollingLeaves(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 4)
	s := coreConverge(t, f)
	serials := map[string]string{}
	for _, m := range s.Members {
		crt, _ := secret(ctx, f.rc, "tls/members/"+m.Name+".crt")
		serials[m.Name] = leafSerial(t, crt)
	}
	op := &domain.Operation{ID: "tls-rotation", Kind: domain.OperationRotateTLS}
	changed := []string{}
	seen := map[string]bool{}
	done := false
	for range 300 {
		r, e := f.p.RunOperation(ctx, f.rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = r.Checkpoint
		s, _ = load(f.rc.Cluster)
		count := 0
		for _, m := range s.Members {
			cfg, e := f.p.MemberConfig(ctx, f.rc, m.Name)
			if e != nil {
				t.Fatal(e)
			}
			if leafSerial(t, []byte(cfg.TLS.CertPEM)) != serials[m.Name] && !seen[m.Name] {
				seen[m.Name] = true
				changed = append(changed, m.Name)
				count++
			}
		}
		if count > 1 {
			t.Fatal("multiple leaves activated in one pass")
		}
		if r.Done {
			done = true
			break
		}
	}
	expected := []string{NodeHostname("demo", 3), NodeHostname("demo", 0), NodeHostname("demo", 1), NodeHostname("demo", 2)}
	if !done || !slices.Equal(changed, expected) {
		t.Fatal("rolling order", changed)
	}
}

func TestTLSRotationChangedUserProvidedCATrust(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	f.rc.Cluster.Spec.TLS.CA = "userProvided"
	ca, err := pki.NewCA("replacement", 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = putSecret(ctx, f.rc, secretCA, ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	if err = putSecret(ctx, f.rc, secretCAKey, ca.KeyPEM); err != nil {
		t.Fatal(err)
	}
	// The still-running server has the old leaf. Rotation's initial quorum gate
	// must retain trust from active member configs while new leaves are issued.
	client, err := f.p.native(ctx, f.rc, s)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err = client.Meta(ctx); err != nil {
		t.Fatal(err)
	}
}
