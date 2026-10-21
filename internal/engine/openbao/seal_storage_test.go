// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"strings"
	"testing"
)

type sealBinding struct {
	c         pgbinding.Connection
	request   pgbinding.Request
	released  bool
	keep      bool
	releaseID string
}

func (f *sealBinding) Ensure(_ context.Context, r pgbinding.Request) (pgbinding.Connection, error) {
	f.request = r
	f.c.BindingID = pgbinding.BindingID(r)
	return f.c, nil
}
func (f *sealBinding) Resolve(_ context.Context, id string) (pgbinding.Connection, error) {
	if id != f.c.BindingID {
		return pgbinding.Connection{}, pgbinding.ErrSourceUnavailable
	}
	return f.c, nil
}
func (f *sealBinding) Password(context.Context, string) (string, error) {
	return "binding-password", nil
}
func (f *sealBinding) Release(_ context.Context, id string, k bool) error {
	f.releaseID = id
	f.released = true
	f.keep = k
	return nil
}

func TestSealStorageAdmission(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{}`, true},
		{`{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"key"}}}`, true},
		{`{"seal":{"type":"transit","transit":{"address":"http://transit.example","keyName":"key"}}}`, false},
		{`{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"key","caPEM":"invalid"}}}`, false},
		{`{"seal":{"type":"awskms","awskms":{"region":"eu-west-1","kmsKeyId":"key"}}}`, true},
		{`{"seal":{"type":"awskms","awskms":{"region":"eu-west-1","kmsKeyId":"key","endpoint":"http://kms"}}}`, false},
		{`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`, true},
		{`{"storage":{"type":"postgresql"}}`, false},
	} {
		o, e := parseOptions(json.RawMessage(tc.raw))
		if e == nil {
			e = validateSealAndStorage(nil, &o)
		}
		if (e == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.raw, e)
		}
	}
}

func TestSealExtractionAndMemberCredentials(t *testing.T) {
	p, rc, _ := edgeFixture(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"key","token":"private-seal-token"}}}`)
	values, e := p.ExtractWriteOnlyOptions(nil, &rc.Cluster.Spec)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(rc.Cluster.Spec.EngineOptions), "private-seal-token") {
		t.Fatal("credential persisted")
	}
	for k, v := range values {
		if e = putSecret(t.Context(), rc, k, v); e != nil {
			t.Fatal(e)
		}
	}
	o, _ := parseOptions(rc.Cluster.Spec.EngineOptions)
	seal, e := p.memberSeal(t.Context(), rc, o)
	if e != nil || seal.Transit.Token != "private-seal-token" || seal.Transit.MountPath != "transit/" {
		t.Fatal(seal, e)
	}
	if strings.Contains(string(rc.Cluster.Internal), "private-seal-token") {
		t.Fatal("credential in state")
	}
	old := rc.Cluster.Spec
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"transit","transit":{"address":"https://other.example","keyName":"key","token":"rotation"}}}`)
	if _, e = p.ExtractWriteOnlyOptions(&old, &rc.Cluster.Spec); e == nil {
		t.Fatal("seal migration accepted")
	}
}

func TestPostgresBindingEnsureAndRotation(t *testing.T) {
	p, rc, s := edgeFixture(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	ca, e := secret(t.Context(), rc, "tls/ca.crt")
	if e != nil {
		t.Fatal(e)
	}
	f := &sealBinding{c: pgbinding.Connection{BindingID: "binding", Host: "postgres.internal", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca}, CredentialGeneration: 1}}
	p.options.Bindings = f
	busy, e := p.ensurePostgresStorage(t.Context(), rc, s)
	if e != nil || !busy || s.BindingID != pgbinding.BindingID(f.request) {
		t.Fatal(busy, e)
	}
	if f.request.ConsumerKind != "openbao" || f.request.MinMajor != 13 || f.request.SourceCluster != "pg" {
		t.Fatal(f.request)
	}
	busy, e = p.ensurePostgresStorage(t.Context(), rc, s)
	if e != nil || busy {
		t.Fatal(busy, e)
	}
	f.c.CredentialGeneration = 2
	busy, e = p.ensurePostgresStorage(t.Context(), rc, s)
	if e != nil || !busy || s.CredentialGeneration != 2 {
		t.Fatal(busy, e)
	}
	storage, e := p.memberPostgresStorage(t.Context(), rc, s)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(storage.PostgreSQL.ConnectionURL, "sslmode=verify-full") || !strings.Contains(storage.PostgreSQL.ConnectionURL, "binding-password") || len(storage.RetryJoin) != 0 {
		t.Fatal(storage)
	}
	if e = s.save(rc.Cluster); e != nil {
		t.Fatal(e)
	}
	config, e := p.MemberConfig(t.Context(), rc, "baom0")
	if e != nil {
		t.Fatal(e)
	}
	if config.Volume == nil || config.Volume.Initialized || config.Volume.ClusterUID != s.UID || config.Volume.Member != "baom0" {
		t.Fatalf("stateless PostgreSQL peer must retain identity without local-data fencing: %+v", config.Volume)
	}
	if strings.Contains(string(rc.Cluster.Internal), "binding-password") {
		t.Fatal("password in state")
	}
	// No binding service is wired by default: admission must fail closed.
	n := domain.Cluster{Name: "pgbao", Spec: rc.Cluster.Spec}
	p.ApplyDefaults(&n.Spec)
	p.options.Bindings = nil
	if e = p.Validate(t.Context(), nil, &n); e == nil {
		t.Fatal("missing binding service admitted")
	}
}

func TestPostgresBindingReleaseAfterRuntimes(t *testing.T) {
	for _, keep := range []bool{false, true} {
		p, rc, platform, _ := setup(t)
		ready(t, p, rc)
		rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
		f := &sealBinding{}
		p.options.Bindings = f
		s := state(t, rc)
		request, err := postgresBindingRequest(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		s.BindingID = pgbinding.BindingID(request)
		if err := s.save(rc.Cluster); err != nil {
			t.Fatal(err)
		}
		done := false
		for range 100 {
			var err error
			done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
			if err != nil {
				t.Fatal(err)
			}
			if f.released {
				for _, m := range state(t, rc).Members {
					if platform.services[m.Name] != nil {
						t.Fatal("binding released before runtime deletion")
					}
				}
			}
			if done {
				break
			}
		}
		if !done || !f.released || f.keep != keep || f.releaseID != pgbinding.BindingID(request) {
			t.Fatal("binding release incomplete")
		}
	}
}

func TestAutoUnsealRecoveryInitializationNoUnseal(t *testing.T) {
	p, rc, _, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"key"}}}`)
	if err := putSecret(t.Context(), rc, SecretTransitToken, []byte("seal-token")); err != nil {
		t.Fatal(err)
	}
	ready(t, p, rc)
	material, err := readUnseal(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if !material.Recovery || material.SealType != "transit" || len(material.Shares) != 5 || material.Threshold != 3 {
		t.Fatal("recovery shares not stored")
	}
	if a.member("demom0").unseals != 0 {
		t.Fatal("auto-unseal received Shamir unseal call")
	}
}

func TestPostgresStorageDoesNotRequireRaftVoters(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	ca, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	p.options.Bindings = &sealBinding{c: pgbinding.Connection{BindingID: "binding", Host: "postgres.internal", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca}, CredentialGeneration: 1}}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	rc.Cluster.Generation++
	for _, b := range a.members {
		b.voter = false
		b.healthy = false
	}
	a.holdVoter = "demom1"
	ready(t, p, rc)
	for _, m := range state(t, rc).Members {
		if m.Sealed || !m.Healthy {
			t.Fatal("PostgreSQL HA peer required Raft membership")
		}
	}
}

func TestAWSSealExtractionRotationFingerprint(t *testing.T) {
	p, rc, _ := edgeFixture(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"awskms","awskms":{"region":"eu-west-1","kmsKeyId":"key","accessKeyId":"private-access","secretAccessKey":"private-secret"}}}`)
	values, err := p.ExtractWriteOnlyOptions(nil, &rc.Cluster.Spec)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range values {
		if err = putSecret(t.Context(), rc, key, value); err != nil {
			t.Fatal(err)
		}
	}
	o, _ := parseOptions(rc.Cluster.Spec.EngineOptions)
	seal, err := p.memberSeal(t.Context(), rc, o)
	if err != nil || seal.AWSKMS.AccessKey != "private-access" || seal.AWSKMS.SecretKey != "private-secret" {
		t.Fatal("AWS credentials not rendered", err)
	}
	if strings.Contains(string(rc.Cluster.Spec.EngineOptions), "private-") {
		t.Fatal("AWS credentials persisted")
	}
	before, err := p.sealCredentialFingerprint(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	old := rc.Cluster.Spec
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"awskms","awskms":{"region":"eu-west-1","kmsKeyId":"key","accessKeyId":"rotated-access","secretAccessKey":"rotated-secret"}}}`)
	values, err = p.ExtractWriteOnlyOptions(&old, &rc.Cluster.Spec)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range values {
		if err = putSecret(t.Context(), rc, key, value); err != nil {
			t.Fatal(err)
		}
	}
	after, err := p.sealCredentialFingerprint(t.Context(), rc)
	if err != nil || after == before {
		t.Fatal("credential rotation did not change restart fingerprint", err)
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"awskms","awskms":{"region":"eu-west-1","kmsKeyId":"key","accessKeyId":"only-one-key"}}}`)
	if _, err = p.ExtractWriteOnlyOptions(&old, &rc.Cluster.Spec); err == nil {
		t.Fatal("partial AWS rotation accepted")
	}
}

func TestAutoUnsealMemberConfigUsesSecretStore(t *testing.T) {
	p, rc, s := edgeFixture(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"key"}}}`)
	s.Members[0].EngineOptions = rc.Cluster.Spec.EngineOptions
	if err := putSecret(t.Context(), rc, SecretTransitToken, []byte("private-transit")); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, SecretManagementToken, []byte("never-management")); err != nil {
		t.Fatal(err)
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	cfg, err := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec baospec.MemberSpec
	if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Seal.Transit == nil || spec.Seal.Transit.Token != "private-transit" {
		t.Fatal("missing transit seal")
	}
	if strings.Contains(string(cfg.EngineSpec), "never-management") || strings.Contains(string(rc.Cluster.Internal), "private-transit") {
		t.Fatal("control-plane credential boundary violated")
	}
}

func TestSealExtractionRequiresCreationCredentials(t *testing.T) {
	p := New(ProviderOptions{})
	for _, raw := range []string{
		`{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"key"}}}`,
		`{"seal":{"type":"awskms","awskms":{"region":"eu-west-1","kmsKeyId":"key"}}}`,
	} {
		spec := domain.ClusterSpec{EngineOptions: json.RawMessage(raw)}
		if _, err := p.ExtractWriteOnlyOptions(nil, &spec); err == nil {
			t.Fatal("missing creation credential accepted")
		}
		old := spec
		if _, err := p.ExtractWriteOnlyOptions(&old, &spec); err != nil {
			t.Fatal("update requires resupplying credential", err)
		}
	}
}

func TestPostgresCleanupReleasesNeverEnsuredBinding(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	f := &sealBinding{}
	p.options.Bindings = f
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	request, err := postgresBindingRequest(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !f.released || f.releaseID != pgbinding.BindingID(request) || state(t, rc).BindingID != "" {
		t.Fatal("never-ensured binding was not released")
	}
	if !done {
		done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil || !done {
			t.Fatal("cleanup did not complete", err)
		}
	}
}

func TestPostgresStorageInitializedWithoutReceiptFailsClosed(t *testing.T) {
	p, rc, _, agents := setup(t)
	// Losing a stateless peer's local disk must not authorize a second init of
	// the already-initialized external database when CP material is missing.
	if err := p.ensureSecrets(t.Context(), rc, &internalState{}); err != nil {
		t.Fatal(err)
	}
	ca, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	p.options.Bindings = &sealBinding{c: pgbinding.Connection{Host: "postgres.internal", Port: 5432, Username: "owner", PasswordSecretRef: "password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: ca}, CredentialGeneration: 1}}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	agents.initWithoutReceipt = true
	lost := false
	for range 100 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if result.ConvergenceError != nil {
			if !strings.Contains(result.ConvergenceError.Error(), "InitializationOutcomeLost") {
				t.Fatal(result.ConvergenceError)
			}
			lost = true
			break
		}
	}
	if !lost || state(t, rc).InitializationOutcomeLost == "" {
		t.Fatal("initialized PostgreSQL storage without a receipt was not fenced")
	}
	requests := agents.member("demom0").initRequests
	p = New(p.options)
	agents.p = p
	for range 5 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil || result.ConvergenceError == nil {
			t.Fatal("lost PostgreSQL initialization did not remain terminal", err)
		}
	}
	if agents.member("demom0").initRequests != requests {
		t.Fatal("initialized PostgreSQL storage was initialized again")
	}
}
