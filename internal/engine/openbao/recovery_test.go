// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func assertRecoveryTLSBoundary(t *testing.T, p *Provider, rc *engine.Context, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	recovery, err := p.recoveryClient(ctx, rc, name)
	if err != nil {
		t.Fatal(err)
	}
	if err = recovery.Do(ctx, http.MethodDelete, "sys/generate-root/attempt", nil, nil); err != nil {
		t.Fatal("dedicated recovery client could not reach native shares-based recovery", err)
	}
	api, err := p.nativeClient(ctx, rc, name, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct{ method, path string }{
		{http.MethodDelete, "sys/generate-root/attempt"},
		{http.MethodPost, "sys/generate-root/update"},
	} {
		if err = api.Do(ctx, route.method, route.path, map[string]any{}, nil); !baoapi.IsStatus(err, http.StatusMethodNotAllowed) {
			t.Fatalf("ordinary API enabled legacy recovery route %s %s: %v", route.method, route.path, err)
		}
	}
	member, err := memberCertificate(ctx, rc, "tls/members/"+name+".pem")
	if err != nil {
		t.Fatal(err)
	}
	memberPair, err := tls.X509KeyPair([]byte(member.CertPEM), []byte(member.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	foreignCA, err := pki.NewCA("other-cluster-recovery", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	crt, key, err := foreignCA.Issue(pki.CertRequest{CommonName: "foreign-control-plane", Client: true, Validity: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	foreignPair, err := tls.X509KeyPair(crt, key)
	if err != nil {
		t.Fatal(err)
	}
	for label, certificate := range map[string]*tls.Certificate{"missing-client": nil, "ordinary-member": &memberPair, "other-cluster": &foreignPair} {
		client, err := p.memberAPIClient(ctx, rc, name, "", baospec.RecoveryPort, certificate)
		if err != nil {
			t.Fatal(err)
		}
		err = client.Do(ctx, http.MethodDelete, "sys/generate-root/attempt", nil, nil)
		var nativeError *baoapi.Error
		if err == nil || errors.As(err, &nativeError) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s was not rejected at the recovery TLS handshake: %v", label, err)
		}
	}
	if err = recovery.Do(ctx, http.MethodDelete, "sys/generate-root/attempt", nil, nil); err != nil {
		t.Fatal("recovery listener did not remain available after negative TLS probes", err)
	}
}

func TestRecoveryListenerRequiresIndependentControlPlaneCertificate(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	assertRecoveryTLSBoundary(t, p, rc, ordered(state(t, rc))[0].Name)
}

func TestRecoveryCredentialsStayOutOfMemberConfiguration(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	caBefore, err := secret(t.Context(), rc, secretRecoveryCA)
	if err != nil {
		t.Fatal(err)
	}
	clientBefore, err := secret(t.Context(), rc, secretRecoveryClient)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ensureRecoveryPKI(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	caAfter, _ := secret(t.Context(), rc, secretRecoveryCA)
	clientAfter, _ := secret(t.Context(), rc, secretRecoveryClient)
	if !bytes.Equal(caBefore, caAfter) || !bytes.Equal(clientBefore, clientAfter) {
		t.Fatal("current recovery PKI was needlessly regenerated")
	}
	client, err := memberCertificate(t.Context(), rc, secretRecoveryClient)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := memberCertificate(t.Context(), rc, secretRecoveryCA)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ordered(state(t, rc)) {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec baospec.MemberSpec
		if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.RecoveryCAPEM != ca.CertPEM || strings.Contains(spec.RecoveryCAPEM, "PRIVATE KEY") {
			t.Fatal("member did not receive only the dedicated public recovery CA")
		}
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, sensitive := range []string{client.CertPEM, client.KeyPEM, ca.KeyPEM} {
			encoded, _ := json.Marshal(sensitive)
			if bytes.Contains(raw, encoded[1:len(encoded)-1]) {
				t.Fatal("control-plane recovery identity leaked into member configuration")
			}
		}
	}
}

type unavailableRecoveryDialer struct {
	engine.ContextDialer
	err      error
	attempts int
}

func (d *unavailableRecoveryDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if port == "8202" {
		d.attempts++
		return nil, d.err
	}
	return d.ContextDialer.DialContext(ctx, network, address)
}

func TestRecoveryTransportFailureNeverFallsBackToOrdinaryAPI(t *testing.T) {
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	agents.tokenValid = false
	blocked := errors.New("private recovery transport unavailable")
	dialer := &unavailableRecoveryDialer{ContextDialer: rc.Dialer, err: blocked}
	rc.Dialer = dialer
	generated := agents.generated
	_, err := p.ensureManagementToken(t.Context(), rc, state(t, rc))
	if !errors.Is(err, blocked) || dialer.attempts != 1 || agents.generated != generated {
		t.Fatalf("private recovery failure did not fail closed: attempts=%d generated=%d err=%v", dialer.attempts, agents.generated-generated, err)
	}
}

func TestRecoveryPortIsNotPublishedToCustomers(t *testing.T) {
	p, rc, infra, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"192.0.2.0/24"}
	s := state(t, rc)
	for range len(s.Members) + 1 {
		busy, err := p.reconcileExposure(t.Context(), rc, s)
		if err != nil {
			t.Fatal(err)
		}
		if !busy {
			break
		}
	}
	if len(infra.publicPorts) != len(s.Members) {
		t.Fatal("did not exercise public API routing")
	}
	for _, route := range infra.publicPorts {
		if route.TargetPort != baospec.APIPort {
			t.Fatal("a private member listener became a public route")
		}
	}
	details, err := p.Connection(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(details)
	if bytes.Contains(raw, []byte(":8202")) || bytes.Contains(raw, []byte("recovery")) {
		t.Fatal("private recovery identity or endpoint was published in customer connection details")
	}
}

func TestRecoveryPKICleanupFollowsRetainedVolumes(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "retain"}[keep], func(t *testing.T) {
			p, rc, _, _ := setup(t)
			ready(t, p, rc)
			done := false
			for range 100 {
				var err error
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done {
				t.Fatal("cleanup did not complete")
			}
			for _, key := range []string{secretRecoveryCA, secretRecoveryClient} {
				bundle, err := secret(t.Context(), rc, key)
				if keep {
					if err != nil || len(bundle) == 0 {
						t.Fatalf("retained cluster lost recovery PKI %s: %v", key, err)
					}
				} else if !missing(err) {
					t.Fatalf("deleted cluster retained recovery PKI %s: %v", key, err)
				}
			}
		})
	}
}
