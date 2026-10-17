// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"bytes"
	"encoding/json"
	"net"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestCertificateStagingRecoversAfterStateWriteCrash(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		name := "generated"
		if supplied {
			name = "userProvided"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			s := converge(t, p, rc)
			old, err := secret(t.Context(), rc, "tls/servers/demop0.crt")
			if err != nil {
				t.Fatal(err)
			}
			if supplied {
				rc.Cluster.Spec.TLS.CA = "userProvided"
				crt, err := secret(t.Context(), rc, "tls/ca.crt")
				if err != nil {
					t.Fatal(err)
				}
				key, err := secret(t.Context(), rc, "tls/ca.key")
				if err != nil {
					t.Fatal(err)
				}
				ca, err := pki.LoadCA(crt, key)
				if err != nil {
					t.Fatal(err)
				}
				crt, key, err = ca.Issue(pki.CertRequest{
					CommonName: "demo", DNSNames: []string{"demop0", "demopx", "localhost"},
					IPs: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, Server: true, Client: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				for suffix, value := range map[string][]byte{".crt": crt, ".key": key} {
					if err := putSecret(t.Context(), rc, "tls/user"+suffix, value); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := ensureCertificates(t.Context(), rc, s, true); err != nil {
				t.Fatal(err)
			}
			pending, err := secret(t.Context(), rc, "tls/servers/demop0.pending.crt")
			if err != nil {
				t.Fatal(err)
			}
			// Simulate restart after the secrets commit but before saving s.
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			version := s.ConfigVersion
			if s.find("demop0").TLSPending {
				t.Fatal("fixture persisted the activation flag before the crash")
			}
			if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
				t.Fatal(err)
			}
			if !s.find("demop0").TLSPending || s.ConfigVersion <= version {
				t.Fatal("pending secrets did not recover activation and config revision")
			}
			version = s.ConfigVersion
			if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
				t.Fatal(err)
			}
			if s.ConfigVersion != version {
				t.Fatal("recovered activation continuously increments config revision")
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, "demop0")
			if err != nil {
				t.Fatal(err)
			}
			var spec pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.ServerCert.CertPEM != string(old) {
				t.Fatal("recovery activated the certificate before its rolling turn")
			}
			converge(t, p, rc)
			active, err := secret(t.Context(), rc, "tls/servers/demop0.crt")
			if err != nil || !bytes.Equal(active, pending) || bytes.Equal(active, old) {
				t.Fatal("rolling reconciliation did not activate the recovered pending certificate", err)
			}
			if _, err := secret(t.Context(), rc, "tls/servers/demop0.pending.crt"); !missing(err) {
				t.Fatal("recovered pending material was not consumed")
			}
		})
	}
}
