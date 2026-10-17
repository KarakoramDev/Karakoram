// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type publicAccessBackend struct{ *fakeBackend }

func (publicAccessBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv4: "192.0.2.30", PublicIPv6: "2001:db8::30"}, nil
}

func TestProxyManagedUsersTLSModes(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.allocate(rc.Cluster)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "access/roles", []byte(`["app"]`)); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, rolePasswordKey("app"), []byte("test-app-password")); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		t.Run(mode, func(t *testing.T) {
			rc.Cluster.Spec.TLS.Mode = mode
			config, err := p.MemberConfig(t.Context(), rc, "demopx")
			if err != nil {
				t.Fatal(err)
			}
			var spec pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.Proxy == nil || len(spec.Proxy.Users) != 2 || !spec.Proxy.BackendTLS {
				t.Fatal("proxy lost app/system users or backend TLS")
			}
			for _, user := range spec.Proxy.Users {
				if user.RequireTLS != (mode == "requireTLS") {
					t.Fatal("frontend user TLS requirement differs from mode")
				}
			}
			if (spec.Proxy.ServerCert != nil) != (mode != "disabled") {
				t.Fatal("frontend certificate differs from mode")
			}
			data, err := p.MemberConfig(t.Context(), rc, "demop0")
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.Postgres.RequireTLS != (mode == "requireTLS") || spec.ServerCert == nil {
				t.Fatal("member HBA TLS requirement differs from mode or internal TLS was removed")
			}
		})
	}
}

func TestAccessConnectionDiscovery(t *testing.T) {
	p, rc, backend := setup(t, "single")
	rc.Backend = publicAccessBackend{backend}
	rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
	for key, value := range map[string]string{"users/postgres": "superuser-test-secret", "tls/ca.crt": "public-test-ca"} {
		if err := putSecret(t.Context(), rc, key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	connection, err := p.Connection(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if connection.Connection.Hosts[0] != "demopx:5432" || connection.Connection.PublicHosts[0] != "192.0.2.30:5432" || connection.CACertificate != "public-test-ca" {
		t.Fatal("connection is not proxy/app oriented with public endpoint and CA")
	}
	uri, err := url.Parse(connection.Connection.URI)
	if err != nil || uri.User != nil || uri.Query().Get("sslmode") != "verify-full" {
		t.Fatal("password-free discovery is not TLS-verifying", err)
	}
	if len(connection.Credentials) != 1 || connection.Credentials[0].Username != "postgres" {
		t.Fatal("credential-bearing view must contain the explicitly privileged account")
	}
	if !connection.Connection.Endpoints[1].Public {
		t.Fatal("public endpoint is not marked public")
	}
	users, err := p.SystemUsers(t.Context(), rc)
	if err != nil || len(users) != len(systemNames) {
		t.Fatal("system roles not listed", err)
	}
}
