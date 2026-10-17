// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"net/url"
	"strings"
	"testing"
)

func TestDatabaseTLSRendering(t *testing.T) {
	for _, component := range []string{"auth", "rest", "storage", "meta", "studio", "functions", "realtime", "pooler", "analytics"} {
		t.Run(component, func(t *testing.T) {
			s := testSpec(component)
			s.DatabaseTLS = true
			p, _, err := Render(s, "/data")
			if err != nil {
				t.Fatal(err)
			}
			environment := strings.Join(p.Env, "\n")
			switch component {
			case "realtime":
				if !contains(p.Env, "DB_SSL=true") || !contains(p.Env, "DB_SSL_CA_CERT=/data/karakoram/supabase/ca.pem") {
					t.Fatal("Realtime lacks TLS trust configuration")
				}
			case "pooler", "analytics":
				if !contains(p.Env, "KARAKORAM_DB_CA_FILE=/data/karakoram/supabase/ca.pem") || strings.Contains(environment, "sslmode=") || strings.Contains(environment, "ssl=true") {
					t.Fatal("Ecto TLS URL overrides native cluster CA configuration")
				}
			default:
				if !strings.Contains(environment, "sslmode=verify-ca") || !strings.Contains(environment, "sslrootcert=%2Fdata%2Fkarakoram%2Fsupabase%2Fca.pem") {
					t.Fatal("database URL lacks cluster CA verification")
				}
			}
		})
	}
}

func TestStudioMetadataConnectionTLS(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		mode := "disable"
		if enabled {
			mode = "verify-ca"
		}
		t.Run(mode, func(t *testing.T) {
			s := testSpec("studio")
			s.DatabaseTLS = enabled
			s.Database = "application database/?"
			s.DBRoles["postgres"] = "database-password"
			p, _, err := Render(s, "/data")
			if err != nil {
				t.Fatal(err)
			}
			env := map[string]string{}
			for _, entry := range p.Env {
				name, value, _ := strings.Cut(entry, "=")
				env[name] = value
			}
			// Match the pinned Studio helper before it AES-encrypts the URL.
			connection := "postgresql://" + env["POSTGRES_USER_READ_WRITE"] + ":" + env["POSTGRES_PASSWORD"] + "@" + env["POSTGRES_HOST"] + ":" + env["POSTGRES_PORT"] + "/" + env["POSTGRES_DB"]
			u, err := url.Parse(connection)
			if err != nil {
				t.Fatal(err)
			}
			if u.Path != "/"+s.Database || u.Query().Get("sslmode") != mode {
				t.Fatal("Studio metadata connection lost database identity or TLS mode")
			}
			wantCA := ""
			if enabled {
				wantCA = "/data/karakoram/supabase/ca.pem"
			}
			if u.Query().Get("sslrootcert") != wantCA || env["PG_META_CRYPTO_KEY"] != s.Studio.MetaCryptoKey {
				t.Fatal("Studio metadata connection lost TLS trust or encryption configuration")
			}
		})
	}
}

func TestPoolerNativeReleaseAndTenantCA(t *testing.T) {
	s := testSpec("pooler")
	s.DatabaseTLS = true
	p, files, err := Render(s, "/data")
	if err != nil {
		t.Fatal(err)
	}
	if !contains(p.Env, "NODE_IP=127.0.0.1") {
		t.Fatal("native release distribution requires a numeric node address")
	}
	seed := string(files["supabase/pooler.exs"])
	if !strings.Contains(seed, "{:ok, der} = Supavisor.Helpers.cert_to_bin(File.read!(path))") ||
		!strings.Contains(seed, "\"require_user\" => false") {
		t.Fatal("tenant CA must be DER while dynamic role authentication stays enabled")
	}
}
