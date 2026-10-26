// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package sbspec

import (
	"reflect"
	"testing"
)

func TestProjectSecrets(t *testing.T) {
	roles := map[string][]string{
		"gateway": nil, "auth": {"supabase_auth_admin"}, "rest": {"authenticator"},
		"realtime": {"supabase_admin"}, "storage": {"supabase_storage_admin"},
		"imgproxy": nil, "meta": {"postgres"}, "studio": {"postgres"},
		"functions": {"postgres"}, "pooler": {"supabase_admin", "pgbouncer"},
		"analytics": {"supabase_admin"}, "vector": nil,
	}
	for _, component := range Components {
		t.Run(component, func(t *testing.T) {
			full := validSpec()
			full.Component = component
			full.Topology = "ha"
			full.DBRoles = map[string]string{}
			for _, role := range []string{"postgres", "supabase_auth_admin", "authenticator", "supabase_admin", "supabase_storage_admin", "pgbouncer"} {
				full.DBRoles[role] = "password-" + role
			}
			full.PatroniUsername, full.PatroniPassword = "patroni", "patroni-password"
			full.JWTKeys, full.JWKS = "private-jwk", "verification-jwk"
			full.PublishableKey, full.SecretKey = "publishable", "secret"
			full.AnonAsymmetricJWT, full.ServiceAsymmetricJWT = "anon-jwt", "service-jwt"
			full.Auth.SMTP.Password = "smtp-password"
			full.Storage = Storage{AccessKey: "s3-access", SecretKey: "s3-secret", ProtocolAccessKey: "protocol-access", ProtocolSecretKey: "protocol-secret"}
			full.Studio = Studio{DashboardUsername: "dashboard", DashboardPassword: "dashboard-password", MetaCryptoKey: "meta-crypto"}
			full.Upstreams = map[string]string{"studio": "studio.example:3000", "analytics": "analytics.example:4000"}
			full.SecretKeyBase, full.RealtimeDBEncKey, full.VaultEncKey = "secret-base", "db-encryption", "vault-encryption"
			full.RealtimeAdminSecret = "realtime-admin-secret"
			full.LogflarePublicToken, full.LogflarePrivateToken = "ingest", "analytics-private"
			full.VectorSinkToken = "sink-token"
			full.Functions = map[string]map[string]string{"hello": {"index.ts": "function-source"}}
			got := full.ProjectSecrets()
			var wantRoles map[string]string
			if len(roles[component]) > 0 {
				wantRoles = map[string]string{}
				for _, role := range roles[component] {
					wantRoles[role] = full.DBRoles[role]
				}
			}
			if !reflect.DeepEqual(got.DBRoles, wantRoles) {
				t.Fatal("incorrect projected database role credentials")
			}
			needsServiceKey := component == "gateway" || component == "auth" || component == "rest" || component == "storage" || component == "meta" || component == "studio" || component == "functions"
			if (got.ServiceRoleKey != "") != needsServiceKey || (needsServiceKey && got.ServiceRoleKey != full.ServiceRoleKey) {
				t.Fatal("service API credential over-distributed or missing")
			}
			if (got.Auth.SMTP.Password != "") != (component == "auth") {
				t.Fatal("SMTP password over-distributed or missing")
			}
			if (got.Storage.SecretKey != "") != (component == "storage") || (got.Storage.AccessKey != "") != (component == "storage") || (got.Storage.ProtocolSecretKey != "") != (component == "storage") {
				t.Fatal("S3 credentials over-distributed or missing")
			}
			if (got.JWTKeys != "") != (component == "auth") {
				t.Fatal("Auth private signing keys over-distributed or missing")
			}
			if (got.Studio.DashboardPassword != "") != (component == "gateway") {
				t.Fatal("dashboard credentials over-distributed or missing")
			}
			if (got.Studio.MetaCryptoKey != "") != (component == "meta" || component == "studio") {
				t.Fatal("meta encryption key over-distributed or missing")
			}
			if (got.SecretKeyBase != "") != (component == "realtime" || component == "pooler") {
				t.Fatal("BEAM secret over-distributed or missing")
			}
			if (got.RealtimeDBEncKey != "") != (component == "realtime") || (got.VaultEncKey != "") != (component == "pooler") {
				t.Fatal("service encryption key over-distributed or missing")
			}
			if (got.RealtimeAdminSecret != "") != (component == "realtime") {
				t.Fatal("Realtime administration secret over-distributed or missing")
			}
			if (got.LogflarePublicToken != "") != (component == "analytics" || component == "vector") || (got.LogflarePrivateToken != "") != (component == "analytics" || component == "studio") {
				t.Fatal("analytics token over-distributed or missing")
			}
			if (got.Functions != nil) != (component == "functions") {
				t.Fatal("function sources over-distributed or missing")
			}
			if (got.PatroniPassword != "") != NeedsDatabase(component) {
				t.Fatal("Patroni password over-distributed or missing")
			}
			if (got.VectorSinkToken != "") != (component == "vector") {
				t.Fatal("unused log sink token distributed")
			}
			if !reflect.DeepEqual(got, got.ProjectSecrets()) {
				t.Fatal("projection is not idempotent")
			}
			if full.DBRoles["postgres"] == "" || full.Auth.SMTP.Password == "" || full.Storage.SecretKey == "" {
				t.Fatal("projection mutated source secrets")
			}
			if component == "imgproxy" || component == "vector" || component == "gateway" {
				if got.DBRoles["postgres"] != "" || got.PatroniPassword != "" || got.JWTSecret != "" || got.JWTKeys != "" || got.JWKS != "" || got.Auth.SMTP.Password != "" || got.Storage.SecretKey != "" {
					t.Fatal("non-database component received database/signing/SMTP/S3 secrets")
				}
			}
		})
	}
}
