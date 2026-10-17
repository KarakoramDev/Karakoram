// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package sbspec

// ProjectSecrets removes credentials not consumed by this component. Apply it
// before hashing or persisting a member configuration, not just before rendering.
func (s Spec) ProjectSecrets() Spec {
	all := s
	s.DBRoles = nil
	s.PatroniUsername, s.PatroniPassword = "", ""
	s.JWTSecret, s.JWTKeys, s.JWKS = "", "", ""
	s.AnonKey, s.ServiceRoleKey, s.PublishableKey, s.SecretKey = "", "", "", ""
	s.AnonAsymmetricJWT, s.ServiceAsymmetricJWT = "", ""
	s.Auth.SMTP = SMTP{}
	s.Storage = Storage{}
	s.Studio.DashboardUsername, s.Studio.DashboardPassword, s.Studio.MetaCryptoKey = "", "", ""
	s.SecretKeyBase, s.RealtimeDBEncKey, s.VaultEncKey = "", "", ""
	s.RealtimeAdminSecret = ""
	s.LogflarePublicToken, s.LogflarePrivateToken = "", ""
	s.Functions = nil
	if s.Component != "vector" && s.VectorSinkURL == "" {
		s.VectorSinkToken = ""
	}
	if NeedsDatabase(s.Component) && s.Topology == "ha" {
		s.PatroniUsername, s.PatroniPassword = all.PatroniUsername, all.PatroniPassword
	}
	roles := []string(nil)
	switch s.Component {
	case "auth":
		roles = []string{"supabase_auth_admin"}
		s.JWTSecret, s.JWTKeys = all.JWTSecret, all.JWTKeys
		s.Auth.SMTP = all.Auth.SMTP
	case "rest":
		roles = []string{"authenticator"}
		s.JWTSecret, s.JWKS = all.JWTSecret, all.JWKS
	case "realtime":
		roles = []string{"supabase_admin"}
		s.JWTSecret, s.JWKS, s.AnonKey = all.JWTSecret, all.JWKS, all.AnonKey
		s.SecretKeyBase, s.RealtimeDBEncKey = all.SecretKeyBase, all.RealtimeDBEncKey
		s.RealtimeAdminSecret = all.RealtimeAdminSecret
	case "storage":
		roles = []string{"supabase_storage_admin"}
		s.JWTSecret, s.JWKS = all.JWTSecret, all.JWKS
		s.AnonKey, s.ServiceRoleKey = all.AnonKey, all.ServiceRoleKey
		s.Storage = all.Storage
	case "meta":
		roles = []string{"postgres"}
		s.Studio.MetaCryptoKey = all.Studio.MetaCryptoKey
		s.ServiceRoleKey, s.ServiceAsymmetricJWT = all.ServiceRoleKey, all.ServiceAsymmetricJWT
	case "studio", "functions":
		roles = []string{"postgres"}
		s.JWTSecret = all.JWTSecret
		s.AnonKey, s.ServiceRoleKey = all.AnonKey, all.ServiceRoleKey
		s.PublishableKey, s.SecretKey = all.PublishableKey, all.SecretKey
		if s.Component == "studio" {
			s.Studio.MetaCryptoKey = all.Studio.MetaCryptoKey
			if s.Upstreams["analytics"] != "" {
				s.LogflarePrivateToken = all.LogflarePrivateToken
			}
		} else {
			s.JWKS, s.Functions = all.JWKS, all.Functions
		}
	case "pooler":
		roles = []string{"supabase_admin", "pgbouncer"}
		s.JWTSecret, s.SecretKeyBase, s.VaultEncKey = all.JWTSecret, all.SecretKeyBase, all.VaultEncKey
	case "analytics":
		roles = []string{"supabase_admin"}
		s.LogflarePublicToken, s.LogflarePrivateToken = all.LogflarePublicToken, all.LogflarePrivateToken
	case "vector":
		s.LogflarePublicToken = all.LogflarePublicToken
	case "gateway":
		s.AnonKey, s.ServiceRoleKey = all.AnonKey, all.ServiceRoleKey
		s.PublishableKey, s.SecretKey = all.PublishableKey, all.SecretKey
		s.AnonAsymmetricJWT, s.ServiceAsymmetricJWT = all.AnonAsymmetricJWT, all.ServiceAsymmetricJWT
		if s.Upstreams["studio"] != "" {
			s.Studio.DashboardUsername, s.Studio.DashboardPassword = all.Studio.DashboardUsername, all.Studio.DashboardPassword
		}
	}
	if len(roles) != 0 {
		s.DBRoles = make(map[string]string, len(roles))
		for _, role := range roles {
			s.DBRoles[role] = all.DBRoles[role]
		}
	}
	return s
}
