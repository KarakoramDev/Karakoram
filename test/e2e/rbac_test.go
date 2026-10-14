//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRBAC(t *testing.T) {
	t.Run("Accounts", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage()
		spec := s.privateSpec(1)
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		var original api.RBACPolicy
		s.expect(&original)(s.client.GetRBACPolicy(s.ctx))
		s.t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s.expect(nil)(s.client.ReplaceRBACPolicy(ctx, &api.ReplaceRBACPolicyParams{}, original))
		})
		viewer, passwordV := s.account()
		operator, passwordO := s.account()
		policy := original.Policy + "\ng, user:" + viewer + ", role:viewer\ng, user:" + operator + ", role:operator\n"
		s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy}))
		view := s.accountToken(viewer, passwordV)
		operate := s.accountToken(operator, passwordO)
		var clusters api.ClusterList
		s.expect(&clusters)(view.ListClusters(s.ctx, &api.ListClustersParams{}))
		visible := false
		for _, c := range clusters.Items {
			if c.Name == name {
				visible = true
			}
		}
		if !visible {
			t.Fatal("viewer cannot read test cluster")
		}
		s.expect(nil, 403)(view.GetClusterConnection(s.ctx, name, nil))
		s.expect(nil, 403)(view.DeleteCluster(s.ctx, name, &api.DeleteClusterParams{}))
		s.expect(nil, 403)(operate.ListAccounts(s.ctx, &api.ListAccountsParams{}))
		s.expect(nil, 403)(operate.CreateAccount(s.ctx, &api.CreateAccountParams{}, api.CreateAccountRequest{Name: s.name(), Password: ptr(s.name() + s.name())}))
		var b api.Backup
		s.expect(&b, 202)(operate.CreateBackup(s.ctx, name, &api.CreateBackupParams{}, api.CreateBackupRequest{Name: s.name(), Storage: storage, Type: api.CreateBackupRequestTypeLogical}))
		s.waitBackup(name, b.Name)
		subject := "user:" + viewer
		var can api.CanResponse
		request := api.CanRequest{Subject: &subject, Resource: "cluster-credentials", Action: api.Read, Object: name}
		s.expect(&can)(s.client.CheckPermission(s.ctx, &api.CheckPermissionParams{}, request))
		if can.Allowed {
			t.Fatal("viewer can reveal credentials before policy grant")
		}
		policy += "p, " + subject + ", cluster-credentials, read, " + name + ", allow\n"
		s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy}))
		s.expect(&can)(s.client.CheckPermission(s.ctx, &api.CheckPermissionParams{}, request))
		if !can.Allowed {
			t.Fatal("policy replacement did not grant credential read")
		}
		var connection api.Connection
		s.expect(&connection)(view.GetClusterConnection(s.ctx, name, nil))
		if len(connection.Credentials) == 0 {
			t.Fatal("credential reveal omitted credentials after grant")
		}
		scoped, scopedPassword := s.account()
		scopedSubject := "user:" + scoped
		policy += "p, " + scopedSubject + ", clusters, read, " + name + ", allow\n"
		s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy}))
		restricted := s.accountToken(scoped, scopedPassword)
		s.expect(&clusters)(restricted.ListClusters(s.ctx, &api.ListClustersParams{}))
		if len(clusters.Items) != 1 || clusters.Items[0].Name != name {
			t.Fatal("resource-scoped cluster collection leaked/omitted resources")
		}
		var permitted api.Cluster
		s.expect(&permitted)(restricted.GetCluster(s.ctx, name, nil))
		s.expect(nil, 403)(restricted.GetCluster(s.ctx, s.name(), nil))
		s.expect(nil, 403)(restricted.GetClusterConnection(s.ctx, name, nil))
		policy += "p, " + scopedSubject + ", cluster-credentials, read, " + name + ", allow\n"
		s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy}))
		s.expect(&connection)(restricted.GetClusterConnection(s.ctx, name, nil))
		s.expect(nil, 403)(restricted.GetClusterConnection(s.ctx, s.name(), nil))
		policy += "p, " + scopedSubject + ", clusters, read, " + name + ", deny\n"
		s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy}))
		s.expect(nil, 403)(restricted.GetCluster(s.ctx, name, nil))
		s.expect(&clusters)(restricted.ListClusters(s.ctx, &api.ListClustersParams{}))
		if len(clusters.Items) != 0 {
			t.Fatal("explicit deny did not filter collection")
		}
	})
	t.Run("OIDC", func(t *testing.T) {
		issuer := os.Getenv("KARAKORAM_E2E_OIDC_ISSUER")
		if issuer == "" {
			t.Skip("set KARAKORAM_E2E_OIDC_ISSUER to the auto-approving test/oidc-issuer for full login")
		}
		s := newSuite(t, false)
		issuerURL, err := url.Parse(issuer)
		if err != nil || issuerURL.Host == "" {
			t.Fatal("invalid OIDC issuer URL")
		}
		r, err := s.client.GetAccount(s.ctx, "oidctest")
		if err != nil {
			t.Fatal("read existing OIDC account failed")
		}
		accountExisted := r.StatusCode == http.StatusOK
		s.expect(nil, 200, 404)(r, nil)
		if !accountExisted {
			t.Cleanup(func() {
				if s.keep {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				s.expect(nil, 204, 404)(s.client.DeleteAccount(ctx, "oidctest", &api.DeleteAccountParams{}))
			})
		}
		var original api.Settings
		s.expect(&original)(s.client.GetSettings(s.ctx))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s.expect(nil)(s.client.ReplaceSettings(ctx, &api.ReplaceSettingsParams{}, original))
		})
		clientID := os.Getenv("KARAKORAM_E2E_OIDC_CLIENT_ID")
		if clientID == "" {
			clientID = "karakoram-e2e"
		}
		configured := api.Settings{Oidc: api.OIDCConfig{Enabled: true, IssuerURL: &issuer, ClientId: &clientID, RedirectURL: ptr(s.base + "/api/v1/auth/oidc/callback"), Scopes: ptr([]string{"openid", "profile", "email"}), GroupsClaim: ptr("groups")}}
		// Preserve the deployed write-only client secret. The TEST ONLY issuer
		// accepts both public clients and basic authentication without checking
		// that secret, so no irreversible secret replacement is necessary.
		s.expect(nil)(s.client.ReplaceSettings(s.ctx, &api.ReplaceSettingsParams{}, configured))
		anonymous := s.newClient("", s.base+"/api/v1", nil)
		r, err = anonymous.OidcLogin(s.ctx, &api.OidcLoginParams{})
		if err != nil {
			t.Fatal("OIDC login transport failed")
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusFound {
			t.Fatalf("OIDC login HTTP %d want=302", r.StatusCode)
		}
		destination, err := url.Parse(r.Header.Get("Location"))
		if err != nil || destination.Host != issuerURL.Host {
			t.Fatal("OIDC login did not redirect to configured test issuer")
		}
		q := destination.Query()
		if q.Get("client_id") != clientID || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("state") == "" {
			t.Fatal("OIDC redirect lacks authorization-code/PKCE parameters")
		}
		t.Log("OIDC login returned 302 to configured test issuer with PKCE")
		var policy api.RBACPolicy
		s.expect(&policy)(s.client.GetRBACPolicy(s.ctx))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s.expect(nil)(s.client.ReplaceRBACPolicy(ctx, &api.ReplaceRBACPolicyParams{}, policy))
		})
		s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy.Policy + "\ng, group:karakoram-admins, role:viewer\n"}))
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		browser := &http.Client{Timeout: 45 * time.Second, Jar: jar}
		response, err := browser.Get(s.base + "/api/v1/auth/oidc/login")
		if err != nil {
			t.Fatal("OIDC full login transport failed")
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("OIDC full login HTTP %d", response.StatusCode)
		}
		signedIn := s.newClient("", s.base+"/api/v1", browser)
		var user api.CurrentUser
		s.expect(&user)(signedIn.GetCurrentUser(s.ctx))
		if user.Name != "oidctest" || len(user.Groups) != 1 || user.Groups[0] != "karakoram-admins" {
			t.Fatal("OIDC callback did not establish the expected test identity/groups")
		}
		var can api.CanResponse
		s.expect(&can)(signedIn.CheckPermission(s.ctx, &api.CheckPermissionParams{XKarakoramCSRF: user.CsrfToken}, api.CanRequest{Resource: "clusters", Action: api.Read, Object: "demo"}))
		if !can.Allowed {
			t.Fatal("OIDC group binding did not grant viewer permission")
		}
		s.expect(nil, 403)(signedIn.GetRBACPolicy(s.ctx))
		t.Logf("OIDC callback established session user=%s groups=%v with group-based viewer permission", user.Name, user.Groups)
		s.expect(nil, 204)(signedIn.Logout(s.ctx, &api.LogoutParams{XKarakoramCSRF: user.CsrfToken}))
		s.expect(nil, 401)(signedIn.GetCurrentUser(s.ctx))
	})
}

func (s *suite) account() (string, string) {
	name, password := s.name(), s.name()+s.name()
	s.t.Cleanup(func() {
		if s.keep {
			s.t.Logf("KEEP account=%s", name)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.expect(nil, 204, 404)(s.client.DeleteAccount(ctx, name, &api.DeleteAccountParams{}))
	})
	var account api.Account
	s.expect(&account, 201)(s.client.CreateAccount(s.ctx, &api.CreateAccountParams{}, api.CreateAccountRequest{Name: name, Password: &password, Enabled: ptr(true), MustChangePassword: ptr(false)}))
	s.expect(&account)(s.client.GetAccount(s.ctx, name))
	if account.Name != name {
		s.t.Fatal("local account read mismatch")
	}
	s.expect(&account)(s.client.ReplaceAccount(s.ctx, name, &api.ReplaceAccountParams{}, api.UpdateAccountRequest{Enabled: true, MustChangePassword: false}))
	return name, password
}
func (s *suite) accountToken(name, password string) *api.Client {
	jar, err := cookiejar.New(nil)
	if err != nil {
		s.t.Fatal(err)
	}
	h := &http.Client{Timeout: 45 * time.Second, Jar: jar, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	session := s.newClient("", s.base+"/api/v1", h)
	var login api.Session
	s.expect(&login)(session.Login(s.ctx, &api.LoginParams{}, api.LoginRequest{Username: name, Password: &password}))
	if login.CsrfToken == nil {
		s.t.Fatal("login omitted CSRF token")
	}
	var created api.CreatedAPIToken
	s.expect(&created, 201)(session.CreateAPIToken(s.ctx, &api.CreateAPITokenParams{XKarakoramCSRF: login.CsrfToken}, api.CreateAPITokenRequest{Name: s.name()}))
	if created.Secret == nil || !strings.HasPrefix(*created.Secret, "kk_") {
		s.t.Fatal("API token creation omitted secret")
	}
	client := s.newClient(*created.Secret, s.base+"/api/v1", nil)
	var current api.CurrentUser
	s.expect(&current)(client.GetCurrentUser(s.ctx))
	if current.Name != name {
		s.t.Fatal("API token acts as wrong account")
	}
	var metadata api.APIToken
	s.expect(&metadata)(session.GetAPIToken(s.ctx, created.Token.Id))
	if metadata.Owner != name {
		s.t.Fatal("API token metadata owner mismatch")
	}
	s.expect(&metadata)(session.ReplaceAPIToken(s.ctx, created.Token.Id, &api.ReplaceAPITokenParams{XKarakoramCSRF: login.CsrfToken}, api.UpdateAPITokenRequest{Name: "renamed", ExpiresAt: nil}))
	var tokens api.APITokenList
	s.expect(&tokens)(session.ListAPITokens(s.ctx, &api.ListAPITokensParams{}))
	if len(tokens.Items) != 1 || tokens.Items[0].Id != created.Token.Id {
		s.t.Fatal("owner-scoped token inventory mismatch")
	}
	// Revoke the first token and prove the same bearer cannot authenticate.
	s.expect(nil, 204)(session.DeleteAPIToken(s.ctx, created.Token.Id, &api.DeleteAPITokenParams{XKarakoramCSRF: login.CsrfToken}))
	s.expect(nil, 401)(client.GetCurrentUser(s.ctx))
	s.expect(&created, 201)(session.CreateAPIToken(s.ctx, &api.CreateAPITokenParams{XKarakoramCSRF: login.CsrfToken}, api.CreateAPITokenRequest{Name: s.name()}))
	if created.Secret == nil || *created.Secret == "" {
		s.t.Fatal("replacement token omitted secret")
	}
	client = s.newClient(*created.Secret, s.base+"/api/v1", nil)
	return client
}
