// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestAPITokenSelfServiceOwnership(t *testing.T) {
	s, _, admin := testServer(t)
	for _, name := range []string{"viewer", "operator"} {
		if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: name}, "test-password-long-enough"); err != nil {
			t.Fatal(err)
		}
	}
	viewer, _, err := s.Auth.CreateToken(t.Context(), "viewer", "initial", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, other, err := s.Auth.CreateToken(t.Context(), "operator", "other", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := call(s, "POST", "/api/v1/api-tokens", `{"name":"self-service"}`, viewer)
	if result.Code != 201 {
		t.Fatalf("create own token HTTP %d", result.Code)
	}
	var created api.CreatedAPIToken
	if err := json.Unmarshal(result.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Token.Owner != "viewer" || created.Secret == nil {
		t.Fatal("wrong token owner or missing one-time secret")
	}
	result = call(s, "GET", "/api/v1/api-tokens", "", viewer)
	var listed api.APITokenList
	if err := json.Unmarshal(result.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if result.Code != 200 || len(listed.Items) != 2 {
		t.Fatalf("own token list HTTP %d count %d", result.Code, len(listed.Items))
	}
	for _, token := range listed.Items {
		if token.Owner != "viewer" {
			t.Fatal("another owner's token disclosed")
		}
	}
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		body := ""
		if method == "PUT" {
			body = `{"name":"changed"}`
		}
		if r := call(s, method, "/api/v1/api-tokens/"+other.ID, body, viewer); r.Code != 403 {
			t.Fatalf("%s another owner's token HTTP %d", method, r.Code)
		}
	}
	for _, tc := range []struct {
		method, body string
		status       int
	}{{"GET", "", 200}, {"PUT", `{"name":"updated"}`, 200}, {"DELETE", "", 204}} {
		if r := call(s, tc.method, "/api/v1/api-tokens/"+created.Token.Id, tc.body, viewer); r.Code != tc.status {
			t.Fatalf("%s own token HTTP %d", tc.method, r.Code)
		}
	}
	if r := call(s, "GET", "/api/v1/api-tokens/"+other.ID, "", admin); r.Code != 200 {
		t.Fatalf("admin token read HTTP %d", r.Code)
	}
}
