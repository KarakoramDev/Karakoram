// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"testing"
)

func TestTokenRestrictionsRoundTrip(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "POST", "/api/v1/api-tokens", `{"name":"scoped","restrictions":{"readOnly":true,"namespaces":["default"],"permissions":["clusters:read"]}}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var created api.CreatedAPIToken
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/api-tokens/" + created.Token.Id
	for _, method := range []string{"GET", "PUT", "GET"} {
		body := ""
		if method == "PUT" {
			body = `{"name":"renamed"}`
		}
		w = call(s, method, path, body, token)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var got api.APIToken
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Restrictions == nil || got.Restrictions.ReadOnly == nil || !*got.Restrictions.ReadOnly || got.Restrictions.Namespaces == nil || (*got.Restrictions.Namespaces)[0] != "default" {
			t.Fatal(w.Body.String())
		}
	}
	w = call(s, "PUT", path, `{"name":"renamed","restrictions":{"readOnly":false}}`, token)
	if w.Code != 400 {
		t.Fatal("update accepted restrictions", w.Code, w.Body.String())
	}
	stored, err := s.Store.GetApiToken(t.Context(), created.Token.Id)
	if err != nil || stored.Restrictions == nil || !stored.Restrictions.ReadOnly {
		t.Fatal(stored, err)
	}
}
