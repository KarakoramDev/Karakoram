// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestAdditionalRBACBeforeReplay(t *testing.T) {
	for id, extra := range api.OperationAdditionalRBAC {
		if handlerAdditionalRBAC(id) {
			continue
		}
		t.Run(id, func(t *testing.T) {
			s, _, _ := testServer(t)
			ctx := t.Context()
			if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "extra-manager"}, "integration-account-password"); err != nil {
				t.Fatal(err)
			}
			token, _, err := s.Auth.CreateToken(ctx, "extra-manager", "test", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			rule := api.OperationRBAC[id]
			policy := s.RBAC.Policy() + "\np, user:extra-manager, " + rule.Resource + ", " + rule.Action + ", default/prod, allow\n"
			if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
				t.Fatal(err)
			}
			called := false
			next := func(context.Context, http.ResponseWriter, *http.Request, any) (any, error) {
				called = true
				return ok(map[string]string{"result": "authorized"}), nil
			}
			invoke := func() response {
				r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters/prod", nil)
				r.SetPathValue("cluster", "prod")
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("Idempotency-Key", "extra-permission")
				result, err := s.middleware(next, strings.ToUpper(id[:1])+id[1:])(ctx, httptest.NewRecorder(), r, nil)
				if err != nil {
					t.Fatal(err)
				}
				out := httptest.NewRecorder()
				resp := result.(response)
				if err := resp.write(out); err != nil {
					t.Fatal(err)
				}
				resp.status = out.Code
				return resp
			}
			if got := invoke(); got.status != http.StatusForbidden || called {
				t.Fatalf("missing additional grant reached execution/replay: %+v, called=%t", got, called)
			}
			// A grant on another cluster cannot authorize this resource.
			if err := s.RBAC.ReplacePolicy(ctx, policy+"p, user:extra-manager, "+extra.Resource+", "+extra.Action+", default/other, allow\n"); err != nil {
				t.Fatal(err)
			}
			if got := invoke(); got.status != http.StatusForbidden || called {
				t.Fatalf("wrong-object grant accepted: %+v", got)
			}
			if err := s.RBAC.ReplacePolicy(ctx, policy+"p, user:extra-manager, "+extra.Resource+", "+extra.Action+", default/prod, allow\n"); err != nil {
				t.Fatal(err)
			}
			if got := invoke(); got.status != http.StatusOK || !called {
				t.Fatalf("same-object grant rejected: %+v", got)
			}
			// The original completed response is cached, but revoking the
			// additional grant must deny a retry before that cache is read.
			if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
				t.Fatal(err)
			}
			called = false
			if got := invoke(); got.status != http.StatusForbidden || called {
				t.Fatalf("revoked additional grant exposed replay: %+v", got)
			}
		})
	}
}

func TestAdditionalRBACConditionalInventory(t *testing.T) {
	// These operations retain body/state-dependent or cross-object checks in
	// their owning handlers. New generated requirements default to middleware.
	conditional := map[string]bool{
		"createPostgresRole": true, "updatePostgresRole": true, "patchPostgresRole": true, "rotatePostgresRolePassword": true,
		"createRabbitMQUser": true, "replaceRabbitMQUser": true,
		"createWeaviateUser": true, "updateWeaviateUser": true, "rotateWeaviateUserKey": true,
		"createOpenBaoDatabaseConnection": true, "deleteOpenBaoDatabaseConnection": true,
		"listEvents": true, "streamEvents": true,
	}
	for id := range api.OperationAdditionalRBAC {
		if got := handlerAdditionalRBAC(id); got != conditional[id] {
			t.Errorf("%s: handler enforcement = %t", id, got)
		}
		delete(conditional, id)
	}
	if len(conditional) != 0 {
		t.Fatalf("conditional operations missing generated requirements: %v", conditional)
	}
}
