// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"strings"
	"testing"
)

func TestOpenBaoWriteOnlySealOptions(t *testing.T) {
	s, _, token := testServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(openbao.New(openbao.ProviderOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"name":"sealtest","spec":{"provider":"openbao","engineOptions":{"seal":{"type":"transit","transit":{"address":"https://transit.example","keyName":"unseal","token":"secret-transit-one"}}}}}`
	w := call(s, "POST", "/api/v1/clusters", body, token)
	if w.Code != 201 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "secret-transit-one") {
		t.Fatal("create echoed token")
	}
	ref := domain.ClusterRef{Name: "sealtest"}
	c, err := s.Store.GetCluster(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(c.Spec.EngineOptions), "secret-transit-one") {
		t.Fatal("persisted token")
	}
	value, err := s.Store.GetClusterSecret(t.Context(), ref, openbao.SecretTransitToken)
	if err != nil || string(value) != "secret-transit-one" {
		t.Fatal("missing secret", err)
	}
	w = call(s, "GET", "/api/v1/clusters/sealtest", "", token)
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret-transit-one") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "PATCH", "/api/v1/clusters/sealtest", `{"spec":{"engineOptions":{"seal":{"transit":{"token":"secret-transit-two"}}}}}`, token)
	if w.Code != 202 && w.Code != 200 {
		t.Fatalf("rotate %d %s", w.Code, w.Body.String())
	}
	value, err = s.Store.GetClusterSecret(t.Context(), ref, openbao.SecretTransitToken)
	if err != nil || string(value) != "secret-transit-two" {
		t.Fatal("rotation missing", err)
	}
	w = call(s, "GET", "/api/v1/clusters/sealtest", "", token)
	if strings.Contains(w.Body.String(), "secret-transit-two") {
		t.Fatal("GET echoed rotated token")
	}
}
