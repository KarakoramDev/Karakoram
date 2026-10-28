// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestRegeneratedCookieCredentialContract(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	if err := rc.Secrets.DeleteClusterSecret(t.Context(), rc.Cluster.Ref(), "erlang-cookie"); err != nil {
		t.Fatal(err)
	}
	recorded := enginetest.RecordCredentials(rc)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	recorded.AssertValues(t, map[string]enginetest.CredentialLimit{"erlang-cookie": generatedCookieLimit}, 0)
	cfg, err := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec rmqspec.MemberSpec
	if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	generatedCookieLimit.Assert(t, "agent cookie", spec.Cookie)
	if spec.Cookie != string(recorded.Values["erlang-cookie"]) {
		t.Fatal("agent cookie differs from regenerated persisted cookie")
	}
}
