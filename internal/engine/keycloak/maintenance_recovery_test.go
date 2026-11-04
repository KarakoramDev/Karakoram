// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
	"testing"
	"time"
)

func TestMaintenanceConfigurationWaitDoesNotBlockBindingAndTransport(t *testing.T) {
	p, rc, bindings, secrets := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	config, err := p.MemberConfig(t.Context(), rc, s.Member)
	if err != nil {
		t.Fatal(err)
	}
	config.AgentTLS = agentapi.Certificate{}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	secrets.values["keycloak/config/"+s.Member] = raw
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"database":{"cluster":"pg"},"hostname":"https://pending.example.test","proxyHeaders":"xforwarded"}`)
	bindings.conn.CredentialGeneration++
	bindings.password = "rotated-test-password"
	converge(t, p, rc)
	config, err = p.MemberConfig(t.Context(), rc, s.Member)
	if err != nil {
		t.Fatal(err)
	}
	var spec kcspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.CredentialGeneration != bindings.conn.CredentialGeneration || spec.Password != bindings.password || config.AgentTLS.CertPEM == "" {
		t.Fatal("application wait blocked exempt binding/transport publication")
	}
	if spec.Hostname != "" || spec.ProxyHeaders != "" {
		t.Fatal("deferred application options leaked")
	}
}
