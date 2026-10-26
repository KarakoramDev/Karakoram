// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type certificateReloadEngine struct{ *fakeEngine }

func (*certificateReloadEngine) SupportsCertificateReloadOnly() bool { return true }

func TestCertificateReloadCapabilityAndTransientCache(t *testing.T) {
	a, c, engine := fakeNode(t, "")
	a.engine = &certificateReloadEngine{engine}
	c.Maintenance = true
	c.CertificateReloadOnly = true
	c.ConfigVersion = 2
	observed := false
	engine.onApply = func(_ *Node, desired agentapi.MemberConfig) error {
		observed = desired.CertificateReloadOnly
		return nil
	}
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if !observed || a.config().CertificateReloadOnly {
		t.Fatal("transient certificate-only intent was lost before Apply or retained after acknowledgement")
	}
	cached, err := os.ReadFile(filepath.Join(a.o.DataDir, "karakoram", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var restored agentapi.MemberConfig
	if err := json.Unmarshal(cached, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.CertificateReloadOnly || restored.ConfigVersion != 2 {
		t.Fatal("cached restart configuration retained transient identity-only intent or lost revision")
	}
	response := requestNode(a, "GET", "/v1/status", "", "token")
	var status agentapi.Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.CertificateReloadOnly || status.ConfigVersion != 2 {
		t.Fatal("generic status did not advertise capability and acknowledged revision")
	}
}
