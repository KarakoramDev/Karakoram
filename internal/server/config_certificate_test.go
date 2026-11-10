// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestConfigPayloadCertificateBoundedAndFailClosed(t *testing.T) {
	s := &Server{configPullState: &configPullState{}}
	for i := range maxConfigPullCertificates + 1 {
		key := configPullKey{cluster: domain.ClusterRef{Name: fmt.Sprintf("cluster%d", i)}, member: "member"}
		cfg := agentapi.MemberConfig{Cluster: key.cluster.Name, Member: key.member}
		if err := s.issueConfigRevision(key, "inputs", "process", "ack", &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.PullRevision == "" || s.conditionalConfigCertificate(key, "inputs", true).revision != cfg.PullRevision {
			t.Fatal("fresh payload certificate missing")
		}
		if s.conditionalConfigCertificate(key, "other-inputs", true).revision != "" || s.conditionalConfigCertificate(key, "inputs", false).revision != "" {
			t.Fatal("certificate authorized uncertain/different inputs")
		}
	}
	if len(s.configPulls) != maxConfigPullCertificates {
		t.Fatal("unbounded payload certificate cache", len(s.configPulls))
	}
	key := configPullKey{cluster: domain.ClusterRef{Name: "uncertain"}, member: "member"}
	cfg := agentapi.MemberConfig{Cluster: "uncertain", Member: "member"}
	if err := s.issueConfigRevision(key, "", "process", "", &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.PullRevision == "" || s.conditionalConfigCertificate(key, "", true).revision != "" {
		t.Fatal("uncertain full response lost payload token or gained a false proof")
	}
}
