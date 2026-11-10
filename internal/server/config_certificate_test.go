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
		if err := s.issueConfigRevision(key, "inputs", "process", &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.PullRevision == "" || s.conditionalConfigRevision(key, "inputs", true) != cfg.PullRevision {
			t.Fatal("fresh payload certificate missing")
		}
		if s.conditionalConfigRevision(key, "other-inputs", true) != "" || s.conditionalConfigRevision(key, "inputs", false) != "" {
			t.Fatal("certificate authorized uncertain/different inputs")
		}
	}
	if len(s.configPulls) != maxConfigPullCertificates {
		t.Fatal("unbounded payload certificate cache", len(s.configPulls))
	}
	key := configPullKey{cluster: domain.ClusterRef{Name: "uncertain"}, member: "member"}
	cfg := agentapi.MemberConfig{Cluster: "uncertain", Member: "member"}
	if err := s.issueConfigRevision(key, "", "process", &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.PullRevision == "" || s.conditionalConfigRevision(key, "", true) != "" {
		t.Fatal("uncertain full response lost payload token or gained a false proof")
	}
}
