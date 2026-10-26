// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFixedSeedFormationHasNoStandaloneFallback(t *testing.T) {
	for _, node := range []string{"rabbit@seed", "rabbit@peer"} {
		s := testSpec()
		s.NodeName = node
		s.Peers = []string{"rabbit@seed", "rabbit@peer"}
		s.FormationSeed = "rabbit@seed"
		c := memberConfig(s)
		if _, err := spec(c); err != nil {
			t.Fatal(err)
		}
		files, _ := render("/data", s, c)
		for _, file := range files {
			switch file.name {
			case "rabbitmq.conf":
				if !strings.Contains(file.content, "cluster_formation.peer_discovery_backend = k8s\n") || !strings.Contains(file.content, "cluster_formation.k8s.seed_node = rabbit@seed\n") {
					t.Fatal("native discovery is not pinned to the designated seed")
				}
				if strings.Contains(file.content, "discovery_retry_limit") || strings.Contains(file.content, "classic_config") {
					t.Fatal("fixed-seed discovery must not retain classic fallback settings")
				}
			case "enabled_plugins":
				if !strings.Contains(file.content, "rabbitmq_peer_discovery_k8s") {
					t.Fatal("fixed-seed backend is not enabled before native boot")
				}
			}
		}
		before := restartFingerprint("/data", s, c)
		s.FormationSeed = ""
		if restartFingerprint("/data", s, memberConfig(s)) != before {
			t.Fatal("finishing initial formation restarts the joined broker")
		}
		s.FormationSeed = "rabbit@other"
		if _, err := spec(memberConfig(s)); err == nil {
			t.Fatal("accepted formation seed outside managed peers")
		}
		s.FormationSeed = "rabbit@seed\nlisteners.tcp=none"
		if _, err := spec(memberConfig(s)); err == nil {
			t.Fatal("accepted malformed formation seed")
		}
	}
}

func TestFormationCapabilityDoesNotRequireNativeBroker(t *testing.T) {
	var runner Runner
	mux := http.NewServeMux()
	runner.Routes(mux, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/rabbitmq/formation-capabilities", strings.NewReader("{}"))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"fixedSeed":true`) {
		t.Fatalf("capability unavailable without a broker: %d %s", response.Code, response.Body.String())
	}
}
