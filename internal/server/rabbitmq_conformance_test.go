// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
)

func testRabbitMQNativeGETResponses(t *testing.T) {
	vhost := `{"name":"/","default_queue_type":"quorum","description":"","tags":null}`
	user := `{"name":"app","tags":null,"password_hash":"private-hash","hashing_algorithm":"rabbit_password_hashing_sha256"}`
	permission := `{"vhost":"/","user":"app","configure":"","write":"","read":""}`
	topic := `{"vhost":"/","user":"app","exchange":"events","write":"","read":""}`
	policy := `{"vhost":"/","name":"app","pattern":".*","apply-to":"queues","priority":0,"definition":{"message-ttl":100}}`
	responses := map[string]string{
		"/api/overview": `{}`, "/api/vhosts": "[" + vhost + "]", "/api/vhosts/%2F": vhost,
		"/api/vhost-limits": `[]`, "/api/vhost-limits/%2F": `[]`,
		"/api/users": "[" + user + "]", "/api/users/app": user,
		"/api/permissions": "[" + permission + "]", "/api/permissions/%2F/app": permission,
		"/api/topic-permissions": "[" + topic + "]", "/api/topic-permissions/%2F/app": "[" + topic + "]",
		"/api/policies": "[" + policy + "]", "/api/policies/%2F/app": policy,
		"/api/operator-policies": "[" + policy + "]", "/api/operator-policies/%2F/app": policy,
		"/api/queues": `[{"name":"orders","vhost":"/","type":"quorum","members":["rabbit@brokersm0"],"leader":"rabbit@brokersm0"}]`,
	}
	s, token := nativeRabbitMQServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.EscapedPath()]
		if !ok {
			t.Errorf("unexpected native GET %s", r.URL.EscapedPath())
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	cluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "brokers"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.Status.Connection.PublicHosts = []string{"broker.example:5672"}
	if err := s.Store.UpdateClusterState(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	completed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	backup := domain.Backup{Name: "saved", Cluster: "brokers", Provider: "rabbitmq", Storage: "archive", Type: "definitions", Trigger: "manual", Phase: domain.PhaseSucceeded, CompletedAt: &completed}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"rabbitmq/vhosts", "rabbitmq/vhosts/%2F", "rabbitmq/users", "rabbitmq/users/app", "rabbitmq/permissions", "rabbitmq/permissions/%2F/app", "rabbitmq/topic-permissions", "rabbitmq/topic-permissions/%2F/app", "rabbitmq/policies", "rabbitmq/policies/%2F/app", "rabbitmq/operator-policies", "rabbitmq/operator-policies/%2F/app", "connection", "backups", "backups/saved"} {
		t.Run(route, func(t *testing.T) {
			w := call(s, "GET", "/api/v1/clusters/brokers/"+route, "", token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "private-hash") || strings.Contains(w.Body.String(), `"tags":null`) {
				t.Fatal("native response was not normalized", w.Body.String())
			}
			if route == "connection" && (!strings.Contains(w.Body.String(), `"database":"/"`) || !strings.Contains(w.Body.String(), "broker.example:5672")) {
				t.Fatal(w.Body.String())
			}
		})
	}
	// Queue inventory is a native Management client contract, not a public
	// Karakoram GET route. Exercise its actual wire shape without inventing one.
	c, err := mgmt.New("http://"+s.namespaces.(rabbitMQAccessNamespaces).address, "karakoram", "control-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	queues, err := c.Queues(t.Context())
	if err != nil || len(queues) != 1 || queues[0].Vhost != "/" || queues[0].Type != "quorum" || len(queues[0].Members) != 1 {
		t.Fatal(queues, err)
	}
	encoded, err := json.Marshal(queues)
	if err != nil || !strings.Contains(string(encoded), `"name":"orders"`) {
		t.Fatal(string(encoded), err)
	}
}
