// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRabbitMQAuditIdentifiesNativeObjects(t *testing.T) {
	for _, tc := range []struct{ resource, operation, body, suffix, target string }{
		{"vhosts", "Vhost", `{"name":"/","tags":[],"description":"private-body-marker"}`, "/%2F", "prod//"},
		{"users", "User", `{"name":"app","tags":[],"password":"private-body-marker"}`, "/app", "prod/app"},
		{"permissions", "Permission", `{"vhost":"/","user":"app","configure":"private-body-marker","write":"","read":""}`, "/%2F/app", "prod///app"},
		{"topic-permissions", "TopicPermission", `{"vhost":"/","user":"app","exchange":"private-body-marker","write":"","read":""}`, "/%2F/app", "prod///app"},
		{"policies", "Policy", `{"vhost":"/","name":"app","pattern":"private-body-marker","applyTo":"queues","priority":0,"definition":{}}`, "/%2F/app", "prod///app"},
		{"operator-policies", "OperatorPolicy", `{"vhost":"/","name":"app","pattern":"private-body-marker","applyTo":"queues","priority":0,"definition":{}}`, "/%2F/app", "prod///app"},
	} {
		t.Run(tc.resource, func(t *testing.T) {
			s, _, token := testServer(t)
			s.Registry, _ = engine.NewRegistry(&fakeRabbitMQAccess{})
			for _, verb := range []struct {
				method, operation string
				status            int
			}{{"POST", "Create", 201}, {"PUT", "Replace", 200}, {"DELETE", "Delete", 204}} {
				path, body := "/api/v1/clusters/prod/rabbitmq/"+tc.resource, tc.body
				if verb.method != "POST" {
					path += tc.suffix
				}
				if verb.method == "DELETE" {
					body = ""
				}
				w := call(s, verb.method, path, body, token)
				if w.Code != verb.status {
					t.Fatal(w.Code, w.Body.String())
				}
				reason := verb.operation + "RabbitMQ" + tc.operation + "Succeeded"
				events := auditEvents(t, s, reason)
				if len(events) != 1 {
					t.Fatal(reason, events)
				}
				e := events[0]
				if e.Target != tc.target || e.Resource != "rabbitmq-"+tc.resource || e.Namespace != "default" || e.Cluster != "prod" || e.Actor == "" {
					t.Fatalf("unexpected audit: %+v", e)
				}
				encoded, err := json.Marshal(e)
				if err != nil || strings.Contains(string(encoded), "private-body-marker") || strings.Contains(string(encoded), "generated-secret") {
					t.Fatal("audit leaked body", string(encoded), err)
				}
			}
			w := call(s, "GET", "/api/v1/events?resource=rabbitmq-"+tc.resource+"&cluster=prod", "", token)
			if w.Code != 200 || !strings.Contains(w.Body.String(), tc.target) {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}
