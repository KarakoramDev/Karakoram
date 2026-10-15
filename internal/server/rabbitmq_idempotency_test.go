// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRabbitMQAccessIdempotency(t *testing.T) {
	for _, tc := range []struct {
		resource, object, body, changed string
	}{
		{"vhosts", "%2F", `{"name":"/","tags":[]}`, `{"name":"/","tags":["blue"]}`},
		{"users", "app", `{"name":"app","tags":[]}`, `{"name":"app","tags":["management"]}`},
		{"permissions", "%2F/app", `{"vhost":"/","user":"app","configure":"","write":"","read":""}`, `{"vhost":"/","user":"app","configure":"","write":"","read":"^orders$"}`},
		{"topic-permissions", "%2F/app", `{"vhost":"/","user":"app","exchange":"events","write":"","read":""}`, `{"vhost":"/","user":"app","exchange":"events","write":"","read":"^orders$"}`},
		{"policies", "%2F/app", `{"vhost":"/","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{"message-ttl":100}}`, `{"vhost":"/","name":"app","pattern":".*","applyTo":"queues","priority":2,"definition":{"message-ttl":100}}`},
		{"operator-policies", "%2F/app", `{"vhost":"/","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{"message-ttl":100}}`, `{"vhost":"/","name":"app","pattern":".*","applyTo":"queues","priority":2,"definition":{"message-ttl":100}}`},
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			t.Run(tc.resource+"/"+method, func(t *testing.T) {
				s, token := namespaceServer(t)
				provider := &fakeRabbitMQAccess{}
				var err error
				s.Registry, err = engine.NewRegistry(provider)
				if err != nil {
					t.Fatal(err)
				}
				path := "/api/v1/clusters/prod/rabbitmq/" + tc.resource
				status := http.StatusCreated
				if method == http.MethodPut {
					path += "/" + tc.object
					status = http.StatusOK
				}
				first := idempotentCall(s, method, path, tc.body, token, "access-request")
				provider.last = ""
				replayed := idempotentCall(s, method, path+"?namespace=default", " \n"+tc.body+"\n", token, "access-request")
				assertReplay(t, first, replayed, status)
				if provider.last != "" {
					t.Fatal("replay invoked the access provider", provider.last)
				}
				if tc.resource == "users" && replayed.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("replayed user response can be cached")
				}
				conflict := idempotentCall(s, method, path, tc.changed, token, "access-request")
				assertIdempotencyConflict(t, conflict, domain.ReasonConflict)
				if provider.last != "" {
					t.Fatal("conflicting reuse invoked the access provider", provider.last)
				}
				other := idempotentCall(s, method, path+"?namespace=dev", tc.changed, token, "access-request")
				if other.Code != status || other.Header().Get("Idempotent-Replayed") != "" || provider.last == "" {
					t.Fatal("different namespace shared a key scope", other.Code, other.Body.String(), provider.last)
				}
			})
		}
	}
}

func TestRabbitMQNativeCreationReplaysWithoutBrokerWrites(t *testing.T) {
	for _, resource := range []string{"vhosts", "users"} {
		t.Run(resource, func(t *testing.T) {
			writes, reads := 0, 0
			s, token := nativeRabbitMQServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/api/overview" {
					fmt.Fprint(w, `{}`)
					return
				}
				if r.URL.Path != "/api/"+resource+"/app" {
					t.Errorf("unexpected broker request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				switch r.Method {
				case http.MethodGet:
					reads++
					if writes == 0 {
						w.WriteHeader(http.StatusNotFound)
						fmt.Fprint(w, `{"error":"not_found","reason":"Not Found"}`)
					} else {
						fmt.Fprint(w, `{"name":"app","tags":[]}`)
					}
				case http.MethodPut:
					writes++
					w.WriteHeader(http.StatusCreated)
				default:
					t.Errorf("unexpected broker method: %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			path := "/api/v1/clusters/brokers/rabbitmq/" + resource
			body := `{"name":"app","tags":[]}`
			first := idempotentCall(s, http.MethodPost, path, body, token, "native-request")
			replayed := idempotentCall(s, http.MethodPost, path, body, token, "native-request")
			assertReplay(t, first, replayed, http.StatusCreated)
			if resource == "users" {
				var user struct{ GeneratedPassword string }
				if err := json.Unmarshal(first.Body.Bytes(), &user); err != nil || user.GeneratedPassword == "" {
					t.Fatal("missing generated password", err)
				}
			}
			changed := strings.Replace(body, `"tags":[]`, `"tags":["management"]`, 1)
			assertIdempotencyConflict(t, idempotentCall(s, http.MethodPost, path, changed, token, "native-request"), domain.ReasonConflict)
			if writes != 1 || reads != 1 {
				t.Fatalf("duplicate creation reached the broker: reads=%d writes=%d", reads, writes)
			}
		})
	}
}
