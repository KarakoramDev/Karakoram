// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
)

type fakeRabbitMQAccess struct {
	fakeProvider
	last string
	err  error
}

var _ rabbitmq.Access = (*fakeRabbitMQAccess)(nil)

func (f *fakeRabbitMQAccess) ListVhosts(context.Context, *engine.Context) ([]rabbitmq.Vhost, error) {
	f.last = "listVhosts"
	return []rabbitmq.Vhost{rabbitmq.Vhost{Name: "app", Tags: []string{}, DefaultQueueType: "quorum"}}, f.err
}
func (f *fakeRabbitMQAccess) GetVhost(ctx context.Context, rc *engine.Context, name string) (*rabbitmq.Vhost, error) {
	f.last = "getVhost"
	return &rabbitmq.Vhost{Name: "app", Tags: []string{}, DefaultQueueType: "quorum"}, f.err
}
func (f *fakeRabbitMQAccess) CreateVhost(ctx context.Context, rc *engine.Context, v rabbitmq.Vhost) error {
	f.last = "createVhost"
	return f.err
}
func (f *fakeRabbitMQAccess) UpdateVhost(ctx context.Context, rc *engine.Context, v rabbitmq.Vhost) error {
	f.last = "updateVhost"
	return f.err
}
func (f *fakeRabbitMQAccess) DeleteVhost(ctx context.Context, rc *engine.Context, name string) error {
	f.last = "deleteVhost"
	return f.err
}
func (f *fakeRabbitMQAccess) ListUsers(context.Context, *engine.Context) ([]rabbitmq.User, error) {
	f.last = "listUsers"
	return []rabbitmq.User{rabbitmq.User{Name: "app", Tags: []string{"management"}}}, f.err
}
func (f *fakeRabbitMQAccess) GetUser(ctx context.Context, rc *engine.Context, name string) (*rabbitmq.User, error) {
	f.last = "getUser"
	return &rabbitmq.User{Name: "app", Tags: []string{"management"}}, f.err
}
func (f *fakeRabbitMQAccess) CreateUser(ctx context.Context, rc *engine.Context, v rabbitmq.UserWrite) (string, error) {
	f.last = "createUser"
	return "generated-secret", f.err
}
func (f *fakeRabbitMQAccess) UpdateUser(ctx context.Context, rc *engine.Context, v rabbitmq.UserWrite) error {
	f.last = "updateUser"
	return f.err
}
func (f *fakeRabbitMQAccess) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	f.last = "deleteUser"
	return f.err
}
func (f *fakeRabbitMQAccess) ListPermissions(context.Context, *engine.Context) ([]rabbitmq.Permission, error) {
	f.last = "listPermissions"
	return []rabbitmq.Permission{rabbitmq.Permission{Vhost: "app", User: "app", Configure: ".*", Read: ".*", Write: ".*"}}, f.err
}
func (f *fakeRabbitMQAccess) GetPermission(ctx context.Context, rc *engine.Context, vhost, user string) (*rabbitmq.Permission, error) {
	f.last = "getPermission"
	return &rabbitmq.Permission{Vhost: "app", User: "app", Configure: ".*", Read: ".*", Write: ".*"}, f.err
}
func (f *fakeRabbitMQAccess) CreatePermission(ctx context.Context, rc *engine.Context, v rabbitmq.Permission) error {
	f.last = "createPermission"
	return f.err
}
func (f *fakeRabbitMQAccess) UpdatePermission(ctx context.Context, rc *engine.Context, v rabbitmq.Permission) error {
	f.last = "updatePermission"
	return f.err
}
func (f *fakeRabbitMQAccess) DeletePermission(ctx context.Context, rc *engine.Context, vhost, user string) error {
	f.last = "deletePermission"
	return f.err
}
func (f *fakeRabbitMQAccess) ListTopicPermissions(context.Context, *engine.Context) ([]rabbitmq.TopicPermission, error) {
	f.last = "listTopicPermissions"
	return []rabbitmq.TopicPermission{rabbitmq.TopicPermission{Vhost: "app", User: "app", Exchange: "events", Read: ".*", Write: ".*"}}, f.err
}
func (f *fakeRabbitMQAccess) GetTopicPermission(ctx context.Context, rc *engine.Context, vhost, user string) ([]rabbitmq.TopicPermission, error) {
	f.last = "getTopicPermission"
	return []rabbitmq.TopicPermission{rabbitmq.TopicPermission{Vhost: "app", User: "app", Exchange: "events", Read: ".*", Write: ".*"}}, f.err
}
func (f *fakeRabbitMQAccess) CreateTopicPermission(ctx context.Context, rc *engine.Context, v rabbitmq.TopicPermission) error {
	f.last = "createTopicPermission"
	return f.err
}
func (f *fakeRabbitMQAccess) UpdateTopicPermission(ctx context.Context, rc *engine.Context, v rabbitmq.TopicPermission) error {
	f.last = "updateTopicPermission"
	return f.err
}
func (f *fakeRabbitMQAccess) DeleteTopicPermission(ctx context.Context, rc *engine.Context, vhost, user string) error {
	f.last = "deleteTopicPermission"
	return f.err
}
func (f *fakeRabbitMQAccess) ListPolicies(context.Context, *engine.Context) ([]rabbitmq.Policy, error) {
	f.last = "listPolicies"
	return []rabbitmq.Policy{rabbitmq.Policy{Vhost: "app", Name: "app", Pattern: ".*", ApplyTo: "queues", Priority: 1, Definition: map[string]any{"message-ttl": 100}}}, f.err
}
func (f *fakeRabbitMQAccess) GetPolicy(ctx context.Context, rc *engine.Context, vhost, name string) (*rabbitmq.Policy, error) {
	f.last = "getPolicy"
	return &rabbitmq.Policy{Vhost: "app", Name: "app", Pattern: ".*", ApplyTo: "queues", Priority: 1, Definition: map[string]any{"message-ttl": 100}}, f.err
}
func (f *fakeRabbitMQAccess) CreatePolicy(ctx context.Context, rc *engine.Context, v rabbitmq.Policy) error {
	f.last = "createPolicy"
	return f.err
}
func (f *fakeRabbitMQAccess) UpdatePolicy(ctx context.Context, rc *engine.Context, v rabbitmq.Policy) error {
	f.last = "updatePolicy"
	return f.err
}
func (f *fakeRabbitMQAccess) DeletePolicy(ctx context.Context, rc *engine.Context, vhost, name string) error {
	f.last = "deletePolicy"
	return f.err
}
func (f *fakeRabbitMQAccess) ListOperatorPolicies(context.Context, *engine.Context) ([]rabbitmq.Policy, error) {
	f.last = "listOperatorPolicies"
	return []rabbitmq.Policy{rabbitmq.Policy{Vhost: "app", Name: "app", Pattern: ".*", ApplyTo: "queues", Priority: 1, Definition: map[string]any{"message-ttl": 100}}}, f.err
}
func (f *fakeRabbitMQAccess) GetOperatorPolicy(ctx context.Context, rc *engine.Context, vhost, name string) (*rabbitmq.Policy, error) {
	f.last = "getOperatorPolicy"
	return &rabbitmq.Policy{Vhost: "app", Name: "app", Pattern: ".*", ApplyTo: "queues", Priority: 1, Definition: map[string]any{"message-ttl": 100}}, f.err
}
func (f *fakeRabbitMQAccess) CreateOperatorPolicy(ctx context.Context, rc *engine.Context, v rabbitmq.Policy) error {
	f.last = "createOperatorPolicy"
	return f.err
}
func (f *fakeRabbitMQAccess) UpdateOperatorPolicy(ctx context.Context, rc *engine.Context, v rabbitmq.Policy) error {
	f.last = "updateOperatorPolicy"
	return f.err
}
func (f *fakeRabbitMQAccess) DeleteOperatorPolicy(ctx context.Context, rc *engine.Context, vhost, name string) error {
	f.last = "deleteOperatorPolicy"
	return f.err
}

type emptyRabbitMQAccess struct{ fakeRabbitMQAccess }

func (*emptyRabbitMQAccess) ListVhosts(context.Context, *engine.Context) ([]rabbitmq.Vhost, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) ListUsers(context.Context, *engine.Context) ([]rabbitmq.User, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) ListPermissions(context.Context, *engine.Context) ([]rabbitmq.Permission, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) ListTopicPermissions(context.Context, *engine.Context) ([]rabbitmq.TopicPermission, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) GetTopicPermission(context.Context, *engine.Context, string, string) ([]rabbitmq.TopicPermission, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) ListPolicies(context.Context, *engine.Context) ([]rabbitmq.Policy, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) ListOperatorPolicies(context.Context, *engine.Context) ([]rabbitmq.Policy, error) {
	return nil, nil
}
func (*emptyRabbitMQAccess) GetVhost(context.Context, *engine.Context, string) (*rabbitmq.Vhost, error) {
	return &rabbitmq.Vhost{Name: "app"}, nil
}
func (*emptyRabbitMQAccess) GetUser(context.Context, *engine.Context, string) (*rabbitmq.User, error) {
	return &rabbitmq.User{Name: "app"}, nil
}

func TestRabbitMQEmptyResponseArraysAndClosedRequests(t *testing.T) {
	s, _, token := testServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(&emptyRabbitMQAccess{})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"vhosts", "users", "permissions", "topic-permissions", "topic-permissions/app/app", "policies", "operator-policies"} {
		w := call(s, "GET", "/api/v1/clusters/prod/rabbitmq/"+route, "", token)
		if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"items":[]}` {
			t.Fatalf("%s: %d %s", route, w.Code, w.Body.String())
		}
	}
	for _, route := range []string{"vhosts/app", "users/app"} {
		w := call(s, "GET", "/api/v1/clusters/prod/rabbitmq/"+route, "", token)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"tags":[]`) {
			t.Fatalf("%s: %d %s", route, w.Code, w.Body.String())
		}
	}
	for _, body := range []string{`{"name":"app","tags":[],"unknown":true}`, `{"name":"app","tags":[],"limits":{"unknown":1}}`} {
		w := call(s, "POST", "/api/v1/clusters/prod/rabbitmq/vhosts", body, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), `"reason":"unknown_field"`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRabbitMQAccessRoutes(t *testing.T) {
	s, _, token := testServer(t)
	f := &fakeRabbitMQAccess{}
	var e error
	s.Registry, e = engine.NewRegistry(f)
	if e != nil {
		t.Fatal(e)
	}
	cases := []struct {
		method, path, body, want string
		status                   int
	}{
		{"GET", "/rabbitmq/vhosts", ``, "listVhosts", 200},
		{"POST", "/rabbitmq/vhosts", `{"name":"app","tags":[],"defaultQueueType":"quorum","limits":{"maxQueues":10}}`, "createVhost", 201},
		{"GET", "/rabbitmq/vhosts/app", ``, "getVhost", 200},
		{"PUT", "/rabbitmq/vhosts/app", `{"name":"app","tags":[],"defaultQueueType":"quorum","limits":{"maxQueues":10}}`, "updateVhost", 200},
		{"DELETE", "/rabbitmq/vhosts/app", ``, "deleteVhost", 204},
		{"GET", "/rabbitmq/users", ``, "listUsers", 200},
		{"POST", "/rabbitmq/users", `{"name":"app","tags":["management"]}`, "createUser", 201},
		{"GET", "/rabbitmq/users/app", ``, "getUser", 200},
		{"PUT", "/rabbitmq/users/app", `{"name":"app","tags":["management"]}`, "updateUser", 200},
		{"DELETE", "/rabbitmq/users/app", ``, "deleteUser", 204},
		{"GET", "/rabbitmq/permissions", ``, "listPermissions", 200},
		{"POST", "/rabbitmq/permissions", `{"vhost":"app","user":"app","configure":".*","read":".*","write":".*"}`, "createPermission", 201},
		{"GET", "/rabbitmq/permissions/app/app", ``, "getPermission", 200},
		{"PUT", "/rabbitmq/permissions/app/app", `{"vhost":"app","user":"app","configure":".*","read":".*","write":".*"}`, "updatePermission", 200},
		{"DELETE", "/rabbitmq/permissions/app/app", ``, "deletePermission", 204},
		{"GET", "/rabbitmq/topic-permissions", ``, "listTopicPermissions", 200},
		{"POST", "/rabbitmq/topic-permissions", `{"vhost":"app","user":"app","exchange":"events","read":".*","write":".*"}`, "createTopicPermission", 201},
		{"GET", "/rabbitmq/topic-permissions/app/app", ``, "getTopicPermission", 200},
		{"PUT", "/rabbitmq/topic-permissions/app/app", `{"vhost":"app","user":"app","exchange":"events","read":".*","write":".*"}`, "updateTopicPermission", 200},
		{"DELETE", "/rabbitmq/topic-permissions/app/app", ``, "deleteTopicPermission", 204},
		{"GET", "/rabbitmq/policies", ``, "listPolicies", 200},
		{"POST", "/rabbitmq/policies", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{"message-ttl":100}}`, "createPolicy", 201},
		{"GET", "/rabbitmq/policies/app/app", ``, "getPolicy", 200},
		{"PUT", "/rabbitmq/policies/app/app", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{"message-ttl":100}}`, "updatePolicy", 200},
		{"DELETE", "/rabbitmq/policies/app/app", ``, "deletePolicy", 204},
		{"GET", "/rabbitmq/operator-policies", ``, "listOperatorPolicies", 200},
		{"POST", "/rabbitmq/operator-policies", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{"message-ttl":100}}`, "createOperatorPolicy", 201},
		{"GET", "/rabbitmq/operator-policies/app/app", ``, "getOperatorPolicy", 200},
		{"PUT", "/rabbitmq/operator-policies/app/app", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{"message-ttl":100}}`, "updateOperatorPolicy", 200},
		{"DELETE", "/rabbitmq/operator-policies/app/app", ``, "deleteOperatorPolicy", 204},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			f.err = nil
			w := call(s, tc.method, "/api/v1/clusters/prod"+tc.path, tc.body, token)
			if w.Code != tc.status || f.last != tc.want {
				t.Fatalf("%d %s dispatched %s", w.Code, w.Body.String(), f.last)
			}
			if tc.status != 204 {
				var v map[string]any
				if e := json.Unmarshal(w.Body.Bytes(), &v); e != nil {
					t.Fatal(e)
				}
			}
			if tc.want == "createUser" {
				if !strings.Contains(w.Body.String(), "generated-secret") || w.Header().Get("Cache-Control") != "no-store" {
					t.Fatal(w.Body.String(), w.Header())
				}
			}
			if tc.want == "updateUser" && strings.Contains(w.Body.String(), "password") {
				t.Fatal("password leaked")
			}
			for _, pair := range []struct {
				err    error
				status int
			}{{engine.ErrInvalid, 400}, {engine.ErrNotFound, 404}, {engine.ErrConflict, 409}} {
				f.err = pair.err
				w = call(s, tc.method, "/api/v1/clusters/prod"+tc.path, tc.body, token)
				if w.Code != pair.status {
					t.Fatal(pair.status, w.Code, w.Body.String())
				}
			}
		})
	}
}
func TestRabbitMQUnsupportedAndIdentity(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "GET", "/api/v1/clusters/prod/rabbitmq/vhosts", "", token)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	f := &fakeRabbitMQAccess{}
	s.Registry, _ = engine.NewRegistry(f)
	for _, path := range []string{"vhosts/app", "users/app", "permissions/app/app", "topic-permissions/app/app", "policies/app/app", "operator-policies/app/app"} {
		var body string
		switch {
		case strings.HasPrefix(path, "vhosts"):
			body = `{"name":"wrong","tags":[]}`
		case strings.HasPrefix(path, "users"):
			body = `{"name":"wrong","tags":[]}`
		case strings.Contains(path, "policies"):
			body = `{"vhost":"wrong","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{}}`
		case strings.HasPrefix(path, "topic"):
			body = `{"vhost":"wrong","user":"app","exchange":"events","read":".*","write":".*"}`
		default:
			body = `{"vhost":"wrong","user":"app","configure":".*","read":".*","write":".*"}`
		}
		w = call(s, "PUT", "/api/v1/clusters/prod/rabbitmq/"+path, body, token)
		if w.Code != 400 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"vhosts", "users", "permissions", "topic-permissions", "policies", "operator-policies"} {
		w = call(s, "GET", "/api/v1/clusters/prod/rabbitmq/"+path, "", "")
		if w.Code != 401 {
			t.Fatal(fmt.Sprint(path, w.Code))
		}
	}
}
