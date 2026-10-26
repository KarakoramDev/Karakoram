// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
)

func rabbitMQVhost(v api.RabbitMQVhost) rabbitmq.Vhost {
	out := rabbitmq.Vhost{Name: v.Name, Tags: v.Tags, Description: value(v.Description)}
	if v.DefaultQueueType != nil {
		out.DefaultQueueType = string(*v.DefaultQueueType)
	}
	if v.Limits != nil {
		out.Limits = &rabbitmq.VhostLimits{MaxConnections: v.Limits.MaxConnections, MaxQueues: v.Limits.MaxQueues}
	}
	return out
}
func rabbitMQUser(v api.RabbitMQUserWrite) rabbitmq.UserWrite {
	tags := make([]string, len(v.Tags))
	for i, t := range v.Tags {
		tags[i] = string(t)
	}
	return rabbitmq.UserWrite{Name: v.Name, Tags: tags, Password: v.Password, PasswordHash: v.PasswordHash}
}
func rabbitMQPermission(v api.RabbitMQPermission) rabbitmq.Permission {
	return rabbitmq.Permission{Vhost: v.Vhost, User: v.User, Configure: v.Configure, Write: v.Write, Read: v.Read}
}
func rabbitMQTopicPermission(v api.RabbitMQTopicPermission) rabbitmq.TopicPermission {
	return rabbitmq.TopicPermission{Vhost: v.Vhost, User: v.User, Exchange: v.Exchange, Write: v.Write, Read: v.Read}
}
func rabbitMQPolicy(v api.RabbitMQPolicy) rabbitmq.Policy {
	return rabbitmq.Policy{Vhost: v.Vhost, Name: v.Name, Pattern: v.Pattern, ApplyTo: string(v.ApplyTo), Priority: v.Priority, Definition: v.Definition}
}

// A user-manager grant may assign management only. All other tags need the
// additional cluster-administration grant, including any future catalog tags.
func (s *Server) rabbitMQUserTagsAllowed(ctx context.Context, name string, rc *engine.Context, tags []api.RabbitMQUserWriteTags) bool {
	for _, tag := range tags {
		if tag == "management" {
			continue
		}
		rule, exists := api.OperationAdditionalRBAC[operationID(name)]
		return exists && !rule.Public && rule.ObjectParam == "cluster" &&
			s.allowed(ctx, rule.Resource, rule.Action, clusterObject(rc.Cluster.Namespace, rc.Cluster.Name))
	}
	return true
}

func (s *Server) rabbitmq(ctx context.Context, name string, in input, rc *engine.Context, p engine.Provider) response {
	a, yes := p.(rabbitmq.Access)
	if !yes {
		return s.unsupportedClusterAction(name, p)
	}
	switch name {
	case "ListRabbitMQVhosts":
		v, e := a.ListVhosts(ctx, rc)
		if e != nil {
			return s.failure(e)
		}
		for i := range v {
			v[i].Tags = nonNil(v[i].Tags)
		}
		return ok(page[rabbitmq.Vhost]{Items: nonNil(v)})
	case "CreateRabbitMQVhost":
		b := in.Body.(*api.RabbitMQVhost)
		v := rabbitMQVhost(*b)
		if e := a.CreateVhost(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 201, body: v, noStore: false}
	case "GetRabbitMQVhost":
		v, e := a.GetVhost(ctx, rc, in.Name)
		if e != nil {
			return s.failure(e)
		}
		v.Tags = nonNil(v.Tags)
		return ok(v)
	case "ReplaceRabbitMQVhost":
		b := in.Body.(*api.RabbitMQVhost)
		v := rabbitMQVhost(*b)
		if v.Name != in.Name {
			return invalidField("name", domain.ReasonInvalid, "name must match the path parameter")
		}
		if e := a.UpdateVhost(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 200, body: v, noStore: false}
	case "DeleteRabbitMQVhost":
		if e := a.DeleteVhost(ctx, rc, in.Name); e != nil {
			return s.failure(e)
		}
		return response{status: 204}
	case "ListRabbitMQUsers":
		v, e := a.ListUsers(ctx, rc)
		if e != nil {
			return s.failure(e)
		}
		for i := range v {
			v[i].Tags = nonNil(v[i].Tags)
		}
		return ok(page[rabbitmq.User]{Items: nonNil(v)})
	case "CreateRabbitMQUser":
		b := in.Body.(*api.RabbitMQUserWrite)
		if !s.rabbitMQUserTagsAllowed(ctx, name, rc, b.Tags) {
			return fail(403, "forbidden", "RabbitMQ privileged user tags require clusters/update permission")
		}
		v := rabbitMQUser(*b)
		password, e := a.CreateUser(ctx, rc, v)
		if e != nil {
			return s.failure(e)
		}
		out := struct {
			rabbitmq.User
			GeneratedPassword *string `json:"generatedPassword,omitempty"`
		}{User: rabbitmq.User{Name: v.Name, Tags: v.Tags}}
		if password != "" {
			out.GeneratedPassword = &password
		}
		return response{status: 201, body: out, noStore: true}
	case "GetRabbitMQUser":
		v, e := a.GetUser(ctx, rc, in.Name)
		if e != nil {
			return s.failure(e)
		}
		v.Tags = nonNil(v.Tags)
		return ok(v)
	case "ReplaceRabbitMQUser":
		b := in.Body.(*api.RabbitMQUserWrite)
		if b.Name != in.Name {
			return invalidField("name", domain.ReasonInvalid, "name must match the path parameter")
		}
		if !s.rabbitMQUserTagsAllowed(ctx, name, rc, b.Tags) {
			return fail(403, "forbidden", "RabbitMQ privileged user tags require clusters/update permission")
		}
		v := rabbitMQUser(*b)
		if e := a.UpdateUser(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 200, body: rabbitmq.User{Name: v.Name, Tags: v.Tags}, noStore: true}
	case "DeleteRabbitMQUser":
		if e := a.DeleteUser(ctx, rc, in.Name); e != nil {
			return s.failure(e)
		}
		return response{status: 204}
	case "ListRabbitMQPermissions":
		v, e := a.ListPermissions(ctx, rc)
		if e != nil {
			return s.failure(e)
		}
		return ok(page[rabbitmq.Permission]{Items: nonNil(v)})
	case "CreateRabbitMQPermission":
		b := in.Body.(*api.RabbitMQPermission)
		v := rabbitMQPermission(*b)
		if e := a.CreatePermission(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 201, body: v, noStore: false}
	case "GetRabbitMQPermission":
		v, e := a.GetPermission(ctx, rc, in.Vhost, in.User)
		if e != nil {
			return s.failure(e)
		}
		return ok(v)
	case "ReplaceRabbitMQPermission":
		b := in.Body.(*api.RabbitMQPermission)
		v := rabbitMQPermission(*b)
		if v.Vhost != in.Vhost {
			return invalidField("vhost", domain.ReasonInvalid, "vhost must match the path parameter")
		}
		if v.User != in.User {
			return invalidField("user", domain.ReasonInvalid, "user must match the path parameter")
		}
		if e := a.UpdatePermission(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 200, body: v, noStore: false}
	case "DeleteRabbitMQPermission":
		if e := a.DeletePermission(ctx, rc, in.Vhost, in.User); e != nil {
			return s.failure(e)
		}
		return response{status: 204}
	case "ListRabbitMQTopicPermissions":
		v, e := a.ListTopicPermissions(ctx, rc)
		if e != nil {
			return s.failure(e)
		}
		return ok(page[rabbitmq.TopicPermission]{Items: nonNil(v)})
	case "CreateRabbitMQTopicPermission":
		b := in.Body.(*api.RabbitMQTopicPermission)
		v := rabbitMQTopicPermission(*b)
		if e := a.CreateTopicPermission(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 201, body: v, noStore: false}
	case "GetRabbitMQTopicPermission":
		v, e := a.GetTopicPermission(ctx, rc, in.Vhost, in.User)
		if e != nil {
			return s.failure(e)
		}
		return ok(page[rabbitmq.TopicPermission]{Items: nonNil(v)})
	case "ReplaceRabbitMQTopicPermission":
		b := in.Body.(*api.RabbitMQTopicPermission)
		v := rabbitMQTopicPermission(*b)
		if v.Vhost != in.Vhost {
			return invalidField("vhost", domain.ReasonInvalid, "vhost must match the path parameter")
		}
		if v.User != in.User {
			return invalidField("user", domain.ReasonInvalid, "user must match the path parameter")
		}
		if e := a.UpdateTopicPermission(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 200, body: v, noStore: false}
	case "DeleteRabbitMQTopicPermission":
		if e := a.DeleteTopicPermission(ctx, rc, in.Vhost, in.User); e != nil {
			return s.failure(e)
		}
		return response{status: 204}
	case "ListRabbitMQPolicies":
		v, e := a.ListPolicies(ctx, rc)
		if e != nil {
			return s.failure(e)
		}
		return ok(page[rabbitmq.Policy]{Items: nonNil(v)})
	case "CreateRabbitMQPolicy":
		b := in.Body.(*api.RabbitMQPolicy)
		v := rabbitMQPolicy(*b)
		if e := a.CreatePolicy(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 201, body: v, noStore: false}
	case "GetRabbitMQPolicy":
		v, e := a.GetPolicy(ctx, rc, in.Vhost, in.Name)
		if e != nil {
			return s.failure(e)
		}
		return ok(v)
	case "ReplaceRabbitMQPolicy":
		b := in.Body.(*api.RabbitMQPolicy)
		v := rabbitMQPolicy(*b)
		if v.Vhost != in.Vhost {
			return invalidField("vhost", domain.ReasonInvalid, "vhost must match the path parameter")
		}
		if v.Name != in.Name {
			return invalidField("name", domain.ReasonInvalid, "name must match the path parameter")
		}
		if e := a.UpdatePolicy(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 200, body: v, noStore: false}
	case "DeleteRabbitMQPolicy":
		if e := a.DeletePolicy(ctx, rc, in.Vhost, in.Name); e != nil {
			return s.failure(e)
		}
		return response{status: 204}
	case "ListRabbitMQOperatorPolicies":
		v, e := a.ListOperatorPolicies(ctx, rc)
		if e != nil {
			return s.failure(e)
		}
		return ok(page[rabbitmq.Policy]{Items: nonNil(v)})
	case "CreateRabbitMQOperatorPolicy":
		b := in.Body.(*api.RabbitMQPolicy)
		v := rabbitMQPolicy(*b)
		if e := a.CreateOperatorPolicy(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 201, body: v, noStore: false}
	case "GetRabbitMQOperatorPolicy":
		v, e := a.GetOperatorPolicy(ctx, rc, in.Vhost, in.Name)
		if e != nil {
			return s.failure(e)
		}
		return ok(v)
	case "ReplaceRabbitMQOperatorPolicy":
		b := in.Body.(*api.RabbitMQPolicy)
		v := rabbitMQPolicy(*b)
		if v.Vhost != in.Vhost {
			return invalidField("vhost", domain.ReasonInvalid, "vhost must match the path parameter")
		}
		if v.Name != in.Name {
			return invalidField("name", domain.ReasonInvalid, "name must match the path parameter")
		}
		if e := a.UpdateOperatorPolicy(ctx, rc, v); e != nil {
			return s.failure(e)
		}
		return response{status: 200, body: v, noStore: false}
	case "DeleteRabbitMQOperatorPolicy":
		if e := a.DeleteOperatorPolicy(ctx, rc, in.Vhost, in.Name); e != nil {
			return s.failure(e)
		}
		return response{status: 204}
	}
	return fail(500, "internal_error", "Unknown RabbitMQ operation")
}

func (s *Server) ListRabbitMQVhosts(ctx context.Context, request api.ListRabbitMQVhostsRequestObject) (api.ListRabbitMQVhostsResponseObject, error) {
	return s.handle(ctx, "ListRabbitMQVhosts", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRabbitMQVhostsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateRabbitMQVhost(ctx context.Context, request api.CreateRabbitMQVhostRequestObject) (api.CreateRabbitMQVhostResponseObject, error) {
	return s.handle(ctx, "CreateRabbitMQVhost", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRabbitMQVhostResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetRabbitMQVhost(ctx context.Context, request api.GetRabbitMQVhostRequestObject) (api.GetRabbitMQVhostResponseObject, error) {
	return s.handle(ctx, "GetRabbitMQVhost", input{Cluster: request.Cluster, Name: request.Vhost}), nil
}
func (r response) VisitGetRabbitMQVhostResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ReplaceRabbitMQVhost(ctx context.Context, request api.ReplaceRabbitMQVhostRequestObject) (api.ReplaceRabbitMQVhostResponseObject, error) {
	return s.handle(ctx, "ReplaceRabbitMQVhost", input{Cluster: request.Cluster, Body: request.Body, Name: request.Vhost}), nil
}
func (r response) VisitReplaceRabbitMQVhostResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteRabbitMQVhost(ctx context.Context, request api.DeleteRabbitMQVhostRequestObject) (api.DeleteRabbitMQVhostResponseObject, error) {
	return s.handle(ctx, "DeleteRabbitMQVhost", input{Cluster: request.Cluster, Name: request.Vhost}), nil
}
func (r response) VisitDeleteRabbitMQVhostResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ListRabbitMQUsers(ctx context.Context, request api.ListRabbitMQUsersRequestObject) (api.ListRabbitMQUsersResponseObject, error) {
	return s.handle(ctx, "ListRabbitMQUsers", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRabbitMQUsersResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateRabbitMQUser(ctx context.Context, request api.CreateRabbitMQUserRequestObject) (api.CreateRabbitMQUserResponseObject, error) {
	return s.handle(ctx, "CreateRabbitMQUser", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRabbitMQUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetRabbitMQUser(ctx context.Context, request api.GetRabbitMQUserRequestObject) (api.GetRabbitMQUserResponseObject, error) {
	return s.handle(ctx, "GetRabbitMQUser", input{Cluster: request.Cluster, Name: request.User}), nil
}
func (r response) VisitGetRabbitMQUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ReplaceRabbitMQUser(ctx context.Context, request api.ReplaceRabbitMQUserRequestObject) (api.ReplaceRabbitMQUserResponseObject, error) {
	return s.handle(ctx, "ReplaceRabbitMQUser", input{Cluster: request.Cluster, Body: request.Body, Name: request.User}), nil
}
func (r response) VisitReplaceRabbitMQUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteRabbitMQUser(ctx context.Context, request api.DeleteRabbitMQUserRequestObject) (api.DeleteRabbitMQUserResponseObject, error) {
	return s.handle(ctx, "DeleteRabbitMQUser", input{Cluster: request.Cluster, Name: request.User}), nil
}
func (r response) VisitDeleteRabbitMQUserResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ListRabbitMQPermissions(ctx context.Context, request api.ListRabbitMQPermissionsRequestObject) (api.ListRabbitMQPermissionsResponseObject, error) {
	return s.handle(ctx, "ListRabbitMQPermissions", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRabbitMQPermissionsResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) CreateRabbitMQPermission(ctx context.Context, request api.CreateRabbitMQPermissionRequestObject) (api.CreateRabbitMQPermissionResponseObject, error) {
	return s.handle(ctx, "CreateRabbitMQPermission", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRabbitMQPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) GetRabbitMQPermission(ctx context.Context, request api.GetRabbitMQPermissionRequestObject) (api.GetRabbitMQPermissionResponseObject, error) {
	return s.handle(ctx, "GetRabbitMQPermission", input{Cluster: request.Cluster, Vhost: request.Vhost, User: request.User}), nil
}
func (r response) VisitGetRabbitMQPermissionResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ReplaceRabbitMQPermission(ctx context.Context, request api.ReplaceRabbitMQPermissionRequestObject) (api.ReplaceRabbitMQPermissionResponseObject, error) {
	return s.handle(ctx, "ReplaceRabbitMQPermission", input{Cluster: request.Cluster, Body: request.Body, Vhost: request.Vhost, User: request.User}), nil
}
func (r response) VisitReplaceRabbitMQPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) DeleteRabbitMQPermission(ctx context.Context, request api.DeleteRabbitMQPermissionRequestObject) (api.DeleteRabbitMQPermissionResponseObject, error) {
	return s.handle(ctx, "DeleteRabbitMQPermission", input{Cluster: request.Cluster, Vhost: request.Vhost, User: request.User}), nil
}
func (r response) VisitDeleteRabbitMQPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListRabbitMQTopicPermissions(ctx context.Context, request api.ListRabbitMQTopicPermissionsRequestObject) (api.ListRabbitMQTopicPermissionsResponseObject, error) {
	return s.handle(ctx, "ListRabbitMQTopicPermissions", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRabbitMQTopicPermissionsResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) CreateRabbitMQTopicPermission(ctx context.Context, request api.CreateRabbitMQTopicPermissionRequestObject) (api.CreateRabbitMQTopicPermissionResponseObject, error) {
	return s.handle(ctx, "CreateRabbitMQTopicPermission", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRabbitMQTopicPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) GetRabbitMQTopicPermission(ctx context.Context, request api.GetRabbitMQTopicPermissionRequestObject) (api.GetRabbitMQTopicPermissionResponseObject, error) {
	return s.handle(ctx, "GetRabbitMQTopicPermission", input{Cluster: request.Cluster, Vhost: request.Vhost, User: request.User}), nil
}
func (r response) VisitGetRabbitMQTopicPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ReplaceRabbitMQTopicPermission(ctx context.Context, request api.ReplaceRabbitMQTopicPermissionRequestObject) (api.ReplaceRabbitMQTopicPermissionResponseObject, error) {
	return s.handle(ctx, "ReplaceRabbitMQTopicPermission", input{Cluster: request.Cluster, Body: request.Body, Vhost: request.Vhost, User: request.User}), nil
}
func (r response) VisitReplaceRabbitMQTopicPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) DeleteRabbitMQTopicPermission(ctx context.Context, request api.DeleteRabbitMQTopicPermissionRequestObject) (api.DeleteRabbitMQTopicPermissionResponseObject, error) {
	return s.handle(ctx, "DeleteRabbitMQTopicPermission", input{Cluster: request.Cluster, Vhost: request.Vhost, User: request.User}), nil
}
func (r response) VisitDeleteRabbitMQTopicPermissionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListRabbitMQPolicies(ctx context.Context, request api.ListRabbitMQPoliciesRequestObject) (api.ListRabbitMQPoliciesResponseObject, error) {
	return s.handle(ctx, "ListRabbitMQPolicies", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRabbitMQPoliciesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) CreateRabbitMQPolicy(ctx context.Context, request api.CreateRabbitMQPolicyRequestObject) (api.CreateRabbitMQPolicyResponseObject, error) {
	return s.handle(ctx, "CreateRabbitMQPolicy", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRabbitMQPolicyResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetRabbitMQPolicy(ctx context.Context, request api.GetRabbitMQPolicyRequestObject) (api.GetRabbitMQPolicyResponseObject, error) {
	return s.handle(ctx, "GetRabbitMQPolicy", input{Cluster: request.Cluster, Vhost: request.Vhost, Name: request.Name}), nil
}
func (r response) VisitGetRabbitMQPolicyResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ReplaceRabbitMQPolicy(ctx context.Context, request api.ReplaceRabbitMQPolicyRequestObject) (api.ReplaceRabbitMQPolicyResponseObject, error) {
	return s.handle(ctx, "ReplaceRabbitMQPolicy", input{Cluster: request.Cluster, Body: request.Body, Vhost: request.Vhost, Name: request.Name}), nil
}
func (r response) VisitReplaceRabbitMQPolicyResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteRabbitMQPolicy(ctx context.Context, request api.DeleteRabbitMQPolicyRequestObject) (api.DeleteRabbitMQPolicyResponseObject, error) {
	return s.handle(ctx, "DeleteRabbitMQPolicy", input{Cluster: request.Cluster, Vhost: request.Vhost, Name: request.Name}), nil
}
func (r response) VisitDeleteRabbitMQPolicyResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) ListRabbitMQOperatorPolicies(ctx context.Context, request api.ListRabbitMQOperatorPoliciesRequestObject) (api.ListRabbitMQOperatorPoliciesResponseObject, error) {
	return s.handle(ctx, "ListRabbitMQOperatorPolicies", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRabbitMQOperatorPoliciesResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) CreateRabbitMQOperatorPolicy(ctx context.Context, request api.CreateRabbitMQOperatorPolicyRequestObject) (api.CreateRabbitMQOperatorPolicyResponseObject, error) {
	return s.handle(ctx, "CreateRabbitMQOperatorPolicy", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRabbitMQOperatorPolicyResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) GetRabbitMQOperatorPolicy(ctx context.Context, request api.GetRabbitMQOperatorPolicyRequestObject) (api.GetRabbitMQOperatorPolicyResponseObject, error) {
	return s.handle(ctx, "GetRabbitMQOperatorPolicy", input{Cluster: request.Cluster, Vhost: request.Vhost, Name: request.Name}), nil
}
func (r response) VisitGetRabbitMQOperatorPolicyResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ReplaceRabbitMQOperatorPolicy(ctx context.Context, request api.ReplaceRabbitMQOperatorPolicyRequestObject) (api.ReplaceRabbitMQOperatorPolicyResponseObject, error) {
	return s.handle(ctx, "ReplaceRabbitMQOperatorPolicy", input{Cluster: request.Cluster, Body: request.Body, Vhost: request.Vhost, Name: request.Name}), nil
}
func (r response) VisitReplaceRabbitMQOperatorPolicyResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) DeleteRabbitMQOperatorPolicy(ctx context.Context, request api.DeleteRabbitMQOperatorPolicyRequestObject) (api.DeleteRabbitMQOperatorPolicyResponseObject, error) {
	return s.handle(ctx, "DeleteRabbitMQOperatorPolicy", input{Cluster: request.Cluster, Vhost: request.Vhost, Name: request.Name}), nil
}
func (r response) VisitDeleteRabbitMQOperatorPolicyResponse(w http.ResponseWriter) error {
	return r.write(w)
}
