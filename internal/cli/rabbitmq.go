// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	apiclient "github.com/KarakoramDev/Karakoram/pkg/client"
)

func rabbitMQCommandSpecs() []commandSpec {
	return []commandSpec{
		{"rabbitmq vhosts list <cluster>", "listRabbitMQVhosts", 1, false, nil},
		{"rabbitmq vhosts create <cluster>", "createRabbitMQVhost", 1, true, nil},
		{"rabbitmq vhosts get <cluster> <name>", "getRabbitMQVhost", 2, false, nil},
		{"rabbitmq vhosts update <cluster> <name>", "replaceRabbitMQVhost", 2, true, nil},
		{"rabbitmq vhosts delete <cluster> <name>", "deleteRabbitMQVhost", 2, false, nil},
		{"rabbitmq users list <cluster>", "listRabbitMQUsers", 1, false, nil},
		{"rabbitmq users create <cluster>", "createRabbitMQUser", 1, true, nil},
		{"rabbitmq users get <cluster> <name>", "getRabbitMQUser", 2, false, nil},
		{"rabbitmq users update <cluster> <name>", "replaceRabbitMQUser", 2, true, nil},
		{"rabbitmq users delete <cluster> <name>", "deleteRabbitMQUser", 2, false, nil},
		{"rabbitmq permissions list <cluster>", "listRabbitMQPermissions", 1, false, nil},
		{"rabbitmq permissions create <cluster>", "createRabbitMQPermission", 1, true, nil},
		{"rabbitmq permissions get <cluster> <vhost> <user>", "getRabbitMQPermission", 3, false, nil},
		{"rabbitmq permissions update <cluster> <vhost> <user>", "replaceRabbitMQPermission", 3, true, nil},
		{"rabbitmq permissions delete <cluster> <vhost> <user>", "deleteRabbitMQPermission", 3, false, nil},
		{"rabbitmq topic-permissions list <cluster>", "listRabbitMQTopicPermissions", 1, false, nil},
		{"rabbitmq topic-permissions create <cluster>", "createRabbitMQTopicPermission", 1, true, nil},
		{"rabbitmq topic-permissions get <cluster> <vhost> <user>", "getRabbitMQTopicPermission", 3, false, nil},
		{"rabbitmq topic-permissions update <cluster> <vhost> <user>", "replaceRabbitMQTopicPermission", 3, true, nil},
		{"rabbitmq topic-permissions delete <cluster> <vhost> <user>", "deleteRabbitMQTopicPermission", 3, false, nil},
		{"rabbitmq policies list <cluster>", "listRabbitMQPolicies", 1, false, nil},
		{"rabbitmq policies create <cluster>", "createRabbitMQPolicy", 1, true, nil},
		{"rabbitmq policies get <cluster> <vhost> <name>", "getRabbitMQPolicy", 3, false, nil},
		{"rabbitmq policies update <cluster> <vhost> <name>", "replaceRabbitMQPolicy", 3, true, nil},
		{"rabbitmq policies delete <cluster> <vhost> <name>", "deleteRabbitMQPolicy", 3, false, nil},
		{"rabbitmq operator-policies list <cluster>", "listRabbitMQOperatorPolicies", 1, false, nil},
		{"rabbitmq operator-policies create <cluster>", "createRabbitMQOperatorPolicy", 1, true, nil},
		{"rabbitmq operator-policies get <cluster> <vhost> <name>", "getRabbitMQOperatorPolicy", 3, false, nil},
		{"rabbitmq operator-policies update <cluster> <vhost> <name>", "replaceRabbitMQOperatorPolicy", 3, true, nil},
		{"rabbitmq operator-policies delete <cluster> <vhost> <name>", "deleteRabbitMQOperatorPolicy", 3, false, nil},
	}
}

func dispatchRabbitMQ(ctx context.Context, c *apiclient.Client, op string, args []string, q, body []byte) (*http.Response, error) {
	switch op {
	case "listRabbitMQVhosts":
		var p apiclient.ListRabbitMQVhostsParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ListRabbitMQVhosts(ctx, args[0], &p)
	case "createRabbitMQVhost":
		var p apiclient.CreateRabbitMQVhostParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.CreateRabbitMQVhostWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getRabbitMQVhost":
		var p apiclient.GetRabbitMQVhostParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.GetRabbitMQVhost(ctx, args[0], args[1], &p)
	case "replaceRabbitMQVhost":
		var p apiclient.ReplaceRabbitMQVhostParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ReplaceRabbitMQVhostWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteRabbitMQVhost":
		var p apiclient.DeleteRabbitMQVhostParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.DeleteRabbitMQVhost(ctx, args[0], args[1], &p)
	case "listRabbitMQUsers":
		var p apiclient.ListRabbitMQUsersParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ListRabbitMQUsers(ctx, args[0], &p)
	case "createRabbitMQUser":
		var p apiclient.CreateRabbitMQUserParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.CreateRabbitMQUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getRabbitMQUser":
		var p apiclient.GetRabbitMQUserParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.GetRabbitMQUser(ctx, args[0], args[1], &p)
	case "replaceRabbitMQUser":
		var p apiclient.ReplaceRabbitMQUserParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ReplaceRabbitMQUserWithBody(ctx, args[0], args[1], &p, "application/json", bytes.NewReader(body))
	case "deleteRabbitMQUser":
		var p apiclient.DeleteRabbitMQUserParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.DeleteRabbitMQUser(ctx, args[0], args[1], &p)
	case "listRabbitMQPermissions":
		var p apiclient.ListRabbitMQPermissionsParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ListRabbitMQPermissions(ctx, args[0], &p)
	case "createRabbitMQPermission":
		var p apiclient.CreateRabbitMQPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.CreateRabbitMQPermissionWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getRabbitMQPermission":
		var p apiclient.GetRabbitMQPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.GetRabbitMQPermission(ctx, args[0], args[1], args[2], &p)
	case "replaceRabbitMQPermission":
		var p apiclient.ReplaceRabbitMQPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ReplaceRabbitMQPermissionWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "deleteRabbitMQPermission":
		var p apiclient.DeleteRabbitMQPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.DeleteRabbitMQPermission(ctx, args[0], args[1], args[2], &p)
	case "listRabbitMQTopicPermissions":
		var p apiclient.ListRabbitMQTopicPermissionsParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ListRabbitMQTopicPermissions(ctx, args[0], &p)
	case "createRabbitMQTopicPermission":
		var p apiclient.CreateRabbitMQTopicPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.CreateRabbitMQTopicPermissionWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getRabbitMQTopicPermission":
		var p apiclient.GetRabbitMQTopicPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.GetRabbitMQTopicPermission(ctx, args[0], args[1], args[2], &p)
	case "replaceRabbitMQTopicPermission":
		var p apiclient.ReplaceRabbitMQTopicPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ReplaceRabbitMQTopicPermissionWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "deleteRabbitMQTopicPermission":
		var p apiclient.DeleteRabbitMQTopicPermissionParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.DeleteRabbitMQTopicPermission(ctx, args[0], args[1], args[2], &p)
	case "listRabbitMQPolicies":
		var p apiclient.ListRabbitMQPoliciesParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ListRabbitMQPolicies(ctx, args[0], &p)
	case "createRabbitMQPolicy":
		var p apiclient.CreateRabbitMQPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.CreateRabbitMQPolicyWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getRabbitMQPolicy":
		var p apiclient.GetRabbitMQPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.GetRabbitMQPolicy(ctx, args[0], args[1], args[2], &p)
	case "replaceRabbitMQPolicy":
		var p apiclient.ReplaceRabbitMQPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ReplaceRabbitMQPolicyWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "deleteRabbitMQPolicy":
		var p apiclient.DeleteRabbitMQPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.DeleteRabbitMQPolicy(ctx, args[0], args[1], args[2], &p)
	case "listRabbitMQOperatorPolicies":
		var p apiclient.ListRabbitMQOperatorPoliciesParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ListRabbitMQOperatorPolicies(ctx, args[0], &p)
	case "createRabbitMQOperatorPolicy":
		var p apiclient.CreateRabbitMQOperatorPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.CreateRabbitMQOperatorPolicyWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getRabbitMQOperatorPolicy":
		var p apiclient.GetRabbitMQOperatorPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.GetRabbitMQOperatorPolicy(ctx, args[0], args[1], args[2], &p)
	case "replaceRabbitMQOperatorPolicy":
		var p apiclient.ReplaceRabbitMQOperatorPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.ReplaceRabbitMQOperatorPolicyWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "deleteRabbitMQOperatorPolicy":
		var p apiclient.DeleteRabbitMQOperatorPolicyParams
		if e := json.Unmarshal(q, &p); e != nil {
			return nil, e
		}
		return c.DeleteRabbitMQOperatorPolicy(ctx, args[0], args[1], args[2], &p)
	}
	return nil, fmt.Errorf("unknown RabbitMQ API operation %q", op)
}
