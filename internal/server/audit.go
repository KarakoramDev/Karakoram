// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// auditAction records identifiers and explicitly selected options, never request
// bodies or responses: both may contain passwords, tokens or private keys.
func (s *Server) auditAction(ctx context.Context, name string, in input, result response) error {
	rule, exists := api.OperationRBAC[operationID(name)]
	if !exists || rule.Public || name == "Logout" || name == "RefreshSession" || (rule.Action == "read" && name != "GetClusterConnection") {
		return nil
	}
	if result.status >= 300 {
		return nil
	}
	p := principal(ctx)
	if p == nil {
		return nil
	}
	e := domain.Event{Cluster: in.Cluster, Resource: rule.Resource, Actor: p.Actor(), Type: "Normal", Reason: name + "Succeeded"}
	if in.Cluster != "" || rule.Resource == "clusters" || rule.Resource == "backup-storages" {
		e.Namespace = domain.NormalizedNamespace(namespaceFilter(ctx))
	}
	e.Target = in.Cluster
	if op, ok := result.body.(api.Operation); ok {
		e.Operation = op.Id
	}
	switch name {
	case "CreateElasticUser":
		e.Target = in.Cluster + "/" + in.Body.(*api.ElasticUserCreate).Username
	case "UpdateElasticUser", "DeleteElasticUser":
		e.Target = in.Cluster + "/" + in.Username
	case "CreateElasticRole":
		e.Target = in.Cluster + "/" + in.Body.(*api.ElasticRoleInput).Name
	case "UpdateElasticRole", "DeleteElasticRole":
		e.Target = in.Cluster + "/" + in.Role
	case "CreateElasticAPIKey":
		if k, ok := result.body.(api.ElasticAPIKeyCreated); ok {
			e.Target = in.Cluster + "/" + k.Id
		}
	case "InvalidateElasticAPIKey":
		e.Target = in.Cluster + "/" + in.Token
	case "CreateCluster":
		b := in.Body.(*api.CreateClusterRequest)
		e.Cluster, e.Target, e.Reason = b.Name, b.Name, "ClusterCreateRequested"
		if b.Spec.DataSource != nil {
			e.Reason = "ClusterCloneRequested"
		}
	case "ReplaceCluster", "PatchCluster":
		e.Reason = "ClusterSpecChangeRequested"
		if c, ok := result.body.(api.Cluster); ok {
			e.Message = fmt.Sprintf("Cluster spec change requested for %s (generation=%d, version=%s, engineReplicas=%d)", c.Name, c.Generation, c.Spec.Version, c.Spec.Components.Engine.Replicas)
		}
	case "DeleteCluster":
		e.Reason = "ClusterDeleteRequested"
		q := request(ctx).URL.Query()
		e.Message = fmt.Sprintf("Cluster deletion requested for %s (keepVolumes=%t, keepBackups=%t)", in.Cluster, q.Get("keepVolumes") == "true", q.Get("keepBackups") == "true")
	case "ResumeCluster", "RestartCluster", "RotateClusterTLS", "ReplaceClusterMember", "RotateSystemUser", "RaiseClusterFCV", "CreateBackup", "DeleteBackup", "CreateRestore":
		e.Reason = strings.TrimSuffix(name, "Cluster") + "Requested"
	case "GetClusterConnection":
		e.Reason = "ConnectionCredentialsRevealed"
	case "CreateNamespace":
		e.Namespace = in.Body.(*api.CreateNamespaceJSONRequestBody).Name
		e.Target, e.Reason = e.Namespace, "NamespaceCreated"
	case "UpdateNamespace", "RotateNamespaceToken", "ValidateNamespace", "DeleteNamespace":
		e.Namespace, e.Target = in.Namespace, in.Namespace
		switch name {
		case "UpdateNamespace":
			e.Reason = "NamespaceUpdated"
		case "RotateNamespaceToken":
			e.Reason = "NamespaceTokenRotated"
		case "ValidateNamespace":
			e.Reason = "NamespaceValidated"
			if n, ok := result.body.(api.Namespace); ok && !n.Token.Valid {
				e.Reason = "NamespaceValidationFailed"
			}
		case "DeleteNamespace":
			e.Reason = "NamespaceDeleting"
		}
	case "CreateAccount":
		e.Target = in.Body.(*api.CreateAccountRequest).Name
	case "ReplaceAccount", "DeleteAccount", "ResetAccountPassword":
		e.Target = in.Account
	case "ChangePassword":
		e.Target = p.Account
	case "CreateAPIToken":
		if b, ok := result.body.(api.CreatedAPIToken); ok {
			e.Target = b.Token.Id
		}
	case "ReplaceAPIToken", "DeleteAPIToken":
		e.Target = in.Token
	case "CreateBackupStorage":
		e.Target = in.Body.(*api.CreateBackupStorageRequest).Name
	case "ReplaceBackupStorage", "DeleteBackupStorage", "VerifyBackupStorage":
		e.Target = in.Storage
	case "ReplaceRBACPolicy":
		e.Target = "policy"
	case "ReplaceSettings":
		e.Target = "oidc"
	case "CreateDatabaseUser":
		b := in.Body.(*api.CreateDatabaseUserRequest)
		e.Target = in.Cluster + "/" + b.Database + "/" + b.Username
	case "ReplaceDatabaseUser", "DeleteDatabaseUser":
		e.Target = in.Cluster + "/" + in.Database + "/" + in.Username
	case "CreateDatabaseRole":
		b := in.Body.(*api.DatabaseRole)
		e.Target = in.Cluster + "/" + b.Database + "/" + b.Name
	case "ReplaceDatabaseRole", "DeleteDatabaseRole":
		e.Target = in.Cluster + "/" + in.Database + "/" + in.Role
	case "CreateRabbitMQVhost", "ReplaceRabbitMQVhost", "DeleteRabbitMQVhost":
		e.Resource = "rabbitmq-vhosts"
		vhost := in.Name
		if body, ok := in.Body.(*api.RabbitMQVhost); ok {
			vhost = body.Name
		}
		e.Target = in.Cluster + "/" + vhost
	case "CreateRabbitMQUser", "ReplaceRabbitMQUser", "DeleteRabbitMQUser":
		e.Resource = "rabbitmq-users"
		user := in.Name
		if body, ok := in.Body.(*api.RabbitMQUserWrite); ok {
			user = body.Name
		}
		e.Target = in.Cluster + "/" + user
	case "CreateRabbitMQPermission", "ReplaceRabbitMQPermission", "DeleteRabbitMQPermission":
		e.Resource = "rabbitmq-permissions"
		vhost, user := in.Vhost, in.User
		if body, ok := in.Body.(*api.RabbitMQPermission); ok {
			vhost, user = body.Vhost, body.User
		}
		e.Target = in.Cluster + "/" + vhost + "/" + user
	case "CreateRabbitMQTopicPermission", "ReplaceRabbitMQTopicPermission", "DeleteRabbitMQTopicPermission":
		e.Resource = "rabbitmq-topic-permissions"
		vhost, user := in.Vhost, in.User
		if body, ok := in.Body.(*api.RabbitMQTopicPermission); ok {
			vhost, user = body.Vhost, body.User
		}
		e.Target = in.Cluster + "/" + vhost + "/" + user
	case "CreateRabbitMQPolicy", "ReplaceRabbitMQPolicy", "DeleteRabbitMQPolicy",
		"CreateRabbitMQOperatorPolicy", "ReplaceRabbitMQOperatorPolicy", "DeleteRabbitMQOperatorPolicy":
		e.Resource = "rabbitmq-policies"
		if strings.Contains(name, "OperatorPolicy") {
			e.Resource = "rabbitmq-operator-policies"
		}
		vhost, policy := in.Vhost, in.Name
		if body, ok := in.Body.(*api.RabbitMQPolicy); ok {
			vhost, policy = body.Vhost, body.Name
		}
		e.Target = in.Cluster + "/" + vhost + "/" + policy
	}
	if in.Member != "" {
		e.Target += "/" + in.Member
	}
	if in.User != "" && !strings.Contains(name, "RabbitMQ") {
		e.Target += "/" + in.User
	}
	if in.Backup != "" {
		e.Target += "/" + in.Backup
	}
	if name == "CreateBackup" {
		e.Target += "/" + in.Body.(*api.CreateBackupRequest).Name
	}
	if name == "CreateRestore" {
		e.Target += "/" + in.Body.(*api.CreateRestoreRequest).Name
	}
	if name == "RaiseClusterFCV" {
		e.Message = "Feature compatibility version " + in.Body.(*api.RaiseFCVRequest).Version + " requested for " + in.Cluster
	}
	if e.Message == "" {
		e.Message = e.Reason + " for " + e.Target
	}
	return s.recordAudit(ctx, e)
}

func (s *Server) recordAudit(ctx context.Context, e domain.Event) error {
	if err := s.Store.AppendEvent(ctx, &e); err != nil {
		return err
	}
	s.Hub.Publish(e)
	return nil
}
