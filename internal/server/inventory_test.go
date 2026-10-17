// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestEveryOperationHasImplementedHandler traverses the actual generated routes,
// authenticated as administrator, so authorization cannot hide absent handlers.
func TestEveryOperationHasImplementedHandler(t *testing.T) {
	s, token := namespaceTestServer(t)
	routes := []struct{ name, method, path, body string }{
		{"GetSupabaseCredentials", "GET", "/api/v1/clusters/prod/supabase/credentials", ""},
		{"SetSupabaseSMTPPassword", "PUT", "/api/v1/clusters/prod/supabase/smtp-password", `{"password":"test-password"}`},
		{"ListSupabaseFunctions", "GET", "/api/v1/clusters/prod/supabase/functions", ""},
		{"GetSupabaseFunction", "GET", "/api/v1/clusters/prod/supabase/functions/hello", ""},
		{"PutSupabaseFunction", "PUT", "/api/v1/clusters/prod/supabase/functions/hello", `{"index.ts":"export {}"}`},
		{"DeleteSupabaseFunction", "DELETE", "/api/v1/clusters/prod/supabase/functions/hello", ""},
		{"RotateSupabaseJWTSecret", "POST", "/api/v1/clusters/prod/supabase/rotate-jwt-secret", ""},
		{"ListPostgresRoles", "GET", "/api/v1/clusters/prod/postgres/roles", ""},
		{"GetPostgresRole", "GET", "/api/v1/clusters/prod/postgres/roles/app", ""},
		{"CreatePostgresRole", "POST", "/api/v1/clusters/prod/postgres/roles", `{"name":"app"}`},
		{"UpdatePostgresRole", "PUT", "/api/v1/clusters/prod/postgres/roles/app", `{"name":"app"}`},
		{"PatchPostgresRole", "PATCH", "/api/v1/clusters/prod/postgres/roles/app", `{"createdb":true}`},
		{"DeletePostgresRole", "DELETE", "/api/v1/clusters/prod/postgres/roles/app", ""},
		{"RotatePostgresRolePassword", "POST", "/api/v1/clusters/prod/postgres/roles/app/password", ""},
		{"ListPostgresDatabases", "GET", "/api/v1/clusters/prod/postgres/databases", ""},
		{"GetPostgresDatabase", "GET", "/api/v1/clusters/prod/postgres/databases/app", ""},
		{"CreatePostgresDatabase", "POST", "/api/v1/clusters/prod/postgres/databases", `{"name":"app","owner":"app"}`},
		{"DeletePostgresDatabase", "DELETE", "/api/v1/clusters/prod/postgres/databases/app?force=true", ""},
		{"ListPostgresGrants", "GET", "/api/v1/clusters/prod/postgres/grants?database=app", ""},
		{"GrantPostgresPrivileges", "POST", "/api/v1/clusters/prod/postgres/grants", `{"role":"app","database":"app","privileges":["CONNECT"]}`},
		{"RevokePostgresPrivileges", "DELETE", "/api/v1/clusters/prod/postgres/grants", `{"role":"app","database":"app","privileges":["CONNECT"]}`},
		{"ListElasticUsers", "GET", "/api/v1/clusters/prod/elasticsearch/users", ``},
		{"CreateElasticUser", "POST", "/api/v1/clusters/prod/elasticsearch/users", `{"username":"test","roles":[],"enabled":true,"password":"test-password"}`},
		{"GetElasticUser", "GET", "/api/v1/clusters/prod/elasticsearch/users/test", ``},
		{"UpdateElasticUser", "PUT", "/api/v1/clusters/prod/elasticsearch/users/test", `{"username":"test","roles":[],"enabled":true,"password":"test-password"}`},
		{"DeleteElasticUser", "DELETE", "/api/v1/clusters/prod/elasticsearch/users/test", ``},
		{"ListElasticRoles", "GET", "/api/v1/clusters/prod/elasticsearch/roles", ``},
		{"CreateElasticRole", "POST", "/api/v1/clusters/prod/elasticsearch/roles", `{"name":"test","cluster":[],"indices":[],"applications":[]}`},
		{"GetElasticRole", "GET", "/api/v1/clusters/prod/elasticsearch/roles/test", ``},
		{"UpdateElasticRole", "PUT", "/api/v1/clusters/prod/elasticsearch/roles/test", `{"name":"test","cluster":[],"indices":[],"applications":[]}`},
		{"DeleteElasticRole", "DELETE", "/api/v1/clusters/prod/elasticsearch/roles/test", ``},
		{"ListElasticAPIKeys", "GET", "/api/v1/clusters/prod/elasticsearch/api-keys", ``},
		{"CreateElasticAPIKey", "POST", "/api/v1/clusters/prod/elasticsearch/api-keys", `{"name":"test"}`},
		{"InvalidateElasticAPIKey", "DELETE", "/api/v1/clusters/prod/elasticsearch/api-keys/test", ``},
		{"ListAccounts", "GET", "/api/v1/accounts", ""},
		{"CreateAccount", "POST", "/api/v1/accounts", "{\"name\":\"other\",\"password\":\"other-test-password\"}"},
		{"DeleteAccount", "DELETE", "/api/v1/accounts/other", ""},
		{"GetAccount", "GET", "/api/v1/accounts/other", ""},
		{"ReplaceAccount", "PUT", "/api/v1/accounts/other", "{\"enabled\":true,\"mustChangePassword\":false}"},
		{"ResetAccountPassword", "POST", "/api/v1/accounts/other/reset-password", "{\"password\":\"new-test-password\"}"},
		{"ListAPITokens", "GET", "/api/v1/api-tokens", ""},
		{"CreateAPIToken", "POST", "/api/v1/api-tokens", "{\"name\":\"inventory\"}"},
		{"DeleteAPIToken", "DELETE", "/api/v1/api-tokens/test-token", ""},
		{"GetAPIToken", "GET", "/api/v1/api-tokens/test-token", ""},
		{"ReplaceAPIToken", "PUT", "/api/v1/api-tokens/test-token", "{\"name\":\"renamed\",\"expiresAt\":null}"},
		{"ChangePassword", "POST", "/api/v1/auth/change-password", "{\"currentPassword\":\"wrong-password\",\"newPassword\":\"new-test-password\"}"},
		{"Login", "POST", "/api/v1/auth/login", "{\"username\":\"admin\",\"password\":\"admin-test-password\"}"},
		{"Logout", "POST", "/api/v1/auth/logout", "{}"},
		{"GetCurrentUser", "GET", "/api/v1/auth/me", ""},
		{"OidcCallback", "GET", "/api/v1/auth/oidc/callback?code=test&state=test", ""},
		{"OidcLogin", "GET", "/api/v1/auth/oidc/login", ""},
		{"RefreshSession", "POST", "/api/v1/auth/refresh", "{}"},
		{"ListBackupStorages", "GET", "/api/v1/backup-storages", ""},
		{"CreateBackupStorage", "POST", "/api/v1/backup-storages", "{\"name\":\"storage\",\"type\":\"s3\",\"endpointURL\":\"http://127.0.0.1:1\",\"region\":\"us-east-1\",\"bucket\":\"test\",\"credentials\":{\"accessKeyId\":\"access\",\"secretAccessKey\":\"secret\"}}"},
		{"DeleteBackupStorage", "DELETE", "/api/v1/backup-storages/storage", ""},
		{"GetBackupStorage", "GET", "/api/v1/backup-storages/storage", ""},
		{"ReplaceBackupStorage", "PUT", "/api/v1/backup-storages/storage", "{\"type\":\"s3\",\"endpointURL\":\"http://127.0.0.1:1\",\"region\":\"us-east-1\",\"bucket\":\"test\",\"forcePathStyle\":true,\"verifyTLS\":true}"},
		{"VerifyBackupStorage", "POST", "/api/v1/backup-storages/storage/verify", "{}"},
		{"ListNamespaces", "GET", "/api/v1/namespaces", ""},
		{"ListBackends", "GET", "/api/v1/backends", ""},
		{"CreateNamespace", "POST", "/api/v1/namespaces", `{"name":"inventoryns","projectId":"proj-inventory","token":"synthetic-token"}`},
		{"GetNamespace", "GET", "/api/v1/namespaces/inventoryns", ""},
		{"UpdateNamespace", "PATCH", "/api/v1/namespaces/inventoryns", `{"description":"Inventory namespace"}`},
		{"RotateNamespaceToken", "PUT", "/api/v1/namespaces/inventoryns/token", `{"token":"replacement-token"}`},
		{"ValidateNamespace", "POST", "/api/v1/namespaces/inventoryns/validate", ""},
		{"DeleteNamespace", "DELETE", "/api/v1/namespaces/inventoryns", ""},
		{"ListClusters", "GET", "/api/v1/clusters", ""},
		{"CreateCluster", "POST", "/api/v1/clusters", "{\"name\":\"inventory\",\"spec\":{\"provider\":\"fake\",\"version\":\"8.0\",\"topology\":{\"type\":\"replicaSet\"},\"components\":{\"engine\":{\"replicas\":1,\"resources\":{\"cpuMode\":\"SHARED\",\"minCpu\":1,\"maxCpu\":1,\"minRam\":1,\"maxRam\":1,\"minDisk\":5,\"maxDisk\":5}}}}}"},
		{"DeleteCluster", "DELETE", "/api/v1/clusters/prod", ""},
		{"GetCluster", "GET", "/api/v1/clusters/prod", ""},
		{"PatchCluster", "PATCH", "/api/v1/clusters/prod", "{\"spec\":{\"version\":\"8.0\"}}"},
		{"ReplaceCluster", "PUT", "/api/v1/clusters/prod", "{\"spec\":{\"provider\":\"fake\",\"version\":\"8.0\",\"topology\":{\"type\":\"replicaSet\"},\"components\":{\"engine\":{\"replicas\":3,\"resources\":{}}}}}"},
		{"ListBackups", "GET", "/api/v1/clusters/prod/backups", ""},
		{"CreateBackup", "POST", "/api/v1/clusters/prod/backups", "{\"name\":\"backup\",\"storage\":\"storage\",\"type\":\"logical\"}"},
		{"DeleteBackup", "DELETE", "/api/v1/clusters/prod/backups/backup", ""},
		{"GetBackup", "GET", "/api/v1/clusters/prod/backups/backup", ""},
		{"GetClusterBalancer", "GET", "/api/v1/clusters/prod/balancer", ""},
		{"ReplaceClusterBalancer", "PUT", "/api/v1/clusters/prod/balancer", "{\"enabled\":true}"},
		{"GetClusterConnection", "GET", "/api/v1/clusters/prod/connection", ""},
		{"ListDatabaseRoles", "GET", "/api/v1/clusters/prod/database-roles", ""},
		{"CreateDatabaseRole", "POST", "/api/v1/clusters/prod/database-roles", "{\"name\":\"custom\",\"database\":\"app\",\"privileges\":[],\"roles\":[]}"},
		{"DeleteDatabaseRole", "DELETE", "/api/v1/clusters/prod/database-roles/app/custom", ""},
		{"GetDatabaseRole", "GET", "/api/v1/clusters/prod/database-roles/app/custom", ""},
		{"ReplaceDatabaseRole", "PUT", "/api/v1/clusters/prod/database-roles/app/custom", "{\"privileges\":[],\"roles\":[]}"},
		{"ListDatabaseUsers", "GET", "/api/v1/clusters/prod/database-users", ""},
		{"CreateDatabaseUser", "POST", "/api/v1/clusters/prod/database-users", "{\"username\":\"app\",\"database\":\"app\",\"roles\":[]}"},
		{"DeleteDatabaseUser", "DELETE", "/api/v1/clusters/prod/database-users/app/app", ""},
		{"GetDatabaseUser", "GET", "/api/v1/clusters/prod/database-users/app/app", ""},
		{"ReplaceDatabaseUser", "PUT", "/api/v1/clusters/prod/database-users/app/app", "{\"roles\":[]}"},
		{"ListClusterMembers", "GET", "/api/v1/clusters/prod/members", ""},
		{"GetClusterMember", "GET", "/api/v1/clusters/prod/members/prodm0", ""},
		{"GetMemberLogs", "GET", "/api/v1/clusters/prod/members/prodm0/logs", ""},
		{"ReplaceClusterMember", "POST", "/api/v1/clusters/prod/members/prodm0/replace", "{}"},
		{"GetClusterMetrics", "GET", "/api/v1/clusters/prod/metrics", ""},
		{"ListClusterOperations", "GET", "/api/v1/clusters/prod/operations", ""},
		{"GetPITRWindow", "GET", "/api/v1/clusters/prod/pitr", ""},
		{"GetClusterPlacement", "GET", "/api/v1/clusters/prod/placement", ""},
		{"RaiseClusterFCV", "POST", "/api/v1/clusters/prod/raise-fcv", "{\"version\":\"8.0\"}"},
		{"RestartCluster", "POST", "/api/v1/clusters/prod/restart", "{}"},
		{"ListRestores", "GET", "/api/v1/clusters/prod/restores", ""},
		{"CreateRestore", "POST", "/api/v1/clusters/prod/restores", "{\"name\":\"restore\",\"source\":{\"sourceCluster\":\"prod\",\"backup\":\"backup\"}}"},
		{"GetRestore", "GET", "/api/v1/clusters/prod/restores/restore", ""},
		{"ResumeCluster", "POST", "/api/v1/clusters/prod/resume", "{}"},
		{"RotateClusterTLS", "POST", "/api/v1/clusters/prod/rotate-tls", "{}"},
		{"GetClusterStatus", "GET", "/api/v1/clusters/prod/status", ""},
		{"ListSystemUsers", "GET", "/api/v1/clusters/prod/system-users", ""},
		{"RotateSystemUser", "POST", "/api/v1/clusters/prod/system-users/clusterAdmin/rotate", "{}"},
		{"ListEvents", "GET", "/api/v1/events", ""},
		{"StreamEvents", "GET", "/api/v1/events/stream", ""},
		{"GetHealth", "GET", "/healthz", ""},
		{"GetPrometheusMetrics", "GET", "/metrics", ""},
		{"ListOperations", "GET", "/api/v1/operations", ""},
		{"ListOrphanedVolumes", "GET", "/api/v1/orphaned-volumes", ""},
		{"GetOperation", "GET", "/api/v1/operations/test-operation", ""},
		{"CancelOperation", "POST", "/api/v1/operations/test-operation/cancel", ""},
		{"GetPermissions", "GET", "/api/v1/permissions", ""},
		{"ListProviders", "GET", "/api/v1/providers", ""},
		{"GetProvider", "GET", "/api/v1/providers/fake", ""},
		{"CheckPermission", "POST", "/api/v1/rbac/can", "{\"resource\":\"clusters\",\"action\":\"read\",\"object\":\"prod\"}"},
		{"GetRBACPolicy", "GET", "/api/v1/rbac/policy", ""},
		{"ReplaceRBACPolicy", "PUT", "/api/v1/rbac/policy", "{\"policy\":\"p, role:admin, *, *, *, allow\"}"},
		{"GetSettings", "GET", "/api/v1/settings", ""},
		{"ReplaceSettings", "PUT", "/api/v1/settings", "{\"oidc\":{\"enabled\":false}}"},
		{"GetVersion", "GET", "/api/v1/version", ""},
		{"GetOpenAPISpec", "GET", "/api/v1/openapi.yaml", ""},
		{"ListRabbitMQVhosts", "GET", "/api/v1/clusters/prod/rabbitmq/vhosts", ``},
		{"CreateRabbitMQVhost", "POST", "/api/v1/clusters/prod/rabbitmq/vhosts", `{"name":"app","tags":[]}`},
		{"GetRabbitMQVhost", "GET", "/api/v1/clusters/prod/rabbitmq/vhosts/app", ``},
		{"ReplaceRabbitMQVhost", "PUT", "/api/v1/clusters/prod/rabbitmq/vhosts/app", `{"name":"app","tags":[]}`},
		{"DeleteRabbitMQVhost", "DELETE", "/api/v1/clusters/prod/rabbitmq/vhosts/app", ``},
		{"ListRabbitMQUsers", "GET", "/api/v1/clusters/prod/rabbitmq/users", ``},
		{"CreateRabbitMQUser", "POST", "/api/v1/clusters/prod/rabbitmq/users", `{"name":"app","tags":[]}`},
		{"GetRabbitMQUser", "GET", "/api/v1/clusters/prod/rabbitmq/users/app", ``},
		{"ReplaceRabbitMQUser", "PUT", "/api/v1/clusters/prod/rabbitmq/users/app", `{"name":"app","tags":[]}`},
		{"DeleteRabbitMQUser", "DELETE", "/api/v1/clusters/prod/rabbitmq/users/app", ``},
		{"ListRabbitMQPermissions", "GET", "/api/v1/clusters/prod/rabbitmq/permissions", ``},
		{"CreateRabbitMQPermission", "POST", "/api/v1/clusters/prod/rabbitmq/permissions", `{"vhost":"app","user":"app","configure":".*","read":".*","write":".*"}`},
		{"GetRabbitMQPermission", "GET", "/api/v1/clusters/prod/rabbitmq/permissions/app/app", ``},
		{"ReplaceRabbitMQPermission", "PUT", "/api/v1/clusters/prod/rabbitmq/permissions/app/app", `{"vhost":"app","user":"app","configure":".*","read":".*","write":".*"}`},
		{"DeleteRabbitMQPermission", "DELETE", "/api/v1/clusters/prod/rabbitmq/permissions/app/app", ``},
		{"ListRabbitMQTopicPermissions", "GET", "/api/v1/clusters/prod/rabbitmq/topic-permissions", ``},
		{"CreateRabbitMQTopicPermission", "POST", "/api/v1/clusters/prod/rabbitmq/topic-permissions", `{"vhost":"app","user":"app","exchange":"events","read":".*","write":".*"}`},
		{"GetRabbitMQTopicPermission", "GET", "/api/v1/clusters/prod/rabbitmq/topic-permissions/app/app", ``},
		{"ReplaceRabbitMQTopicPermission", "PUT", "/api/v1/clusters/prod/rabbitmq/topic-permissions/app/app", `{"vhost":"app","user":"app","exchange":"events","read":".*","write":".*"}`},
		{"DeleteRabbitMQTopicPermission", "DELETE", "/api/v1/clusters/prod/rabbitmq/topic-permissions/app/app", ``},
		{"ListRabbitMQPolicies", "GET", "/api/v1/clusters/prod/rabbitmq/policies", ``},
		{"CreateRabbitMQPolicy", "POST", "/api/v1/clusters/prod/rabbitmq/policies", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{}}`},
		{"GetRabbitMQPolicy", "GET", "/api/v1/clusters/prod/rabbitmq/policies/app/app", ``},
		{"ReplaceRabbitMQPolicy", "PUT", "/api/v1/clusters/prod/rabbitmq/policies/app/app", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{}}`},
		{"DeleteRabbitMQPolicy", "DELETE", "/api/v1/clusters/prod/rabbitmq/policies/app/app", ``},
		{"ListRabbitMQOperatorPolicies", "GET", "/api/v1/clusters/prod/rabbitmq/operator-policies", ``},
		{"CreateRabbitMQOperatorPolicy", "POST", "/api/v1/clusters/prod/rabbitmq/operator-policies", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{}}`},
		{"GetRabbitMQOperatorPolicy", "GET", "/api/v1/clusters/prod/rabbitmq/operator-policies/app/app", ``},
		{"ReplaceRabbitMQOperatorPolicy", "PUT", "/api/v1/clusters/prod/rabbitmq/operator-policies/app/app", `{"vhost":"app","name":"app","pattern":".*","applyTo":"queues","priority":1,"definition":{}}`},
		{"DeleteRabbitMQOperatorPolicy", "DELETE", "/api/v1/clusters/prod/rabbitmq/operator-policies/app/app", ``},
	}
	seen := map[string]bool{}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if route.name == "StreamEvents" || route.name == "VerifyBackupStorage" {
				cancel()
			}
			defer cancel()
			r := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body)).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			testHandler(s).ServeHTTP(w, r)
			if w.Code == 501 || w.Code == 500 || strings.Contains(w.Body.String(), "Unknown ") {
				t.Fatalf("missing handler: %d %s", w.Code, w.Body.String())
			}
			seen[operationID(route.name)] = true
		})
	}
	for id := range api.OperationRBAC {
		if !seen[id] {
			t.Errorf("no tested handler for %s", id)
		}
	}
}
