// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import "strings"

var commandGroupDescriptions = map[string]string{
	"accounts":                   "Manage local control-plane accounts",
	"auth":                       "Manage cookie sessions and OpenID Connect sign-in",
	"backups":                    "Create, inspect and delete database backups",
	"backends":                   "Inspect available infrastructure backends",
	"clusters":                   "Provision and manage database cluster lifecycles",
	"clusters balancer":          "Inspect and control MongoDB shard balancing",
	"db-roles":                   "Manage MongoDB custom database roles",
	"db-users":                   "Manage MongoDB database users",
	"events":                     "Inspect and stream control-plane events",
	"namespaces":                 "Manage project namespaces and platform tokens",
	"oauth-grants":               "Inspect and revoke connected OAuth application grants",
	"operations":                 "Inspect, wait for and cancel asynchronous work",
	"orphaned-volumes":           "Inspect volumes awaiting platform cleanup",
	"providers":                  "Inspect engine catalogs, topologies and defaults",
	"rbac":                       "Inspect permissions and manage access policies",
	"rbac policy":                "Read and replace the access-control policy",
	"restores":                   "Restore database backups and inspect recovery progress",
	"settings":                   "Configure control-plane sign-in settings",
	"storages":                   "Register and manage S3 backup destinations",
	"system-users":               "Inspect and rotate provider-managed database credentials",
	"tokens":                     "Create, inspect and revoke control-plane API credentials",
	"postgres":                   "Manage PostgreSQL application roles, databases and grants",
	"postgres roles":             "Manage PostgreSQL application roles and passwords",
	"postgres bindings":          "Inspect PostgreSQL consumer bindings and rotate their credentials",
	"postgres databases":         "Manage PostgreSQL application databases",
	"postgres grants":            "Manage PostgreSQL database and schema privileges",
	"rabbitmq":                   "Manage RabbitMQ vhosts, users, permissions and policies",
	"rabbitmq vhosts":            "Manage RabbitMQ virtual hosts",
	"rabbitmq users":             "Manage RabbitMQ application users",
	"rabbitmq permissions":       "Manage RabbitMQ virtual-host permission grants",
	"rabbitmq topic-permissions": "Manage RabbitMQ topic permission grants",
	"rabbitmq policies":          "Manage RabbitMQ messaging policies",
	"rabbitmq operator-policies": "Manage RabbitMQ operator policy constraints",
	"elasticsearch":              "Manage Elasticsearch native users, roles and API keys",
	"elasticsearch users":        "Manage Elasticsearch native user accounts",
	"elasticsearch roles":        "Manage Elasticsearch security role definitions",
	"elasticsearch api-keys":     "Manage Elasticsearch API credentials",
}

var commandDescriptions = map[string]string{
	"postgres rebalance":             "Rebalance shards across distributed PostgreSQL worker groups",
	"postgres roles rotate-password": "Generate and rotate a PostgreSQL role password (returned once)",
	"postgres bindings rotate":       "Rotate a binding password and refresh its managed consumer",
	"postgres grants grant":          "Grant PostgreSQL database or schema privileges",
	"postgres grants revoke":         "Revoke PostgreSQL database or schema privileges",
	"whoami":                         "Show the authenticated control-plane identity",
	"version":                        "Show client and server versions",
	"health":                         "Check control-plane health",
	"openapi":                        "Download the server's OpenAPI document",
	"metrics":                        "Export control-plane Prometheus metrics",
	"permissions":                    "Show effective control-plane permissions",
	"settings get":                   "Show control-plane sign-in settings",
	"settings set-oidc":              "Update OpenID Connect sign-in settings",
	"rbac policy get":                "Show the access-control policy",
	"rbac policy set":                "Replace the complete access-control policy",
	"rbac can":                       "Check whether an action is permitted on a resource",
	"accounts list":                  "List local control-plane accounts",
	"accounts get":                   "Show a local account",
	"accounts create":                "Create a local control-plane account",
	"accounts set":                   "Update a local account's settings",
	"accounts delete":                "Delete a local control-plane account",
	"accounts reset-password":        "Reset a local account's password",
	"accounts change-password":       "Change the current account's password",
	"tokens list":                    "List control-plane API tokens",
	"tokens get":                     "Show API token metadata without its secret",
	"tokens create":                  "Create an API token and reveal its secret once",
	"tokens set":                     "Update API token metadata and expiry",
	"tokens delete":                  "Revoke a control-plane API token",
	"oauth-grants list":              "List visible connected OAuth application grants",
	"oauth-grants delete":            "Revoke a connected OAuth application grant",
	"backends list":                  "List infrastructure backends, capabilities and settings",
	"providers list":                 "List database engine providers and catalog summaries",
	"providers get":                  "Show an engine provider's versions, defaults and limits",
	"namespaces list":                "List authorized project namespaces",
	"namespaces get":                 "Show a project namespace and its connection status",
	"namespaces create":              "Register a project namespace with a platform token",
	"namespaces update":              "Update a project namespace's description",
	"namespaces rotate-token":        "Replace a namespace's platform token",
	"namespaces validate":            "Check a namespace's platform connection",
	"namespaces delete":              "Delete an unused project namespace",
	"clusters list":                  "List authorized database clusters",
	"clusters get":                   "Show a cluster's desired state and observed status",
	"clusters create":                "Provision a database cluster",
	"clusters edit":                  "Edit and replace a cluster's complete specification",
	"clusters patch":                 "Apply a partial update to a cluster's specification",
	"clusters scale":                 "Change cluster replicas, shards or resource bounds",
	"clusters upgrade":               "Change a cluster's engine version",
	"clusters delete":                "Delete a cluster or suspend it with retained volumes",
	"clusters resume":                "Resume a suspended cluster from retained volumes",
	"clusters restart":               "Restart cluster members through a rolling operation",
	"clusters retry-deployments":     "Re-arm platform deployments stopped by the retry limit",
	"clusters raise-fcv":             "Raise MongoDB feature compatibility after an upgrade",
	"clusters replace-member":        "Replace a database member with a new instance",
	"clusters rotate-tls":            "Rotate a cluster's TLS certificates",
	"clusters status":                "Show cluster health, conditions and member status",
	"clusters members":               "List cluster members or show one member",
	"clusters connection":            "Show database endpoints and TLS connection details",
	"clusters ca":                    "Export a cluster's CA certificate as PEM",
	"clusters wait":                  "Wait for a cluster's desired generation to be observed",
	"clusters placement":             "Show cluster member placement and constraints",
	"clusters balancer get":          "Show MongoDB shard-balancer status",
	"clusters balancer set":          "Enable or disable MongoDB shard balancing",
	"clusters logs":                  "Read a cluster member's process logs",
	"clusters metrics":               "Show database metrics for a cluster",
	"system-users list":              "List provider-managed database identities",
	"system-users rotate":            "Rotate a provider-managed database user's credentials",
	"db-users list":                  "List MongoDB database users",
	"db-users get":                   "Show a MongoDB database user",
	"db-users create":                "Create a MongoDB database user",
	"db-users set":                   "Update a MongoDB database user's password and roles",
	"db-users delete":                "Delete a MongoDB database user",
	"db-roles list":                  "List MongoDB custom database roles",
	"db-roles get":                   "Show a MongoDB custom database role",
	"db-roles create":                "Create a MongoDB custom database role",
	"db-roles set":                   "Update a MongoDB custom role's privileges and inheritance",
	"db-roles delete":                "Delete a MongoDB custom database role",
	"backups list":                   "List a cluster's backup records",
	"backups create":                 "Start a database backup",
	"backups get":                    "Show a backup's status and recovery metadata",
	"backups delete":                 "Delete a backup and its stored snapshot",
	"backups pitr-window":            "Show the available point-in-time recovery window",
	"restores list":                  "List a cluster's restore records",
	"restores create":                "Restore database data or clone into a new cluster",
	"restores get":                   "Show a restore's progress and result",
	"storages list":                  "List registered S3 backup destinations",
	"storages get":                   "Show a backup destination without its secret credentials",
	"storages create":                "Register an S3 backup destination",
	"storages set":                   "Update an S3 backup destination and its credentials",
	"storages delete":                "Remove an unused backup destination registration",
	"storages verify":                "Verify read, write and delete access to a backup bucket",
	"orphaned-volumes list":          "List retained volumes awaiting platform cleanup",
	"operations list":                "List asynchronous control-plane operations",
	"operations cluster":             "List asynchronous operations for a cluster",
	"operations get":                 "Show an operation's progress and result",
	"operations wait":                "Wait for an operation to reach a terminal state",
	"operations cancel":              "Request cancellation at an operation's safe checkpoint",
	"events list":                    "List control-plane events and audit records",
	"events watch":                   "Stream control-plane events as they arrive",
	"auth refresh":                   "Refresh the current cookie-based session",
	"auth oidc-login":                "Start OpenID Connect sign-in and show the redirect",
	"auth oidc-callback":             "Complete an OpenID Connect authorization callback",
}

type nativeResourceDescription struct{ singular, plural string }

var nativeResourceDescriptions = map[string]nativeResourceDescription{
	"postgres roles":             {"a PostgreSQL application role", "PostgreSQL application roles"},
	"postgres bindings":          {"a PostgreSQL consumer binding", "PostgreSQL consumer bindings"},
	"postgres databases":         {"a PostgreSQL database", "PostgreSQL databases"},
	"postgres grants":            {"a PostgreSQL privilege grant", "PostgreSQL privilege grants"},
	"rabbitmq vhosts":            {"a RabbitMQ virtual host", "RabbitMQ virtual hosts"},
	"rabbitmq users":             {"a RabbitMQ application user", "RabbitMQ application users"},
	"rabbitmq permissions":       {"a RabbitMQ virtual-host permission grant", "RabbitMQ virtual-host permission grants"},
	"rabbitmq topic-permissions": {"a RabbitMQ topic permission grant", "RabbitMQ topic permission grants"},
	"rabbitmq policies":          {"a RabbitMQ messaging policy", "RabbitMQ messaging policies"},
	"rabbitmq operator-policies": {"a RabbitMQ operator policy", "RabbitMQ operator policies"},
	"elasticsearch users":        {"an Elasticsearch native user", "Elasticsearch native users"},
	"elasticsearch roles":        {"an Elasticsearch security role", "Elasticsearch security roles"},
	"elasticsearch api-keys":     {"an Elasticsearch API key", "Elasticsearch API keys"},
}

func commandDescription(path string) string {
	words := strings.Fields(path)
	for i, word := range words {
		if strings.HasPrefix(word, "<") || strings.HasPrefix(word, "[") {
			words = words[:i]
			break
		}
	}
	path = strings.Join(words, " ")
	if description, ok := commandDescriptions[path]; ok {
		return description
	}
	if len(words) != 3 {
		return ""
	}
	resource, ok := nativeResourceDescriptions[strings.Join(words[:2], " ")]
	if !ok {
		return ""
	}
	switch words[2] {
	case "list":
		return "List " + resource.plural
	case "get":
		return "Show " + resource.singular
	case "create":
		return "Create " + resource.singular
	case "update":
		return "Update " + resource.singular
	case "patch":
		return "Update selected attributes of " + resource.singular
	case "delete":
		return "Delete " + resource.singular
	case "invalidate":
		return "Invalidate " + resource.singular
	default:
		return ""
	}
}
