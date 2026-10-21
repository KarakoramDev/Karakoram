// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package api

import "testing"

func TestSecuritySensitiveOperationsRequireConfirmation(t *testing.T) {
	for _, operation := range []string{
		"changePassword",
		"setSupabaseSMTPPassword", "putSupabaseFunction",
		"createPostgresRole", "updatePostgresRole", "patchPostgresRole", "grantPostgresPrivileges",
		"createRabbitMQUser", "replaceRabbitMQUser",
		"createRabbitMQPermission", "replaceRabbitMQPermission", "createRabbitMQTopicPermission", "replaceRabbitMQTopicPermission",
		"createRabbitMQPolicy", "replaceRabbitMQPolicy", "createRabbitMQOperatorPolicy", "replaceRabbitMQOperatorPolicy",
		"createDatabaseUser", "replaceDatabaseUser", "createDatabaseRole", "replaceDatabaseRole",
		"createElasticUser", "updateElasticUser", "createElasticRole", "updateElasticRole", "createElasticAPIKey",
	} {
		t.Run(operation, func(t *testing.T) {
			rule, exists := OperationAgent[operation]
			if !exists || rule.Risk != "destructive" || rule.Confirm != "confirm" {
				t.Fatalf("security mutation must require confirmation: %+v", rule)
			}
		})
	}
}
