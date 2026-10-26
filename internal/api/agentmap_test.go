// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package api

import "testing"

func TestSecuritySensitiveOperationsRequireConfirmation(t *testing.T) {
	for _, operation := range []string{
		"changePassword",
		"revealOpenBaoUnsealKeys", "createOpenBaoDatabaseConnection", "deleteOpenBaoDatabaseConnection",
		"setSupabaseSMTPPassword", "putSupabaseFunction",
		"createPostgresRole", "updatePostgresRole", "patchPostgresRole", "grantPostgresPrivileges",
		"rotatePostgresBinding", "rebalancePostgres",
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

func TestOpenBaoInitializationMaterialIsSecret(t *testing.T) {
	rule, exists := OperationAgent["revealOpenBaoUnsealKeys"]
	if !exists || !rule.Secret {
		t.Fatalf("initialization material must be classified as secret: %+v", rule)
	}
}
