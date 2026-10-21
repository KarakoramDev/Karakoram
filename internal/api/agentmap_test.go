// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package api

import "testing"

func TestSecuritySensitiveOperationsRequireConfirmation(t *testing.T) {
	for _, operation := range []string{
		"changePassword",
		"revealOpenBaoUnsealKeys", "createOpenBaoDatabaseConnection", "deleteOpenBaoDatabaseConnection",
		"issueTemporalClientCertificate", "deleteTemporalNamespace", "setTemporalUIOIDCClientSecret",
		"setSupabaseSMTPPassword", "putSupabaseFunction",
		"createPostgresRole", "updatePostgresRole", "patchPostgresRole", "grantPostgresPrivileges",
		"rotatePostgresBinding", "rebalancePostgres",
		"createMySQLUser", "patchMySQLUser", "deleteMySQLUser", "rotateMySQLUserPassword",
		"discardMySQLUserOldPassword", "grantMySQLPrivileges", "revokeMySQLPrivileges", "recoverMySQLQuorum",
		"createRabbitMQUser", "replaceRabbitMQUser",
		"createNeo4jUser", "deleteNeo4jUser", "setNeo4jUserPassword",
		"createWeaviateUser", "updateWeaviateUser", "rotateWeaviateUserKey",
		"createWeaviateRole", "updateWeaviateRole",
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

func TestTemporalCertificateIssuanceReturnsSecret(t *testing.T) {
	rule, exists := OperationAgent["issueTemporalClientCertificate"]
	if !exists || !rule.Secret {
		t.Fatalf("certificate issuance returns a private key and must be secret: %+v", rule)
	}
}

func TestNeo4jPasswordOperationsAreSecret(t *testing.T) {
	for _, operation := range []string{"createNeo4jUser", "setNeo4jUserPassword"} {
		if !OperationAgent[operation].Secret {
			t.Errorf("%s must require secret access", operation)
		}
	}
}
