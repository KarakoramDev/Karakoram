// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/pkg/client"
	"net/http"
	"strings"
)

// dispatch uses only generated request methods; root endpoints use a separate base client.
func dispatch(ctx context.Context, c, root *client.Client, op string, args []string, query map[string]any, body []byte) (*http.Response, error) {
	q, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	_ = q
	switch op {
	case "dryRunCreateCluster":
		var p client.DryRunCreateClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DryRunCreateClusterWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "dryRunReplaceCluster":
		var p client.DryRunReplaceClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DryRunReplaceClusterWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "dryRunPatchCluster":
		var p client.DryRunPatchClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DryRunPatchClusterWithBody(ctx, args[0], &p, "application/merge-patch+json", bytes.NewReader(body))
	case "dryRunCreateRestore":
		var p client.DryRunCreateRestoreParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DryRunCreateRestoreWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "revealOpenBaoUnsealKeys", "listOpenBaoDatabaseConnections", "getOpenBaoDatabaseConnection", "createOpenBaoDatabaseConnection", "deleteOpenBaoDatabaseConnection":
		return dispatchOpenBao(ctx, c, op, args, q, body)
	case "issueTemporalClientCertificate", "listTemporalClientCertificates", "listTemporalNamespaces", "deleteTemporalNamespace", "setTemporalUIOIDCClientSecret":
		return dispatchTemporal(ctx, c, op, args, q, body)
	case "listBackupDrills":
		var p client.ListBackupDrillsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListBackupDrills(ctx, args[0], &p)
	case "getBackupDrill":
		var p client.GetBackupDrillParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetBackupDrill(ctx, args[0], args[1], &p)
	case "createBackupDrill":
		var p client.CreateBackupDrillParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateBackupDrillWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "listNotificationChannels":
		var p client.ListNotificationChannelsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListNotificationChannels(ctx, &p)
	case "getNotificationChannel":
		return c.GetNotificationChannel(ctx, args[0])
	case "createNotificationChannel":
		return c.CreateNotificationChannelWithBody(ctx, nil, "application/json", bytes.NewReader(body))
	case "replaceNotificationChannel":
		return c.ReplaceNotificationChannelWithBody(ctx, args[0], nil, "application/json", bytes.NewReader(body))
	case "deleteNotificationChannel":
		return c.DeleteNotificationChannel(ctx, args[0], nil)
	case "testNotificationChannel":
		return c.TestNotificationChannel(ctx, args[0], nil)
	case "getAgentRolloutSettings":
		return c.GetAgentRolloutSettings(ctx)
	case "updateAgentRolloutSettings":
		var p client.UpdateAgentRolloutSettingsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.UpdateAgentRolloutSettingsWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "upgradeClusterAgents":
		var p client.UpgradeClusterAgentsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.UpgradeClusterAgentsWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "getControlPlaneBackupPolicy":
		return c.GetControlPlaneBackupPolicy(ctx)
	case "updateControlPlaneBackupPolicy":
		var p client.UpdateControlPlaneBackupPolicyParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.UpdateControlPlaneBackupPolicyWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "listControlPlaneBackups":
		var p client.ListControlPlaneBackupsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListControlPlaneBackups(ctx, &p)
	case "createControlPlaneBackup":
		var p client.CreateControlPlaneBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateControlPlaneBackup(ctx, &p)
	case "getSupabaseCredentials", "setSupabaseSMTPPassword", "listSupabaseFunctions", "getSupabaseFunction", "putSupabaseFunction", "deleteSupabaseFunction", "rotateSupabaseJWTSecret":
		return dispatchSupabase(ctx, c, op, args, q, body)
	case "listElasticUsers", "getElasticUser", "createElasticUser", "updateElasticUser", "deleteElasticUser", "listElasticRoles", "getElasticRole", "createElasticRole", "updateElasticRole", "deleteElasticRole", "listElasticAPIKeys", "createElasticAPIKey", "invalidateElasticAPIKey":
		return dispatchElastic(ctx, c, op, args, q, body)
	case "listBackends":
		return c.ListBackends(ctx)
	case "listNamespaces":
		var p client.ListNamespacesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListNamespaces(ctx, &p)
	case "getNamespace":
		return c.GetNamespace(ctx, args[0])
	case "createNamespace":
		return c.CreateNamespaceWithBody(ctx, nil, "application/json", bytes.NewReader(body))
	case "updateNamespace":
		return c.UpdateNamespaceWithBody(ctx, args[0], nil, "application/json", bytes.NewReader(body))
	case "rotateNamespaceToken":
		return c.RotateNamespaceTokenWithBody(ctx, args[0], nil, "application/json", bytes.NewReader(body))
	case "validateNamespace":
		return c.ValidateNamespace(ctx, args[0], nil)
	case "deleteNamespace":
		return c.DeleteNamespace(ctx, args[0], nil)
	case "changePassword":
		var p client.ChangePasswordParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ChangePasswordWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "checkPermission":
		var p client.CheckPermissionParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CheckPermissionWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "listOAuthGrants":
		return c.ListOAuthGrants(ctx)
	case "deleteOAuthGrant":
		var p client.DeleteOAuthGrantParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteOAuthGrant(ctx, args[0], &p)
	case "createAPIToken":
		var p client.CreateAPITokenParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateAPITokenWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createAccount":
		var p client.CreateAccountParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateAccountWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createBackup":
		var p client.CreateBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateBackupWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "createBackupStorage":
		var p client.CreateBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateBackupStorageWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createCluster":
		var p client.CreateClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateClusterWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createDatabaseRole":
		var p client.CreateDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateDatabaseRoleWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "createDatabaseUser":
		var p client.CreateDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateDatabaseUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "createRestore":
		var p client.CreateRestoreParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateRestoreWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "deleteAPIToken":
		var p client.DeleteAPITokenParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteAPIToken(ctx, args[0], &p)
	case "deleteAccount":
		var p client.DeleteAccountParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteAccount(ctx, args[0], &p)
	case "deleteBackup":
		var p client.DeleteBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteBackup(ctx, args[0], args[1], &p)
	case "deleteBackupStorage":
		var p client.DeleteBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteBackupStorage(ctx, args[0], &p)
	case "deleteCluster":
		var p client.DeleteClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteCluster(ctx, args[0], &p)
	case "deleteDatabaseRole":
		var p client.DeleteDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteDatabaseRole(ctx, args[0], args[1], args[2], &p)
	case "deleteDatabaseUser":
		var p client.DeleteDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteDatabaseUser(ctx, args[0], args[1], args[2], &p)
	case "getAPIToken":
		return c.GetAPIToken(ctx, args[0])
	case "getAccount":
		return c.GetAccount(ctx, args[0])
	case "getBackup":
		var p client.GetBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetBackup(ctx, args[0], args[1], &p)
	case "getBackupStorage":
		var p client.GetBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetBackupStorage(ctx, args[0], &p)
	case "getCluster":
		var p client.GetClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetCluster(ctx, args[0], &p)
	case "getClusterBalancer":
		var p client.GetClusterBalancerParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterBalancer(ctx, args[0], &p)
	case "getClusterConnection":
		var p client.GetClusterConnectionParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterConnection(ctx, args[0], &p)
	case "getClusterMember":
		var p client.GetClusterMemberParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterMember(ctx, args[0], args[1], &p)
	case "getClusterMetrics":
		var p client.GetClusterMetricsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterMetrics(ctx, args[0], &p)
	case "getClusterPlacement":
		var p client.GetClusterPlacementParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterPlacement(ctx, args[0], &p)
	case "getClusterStatus":
		var p client.GetClusterStatusParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterStatus(ctx, args[0], &p)
	case "getCurrentUser":
		return c.GetCurrentUser(ctx)
	case "getDatabaseRole":
		var p client.GetDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetDatabaseRole(ctx, args[0], args[1], args[2], &p)
	case "getDatabaseUser":
		var p client.GetDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetDatabaseUser(ctx, args[0], args[1], args[2], &p)
	case "getHealth":
		return root.GetHealth(ctx)
	case "getOpenAPISpec":
		return c.GetOpenAPISpec(ctx)
	case "getMemberLogs":
		var p client.GetMemberLogsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetMemberLogs(ctx, args[0], args[1], &p)
	case "getOperation":
		return c.GetOperation(ctx, args[0])
	case "cancelOperation":
		var p client.CancelOperationParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CancelOperation(ctx, args[0], &p)
	case "getPITRWindow":
		var p client.GetPITRWindowParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetPITRWindow(ctx, args[0], &p)
	case "getPermissions":
		return c.GetPermissions(ctx)
	case "getPrometheusMetrics":
		var p client.GetPrometheusMetricsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return root.GetPrometheusMetrics(ctx, &p)
	case "getProvider":
		return c.GetProvider(ctx, args[0])
	case "getRBACPolicy":
		return c.GetRBACPolicy(ctx)
	case "getRestore":
		var p client.GetRestoreParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetRestore(ctx, args[0], args[1], &p)
	case "getSettings":
		return c.GetSettings(ctx)
	case "getVersion":
		return c.GetVersion(ctx)
	case "listAPITokens":
		var p client.ListAPITokensParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListAPITokens(ctx, &p)
	case "listAccounts":
		var p client.ListAccountsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListAccounts(ctx, &p)
	case "listBackupStorages":
		var p client.ListBackupStoragesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListBackupStorages(ctx, &p)
	case "listBackups":
		var p client.ListBackupsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListBackups(ctx, args[0], &p)
	case "listClusterMembers":
		var p client.ListClusterMembersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListClusterMembers(ctx, args[0], &p)
	case "listClusterOperations":
		var p client.ListClusterOperationsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListClusterOperations(ctx, args[0], &p)
	case "listClusters":
		var p client.ListClustersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListClusters(ctx, &p)
	case "listDatabaseRoles":
		var p client.ListDatabaseRolesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListDatabaseRoles(ctx, args[0], &p)
	case "listDatabaseUsers":
		var p client.ListDatabaseUsersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListDatabaseUsers(ctx, args[0], &p)
	case "listEvents":
		var p client.ListEventsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListEvents(ctx, &p)
	case "listOrphanedVolumes":
		var p client.ListOrphanedVolumesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListOrphanedVolumes(ctx, &p)
	case "listOperations":
		var p client.ListOperationsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListOperations(ctx, &p)
	case "listProviders":
		return c.ListProviders(ctx)
	case "listRestores":
		var p client.ListRestoresParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListRestores(ctx, args[0], &p)
	case "listSystemUsers":
		var p client.ListSystemUsersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListSystemUsers(ctx, args[0], &p)
	case "login":
		var p client.LoginParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.LoginWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "logout":
		var p client.LogoutParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.Logout(ctx, &p)
	case "oidcCallback":
		var p client.OidcCallbackParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.OidcCallback(ctx, &p)
	case "oidcLogin":
		var p client.OidcLoginParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.OidcLogin(ctx, &p)
	case "patchCluster":
		var p client.PatchClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.PatchClusterWithBody(ctx, args[0], &p, "application/merge-patch+json", bytes.NewReader(body))
	case "raiseClusterFCV":
		var p client.RaiseClusterFCVParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RaiseClusterFCVWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "refreshSession":
		var p client.RefreshSessionParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RefreshSession(ctx, &p)
	case "replaceAPIToken":
		var p client.ReplaceAPITokenParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceAPITokenWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceAccount":
		var p client.ReplaceAccountParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceAccountWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceBackupStorage":
		var p client.ReplaceBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceBackupStorageWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceCluster":
		var p client.ReplaceClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceClusterWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceClusterBalancer":
		var p client.ReplaceClusterBalancerParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceClusterBalancerWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "moveClusterMember":
		var p client.MoveClusterMemberParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.MoveClusterMember(ctx, args[0], args[1], &p)
	case "replaceClusterMember":
		var p client.ReplaceClusterMemberParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceClusterMember(ctx, args[0], args[1], &p)
	case "replaceDatabaseRole":
		var p client.ReplaceDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceDatabaseRoleWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "replaceDatabaseUser":
		var p client.ReplaceDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceDatabaseUserWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "replaceRBACPolicy":
		var p client.ReplaceRBACPolicyParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceRBACPolicyWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "replaceSettings":
		var p client.ReplaceSettingsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceSettingsWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "resetAccountPassword":
		var p client.ResetAccountPasswordParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ResetAccountPasswordWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "retryClusterDeployments":
		var p client.RetryClusterDeploymentsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RetryClusterDeployments(ctx, args[0], &p)
	case "restartCluster":
		var p client.RestartClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RestartCluster(ctx, args[0], &p)
	case "resumeCluster":
		var p client.ResumeClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ResumeCluster(ctx, args[0], &p)
	case "rotateClusterTLS":
		var p client.RotateClusterTLSParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotateClusterTLS(ctx, args[0], &p)
	case "rotateSystemUser":
		var p client.RotateSystemUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotateSystemUser(ctx, args[0], args[1], &p)
	case "streamEvents":
		var p client.StreamEventsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.StreamEvents(ctx, &p)
	case "verifyBackupStorage":
		var p client.VerifyBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.VerifyBackupStorage(ctx, args[0], &p)
	default:
		if strings.Contains(op, "MySQL") {
			return dispatchMySQL(ctx, c, op, args, q, body)
		}
		if strings.Contains(op, "Neo4j") {
			return dispatchNeo4j(ctx, c, op, args, q, body)
		}
		if strings.Contains(op, "Postgres") {
			return dispatchPostgres(ctx, c, op, args, q, body)
		}
		if strings.Contains(op, "Weaviate") {
			return dispatchWeaviate(ctx, c, op, args, q, body)
		}
		if strings.Contains(op, "RabbitMQ") {
			return dispatchRabbitMQ(ctx, c, op, args, q, body)
		}
		return nil, fmt.Errorf("unknown API operation %q", op)
	}
}
