// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/apiclient"
	"net/http"
)

// dispatch uses only generated request methods; root endpoints use a separate base client.
func dispatch(ctx context.Context, c, root *apiclient.Client, op string, args []string, query map[string]any, body []byte) (*http.Response, error) {
	q, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	_ = q
	switch op {
	case "listNamespaces":
		var p apiclient.ListNamespacesParams
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
		var p apiclient.ChangePasswordParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ChangePasswordWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "checkPermission":
		var p apiclient.CheckPermissionParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CheckPermissionWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createAPIToken":
		var p apiclient.CreateAPITokenParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateAPITokenWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createAccount":
		var p apiclient.CreateAccountParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateAccountWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createBackup":
		var p apiclient.CreateBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateBackupWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "createBackupStorage":
		var p apiclient.CreateBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateBackupStorageWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createCluster":
		var p apiclient.CreateClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateClusterWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "createDatabaseRole":
		var p apiclient.CreateDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateDatabaseRoleWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "createDatabaseUser":
		var p apiclient.CreateDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateDatabaseUserWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "createRestore":
		var p apiclient.CreateRestoreParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.CreateRestoreWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "deleteAPIToken":
		var p apiclient.DeleteAPITokenParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteAPIToken(ctx, args[0], &p)
	case "deleteAccount":
		var p apiclient.DeleteAccountParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteAccount(ctx, args[0], &p)
	case "deleteBackup":
		var p apiclient.DeleteBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteBackup(ctx, args[0], args[1], &p)
	case "deleteBackupStorage":
		var p apiclient.DeleteBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteBackupStorage(ctx, args[0], &p)
	case "deleteCluster":
		var p apiclient.DeleteClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteCluster(ctx, args[0], &p)
	case "deleteDatabaseRole":
		var p apiclient.DeleteDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteDatabaseRole(ctx, args[0], args[1], args[2], &p)
	case "deleteDatabaseUser":
		var p apiclient.DeleteDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.DeleteDatabaseUser(ctx, args[0], args[1], args[2], &p)
	case "getAPIToken":
		return c.GetAPIToken(ctx, args[0])
	case "getAccount":
		return c.GetAccount(ctx, args[0])
	case "getBackup":
		var p apiclient.GetBackupParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetBackup(ctx, args[0], args[1], &p)
	case "getBackupStorage":
		var p apiclient.GetBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetBackupStorage(ctx, args[0], &p)
	case "getCluster":
		var p apiclient.GetClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetCluster(ctx, args[0], &p)
	case "getClusterBalancer":
		var p apiclient.GetClusterBalancerParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterBalancer(ctx, args[0], &p)
	case "getClusterConnection":
		var p apiclient.GetClusterConnectionParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterConnection(ctx, args[0], &p)
	case "getClusterMember":
		var p apiclient.GetClusterMemberParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterMember(ctx, args[0], args[1], &p)
	case "getClusterMetrics":
		var p apiclient.GetClusterMetricsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterMetrics(ctx, args[0], &p)
	case "getClusterPlacement":
		var p apiclient.GetClusterPlacementParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterPlacement(ctx, args[0], &p)
	case "getClusterStatus":
		var p apiclient.GetClusterStatusParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetClusterStatus(ctx, args[0], &p)
	case "getCurrentUser":
		return c.GetCurrentUser(ctx)
	case "getDatabaseRole":
		var p apiclient.GetDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetDatabaseRole(ctx, args[0], args[1], args[2], &p)
	case "getDatabaseUser":
		var p apiclient.GetDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetDatabaseUser(ctx, args[0], args[1], args[2], &p)
	case "getHealth":
		return root.GetHealth(ctx)
	case "getMemberLogs":
		var p apiclient.GetMemberLogsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetMemberLogs(ctx, args[0], args[1], &p)
	case "getOperation":
		return c.GetOperation(ctx, args[0])
	case "getPITRWindow":
		var p apiclient.GetPITRWindowParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetPITRWindow(ctx, args[0], &p)
	case "getPermissions":
		return c.GetPermissions(ctx)
	case "getPrometheusMetrics":
		var p apiclient.GetPrometheusMetricsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return root.GetPrometheusMetrics(ctx, &p)
	case "getProvider":
		return c.GetProvider(ctx, args[0])
	case "getRBACPolicy":
		return c.GetRBACPolicy(ctx)
	case "getRestore":
		var p apiclient.GetRestoreParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.GetRestore(ctx, args[0], args[1], &p)
	case "getSettings":
		return c.GetSettings(ctx)
	case "getVersion":
		return c.GetVersion(ctx)
	case "listAPITokens":
		var p apiclient.ListAPITokensParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListAPITokens(ctx, &p)
	case "listAccounts":
		var p apiclient.ListAccountsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListAccounts(ctx, &p)
	case "listBackupStorages":
		var p apiclient.ListBackupStoragesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListBackupStorages(ctx, &p)
	case "listBackups":
		var p apiclient.ListBackupsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListBackups(ctx, args[0], &p)
	case "listClusterMembers":
		var p apiclient.ListClusterMembersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListClusterMembers(ctx, args[0], &p)
	case "listClusterOperations":
		var p apiclient.ListClusterOperationsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListClusterOperations(ctx, args[0], &p)
	case "listClusters":
		var p apiclient.ListClustersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListClusters(ctx, &p)
	case "listDatabaseRoles":
		var p apiclient.ListDatabaseRolesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListDatabaseRoles(ctx, args[0], &p)
	case "listDatabaseUsers":
		var p apiclient.ListDatabaseUsersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListDatabaseUsers(ctx, args[0], &p)
	case "listEvents":
		var p apiclient.ListEventsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListEvents(ctx, &p)
	case "listOrphanedVolumes":
		var p apiclient.ListOrphanedVolumesParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListOrphanedVolumes(ctx, &p)
	case "listOperations":
		var p apiclient.ListOperationsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListOperations(ctx, &p)
	case "listProviders":
		return c.ListProviders(ctx)
	case "listRestores":
		var p apiclient.ListRestoresParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListRestores(ctx, args[0], &p)
	case "listSystemUsers":
		var p apiclient.ListSystemUsersParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ListSystemUsers(ctx, args[0], &p)
	case "login":
		var p apiclient.LoginParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.LoginWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "logout":
		var p apiclient.LogoutParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.Logout(ctx, &p)
	case "oidcCallback":
		var p apiclient.OidcCallbackParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.OidcCallback(ctx, &p)
	case "oidcLogin":
		var p apiclient.OidcLoginParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.OidcLogin(ctx, &p)
	case "patchCluster":
		var p apiclient.PatchClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.PatchClusterWithBody(ctx, args[0], &p, "application/merge-patch+json", bytes.NewReader(body))
	case "raiseClusterFCV":
		var p apiclient.RaiseClusterFCVParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RaiseClusterFCVWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "refreshSession":
		var p apiclient.RefreshSessionParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RefreshSession(ctx, &p)
	case "replaceAPIToken":
		var p apiclient.ReplaceAPITokenParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceAPITokenWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceAccount":
		var p apiclient.ReplaceAccountParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceAccountWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceBackupStorage":
		var p apiclient.ReplaceBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceBackupStorageWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceCluster":
		var p apiclient.ReplaceClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceClusterWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceClusterBalancer":
		var p apiclient.ReplaceClusterBalancerParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceClusterBalancerWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "replaceClusterMember":
		var p apiclient.ReplaceClusterMemberParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceClusterMember(ctx, args[0], args[1], &p)
	case "replaceDatabaseRole":
		var p apiclient.ReplaceDatabaseRoleParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceDatabaseRoleWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "replaceDatabaseUser":
		var p apiclient.ReplaceDatabaseUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceDatabaseUserWithBody(ctx, args[0], args[1], args[2], &p, "application/json", bytes.NewReader(body))
	case "replaceRBACPolicy":
		var p apiclient.ReplaceRBACPolicyParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceRBACPolicyWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "replaceSettings":
		var p apiclient.ReplaceSettingsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ReplaceSettingsWithBody(ctx, &p, "application/json", bytes.NewReader(body))
	case "resetAccountPassword":
		var p apiclient.ResetAccountPasswordParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ResetAccountPasswordWithBody(ctx, args[0], &p, "application/json", bytes.NewReader(body))
	case "restartCluster":
		var p apiclient.RestartClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RestartCluster(ctx, args[0], &p)
	case "resumeCluster":
		var p apiclient.ResumeClusterParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.ResumeCluster(ctx, args[0], &p)
	case "rotateClusterTLS":
		var p apiclient.RotateClusterTLSParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotateClusterTLS(ctx, args[0], &p)
	case "rotateSystemUser":
		var p apiclient.RotateSystemUserParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.RotateSystemUser(ctx, args[0], args[1], &p)
	case "streamEvents":
		var p apiclient.StreamEventsParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.StreamEvents(ctx, &p)
	case "verifyBackupStorage":
		var p apiclient.VerifyBackupStorageParams
		if err = json.Unmarshal(q, &p); err != nil {
			return nil, err
		}
		return c.VerifyBackupStorage(ctx, args[0], &p)
	default:
		return nil, fmt.Errorf("unknown API operation %q", op)
	}
}
