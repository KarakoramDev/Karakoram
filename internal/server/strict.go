// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Code generated from the strict API request/response inventory; DO NOT EDIT.
package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"net/http"
)

var _ api.StrictServerInterface = (*Server)(nil)

// ListAccounts implements the generated strict API operation.
func (s *Server) ListAccounts(ctx context.Context, request api.ListAccountsRequestObject) (api.ListAccountsResponseObject, error) {
	return s.handle(ctx, "ListAccounts", input{}), nil
}
func (r response) VisitListAccountsResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateAccount implements the generated strict API operation.
func (s *Server) CreateAccount(ctx context.Context, request api.CreateAccountRequestObject) (api.CreateAccountResponseObject, error) {
	return s.handle(ctx, "CreateAccount", input{Body: request.Body}), nil
}
func (r response) VisitCreateAccountResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteAccount implements the generated strict API operation.
func (s *Server) DeleteAccount(ctx context.Context, request api.DeleteAccountRequestObject) (api.DeleteAccountResponseObject, error) {
	return s.handle(ctx, "DeleteAccount", input{Account: request.Account}), nil
}
func (r response) VisitDeleteAccountResponse(w http.ResponseWriter) error { return r.write(w) }

// GetAccount implements the generated strict API operation.
func (s *Server) GetAccount(ctx context.Context, request api.GetAccountRequestObject) (api.GetAccountResponseObject, error) {
	return s.handle(ctx, "GetAccount", input{Account: request.Account}), nil
}
func (r response) VisitGetAccountResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceAccount implements the generated strict API operation.
func (s *Server) ReplaceAccount(ctx context.Context, request api.ReplaceAccountRequestObject) (api.ReplaceAccountResponseObject, error) {
	return s.handle(ctx, "ReplaceAccount", input{Account: request.Account, Body: request.Body}), nil
}
func (r response) VisitReplaceAccountResponse(w http.ResponseWriter) error { return r.write(w) }

// ResetAccountPassword implements the generated strict API operation.
func (s *Server) ResetAccountPassword(ctx context.Context, request api.ResetAccountPasswordRequestObject) (api.ResetAccountPasswordResponseObject, error) {
	return s.handle(ctx, "ResetAccountPassword", input{Account: request.Account, Body: request.Body}), nil
}
func (r response) VisitResetAccountPasswordResponse(w http.ResponseWriter) error { return r.write(w) }

// ListAPITokens implements the generated strict API operation.
func (s *Server) ListAPITokens(ctx context.Context, request api.ListAPITokensRequestObject) (api.ListAPITokensResponseObject, error) {
	return s.handle(ctx, "ListAPITokens", input{}), nil
}
func (r response) VisitListAPITokensResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateAPIToken implements the generated strict API operation.
func (s *Server) CreateAPIToken(ctx context.Context, request api.CreateAPITokenRequestObject) (api.CreateAPITokenResponseObject, error) {
	return s.handle(ctx, "CreateAPIToken", input{Body: request.Body}), nil
}
func (r response) VisitCreateAPITokenResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteAPIToken implements the generated strict API operation.
func (s *Server) DeleteAPIToken(ctx context.Context, request api.DeleteAPITokenRequestObject) (api.DeleteAPITokenResponseObject, error) {
	return s.handle(ctx, "DeleteAPIToken", input{Token: request.Token}), nil
}
func (r response) VisitDeleteAPITokenResponse(w http.ResponseWriter) error { return r.write(w) }

// GetAPIToken implements the generated strict API operation.
func (s *Server) GetAPIToken(ctx context.Context, request api.GetAPITokenRequestObject) (api.GetAPITokenResponseObject, error) {
	return s.handle(ctx, "GetAPIToken", input{Token: request.Token}), nil
}
func (r response) VisitGetAPITokenResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceAPIToken implements the generated strict API operation.
func (s *Server) ReplaceAPIToken(ctx context.Context, request api.ReplaceAPITokenRequestObject) (api.ReplaceAPITokenResponseObject, error) {
	return s.handle(ctx, "ReplaceAPIToken", input{Token: request.Token, Body: request.Body}), nil
}
func (r response) VisitReplaceAPITokenResponse(w http.ResponseWriter) error { return r.write(w) }

// ChangePassword implements the generated strict API operation.
func (s *Server) ChangePassword(ctx context.Context, request api.ChangePasswordRequestObject) (api.ChangePasswordResponseObject, error) {
	return s.handle(ctx, "ChangePassword", input{Body: request.Body}), nil
}
func (r response) VisitChangePasswordResponse(w http.ResponseWriter) error { return r.write(w) }

// Login implements the generated strict API operation.
func (s *Server) Login(ctx context.Context, request api.LoginRequestObject) (api.LoginResponseObject, error) {
	return s.handle(ctx, "Login", input{Body: request.Body}), nil
}
func (r response) VisitLoginResponse(w http.ResponseWriter) error { return r.write(w) }

// Logout implements the generated strict API operation.
func (s *Server) Logout(ctx context.Context, request api.LogoutRequestObject) (api.LogoutResponseObject, error) {
	return s.handle(ctx, "Logout", input{}), nil
}
func (r response) VisitLogoutResponse(w http.ResponseWriter) error { return r.write(w) }

// GetCurrentUser implements the generated strict API operation.
func (s *Server) GetCurrentUser(ctx context.Context, request api.GetCurrentUserRequestObject) (api.GetCurrentUserResponseObject, error) {
	return s.handle(ctx, "GetCurrentUser", input{}), nil
}
func (r response) VisitGetCurrentUserResponse(w http.ResponseWriter) error { return r.write(w) }

// OidcCallback implements the generated strict API operation.
func (s *Server) OidcCallback(ctx context.Context, request api.OidcCallbackRequestObject) (api.OidcCallbackResponseObject, error) {
	return s.handle(ctx, "OidcCallback", input{}), nil
}
func (r response) VisitOidcCallbackResponse(w http.ResponseWriter) error { return r.write(w) }

// OidcLogin implements the generated strict API operation.
func (s *Server) OidcLogin(ctx context.Context, request api.OidcLoginRequestObject) (api.OidcLoginResponseObject, error) {
	return s.handle(ctx, "OidcLogin", input{}), nil
}
func (r response) VisitOidcLoginResponse(w http.ResponseWriter) error { return r.write(w) }

// RefreshSession implements the generated strict API operation.
func (s *Server) RefreshSession(ctx context.Context, request api.RefreshSessionRequestObject) (api.RefreshSessionResponseObject, error) {
	return s.handle(ctx, "RefreshSession", input{}), nil
}
func (r response) VisitRefreshSessionResponse(w http.ResponseWriter) error { return r.write(w) }

// ListBackupStorages implements the generated strict API operation.
func (s *Server) ListBackupStorages(ctx context.Context, request api.ListBackupStoragesRequestObject) (api.ListBackupStoragesResponseObject, error) {
	return s.handle(ctx, "ListBackupStorages", input{}), nil
}
func (r response) VisitListBackupStoragesResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateBackupStorage implements the generated strict API operation.
func (s *Server) CreateBackupStorage(ctx context.Context, request api.CreateBackupStorageRequestObject) (api.CreateBackupStorageResponseObject, error) {
	return s.handle(ctx, "CreateBackupStorage", input{Body: request.Body}), nil
}
func (r response) VisitCreateBackupStorageResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteBackupStorage implements the generated strict API operation.
func (s *Server) DeleteBackupStorage(ctx context.Context, request api.DeleteBackupStorageRequestObject) (api.DeleteBackupStorageResponseObject, error) {
	return s.handle(ctx, "DeleteBackupStorage", input{Storage: request.Storage}), nil
}
func (r response) VisitDeleteBackupStorageResponse(w http.ResponseWriter) error { return r.write(w) }

// GetBackupStorage implements the generated strict API operation.
func (s *Server) GetBackupStorage(ctx context.Context, request api.GetBackupStorageRequestObject) (api.GetBackupStorageResponseObject, error) {
	return s.handle(ctx, "GetBackupStorage", input{Storage: request.Storage}), nil
}
func (r response) VisitGetBackupStorageResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceBackupStorage implements the generated strict API operation.
func (s *Server) ReplaceBackupStorage(ctx context.Context, request api.ReplaceBackupStorageRequestObject) (api.ReplaceBackupStorageResponseObject, error) {
	return s.handle(ctx, "ReplaceBackupStorage", input{Storage: request.Storage, Body: request.Body}), nil
}
func (r response) VisitReplaceBackupStorageResponse(w http.ResponseWriter) error { return r.write(w) }

// VerifyBackupStorage implements the generated strict API operation.
func (s *Server) VerifyBackupStorage(ctx context.Context, request api.VerifyBackupStorageRequestObject) (api.VerifyBackupStorageResponseObject, error) {
	return s.handle(ctx, "VerifyBackupStorage", input{Storage: request.Storage}), nil
}
func (r response) VisitVerifyBackupStorageResponse(w http.ResponseWriter) error { return r.write(w) }

// ListClusters implements the generated strict API operation.
func (s *Server) ListClusters(ctx context.Context, request api.ListClustersRequestObject) (api.ListClustersResponseObject, error) {
	return s.handle(ctx, "ListClusters", input{}), nil
}
func (r response) VisitListClustersResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateCluster implements the generated strict API operation.
func (s *Server) CreateCluster(ctx context.Context, request api.CreateClusterRequestObject) (api.CreateClusterResponseObject, error) {
	return s.handle(ctx, "CreateCluster", input{Body: request.Body}), nil
}
func (r response) VisitCreateClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteCluster implements the generated strict API operation.
func (s *Server) DeleteCluster(ctx context.Context, request api.DeleteClusterRequestObject) (api.DeleteClusterResponseObject, error) {
	return s.handle(ctx, "DeleteCluster", input{Cluster: request.Cluster}), nil
}
func (r response) VisitDeleteClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// GetCluster implements the generated strict API operation.
func (s *Server) GetCluster(ctx context.Context, request api.GetClusterRequestObject) (api.GetClusterResponseObject, error) {
	return s.handle(ctx, "GetCluster", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// PatchCluster implements the generated strict API operation.
func (s *Server) PatchCluster(ctx context.Context, request api.PatchClusterRequestObject) (api.PatchClusterResponseObject, error) {
	return s.handle(ctx, "PatchCluster", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitPatchClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceCluster implements the generated strict API operation.
func (s *Server) ReplaceCluster(ctx context.Context, request api.ReplaceClusterRequestObject) (api.ReplaceClusterResponseObject, error) {
	return s.handle(ctx, "ReplaceCluster", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitReplaceClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// ListBackups implements the generated strict API operation.
func (s *Server) ListBackups(ctx context.Context, request api.ListBackupsRequestObject) (api.ListBackupsResponseObject, error) {
	return s.handle(ctx, "ListBackups", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListBackupsResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateBackup implements the generated strict API operation.
func (s *Server) CreateBackup(ctx context.Context, request api.CreateBackupRequestObject) (api.CreateBackupResponseObject, error) {
	return s.handle(ctx, "CreateBackup", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateBackupResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteBackup implements the generated strict API operation.
func (s *Server) DeleteBackup(ctx context.Context, request api.DeleteBackupRequestObject) (api.DeleteBackupResponseObject, error) {
	return s.handle(ctx, "DeleteBackup", input{Cluster: request.Cluster, Backup: request.Backup}), nil
}
func (r response) VisitDeleteBackupResponse(w http.ResponseWriter) error { return r.write(w) }

// GetBackup implements the generated strict API operation.
func (s *Server) GetBackup(ctx context.Context, request api.GetBackupRequestObject) (api.GetBackupResponseObject, error) {
	return s.handle(ctx, "GetBackup", input{Cluster: request.Cluster, Backup: request.Backup}), nil
}
func (r response) VisitGetBackupResponse(w http.ResponseWriter) error { return r.write(w) }

// GetClusterBalancer implements the generated strict API operation.
func (s *Server) GetClusterBalancer(ctx context.Context, request api.GetClusterBalancerRequestObject) (api.GetClusterBalancerResponseObject, error) {
	return s.handle(ctx, "GetClusterBalancer", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetClusterBalancerResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceClusterBalancer implements the generated strict API operation.
func (s *Server) ReplaceClusterBalancer(ctx context.Context, request api.ReplaceClusterBalancerRequestObject) (api.ReplaceClusterBalancerResponseObject, error) {
	return s.handle(ctx, "ReplaceClusterBalancer", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitReplaceClusterBalancerResponse(w http.ResponseWriter) error { return r.write(w) }

// GetClusterConnection implements the generated strict API operation.
func (s *Server) GetClusterConnection(ctx context.Context, request api.GetClusterConnectionRequestObject) (api.GetClusterConnectionResponseObject, error) {
	return s.handle(ctx, "GetClusterConnection", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetClusterConnectionResponse(w http.ResponseWriter) error { return r.write(w) }

// ListDatabaseRoles implements the generated strict API operation.
func (s *Server) ListDatabaseRoles(ctx context.Context, request api.ListDatabaseRolesRequestObject) (api.ListDatabaseRolesResponseObject, error) {
	return s.handle(ctx, "ListDatabaseRoles", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListDatabaseRolesResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateDatabaseRole implements the generated strict API operation.
func (s *Server) CreateDatabaseRole(ctx context.Context, request api.CreateDatabaseRoleRequestObject) (api.CreateDatabaseRoleResponseObject, error) {
	return s.handle(ctx, "CreateDatabaseRole", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateDatabaseRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteDatabaseRole implements the generated strict API operation.
func (s *Server) DeleteDatabaseRole(ctx context.Context, request api.DeleteDatabaseRoleRequestObject) (api.DeleteDatabaseRoleResponseObject, error) {
	return s.handle(ctx, "DeleteDatabaseRole", input{Cluster: request.Cluster, Database: request.Database, Role: request.Role}), nil
}
func (r response) VisitDeleteDatabaseRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// GetDatabaseRole implements the generated strict API operation.
func (s *Server) GetDatabaseRole(ctx context.Context, request api.GetDatabaseRoleRequestObject) (api.GetDatabaseRoleResponseObject, error) {
	return s.handle(ctx, "GetDatabaseRole", input{Cluster: request.Cluster, Database: request.Database, Role: request.Role}), nil
}
func (r response) VisitGetDatabaseRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceDatabaseRole implements the generated strict API operation.
func (s *Server) ReplaceDatabaseRole(ctx context.Context, request api.ReplaceDatabaseRoleRequestObject) (api.ReplaceDatabaseRoleResponseObject, error) {
	return s.handle(ctx, "ReplaceDatabaseRole", input{Cluster: request.Cluster, Database: request.Database, Role: request.Role, Body: request.Body}), nil
}
func (r response) VisitReplaceDatabaseRoleResponse(w http.ResponseWriter) error { return r.write(w) }

// ListDatabaseUsers implements the generated strict API operation.
func (s *Server) ListDatabaseUsers(ctx context.Context, request api.ListDatabaseUsersRequestObject) (api.ListDatabaseUsersResponseObject, error) {
	return s.handle(ctx, "ListDatabaseUsers", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListDatabaseUsersResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateDatabaseUser implements the generated strict API operation.
func (s *Server) CreateDatabaseUser(ctx context.Context, request api.CreateDatabaseUserRequestObject) (api.CreateDatabaseUserResponseObject, error) {
	return s.handle(ctx, "CreateDatabaseUser", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateDatabaseUserResponse(w http.ResponseWriter) error { return r.write(w) }

// DeleteDatabaseUser implements the generated strict API operation.
func (s *Server) DeleteDatabaseUser(ctx context.Context, request api.DeleteDatabaseUserRequestObject) (api.DeleteDatabaseUserResponseObject, error) {
	return s.handle(ctx, "DeleteDatabaseUser", input{Cluster: request.Cluster, Database: request.Database, Username: request.Username}), nil
}
func (r response) VisitDeleteDatabaseUserResponse(w http.ResponseWriter) error { return r.write(w) }

// GetDatabaseUser implements the generated strict API operation.
func (s *Server) GetDatabaseUser(ctx context.Context, request api.GetDatabaseUserRequestObject) (api.GetDatabaseUserResponseObject, error) {
	return s.handle(ctx, "GetDatabaseUser", input{Cluster: request.Cluster, Database: request.Database, Username: request.Username}), nil
}
func (r response) VisitGetDatabaseUserResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceDatabaseUser implements the generated strict API operation.
func (s *Server) ReplaceDatabaseUser(ctx context.Context, request api.ReplaceDatabaseUserRequestObject) (api.ReplaceDatabaseUserResponseObject, error) {
	return s.handle(ctx, "ReplaceDatabaseUser", input{Cluster: request.Cluster, Database: request.Database, Username: request.Username, Body: request.Body}), nil
}
func (r response) VisitReplaceDatabaseUserResponse(w http.ResponseWriter) error { return r.write(w) }

// ListClusterMembers implements the generated strict API operation.
func (s *Server) ListClusterMembers(ctx context.Context, request api.ListClusterMembersRequestObject) (api.ListClusterMembersResponseObject, error) {
	return s.handle(ctx, "ListClusterMembers", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListClusterMembersResponse(w http.ResponseWriter) error { return r.write(w) }

// GetClusterMember implements the generated strict API operation.
func (s *Server) GetClusterMember(ctx context.Context, request api.GetClusterMemberRequestObject) (api.GetClusterMemberResponseObject, error) {
	return s.handle(ctx, "GetClusterMember", input{Cluster: request.Cluster, Member: request.Member}), nil
}
func (r response) VisitGetClusterMemberResponse(w http.ResponseWriter) error { return r.write(w) }

// GetMemberLogs implements the generated strict API operation.
func (s *Server) GetMemberLogs(ctx context.Context, request api.GetMemberLogsRequestObject) (api.GetMemberLogsResponseObject, error) {
	return s.handle(ctx, "GetMemberLogs", input{Cluster: request.Cluster, Member: request.Member}), nil
}
func (r response) VisitGetMemberLogsResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceClusterMember implements the generated strict API operation.
func (s *Server) ReplaceClusterMember(ctx context.Context, request api.ReplaceClusterMemberRequestObject) (api.ReplaceClusterMemberResponseObject, error) {
	return s.handle(ctx, "ReplaceClusterMember", input{Cluster: request.Cluster, Member: request.Member}), nil
}
func (r response) VisitReplaceClusterMemberResponse(w http.ResponseWriter) error { return r.write(w) }

// GetClusterMetrics implements the generated strict API operation.
func (s *Server) GetClusterMetrics(ctx context.Context, request api.GetClusterMetricsRequestObject) (api.GetClusterMetricsResponseObject, error) {
	return s.handle(ctx, "GetClusterMetrics", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetClusterMetricsResponse(w http.ResponseWriter) error { return r.write(w) }

// ListClusterOperations implements the generated strict API operation.
func (s *Server) ListClusterOperations(ctx context.Context, request api.ListClusterOperationsRequestObject) (api.ListClusterOperationsResponseObject, error) {
	return s.handle(ctx, "ListClusterOperations", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListClusterOperationsResponse(w http.ResponseWriter) error { return r.write(w) }

// GetPITRWindow implements the generated strict API operation.
func (s *Server) GetPITRWindow(ctx context.Context, request api.GetPITRWindowRequestObject) (api.GetPITRWindowResponseObject, error) {
	return s.handle(ctx, "GetPITRWindow", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetPITRWindowResponse(w http.ResponseWriter) error { return r.write(w) }

// GetClusterPlacement implements the generated strict API operation.
func (s *Server) GetClusterPlacement(ctx context.Context, request api.GetClusterPlacementRequestObject) (api.GetClusterPlacementResponseObject, error) {
	return s.handle(ctx, "GetClusterPlacement", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetClusterPlacementResponse(w http.ResponseWriter) error { return r.write(w) }

// RaiseClusterFCV implements the generated strict API operation.
func (s *Server) RaiseClusterFCV(ctx context.Context, request api.RaiseClusterFCVRequestObject) (api.RaiseClusterFCVResponseObject, error) {
	return s.handle(ctx, "RaiseClusterFCV", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitRaiseClusterFCVResponse(w http.ResponseWriter) error { return r.write(w) }

// RestartCluster implements the generated strict API operation.
func (s *Server) RestartCluster(ctx context.Context, request api.RestartClusterRequestObject) (api.RestartClusterResponseObject, error) {
	return s.handle(ctx, "RestartCluster", input{Cluster: request.Cluster}), nil
}
func (r response) VisitRestartClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// ListRestores implements the generated strict API operation.
func (s *Server) ListRestores(ctx context.Context, request api.ListRestoresRequestObject) (api.ListRestoresResponseObject, error) {
	return s.handle(ctx, "ListRestores", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListRestoresResponse(w http.ResponseWriter) error { return r.write(w) }

// CreateRestore implements the generated strict API operation.
func (s *Server) CreateRestore(ctx context.Context, request api.CreateRestoreRequestObject) (api.CreateRestoreResponseObject, error) {
	return s.handle(ctx, "CreateRestore", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateRestoreResponse(w http.ResponseWriter) error { return r.write(w) }

// GetRestore implements the generated strict API operation.
func (s *Server) GetRestore(ctx context.Context, request api.GetRestoreRequestObject) (api.GetRestoreResponseObject, error) {
	return s.handle(ctx, "GetRestore", input{Cluster: request.Cluster, Restore: request.Restore}), nil
}
func (r response) VisitGetRestoreResponse(w http.ResponseWriter) error { return r.write(w) }

// ResumeCluster implements the generated strict API operation.
func (s *Server) ResumeCluster(ctx context.Context, request api.ResumeClusterRequestObject) (api.ResumeClusterResponseObject, error) {
	return s.handle(ctx, "ResumeCluster", input{Cluster: request.Cluster}), nil
}
func (r response) VisitResumeClusterResponse(w http.ResponseWriter) error { return r.write(w) }

// RotateClusterTLS implements the generated strict API operation.
func (s *Server) RotateClusterTLS(ctx context.Context, request api.RotateClusterTLSRequestObject) (api.RotateClusterTLSResponseObject, error) {
	return s.handle(ctx, "RotateClusterTLS", input{Cluster: request.Cluster}), nil
}
func (r response) VisitRotateClusterTLSResponse(w http.ResponseWriter) error { return r.write(w) }

// GetClusterStatus implements the generated strict API operation.
func (s *Server) GetClusterStatus(ctx context.Context, request api.GetClusterStatusRequestObject) (api.GetClusterStatusResponseObject, error) {
	return s.handle(ctx, "GetClusterStatus", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetClusterStatusResponse(w http.ResponseWriter) error { return r.write(w) }

// ListSystemUsers implements the generated strict API operation.
func (s *Server) ListSystemUsers(ctx context.Context, request api.ListSystemUsersRequestObject) (api.ListSystemUsersResponseObject, error) {
	return s.handle(ctx, "ListSystemUsers", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListSystemUsersResponse(w http.ResponseWriter) error { return r.write(w) }

// RotateSystemUser implements the generated strict API operation.
func (s *Server) RotateSystemUser(ctx context.Context, request api.RotateSystemUserRequestObject) (api.RotateSystemUserResponseObject, error) {
	return s.handle(ctx, "RotateSystemUser", input{Cluster: request.Cluster, User: request.User}), nil
}
func (r response) VisitRotateSystemUserResponse(w http.ResponseWriter) error { return r.write(w) }

// ListEvents implements the generated strict API operation.
func (s *Server) ListEvents(ctx context.Context, request api.ListEventsRequestObject) (api.ListEventsResponseObject, error) {
	return s.handle(ctx, "ListEvents", input{}), nil
}
func (r response) VisitListEventsResponse(w http.ResponseWriter) error { return r.write(w) }

// StreamEvents implements the generated strict API operation.
func (s *Server) StreamEvents(ctx context.Context, request api.StreamEventsRequestObject) (api.StreamEventsResponseObject, error) {
	return s.handle(ctx, "StreamEvents", input{}), nil
}
func (r response) VisitStreamEventsResponse(w http.ResponseWriter) error { return r.write(w) }

// GetHealth implements the generated strict API operation.
func (s *Server) GetHealth(ctx context.Context, request api.GetHealthRequestObject) (api.GetHealthResponseObject, error) {
	return s.handle(ctx, "GetHealth", input{}), nil
}
func (r response) VisitGetHealthResponse(w http.ResponseWriter) error { return r.write(w) }

// GetPrometheusMetrics implements the generated strict API operation.
func (s *Server) GetPrometheusMetrics(ctx context.Context, request api.GetPrometheusMetricsRequestObject) (api.GetPrometheusMetricsResponseObject, error) {
	return s.handle(ctx, "GetPrometheusMetrics", input{}), nil
}
func (r response) VisitGetPrometheusMetricsResponse(w http.ResponseWriter) error { return r.write(w) }

// ListOperations implements the generated strict API operation.
func (s *Server) ListOperations(ctx context.Context, request api.ListOperationsRequestObject) (api.ListOperationsResponseObject, error) {
	return s.handle(ctx, "ListOperations", input{}), nil
}
func (r response) VisitListOperationsResponse(w http.ResponseWriter) error { return r.write(w) }

// GetOperation implements the generated strict API operation.
func (s *Server) GetOperation(ctx context.Context, request api.GetOperationRequestObject) (api.GetOperationResponseObject, error) {
	return s.handle(ctx, "GetOperation", input{Operation: request.Operation}), nil
}
func (r response) VisitGetOperationResponse(w http.ResponseWriter) error { return r.write(w) }

// GetPermissions implements the generated strict API operation.
func (s *Server) GetPermissions(ctx context.Context, request api.GetPermissionsRequestObject) (api.GetPermissionsResponseObject, error) {
	return s.handle(ctx, "GetPermissions", input{}), nil
}
func (r response) VisitGetPermissionsResponse(w http.ResponseWriter) error { return r.write(w) }

// ListProviders implements the generated strict API operation.
func (s *Server) ListProviders(ctx context.Context, request api.ListProvidersRequestObject) (api.ListProvidersResponseObject, error) {
	return s.handle(ctx, "ListProviders", input{}), nil
}
func (r response) VisitListProvidersResponse(w http.ResponseWriter) error { return r.write(w) }

// GetProvider implements the generated strict API operation.
func (s *Server) GetProvider(ctx context.Context, request api.GetProviderRequestObject) (api.GetProviderResponseObject, error) {
	return s.handle(ctx, "GetProvider", input{Provider: request.Provider}), nil
}
func (r response) VisitGetProviderResponse(w http.ResponseWriter) error { return r.write(w) }

// CheckPermission implements the generated strict API operation.
func (s *Server) CheckPermission(ctx context.Context, request api.CheckPermissionRequestObject) (api.CheckPermissionResponseObject, error) {
	return s.handle(ctx, "CheckPermission", input{Body: request.Body}), nil
}
func (r response) VisitCheckPermissionResponse(w http.ResponseWriter) error { return r.write(w) }

// GetRBACPolicy implements the generated strict API operation.
func (s *Server) GetRBACPolicy(ctx context.Context, request api.GetRBACPolicyRequestObject) (api.GetRBACPolicyResponseObject, error) {
	return s.handle(ctx, "GetRBACPolicy", input{}), nil
}
func (r response) VisitGetRBACPolicyResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceRBACPolicy implements the generated strict API operation.
func (s *Server) ReplaceRBACPolicy(ctx context.Context, request api.ReplaceRBACPolicyRequestObject) (api.ReplaceRBACPolicyResponseObject, error) {
	return s.handle(ctx, "ReplaceRBACPolicy", input{Body: request.Body}), nil
}
func (r response) VisitReplaceRBACPolicyResponse(w http.ResponseWriter) error { return r.write(w) }

// GetSettings implements the generated strict API operation.
func (s *Server) GetSettings(ctx context.Context, request api.GetSettingsRequestObject) (api.GetSettingsResponseObject, error) {
	return s.handle(ctx, "GetSettings", input{}), nil
}
func (r response) VisitGetSettingsResponse(w http.ResponseWriter) error { return r.write(w) }

// ReplaceSettings implements the generated strict API operation.
func (s *Server) ReplaceSettings(ctx context.Context, request api.ReplaceSettingsRequestObject) (api.ReplaceSettingsResponseObject, error) {
	return s.handle(ctx, "ReplaceSettings", input{Body: request.Body}), nil
}
func (r response) VisitReplaceSettingsResponse(w http.ResponseWriter) error { return r.write(w) }

// GetVersion implements the generated strict API operation.
func (s *Server) GetVersion(ctx context.Context, request api.GetVersionRequestObject) (api.GetVersionResponseObject, error) {
	return s.handle(ctx, "GetVersion", input{}), nil
}
func (r response) VisitGetVersionResponse(w http.ResponseWriter) error { return r.write(w) }
