//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

func TestMultiNamespace(t *testing.T) {
	project, token := os.Getenv("KARAKORAM_E2E_NAMESPACE_PROJECT_ID"), os.Getenv("KARAKORAM_E2E_NAMESPACE_TOKEN")
	if project == "" || token == "" {
		t.Skip("set KARAKORAM_E2E_NAMESPACE_PROJECT_ID and KARAKORAM_E2E_NAMESPACE_TOKEN")
	}
	s := newSuite(t, true)
	for _, key := range []string{"ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET", "REGION"} {
		if os.Getenv("KARAKORAM_E2E_STORAGE_"+key) == "" {
			t.Skip("set all five S3 storage creation variables for namespace-local storage")
		}
	}
	namespace := os.Getenv("KARAKORAM_E2E_NAMESPACE")
	if namespace == "" {
		namespace = s.prefix + "ns"
	}
	if namespace == "default" {
		t.Fatal("the test namespace must not be default")
	}
	var registered api.Namespace
	response, err := s.client.GetNamespace(s.ctx, namespace)
	if err != nil {
		t.Fatal("read namespace transport failed")
	}
	exists := response.StatusCode == http.StatusOK
	if exists {
		s.expect(&registered, 200)(response, nil)
		if registered.ProjectId != project {
			t.Fatal("existing namespace belongs to a different project")
		}
	} else {
		s.expect(nil, 404)(response, nil)
	}
	// Register first so cluster and storage cleanup precede namespace teardown.
	t.Cleanup(func() { s.cleanupNamespace(namespace) })
	if !exists {
		s.expect(&registered, 201)(s.client.CreateNamespace(s.ctx, &api.CreateNamespaceParams{}, api.NamespaceCreate{Name: namespace, ProjectId: project, Token: &token, Description: ptr("Multi-project end-to-end test")}))
	}
	s.poll("namespace relay ready", 15*time.Minute, func() bool {
		s.expect(&registered)(s.client.GetNamespace(s.ctx, namespace))
		if registered.Phase == api.NamespacePhaseError {
			t.Fatal("namespace entered Error")
		}
		return registered.Phase == api.NamespacePhaseReady && registered.Relay.Phase == api.RelayStatusPhaseReady && registered.Relay.Connected
	})
	scoped := s.inNamespace(namespace)
	storage := scoped.storage()
	spec := scoped.spec(3)
	spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
	name := scoped.create(spec)
	cluster := scoped.phase(name, api.ClusterStatusPhaseReady)
	if cluster.Namespace == nil || *cluster.Namespace != namespace {
		t.Fatal("cluster response omitted its namespace")
	}
	var status api.ClusterStatus
	s.expect(&status)(s.client.GetClusterStatus(s.ctx, name, &api.GetClusterStatusParams{Namespace: &namespace}))
	if status.Phase != api.ClusterStatusPhaseReady {
		t.Fatal("namespace cluster status is not Ready")
	}
	members := scoped.members(name)
	if len(members) != 3 {
		t.Fatalf("member count=%d want=3", len(members))
	}
	primary := 0
	for _, member := range members {
		if !member.Health {
			t.Fatal("namespace member is unhealthy")
		}
		if member.Role == api.MemberRolePrimary {
			primary++
		}
	}
	if primary != 1 {
		t.Fatal("replica set does not have exactly one primary")
	}
	connection := scoped.connection(name)
	if len(connection.Credentials) == 0 {
		t.Fatal("admin connection response omitted credentials")
	}
	password := scoped.name() + scoped.name()
	var user api.DatabaseUser
	s.expect(&user, 201)(s.client.CreateDatabaseUser(s.ctx, name, &api.CreateDatabaseUserParams{Namespace: &namespace}, api.CreateDatabaseUserRequest{Database: "e2e", Username: "application", Password: &password, Roles: []api.RoleReference{{Db: "e2e", Role: "readWrite"}}}))
	if user.Username != "application" {
		t.Fatal("database user was not created")
	}
	scoped.put(name, "namespace-proof", "original")
	backup := scoped.backup(name, storage, "logical")
	cloneSpec := scoped.cluster(name).Spec
	cloneSpec.Backup = nil // Storage references are local to the source namespace.
	cloneSpec.DataSource = &api.DataSource{Cluster: name, Namespace: &namespace, Backup: &backup.Name}
	target := s.inNamespace("default")
	clone := target.create(cloneSpec)
	target.proof(clone, "namespace-proof", "original")
	var restores api.RestoreList
	s.expect(&restores)(s.client.ListRestores(s.ctx, clone, &api.ListRestoresParams{Namespace: ptr("default")}))
	if len(restores.Items) == 0 {
		t.Fatal("cross-namespace clone has no restore record")
	}
	for _, restore := range restores.Items {
		if restore.Phase != api.RestorePhaseSucceeded {
			t.Fatal("cross-namespace restore did not succeed")
		}
	}
	s.namespaceRBAC(namespace, name, clone, cloneSpec)

	var before, after api.Namespace
	s.expect(&before)(s.client.GetNamespace(s.ctx, namespace))
	if before.Relay.ConnectedSince == nil || !before.Relay.Connected || before.Phase != api.NamespacePhaseReady {
		t.Fatal("relay is not connected before token rotation")
	}
	s.expect(&after)(s.client.RotateNamespaceToken(s.ctx, namespace, &api.RotateNamespaceTokenParams{}, api.NamespaceTokenRotate{Token: &token}))
	assertRotation := func(current api.Namespace) {
		if current.Phase != api.NamespacePhaseReady || !current.Relay.Connected || current.Relay.ConnectedSince == nil || !current.Relay.ConnectedSince.Equal(*before.Relay.ConnectedSince) {
			t.Fatal("token rotation changed the relay session or namespace readiness")
		}
	}
	assertRotation(after)
	// Observe beyond one reconciliation interval, not just the rotation response.
	select {
	case <-s.ctx.Done():
		t.Fatal("test context expired during rotation observation")
	case <-time.After(35 * time.Second):
	}
	s.expect(&after)(s.client.GetNamespace(s.ctx, namespace))
	assertRotation(after)
	s.expect(nil, 409)(s.client.DeleteNamespace(s.ctx, namespace, &api.DeleteNamespaceParams{}))
}

func (s *suite) namespaceRBAC(namespace, source, clone string, cloneSpec api.ClusterSpec) {
	s.t.Helper()
	var original api.RBACPolicy
	s.expect(&original)(s.client.GetRBACPolicy(s.ctx))
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.expect(nil)(s.client.ReplaceRBACPolicy(ctx, &api.ReplaceRBACPolicyParams{}, original))
	})
	account, password := s.account()
	subject := "user:" + account
	policy := original.Policy + "\np, " + subject + ", *, *, default/**, allow\np, " + subject + ", *, read, " + namespace + "/**, allow\np, " + subject + ", cluster-credentials, read, " + namespace + "/**, deny\n"
	s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, api.RBACPolicy{Policy: policy}))
	client := s.accountToken(account, password)
	s.expect(nil, 200)(client.GetCluster(s.ctx, source, &api.GetClusterParams{Namespace: &namespace}))
	s.expect(nil, 200)(client.GetClusterStatus(s.ctx, source, &api.GetClusterStatusParams{Namespace: &namespace}))
	s.expect(nil, 200)(client.ListClusterMembers(s.ctx, source, &api.ListClusterMembersParams{Namespace: &namespace}))
	s.expect(nil, 200)(client.ListBackups(s.ctx, source, &api.ListBackupsParams{Namespace: &namespace}))
	s.expect(nil, 200)(client.GetClusterConnection(s.ctx, clone, &api.GetClusterConnectionParams{Namespace: ptr("default")}))
	s.expect(nil, 403)(client.GetClusterConnection(s.ctx, source, &api.GetClusterConnectionParams{Namespace: &namespace}))
	s.expect(nil, 403)(client.DeleteCluster(s.ctx, source, &api.DeleteClusterParams{Namespace: &namespace}))
	denied := s.name()
	scoped := s.inNamespace(namespace)
	scoped.cleanupCluster(denied)
	s.expect(nil, 403)(client.CreateClusterWithBody(s.ctx, &api.CreateClusterParams{Namespace: &namespace}, "application/json", s.clusterBody(clusterWriteEnvelope{Name: denied, Spec: scoped.privateSpec(3)})))
	deniedClone := s.name()
	s.inNamespace("default").cleanupCluster(deniedClone)
	s.expect(nil, 403)(client.CreateClusterWithBody(s.ctx, &api.CreateClusterParams{Namespace: ptr("default")}, "application/json", s.clusterBody(clusterWriteEnvelope{Name: deniedClone, Spec: cloneSpec})))
	var permission api.CanResponse
	s.expect(&permission)(s.client.CheckPermission(s.ctx, &api.CheckPermissionParams{}, api.CanRequest{Subject: &subject, Resource: "backups", Action: api.CanRequestActionExport, Object: namespace + "/" + source}))
	if permission.Allowed {
		s.t.Fatal("read-only namespace policy granted backup export")
	}
	s.expect(nil)(s.client.ReplaceRBACPolicy(s.ctx, &api.ReplaceRBACPolicyParams{}, original))
}

func (s *suite) cleanupNamespace(namespace string) {
	if s.keep {
		s.t.Logf("KEEP namespace=%s", namespace)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	accepted := false
	for {
		response, err := s.client.GetNamespace(ctx, namespace)
		if err != nil {
			s.t.Error("namespace teardown transport failure")
			return
		}
		status := response.StatusCode
		response.Body.Close()
		if status == 404 {
			return
		}
		if status != 200 {
			s.t.Errorf("namespace teardown GET HTTP %d", status)
			return
		}
		if !accepted {
			response, err = s.client.DeleteNamespace(ctx, namespace, &api.DeleteNamespaceParams{})
			if err != nil {
				s.t.Error("namespace deletion transport failure")
				return
			}
			status = response.StatusCode
			response.Body.Close()
			if status == 404 {
				return
			}
			if status == 202 {
				accepted = true
			} else if status != 409 {
				s.t.Errorf("namespace deletion HTTP %d", status)
				return
			}
		}
		// Orphaned volumes may temporarily keep the deletion guard active.
		select {
		case <-ctx.Done():
			s.t.Error("namespace deletion timed out")
			return
		case <-time.After(5 * time.Second):
		}
	}
}
