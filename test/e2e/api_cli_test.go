//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAPICLI(t *testing.T) {
	t.Run("Workflow", testAPIWorkflow)
}

func testAPIWorkflow(t *testing.T) {
	binary := os.Getenv("KARAKORAM_E2E_KARACTL")
	if binary == "" {
		t.Skip("set KARAKORAM_E2E_KARACTL to the built karactl binary")
	}
	s := newSuite(t, false)
	storage := s.storage()
	run := s.karactlRunner()
	var identity api.CurrentUser
	run(nil, &identity, "whoami")
	if identity.Name == "" {
		t.Fatal("CLI whoami omitted account")
	}
	var provider api.Provider
	run(nil, &provider, "providers", "get", "psmdb")
	if len(provider.Versions) == 0 {
		t.Fatal("CLI provider catalog empty")
	}
	name := s.name()
	s.cleanupCluster(name)
	spec := s.privateSpec(1)
	spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
	var created api.Cluster
	run(clusterWriteEnvelope{Name: name, Spec: spec}, &created, "clusters", "create", "-f", "-")
	if created.Name != name {
		t.Fatal("CLI cluster create name mismatch")
	}
	s.ready(name)
	var list api.ClusterList
	run(nil, &list, "clusters", "list")
	found := false
	for _, c := range list.Items {
		if c.Name == name {
			found = true
		}
	}
	if !found {
		t.Fatal("CLI list omitted created cluster")
	}
	var got api.Cluster
	run(nil, &got, "clusters", "get", name)
	if got.Name != name || got.Status.Phase != api.ClusterStatusPhaseReady {
		t.Fatal("CLI cluster get mismatch")
	}
	var members api.MemberList
	run(nil, &members, "clusters", "members", name)
	if len(members.Items) != 1 {
		t.Fatal("CLI member list mismatch")
	}
	backupName := s.name()
	var backup api.Backup
	run(nil, &backup, "backups", "create", name, backupName, "--type", "logical", "--storage", storage)
	if backup.Name != backupName {
		t.Fatal("CLI backup create mismatch")
	}
	s.waitBackup(name, backupName)
	run(nil, &backup, "backups", "get", name, backupName)
	if backup.Phase != api.BackupPhaseSucceeded {
		t.Fatal("CLI backup get omitted successful phase")
	}
	var backups api.BackupList
	run(nil, &backups, "backups", "list", name)
	found = false
	for _, b := range backups.Items {
		if b.Name == backupName {
			found = true
		}
	}
	if !found {
		t.Fatal("CLI backup inventory omitted created backup")
	}
	s.snapshot(name+"-cli-backups", backups)
	restoreName := s.name()
	var restore api.Restore
	run(api.CreateRestoreRequest{Name: restoreName, Source: api.RestoreSource{Backup: ptr(backupName), SourceCluster: ptr(name)}}, &restore, "restores", "create", name, "-f", "-", "--yes")
	s.waitRestore(name, restoreName)
	run(nil, &restore, "restores", "get", name, restoreName)
	if restore.Phase != api.RestorePhaseSucceeded {
		t.Fatal("CLI restore did not succeed")
	}
	var restores api.RestoreList
	run(nil, &restores, "restores", "list", name)
	if len(restores.Items) != 1 || restores.Items[0].Name != restoreName {
		t.Fatal("CLI restore inventory mismatch")
	}
	var storageObject api.BackupStorage
	run(nil, &storageObject, "storages", "get", storage)
	var storageList api.BackupStorageList
	run(nil, &storageList, "storages", "list")
	var verification api.StorageVerification
	run(nil, &verification, "storages", "verify", storage)
	if !verification.Verified {
		t.Fatal("CLI storage verification failed")
	}
	role := api.DatabaseRole{Database: "e2e", Name: "cliapp", Privileges: []api.Privilege{{Resource: api.PrivilegeResource{Db: ptr("e2e"), Collection: ptr("")}, Actions: []string{"find", "insert"}}}, Roles: []api.RoleReference{}}
	var roleObject api.DatabaseRole
	run(role, &roleObject, "db-roles", "create", name, "-f", "-")
	run(nil, &roleObject, "db-roles", "get", name, "e2e", role.Name)
	var roleList api.DatabaseRoleList
	run(nil, &roleList, "db-roles", "list", name)
	role.Privileges[0].Actions = append(role.Privileges[0].Actions, "update")
	run(api.UpdateDatabaseRoleRequest{Privileges: role.Privileges, Roles: role.Roles}, &roleObject, "db-roles", "set", name, "e2e", role.Name, "-f", "-")
	var appUser api.DatabaseUser
	run(api.CreateDatabaseUserRequest{Database: "e2e", Username: "cliapp", Password: ptr(s.name() + s.name()), Roles: []api.RoleReference{{Db: "e2e", Role: role.Name}}}, &appUser, "db-users", "create", name, "-f", "-")
	run(nil, &appUser, "db-users", "get", name, "e2e", "cliapp")
	var userList api.DatabaseUserList
	run(nil, &userList, "db-users", "list", name)
	run(api.UpdateDatabaseUserRequest{Password: ptr(s.name() + s.name()), Roles: appUser.Roles}, &appUser, "db-users", "set", name, "e2e", "cliapp", "-f", "-")
	run(nil, nil, "db-users", "delete", name, "e2e", "cliapp", "--yes")
	run(nil, nil, "db-roles", "delete", name, "e2e", role.Name, "--yes")
	accountName := s.name()
	t.Cleanup(func() {
		if s.keep {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.expect(nil, 204, 404)(s.client.DeleteAccount(ctx, accountName, &api.DeleteAccountParams{}))
	})
	var account api.Account
	run(api.CreateAccountRequest{Name: accountName, Password: ptr(s.name() + s.name()), Enabled: ptr(true), MustChangePassword: ptr(false)}, &account, "accounts", "create", "-f", "-")
	run(nil, &account, "accounts", "get", accountName)
	var accountList api.AccountList
	run(nil, &accountList, "accounts", "list")
	run(api.UpdateAccountRequest{Enabled: true, MustChangePassword: false}, &account, "accounts", "set", accountName, "-f", "-")
	run(api.ResetPasswordRequest{Password: ptr(s.name() + s.name()), MustChangePassword: ptr(false)}, nil, "accounts", "reset-password", accountName, "-f", "-", "--yes")
	if !s.keep {
		run(nil, nil, "accounts", "delete", accountName, "--yes")
	}
	var token api.CreatedAPIToken
	run(api.CreateAPITokenRequest{Name: s.name()}, &token, "tokens", "create", "-f", "-")
	if token.Secret == nil || token.Token.Id == "" {
		t.Fatal("CLI token creation omitted secret/ID")
	}
	t.Cleanup(func() {
		if s.keep {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.expect(nil, 204, 404)(s.client.DeleteAPIToken(ctx, token.Token.Id, &api.DeleteAPITokenParams{}))
	})
	var tokenObject api.APIToken
	run(nil, &tokenObject, "tokens", "get", token.Token.Id)
	var tokenList api.APITokenList
	run(nil, &tokenList, "tokens", "list")
	run(api.UpdateAPITokenRequest{Name: "cli-renamed", ExpiresAt: nil}, &tokenObject, "tokens", "set", token.Token.Id, "-f", "-")
	bearer := s.newClient(*token.Secret, s.base+"/api/v1", nil)
	s.expect(&identity)(bearer.GetCurrentUser(s.ctx))
	if !s.keep {
		run(nil, nil, "tokens", "delete", token.Token.Id, "--yes")
		s.expect(nil, 401)(bearer.GetCurrentUser(s.ctx))
	}
	var policy api.RBACPolicy
	run(nil, &policy, "rbac", "policy", "get")
	if policy.Policy == "" {
		t.Fatal("CLI policy read empty")
	}
	var can api.CanResponse
	run(api.CanRequest{Resource: "clusters", Action: api.CanRequestActionRead, Object: name}, &can, "rbac", "can", "-f", "-")
	if !can.Allowed {
		t.Fatal("CLI permission check denied administrator")
	}
	var operations api.OperationList
	run(nil, &operations, "operations", "cluster", name)
	if len(operations.Items) == 0 {
		t.Fatal("CLI operations inventory empty")
	}
	run(nil, &operations, "operations", "list", "--cluster", name)
	var operation api.Operation
	run(nil, &operation, "operations", "get", operations.Items[0].Id)
	run(nil, &operation, "operations", "wait", operations.Items[0].Id)
	var events api.EventList
	run(nil, &events, "events", "list", "--cluster", name)
	if len(events.Items) == 0 {
		t.Fatal("CLI cluster events empty")
	}
	s.snapshot(name+"-cli-operations", operations)
	s.snapshot(name+"-cli-events", events)
	if !s.keep {
		run(nil, nil, "backups", "delete", name, backupName, "--yes")
		s.poll("CLI backup deletion", operationTimeout, func() bool {
			r, err := s.client.GetBackup(s.ctx, name, backupName, nil)
			if err != nil {
				t.Fatal("backup deletion status transport failed")
			}
			defer r.Body.Close()
			if r.StatusCode == 404 {
				return true
			}
			if r.StatusCode != 200 {
				t.Fatalf("backup deletion status HTTP %d", r.StatusCode)
			}
			return false
		})
	}
}

// karactlRunner shares the CLI workflow's isolated HOME and credential handling.
func (s *suite) karactlRunner() func(input any, out any, args ...string) {
	t := s.t
	t.Helper()
	binary := os.Getenv("KARAKORAM_E2E_KARACTL")
	if binary == "" {
		t.Skip("set KARAKORAM_E2E_KARACTL to the built karactl binary")
	}
	absolute, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(absolute); err != nil || info.IsDir() {
		t.Fatal("KARAKORAM_E2E_KARACTL is not a binary file")
	}
	home := t.TempDir()
	run := func(input any, out any, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(s.ctx, 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, absolute, append([]string{"--output", "json"}, args...)...)
		for _, v := range os.Environ() {
			if strings.HasPrefix(v, "HOME=") || strings.HasPrefix(v, "KARAKORAM_") {
				continue
			}
			command.Env = append(command.Env, v)
		}
		command.Env = append(command.Env, "HOME="+home, "KARAKORAM_SERVER="+s.base, "KARAKORAM_TOKEN="+s.token)
		if input != nil {
			b, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			command.Stdin = bytes.NewReader(b)
		}
		result, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("karactl %s failed: %v (output withheld to prevent credential disclosure)", strings.Join(args, " "), err)
		}
		if out != nil {
			if err := json.Unmarshal(result, out); err != nil {
				t.Fatalf("karactl %s returned invalid JSON: %v", strings.Join(args, " "), err)
			}
		}
		t.Logf("karactl %s succeeded", strings.Join(args, " "))
	}
	return run
}
