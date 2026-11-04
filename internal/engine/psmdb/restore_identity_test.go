// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const identityOperation = "pitr-operation"

type identityFixture struct {
	t                                                 *testing.T
	p                                                 *Provider
	rc                                                *engine.Context
	mu                                                sync.Mutex
	exists                                            bool
	created, removed, dropAttempts, credentialChanges int
	appliedUser                                       string
	appliedVersion                                    int64
	ignoreReload                                      bool
}

func newIdentityFixture(t *testing.T) *identityFixture {
	t.Helper()
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
	s.Sets["rs0"].Stage = "ready"
	if err := rc.Store.(*store.Store).UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	f := &identityFixture{t: t, p: p, rc: rc, appliedUser: "backup", appliedVersion: s.Members[0].ConfigVersion}
	f.commit()
	native := nativeHealingFixture(t, rc, s)
	native.command = func(command bson.Raw) bson.M {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case command.Lookup("usersInfo").Type != 0:
			if f.exists {
				return bson.M{"ok": 1, "users": bson.A{bson.M{"user": f.username(), "db": "admin", "roles": bson.A{}}}}
			}
			return bson.M{"ok": 1, "users": bson.A{}}
		case command.Lookup("rolesInfo").Type != 0:
			values, _ := command.Lookup("rolesInfo").Array().Values()
			roles := bson.A{}
			for _, v := range values {
				roles = append(roles, v.Document())
			}
			return bson.M{"ok": 1, "roles": roles}
		case command.Lookup("createUser").Type != 0:
			if command.Lookup("createUser").StringValue() != f.username() {
				t.Error("created a non-scoped user")
			}
			password := command.Lookup("pwd").StringValue()
			generatedPasswordLimit.Assert(t, "native restore identity password", password)
			identity, err := loadRestorePBMIdentity(rc.Cluster.Internal)
			if err != nil || identity == nil {
				t.Error("missing restore identity", err)
				return bson.M{"ok": 0}
			}
			persisted, err := secret(t.Context(), rc, identity.Secret)
			if err != nil {
				t.Error(err)
			}
			generatedPasswordLimit.Assert(t, "persisted restore identity password", string(persisted))
			if password != string(persisted) {
				t.Error("native restore password differs from persisted identity")
			}
			f.created++
			f.exists = true
		case command.Lookup("dropUser").Type != 0:
			f.dropAttempts++
			if f.appliedUser != "backup" {
				t.Error("temporary account revoked before ordinary credentials were applied")
			}
			cluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Error(err)
			} else {
				identity, err := loadRestorePBMIdentity(cluster.Internal)
				if err != nil || identity == nil || identity.Stage != "releasing" || f.appliedVersion < identity.ConfigVersion {
					t.Error("temporary account revoked before durable release acknowledgement")
				}
			}
			if !f.exists {
				return bson.M{"ok": 0, "code": 11, "codeName": "UserNotFound", "errmsg": "user not found"}
			}
			f.removed++
			f.exists = false
		}
		return bson.M{"ok": 1}
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{PBMRestoreIdentity: true, ConfigVersion: f.appliedVersion})
		case "/v1/reload":
			// The real agent endpoint reloads a separately loaded STORED cluster,
			// never the hook's mutable in-memory desired object.
			storedRC := *rc
			cluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Error(err)
				http.Error(w, "store unavailable", 500)
				return
			}
			storedRC.Cluster = cluster
			cfg, err := p.MemberConfig(t.Context(), &storedRC, s.Members[0].Name)
			if err != nil {
				t.Error(err)
				http.Error(w, "configuration unavailable", 500)
				return
			}
			if cfg.SystemUsers["backup"].Username == f.username() {
				generatedPasswordLimit.Assert(t, "agent-loaded restore password", cfg.SystemUsers["backup"].Password)
				identity, err := loadRestorePBMIdentity(cluster.Internal)
				if err != nil || identity == nil {
					t.Error("missing published restore identity", err)
				} else if password, err := secret(t.Context(), &storedRC, identity.Secret); err != nil || cfg.SystemUsers["backup"].Password != string(password) {
					t.Error("agent-loaded restore password differs from persisted identity", err)
				}
			}
			if !f.ignoreReload {
				if cfg.ConfigVersion < f.appliedVersion {
					t.Error("published configuration generation regressed")
				}
				if cfg.SystemUsers["backup"].Username != f.appliedUser {
					f.credentialChanges++
				}
				f.appliedUser, f.appliedVersion = cfg.SystemUsers["backup"].Username, cfg.ConfigVersion
			}
			_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/pbm":
			if f.appliedUser != "backup" && !f.exists {
				t.Error("PBM used a revoked identity")
			}
			_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(`{}`)})
		}
	})
	return f
}

func (f *identityFixture) username() string { return "restore_" + secrets.Hash(identityOperation)[:24] }
func (f *identityFixture) hook() hooks {
	s := retryState(f.t, f.rc)
	return hooks{p: f.p, rc: f.rc, state: s}
}
func (f *identityFixture) commit() {
	f.t.Helper()
	if err := f.rc.Store.(*store.Store).UpdateClusterState(f.t.Context(), f.rc.Cluster); err != nil {
		f.t.Fatal(err)
	}
	f.restart()
}
func (f *identityFixture) restart() {
	f.t.Helper()
	cluster, err := f.rc.Store.GetCluster(f.t.Context(), f.rc.Cluster.Ref())
	if err != nil {
		f.t.Fatal(err)
	}
	f.rc.Cluster = cluster
	f.p = New(f.p.options)
}
func (f *identityFixture) prepare(wantDone bool) {
	f.t.Helper()
	done, err := f.hook().PreparePITRIdentity(f.t.Context(), identityOperation)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil || done != wantDone {
		f.t.Fatal("prepare result", done, err)
	}
}
func (f *identityFixture) finish(wantDone bool) {
	f.t.Helper()
	done, err := f.hook().FinishPITRIdentity(f.t.Context(), identityOperation)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil || done != wantDone {
		f.t.Fatal("release result", done, err)
	}
}

func TestPITRIdentityIsDurablyPublishedAndAcknowledgedBeforeUseOrRevocation(t *testing.T) {
	f := newIdentityFixture(t)
	f.prepare(false) // publish creating; no secret or external account yet
	f.prepare(false) // no commit means no external side effect
	if f.created != 0 {
		t.Fatal("account created before ownership commit")
	}
	f.commit()
	f.prepare(false) // account created, active intent only in memory
	f.prepare(false) // agent must not reload the still-stored creating intent
	if f.created != 1 || f.credentialChanges != 0 {
		t.Fatal("uncommitted active identity reached agents", f.created, f.credentialChanges)
	}
	f.commit()
	f.prepare(true)
	f.prepare(true) // lost prepared receipt: same generation/user, no restart
	if f.credentialChanges != 1 || f.appliedUser != f.username() {
		t.Fatal("isolated identity not actually acknowledged")
	}
	activeVersion := f.appliedVersion
	f.finish(false) // ordinary credential publication is not yet committed
	f.finish(false)
	if f.removed != 0 || f.appliedUser != f.username() {
		t.Fatal("uncommitted release revoked active identity")
	}
	f.commit()
	f.finish(true) // config now reads durable releasing state, then revoke
	f.restart()    // lose final state/checkpoint write, but secret/user are gone
	f.finish(true) // releasing replay is idempotent with ordinary credentials
	if f.removed != 1 || f.dropAttempts != 2 || f.credentialChanges != 2 || f.appliedUser != "backup" || f.appliedVersion <= activeVersion {
		t.Fatal("release ordering/idempotence failed", f.removed, f.dropAttempts, f.credentialChanges)
	}
	f.commit()
	identity, err := loadRestorePBMIdentity(f.rc.Cluster.Internal)
	if err != nil || identity != nil {
		t.Fatal("ownership survived completed release", identity, err)
	}
	if _, err := secret(t.Context(), f.rc, "restores/"+identityOperation+"/pbm"); !missing(err) {
		t.Fatal("temporary password retained", err)
	}
}

func TestPITRPreparationCancellationAfterLostCreationResponseCleansDurableOwnership(t *testing.T) {
	f := newIdentityFixture(t)
	f.prepare(false)
	f.commit()
	f.prepare(false) // external creation succeeds; lose active publication
	if f.created != 1 {
		t.Fatal("fixture did not create native account")
	}
	f.restart() // durable creating record must still identify account and secret
	f.finish(false)
	f.commit()
	f.finish(true)
	f.commit()
	if f.exists || f.removed != 1 || f.appliedUser != "backup" {
		t.Fatal("pre-native cancellation orphaned account")
	}
	if _, err := secret(t.Context(), f.rc, "restores/"+identityOperation+"/pbm"); !missing(err) {
		t.Fatal("pre-native cancellation orphaned password", err)
	}
}

func TestPITRIdentityRequiresAppliedConfigurationGeneration(t *testing.T) {
	f := newIdentityFixture(t)
	f.prepare(false)
	f.commit()
	f.prepare(false)
	f.commit()
	f.ignoreReload = true
	f.prepare(false)
	if f.appliedUser != "backup" {
		t.Fatal("fixture did not retain old configuration")
	}
	f.ignoreReload = false
	f.prepare(true)
	f.finish(false)
	f.commit()
	f.ignoreReload = true
	f.finish(false)
	if f.removed != 0 {
		t.Fatal("stale active generation passed release acknowledgement")
	}
	f.ignoreReload = false
	f.finish(true)
}

func TestPITRIdentityRequiresCapableAgentsBeforeNativeUserChanges(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(agentapi.Status{}) })
	if done, err := (hooks{p: p, rc: rc}).PreparePITRIdentity(t.Context(), "operation"); err == nil || done {
		t.Fatal("old agent admitted PITR", done, err)
	}
	identity, err := loadRestorePBMIdentity(rc.Cluster.Internal)
	if err != nil || identity != nil {
		t.Fatal("old agent admission created native identity", identity, err)
	}
}

func TestPITRIsolatedIdentityRecoversManagedUsersAfterHistoricalRotation(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
	s.Sets["rs0"].Stage = "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	identity := &restorePBMIdentity{User: "restore_isolated", Secret: "restores/operation/pbm", Stage: "active"}
	if err := putSecret(t.Context(), rc, identity.Secret, []byte("target-only-password")); err != nil {
		t.Fatal(err)
	}
	if err := setInternal(rc.Cluster, "pbmRestoreIdentity", identity); err != nil {
		t.Fatal(err)
	}
	native := nativeHealingFixture(t, rc, s)
	client, err := scram.SHA256.NewClient(identity.User, "target-only-password", "")
	if err != nil {
		t.Fatal(err)
	}
	verifier := client.GetStoredCredentials(scram.KeyFactors{Salt: "identity-fixture", Iters: 4096})
	native.auth, err = scram.SHA256.NewServer(func(name string) (scram.StoredCredentials, error) {
		if name != identity.User {
			return scram.StoredCredentials{}, fmt.Errorf("managed credentials were replaced during replay")
		}
		return verifier, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	repaired := map[string]bool{}
	native.command = func(command bson.Raw) bson.M {
		if command.Lookup("rolesInfo").Type != 0 {
			return bson.M{"ok": 1, "roles": bson.A{}}
		}
		if command.Lookup("usersInfo").Type != 0 {
			return bson.M{"ok": 1, "users": bson.A{}}
		}
		if command.Lookup("createUser").Type != 0 {
			repaired[command.Lookup("createUser").StringValue()] = true
		}
		return bson.M{"ok": 1}
	}
	if err := (hooks{p: p, rc: rc}).ResetSystemUsers(t.Context(), "unavailable-historical-admin"); err != nil {
		t.Fatal(err)
	}
	if len(repaired) != 5 || !repaired["backup"] || !repaired["userAdmin"] || repaired[identity.User] {
		t.Fatal("isolated identity failed to reassert managed users", repaired)
	}
}

func TestClusterDeletionRemovesRetainedPITRIdentitySecret(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	identity := &restorePBMIdentity{User: "restore_uncertain", Secret: "restores/operation/pbm", Stage: "active"}
	if err := putSecret(t.Context(), rc, identity.Secret, []byte("target-only-password")); err != nil {
		t.Fatal(err)
	}
	if err := setInternal(rc.Cluster, "pbmRestoreIdentity", identity); err != nil {
		t.Fatal(err)
	}
	for range 60 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if _, err := secret(t.Context(), rc, identity.Secret); !missing(err) {
				t.Fatal("deleted target retained temporary recovery password", err)
			}
			return
		}
	}
	t.Fatal("target deletion did not converge")
}
