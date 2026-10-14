//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

// Each scenario provisions its own cluster, so selecting a phase with -run
// never depends on a previous subtest's resources or side effects.
func TestRabbitMQCluster(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		s.rabbitData("PublishConsume", func(s *suite) {
			s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue))
			s.rabbitProof(name, "/", "admin", "", queue, rabbitPayload(queue))
		})
		stream := s.name()
		s.rabbitData("StreamPublishConsume", func(s *suite) {
			s.rabbitStreamPublish(name, "/", "admin", "", stream, rabbitPayload(stream))
			s.rabbitStreamProof(name, "/", "admin", "", stream, rabbitPayload(stream))
		})
	})
	t.Run("Restart", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		stream := s.name()
		if !s.rabbitData("Seed", func(s *suite) {
			s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue))
			s.rabbitStreamPublish(name, "/", "admin", "", stream, rabbitPayload(stream))
		}) {
			t.FailNow()
		}
		before := s.members(name)
		observedAt := time.Now()
		s.operation(s.client.RestartCluster(s.ctx, name, &api.RestartClusterParams{}))
		s.assertRabbitMembers(name, 3, "4.3.6")
		after := s.members(name)
		finishedAt := time.Now()
		for _, old := range before {
			index := slices.IndexFunc(after, func(m observedMember) bool { return m.Name == old.Name })
			if index < 0 || old.Uptime == nil || after[index].Uptime == nil {
				t.Fatal("restart lacks stable membership or uptime evidence")
			}
			oldBoot := observedAt.Add(-time.Duration(*old.Uptime) * time.Second)
			newBoot := finishedAt.Add(-time.Duration(*after[index].Uptime) * time.Second)
			if !newBoot.After(oldBoot.Add(time.Second)) {
				t.Fatalf("member %s has no observed uptime reset", old.Name)
			}
		}
		s.snapshot(name+"-restart-before", before)
		s.snapshot(name+"-restart-after", after)
		s.rabbitData("Persistence", func(s *suite) { s.rabbitProof(name, "/", "admin", "", queue, rabbitPayload(queue)) })
		s.rabbitData("StreamPersistence", func(s *suite) { s.rabbitStreamProof(name, "/", "admin", "", stream, rabbitPayload(stream)) })
	})
	t.Run("ReplaceMember", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		stream := s.name()
		if !s.rabbitData("Seed", func(s *suite) {
			s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue))
			s.rabbitStreamPublish(name, "/", "admin", "", stream, rabbitPayload(stream))
		}) {
			t.FailNow()
		}
		old := s.members(name)[0].Name
		s.operation(s.client.ReplaceClusterMember(s.ctx, name, old, &api.ReplaceClusterMemberParams{}))
		s.assertRabbitMembers(name, 3, "4.3.6")
		if slices.ContainsFunc(s.members(name), func(m observedMember) bool { return m.Name == old }) {
			t.Fatal("replaced broker is still present")
		}
		s.expect(nil, 404)(s.client.GetClusterMember(s.ctx, name, old, nil))
		s.rabbitData("Persistence", func(s *suite) { s.rabbitProof(name, "/", "admin", "", queue, rabbitPayload(queue)) })
		s.rabbitData("StreamPersistence", func(s *suite) { s.rabbitStreamProof(name, "/", "admin", "", stream, rabbitPayload(stream)) })
	})
	t.Run("Access", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		s.exerciseRabbitAccess(name)
		t.Run("CLI", func(t *testing.T) {
			if os.Getenv("KARAKORAM_E2E_KARACTL") == "" {
				t.Skip("set KARAKORAM_E2E_KARACTL to the built karactl binary")
			}
			child := *s
			child.t = t
			child.exerciseRabbitCLI(name)
		})
		t.Run("TLS", func(t *testing.T) {
			if os.Getenv("KARAKORAM_E2E_PRIVATE") != "1" {
				t.Skip("set KARAKORAM_E2E_PRIVATE=1 on a runner with private broker DNS and connectivity")
			}
			child := *s
			child.t = t
			child.assertRabbitTLS(name)
		})
	})
	t.Run("Definitions", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage() // Preflight before provisioning paid brokers.
		spec := s.rabbitSpec("4.3.6")
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.assertRabbitMembers(name, 3, "4.3.6")
		vhost := s.rabbitVhost(name)
		backup := s.backup(name, storage, "definitions")
		if backup.Type != api.BackupTypeDefinitions {
			t.Fatal("RabbitMQ backup did not record definitions type")
		}
		if !slices.ContainsFunc(s.inventory(name), func(b api.Backup) bool { return b.Name == backup.Name && b.Phase == api.BackupPhaseSucceeded }) {
			t.Fatal("definitions backup missing from inventory")
		}
		s.expect(nil, 204)(s.client.DeleteRabbitMQVhost(s.ctx, name, vhost, nil))
		s.expect(nil, 404)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
		s.restore(name, api.RestoreSource{SourceCluster: ptr(name), Backup: ptr(backup.Name)})
		var restored api.RabbitMQVhost
		s.expect(&restored)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
		if restored.Name != vhost || restored.DefaultQueueType == nil || *restored.DefaultQueueType != "quorum" || restored.Description == nil || *restored.Description != "RabbitMQ end-to-end test" || !slices.Contains(restored.Tags, "e2e") {
			t.Fatal("definitions restore did not recover vhost metadata")
		}
		s.snapshot(name+"-restored-vhost", restored)
		s.assertRabbitMembers(name, 3, "4.3.6")
		t.Run("Clone", func(t *testing.T) {
			child := *s
			child.t = t
			cloneSpec := spec
			cloneSpec.Components.Engine.Replicas = 1
			cloneSpec.DataSource = &api.DataSource{Cluster: name, Backup: ptr(backup.Name)}
			clone := child.create(cloneSpec)
			child.assertRabbitMembers(clone, 1, "4.3.6")
			var cloned api.RabbitMQVhost
			child.expect(&cloned)(child.client.GetRabbitMQVhost(child.ctx, clone, vhost, nil))
			if cloned.Name != vhost || cloned.DefaultQueueType == nil || *cloned.DefaultQueueType != "quorum" || cloned.Description == nil || *cloned.Description != "RabbitMQ end-to-end test" || !slices.Contains(cloned.Tags, "e2e") {
				t.Fatal("definitions clone did not recover vhost metadata")
			}
			child.snapshot(clone+"-cloned-vhost", cloned)
		})
		// Definitions backups contain topology/access objects, not queue messages.
	})
	t.Run("Upgrade", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.5"))
		s.assertRabbitMembers(name, 3, "4.3.5")
		queue := s.name()
		if !s.rabbitData("Seed", func(s *suite) { s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue)) }) {
			t.FailNow()
		}
		s.rabbitUpgrade(name)
		s.rabbitData("Persistence", func(s *suite) { s.rabbitProof(name, "/", "admin", "", queue, rabbitPayload(queue)) })
	})
	t.Run("Scaling", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		if !s.rabbitData("Seed", func(s *suite) { s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue)) }) {
			t.FailNow()
		}
		for _, count := range []int{5, 3} {
			s.update(name, func(spec *api.ClusterSpec) { spec.Components.Engine.Replicas = count })
			s.assertRabbitMembers(name, count, "4.3.6")
			phase := "FiveMemberPersistence"
			if count == 3 {
				phase = "ThreeMemberPersistence"
			}
			if !s.rabbitData(phase, func(s *suite) { s.rabbitProof(name, "/", "admin", "", queue, rabbitPayload(queue)) }) {
				t.FailNow()
			}
		}
	})
	t.Run("Metrics", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		s.assertRabbitMetrics(name)
	})
	t.Run("Delete", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		// Explicit deletion is the scenario under test and runs even with KEEP=1.
		s.deleteClusterOptions(name, false, false)
		s.expect(nil, 404)(s.client.GetCluster(s.ctx, name, nil))
		s.expect(nil, 404)(s.client.ListClusterMembers(s.ctx, name, nil))
		s.expect(nil, 404)(s.client.GetClusterConnection(s.ctx, name, nil))
		var clusters api.ClusterList
		s.expect(&clusters)(s.client.ListClusters(s.ctx, &api.ListClustersParams{Limit: ptr(100)}))
		if slices.ContainsFunc(clusters.Items, func(c api.Cluster) bool { return c.Name == name }) {
			t.Fatal("deleted RabbitMQ cluster remains in inventory")
		}
		s.snapshot(name+"-deleted", map[string]bool{"clusterAbsent": true, "membersAbsent": true, "connectionAbsent": true})
	})
	t.Run("DeleteKeepVolumes", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage()
		spec := s.rabbitSpec("4.3.6")
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		if !s.rabbitData("Seed", func(s *suite) { s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue)) }) {
			t.FailNow()
		}
		backup := s.backup(name, storage, "definitions")
		s.deleteClusterOptions(name, true, true)
		s.phase(name, api.ClusterStatusPhaseSuspended)
		var retained api.Backup
		s.expect(&retained)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
		if retained.Type != api.BackupTypeDefinitions || retained.Phase != api.BackupPhaseSucceeded {
			t.Fatal("suspension did not retain successful definitions backup")
		}
		var resumed api.Cluster
		s.expect(&resumed, 202)(s.client.ResumeCluster(s.ctx, name, &api.ResumeClusterParams{}))
		s.ready(name)
		s.assertRabbitMembers(name, 3, "4.3.6")
		s.rabbitData("Persistence", func(s *suite) { s.rabbitProof(name, "/", "admin", "", queue, rabbitPayload(queue)) })
		s.deleteClusterOptions(name, false, false)
		s.expect(nil, 404)(s.client.GetCluster(s.ctx, name, nil))
		s.expect(nil, 404)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
		s.expect(nil, 404)(s.client.ListBackups(s.ctx, name, nil))
		s.snapshot(name+"-delete-volumes-and-backups", map[string]bool{"clusterAbsent": true, "definitionsBackupAbsent": true, "retainedInventoryAbsent": true})
	})
	t.Run("DeleteKeepBackups", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage()
		spec := s.rabbitSpec("4.3.6")
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.assertRabbitMembers(name, 3, "4.3.6")
		backup := s.backup(name, storage, "definitions")
		// The retained record needs separate cleanup if an assertion fails after
		// deleting the source; ordinary cluster cleanup cannot find it anymore.
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			r, err := s.client.GetCluster(ctx, name, nil)
			if err != nil {
				t.Error("retained-backup cleanup cluster lookup failed")
				return
			}
			r.Body.Close()
			if r.StatusCode == 404 {
				s.expect(nil, 204)(s.client.DeleteBackup(ctx, name, backup.Name, &api.DeleteBackupParams{}))
			}
		})
		s.deleteClusterOptions(name, false, true)
		s.expect(nil, 404)(s.client.GetCluster(s.ctx, name, nil))
		if !slices.ContainsFunc(s.inventory(name), func(b api.Backup) bool {
			return b.Name == backup.Name && b.Type == api.BackupTypeDefinitions && b.Phase == api.BackupPhaseSucceeded
		}) {
			t.Fatal("deleted RabbitMQ source's definitions backup is not retained")
		}
		var retained api.Backup
		s.expect(&retained)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
		if retained.Name != backup.Name || retained.Type != api.BackupTypeDefinitions || retained.Phase != api.BackupPhaseSucceeded {
			t.Fatal("retained RabbitMQ definitions backup metadata changed")
		}
		s.snapshot(name+"-retained-definitions-backup", retained)
		s.expect(nil, 204)(s.client.DeleteBackup(s.ctx, name, backup.Name, &api.DeleteBackupParams{}))
		s.expect(nil, 404)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
		s.expect(nil, 404)(s.client.ListBackups(s.ctx, name, nil))
		s.snapshot(name+"-retained-backup-deleted", map[string]bool{"clusterAbsent": true, "definitionsBackupAbsent": true, "retainedInventoryAbsent": true})
	})
}

func (s *suite) exerciseRabbitAccess(name string) {
	vhost := s.rabbitVhost(name)
	var vh api.RabbitMQVhost
	s.expect(&vh)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
	vh.Description = ptr("Updated RabbitMQ end-to-end vhost")
	vh.Limits = &api.RabbitMQVhostLimits{MaxConnections: ptr(20), MaxQueues: ptr(20)}
	s.expect(nil)(s.client.ReplaceRabbitMQVhost(s.ctx, name, vhost, nil, vh))
	var got api.RabbitMQVhost
	s.expect(&got)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
	if got.Description == nil || *got.Description != *vh.Description || got.Limits == nil || !reflect.DeepEqual(got.Limits, vh.Limits) {
		s.t.Fatal("vhost update was not persisted")
	}
	var vhosts api.RabbitMQVhostList
	s.expect(&vhosts)(s.client.ListRabbitMQVhosts(s.ctx, name, nil))
	if !slices.ContainsFunc(vhosts.Items, func(v api.RabbitMQVhost) bool { return v.Name == vhost }) {
		s.t.Fatal("vhost missing from list")
	}

	user := s.name()
	var created api.CreatedRabbitMQUser
	s.expect(&created, 201)(s.client.CreateRabbitMQUser(s.ctx, name, nil, api.RabbitMQUserWrite{Name: user, Tags: []api.RabbitMQUserWriteTags{}}))
	if created.Name != user || created.GeneratedPassword == nil || *created.GeneratedPassword == "" {
		s.t.Fatal("user creation omitted generated password")
	}
	password := *created.GeneratedPassword
	var u api.RabbitMQUser
	s.expect(&u)(s.client.GetRabbitMQUser(s.ctx, name, user, nil))
	if u.Name != user || len(u.Tags) != 0 {
		s.t.Fatal("created RabbitMQ user metadata mismatch")
	}
	var users api.RabbitMQUserList
	s.expect(&users)(s.client.ListRabbitMQUsers(s.ctx, name, nil))
	if !slices.ContainsFunc(users.Items, func(u api.RabbitMQUser) bool { return u.Name == user }) {
		s.t.Fatal("user missing from list")
	}
	permission := api.RabbitMQPermission{Vhost: vhost, User: user, Configure: ".*", Read: ".*", Write: ".*"}
	s.expect(nil, 201)(s.client.CreateRabbitMQPermission(s.ctx, name, nil, permission))
	var p api.RabbitMQPermission
	s.expect(&p)(s.client.GetRabbitMQPermission(s.ctx, name, vhost, user, nil))
	if !reflect.DeepEqual(p, permission) {
		s.t.Fatal("created permission mismatch")
	}
	permission.Configure, permission.Read = "^e2e.*", "^e2e.*"
	// RabbitMQ authorizes publishing to the unnamed default exchange as
	// amq.default, independently of the queue's configure/read permissions.
	permission.Write = `^(e2e.*|amq\.default)$`
	s.expect(nil)(s.client.ReplaceRabbitMQPermission(s.ctx, name, vhost, user, nil, permission))
	s.expect(&p)(s.client.GetRabbitMQPermission(s.ctx, name, vhost, user, nil))
	if !reflect.DeepEqual(p, permission) {
		s.t.Fatal("permission update was not persisted")
	}
	var permissions api.RabbitMQPermissionList
	s.expect(&permissions)(s.client.ListRabbitMQPermissions(s.ctx, name, nil))
	if !slices.ContainsFunc(permissions.Items, func(p api.RabbitMQPermission) bool { return reflect.DeepEqual(p, permission) }) {
		s.t.Fatal("permission missing from list")
	}
	policy := api.RabbitMQPolicy{Name: s.name(), Vhost: vhost, Pattern: "^e2e.*", ApplyTo: api.QuorumQueues, Priority: 1, Definition: map[string]interface{}{"max-length": float64(100)}}
	s.expect(nil, 201)(s.client.CreateRabbitMQPolicy(s.ctx, name, nil, policy))
	var pol api.RabbitMQPolicy
	s.expect(&pol)(s.client.GetRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil))
	if !reflect.DeepEqual(pol, policy) {
		s.t.Fatal("created policy mismatch")
	}
	policy.Priority = 2
	policy.Definition["max-length"] = float64(200)
	s.expect(nil)(s.client.ReplaceRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil, policy))
	s.expect(&pol)(s.client.GetRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil))
	if !reflect.DeepEqual(pol, policy) {
		s.t.Fatal("policy update was not persisted")
	}
	var policies api.RabbitMQPolicyList
	s.expect(&policies)(s.client.ListRabbitMQPolicies(s.ctx, name, nil))
	if !slices.ContainsFunc(policies.Items, func(p api.RabbitMQPolicy) bool { return reflect.DeepEqual(p, policy) }) {
		s.t.Fatal("policy missing from list")
	}
	s.snapshot(name+"-access-vhost", got)
	s.snapshot(name+"-access-permission", p)
	s.snapshot(name+"-access-policy", pol)
	queue := "e2e" + s.name()
	if !s.rabbitData("CreatedUserLogin", func(s *suite) {
		s.rabbitPublish(name, vhost, user, password, queue, rabbitPayload(queue))
		s.rabbitProof(name, vhost, user, password, queue, rabbitPayload(queue))
	}) {
		s.t.FailNow()
	}
	newPassword := "E2e!" + s.name() + s.name()
	s.expect(nil)(s.client.ReplaceRabbitMQUser(s.ctx, name, user, nil, api.RabbitMQUserWrite{Name: user, Password: &newPassword, Tags: []api.RabbitMQUserWriteTags{api.Management}}))
	s.expect(&u)(s.client.GetRabbitMQUser(s.ctx, name, user, nil))
	if u.Name != user || len(u.Tags) != 1 || u.Tags[0] != api.RabbitMQUserTagsManagement {
		s.t.Fatal("user update was not persisted")
	}
	if !s.rabbitData("UpdatedUserLogin", func(s *suite) { s.rabbitProof(name, vhost, user, newPassword, queue, rabbitPayload(queue)) }) {
		s.t.FailNow()
	}
	s.expect(nil, 204)(s.client.DeleteRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil))
	s.expect(nil, 404)(s.client.GetRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil))
	s.expect(nil, 204)(s.client.DeleteRabbitMQPermission(s.ctx, name, vhost, user, nil))
	s.expect(nil, 404)(s.client.GetRabbitMQPermission(s.ctx, name, vhost, user, nil))
	s.expect(nil, 204)(s.client.DeleteRabbitMQUser(s.ctx, name, user, nil))
	s.expect(nil, 404)(s.client.GetRabbitMQUser(s.ctx, name, user, nil))
	s.expect(nil, 204)(s.client.DeleteRabbitMQVhost(s.ctx, name, vhost, nil))
	s.expect(nil, 404)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
}

func (s *suite) exerciseRabbitCLI(name string) {
	run := s.karactlRunner()
	vhost := s.name()
	wantVhost := api.RabbitMQVhost{Name: vhost, Tags: []string{"e2e"}, Description: ptr("RabbitMQ CLI end-to-end test"), DefaultQueueType: ptr(api.Quorum)}
	var createdVhost api.RabbitMQVhost
	run(wantVhost, &createdVhost, "rabbitmq", "vhosts", "create", name, "-f", "-")
	var gotVhost api.RabbitMQVhost
	s.expect(&gotVhost)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
	checkVhost := func(v api.RabbitMQVhost) {
		if v.Name != vhost || v.Description == nil || *v.Description != *wantVhost.Description || v.DefaultQueueType == nil || *v.DefaultQueueType != "quorum" || !slices.Contains(v.Tags, "e2e") {
			s.t.Fatal("CLI-created vhost metadata mismatch")
		}
	}
	checkVhost(createdVhost)
	checkVhost(gotVhost)
	run(nil, &gotVhost, "rabbitmq", "vhosts", "get", name, vhost)
	checkVhost(gotVhost)
	var vhosts api.RabbitMQVhostList
	run(nil, &vhosts, "rabbitmq", "vhosts", "list", name)
	if !slices.ContainsFunc(vhosts.Items, func(v api.RabbitMQVhost) bool { return v.Name == vhost }) {
		s.t.Fatal("CLI vhost list omitted created vhost")
	}
	user := s.name()
	var created api.CreatedRabbitMQUser
	run(api.RabbitMQUserWrite{Name: user, Tags: []api.RabbitMQUserWriteTags{}}, &created, "rabbitmq", "users", "create", name, "-f", "-")
	if created.Name != user || created.GeneratedPassword == nil || *created.GeneratedPassword == "" {
		s.t.Fatal("CLI user creation omitted generated password")
	}
	password := *created.GeneratedPassword
	var gotUser api.RabbitMQUser
	s.expect(&gotUser)(s.client.GetRabbitMQUser(s.ctx, name, user, nil))
	if gotUser.Name != user || len(gotUser.Tags) != 0 {
		s.t.Fatal("CLI-created user metadata mismatch")
	}
	permission := api.RabbitMQPermission{Vhost: vhost, User: user, Configure: ".*", Read: ".*", Write: ".*"}
	run(permission, nil, "rabbitmq", "permissions", "create", name, "-f", "-")
	var gotPermission api.RabbitMQPermission
	s.expect(&gotPermission)(s.client.GetRabbitMQPermission(s.ctx, name, vhost, user, nil))
	if !reflect.DeepEqual(gotPermission, permission) {
		s.t.Fatal("CLI-created permission mismatch")
	}
	run(nil, &gotPermission, "rabbitmq", "permissions", "get", name, vhost, user)
	if !reflect.DeepEqual(gotPermission, permission) {
		s.t.Fatal("CLI permission get mismatch")
	}
	var permissions api.RabbitMQPermissionList
	run(nil, &permissions, "rabbitmq", "permissions", "list", name)
	if !slices.ContainsFunc(permissions.Items, func(p api.RabbitMQPermission) bool { return reflect.DeepEqual(p, permission) }) {
		s.t.Fatal("CLI permission list omitted created permission")
	}
	policy := api.RabbitMQPolicy{Name: s.name(), Vhost: vhost, Pattern: "^e2e.*", ApplyTo: api.QuorumQueues, Priority: 1, Definition: map[string]interface{}{"max-length": float64(100)}}
	run(policy, nil, "rabbitmq", "policies", "create", name, "-f", "-")
	var gotPolicy api.RabbitMQPolicy
	s.expect(&gotPolicy)(s.client.GetRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil))
	if !reflect.DeepEqual(gotPolicy, policy) {
		s.t.Fatal("CLI-created policy mismatch")
	}
	run(nil, &gotPolicy, "rabbitmq", "policies", "get", name, vhost, policy.Name)
	if !reflect.DeepEqual(gotPolicy, policy) {
		s.t.Fatal("CLI policy get mismatch")
	}
	var policies api.RabbitMQPolicyList
	run(nil, &policies, "rabbitmq", "policies", "list", name)
	if !slices.ContainsFunc(policies.Items, func(p api.RabbitMQPolicy) bool { return reflect.DeepEqual(p, policy) }) {
		s.t.Fatal("CLI policy list omitted created policy")
	}
	queue := "e2e" + s.name()
	if !s.rabbitData("CreatedUserLogin", func(s *suite) {
		s.rabbitPublish(name, vhost, user, password, queue, rabbitPayload(queue))
		s.rabbitProof(name, vhost, user, password, queue, rabbitPayload(queue))
	}) {
		s.t.FailNow()
	}
	run(nil, nil, "rabbitmq", "policies", "delete", name, vhost, policy.Name)
	s.expect(nil, 404)(s.client.GetRabbitMQPolicy(s.ctx, name, vhost, policy.Name, nil))
	run(nil, nil, "rabbitmq", "permissions", "delete", name, vhost, user)
	s.expect(nil, 404)(s.client.GetRabbitMQPermission(s.ctx, name, vhost, user, nil))
	run(nil, nil, "rabbitmq", "users", "delete", name, user)
	s.expect(nil, 404)(s.client.GetRabbitMQUser(s.ctx, name, user, nil))
	run(nil, nil, "rabbitmq", "vhosts", "delete", name, vhost)
	s.expect(nil, 404)(s.client.GetRabbitMQVhost(s.ctx, name, vhost, nil))
	s.snapshot(name+"-cli-access", map[string]bool{"vhostCRUD": true, "userCRUD": true, "permissionCRUD": true, "policyCRUD": true})
}
