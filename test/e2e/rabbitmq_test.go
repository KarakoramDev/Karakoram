//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
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
	})
	t.Run("Restart", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		if !s.rabbitData("Seed", func(s *suite) { s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue)) }) {
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
	})
	t.Run("ReplaceMember", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		queue := s.name()
		if !s.rabbitData("Seed", func(s *suite) { s.rabbitPublish(name, "/", "admin", "", queue, rabbitPayload(queue)) }) {
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
	})
	t.Run("Access", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.rabbitSpec("4.3.6"))
		s.assertRabbitMembers(name, 3, "4.3.6")
		s.exerciseRabbitAccess(name)
	})
	t.Run("Definitions", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage() // Preflight before provisioning paid brokers.
		spec := s.rabbitSpec("4.3.6")
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.assertRabbitMembers(name, 3, "4.3.6")
		vhost := s.rabbitVhost(name)
		backup := s.backup(name, storage, api.CreateBackupRequestTypeDefinitions)
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
	permission.Configure, permission.Read, permission.Write = "^e2e.*", "^e2e.*", "^e2e.*"
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
