//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSharding(t *testing.T) {
	t.Run("Topology", func(t *testing.T) {
		s := newSuite(t, true)
		name := s.create(s.shardedSpec(true))
		s.seedSharded(name)
		s.update(name, func(spec *api.ClusterSpec) { spec.Topology.Shards = ptr(3) })
		s.assertShardedMembers(name, 3)
		admin := s.mongoAs(name, "databaseAdmin", "", "")
		var shards struct {
			Shards []bson.M `bson:"shards"`
		}
		if err := retryPublicOperation(s.ctx, func(attempt context.Context) error {
			return admin.Database("admin").RunCommand(attempt, bson.D{{Key: "listShards", Value: 1}}).Decode(&shards)
		}); err != nil {
			t.Fatal(err)
		}
		if len(shards.Shards) != 3 {
			t.Fatalf("mongos sees %d shards want=3", len(shards.Shards))
		}
		var initial api.Balancer
		s.expect(&initial)(s.client.GetClusterBalancer(s.ctx, name, nil))
		if initial.Mode == nil || initial.Running == nil {
			t.Fatal("balancer status lacks mode/running observations")
		}
		for _, enabled := range []bool{false, true} {
			var changed api.Balancer
			s.expect(&changed)(s.client.ReplaceClusterBalancer(s.ctx, name, &api.ReplaceClusterBalancerParams{}, api.Balancer{Enabled: enabled}))
			var observed api.Balancer
			s.expect(&observed)(s.client.GetClusterBalancer(s.ctx, name, nil))
			if observed.Enabled != enabled {
				t.Fatalf("balancer=%t want=%t", observed.Enabled, enabled)
			}
			s.snapshot(name+"-balancer", observed)
		}
		s.expect(nil)(s.client.ReplaceClusterBalancer(s.ctx, name, &api.ReplaceClusterBalancerParams{}, api.Balancer{Enabled: initial.Enabled}))
		s.shardedProof(name)
	})
	t.Run("Recovery", func(t *testing.T) {
		s := newSuite(t, true)
		storage := s.storage()
		spec := s.shardedSpec(true)
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.seedSharded(name)
		s.exerciseRecovery(name, storage, s.shardedProof)
	})
	t.Run("ScheduleRetention", func(t *testing.T) {
		s := newSuite(t, false)
		storage := s.storage()
		spec := s.shardedSpec(false)
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		s.exerciseRetention(name, storage)
	})
}

func (s *suite) shardedSpec(public bool) api.ClusterSpec {
	spec := s.privateSpec(1)
	if public {
		spec.Exposure = &api.Exposure{Public: true, IpType: ptr(api.IPV6), Allowlist: ptr([]string{requireCIDR(s.t)})}
	}
	spec.Topology = api.Topology{Type: api.TopologyTypeSharded, Shards: ptr(2)}
	configReplicas := 0
	for _, topology := range s.provider().Topologies {
		if topology.Type == api.ProviderTopologyTypeSharded {
			raw, err := json.Marshal(topology.Limits["configServer"])
			if err != nil {
				s.t.Fatal(err)
			}
			var limits struct {
				MinReplicas int `json:"minReplicas"`
			}
			if err := json.Unmarshal(raw, &limits); err != nil {
				s.t.Fatal(err)
			}
			configReplicas = limits.MinReplicas
		}
	}
	if configReplicas != 1 && configReplicas != 3 {
		s.t.Fatal("provider does not advertise valid config-server minimums (1 or 3)")
	}
	spec.Components.ConfigServer = &api.Component{Replicas: configReplicas, Resources: smallResources()}
	spec.Components.Router = &api.Component{Replicas: 1, Resources: smallResources()}
	return spec
}

func (s *suite) assertShardedMembers(name string, shards int) {
	config := 0
	sets := map[string]int{}
	routers := 0
	for _, m := range s.members(name) {
		if !m.Health {
			s.t.Fatalf("unhealthy sharded member %s", m.Name)
		}
		if m.Role == api.MemberRoleRouter {
			routers++
			continue
		}
		if m.Set == "cfg" {
			config++
		} else {
			sets[m.Set]++
		}
	}
	spec := s.cluster(name).Spec
	if config != spec.Components.ConfigServer.Replicas || len(sets) != shards || routers != spec.Components.Router.Replicas {
		s.t.Fatalf("sharded members config=%d shardSets=%d routers=%d", config, len(sets), routers)
	}
	for set, members := range sets {
		if members != spec.Components.Engine.Replicas {
			s.t.Fatalf("shard %s members=%d", set, members)
		}
	}
	s.snapshot(name+"-sharded-members", s.members(name))
}

func (s *suite) seedSharded(name string) {
	s.assertShardedMembers(name, 2)
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	var hello bson.M
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		s.t.Fatal(err)
	}
	if hello["msg"] != "isdbgrid" {
		s.t.Fatal("public endpoint is not mongos")
	}
	// Sharding a collection needs enableSharding, which the revealed
	// databaseAdmin deliberately lacks; use a purpose-made application user,
	// as a real operator would.
	password := s.name() + s.name()
	var user api.DatabaseUser
	s.expect(&user, 201)(s.client.CreateDatabaseUser(s.ctx, name, &api.CreateDatabaseUserParams{}, api.CreateDatabaseUserRequest{Database: "admin", Username: "sharder", Password: &password, Roles: []api.RoleReference{{Db: "admin", Role: "enableSharding"}, {Db: "e2e", Role: "readWrite"}}}))
	admin := s.mongoAs(name, "sharder", password, "admin")
	for _, command := range []bson.D{{{Key: "enableSharding", Value: "e2e"}}, {{Key: "shardCollection", Value: "e2e.sharded"}, {Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}}}} {
		if err := admin.Database("admin").RunCommand(ctx, command).Err(); err != nil {
			s.t.Fatal(err)
		}
	}
	docs := make([]any, 128)
	for i := range docs {
		docs[i] = bson.M{"_id": i, "value": "sharded-proof"}
	}
	if _, err := c.Database("e2e").Collection("sharded").InsertMany(ctx, docs); err != nil {
		s.t.Fatal(err)
	}
	s.shardedProof(name)
}

func (s *suite) shardedProof(name string) {
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	if count, err := c.Database("e2e").Collection("sharded").CountDocuments(ctx, bson.M{"value": "sharded-proof"}); err != nil || count != 128 {
		s.t.Fatalf("sharded count=%d err=%v", count, err)
	}
}
