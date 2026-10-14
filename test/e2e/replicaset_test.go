//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"testing"
)

func TestReplicaSet(t *testing.T) {
	t.Run("Singleton", func(t *testing.T) {
		s := newSuite(t, true)
		one := s.create(s.spec(1))
		s.assertReplicaMembers(one, 1)
		s.put(one, "singleton", "durable")
		s.proof(one, "singleton", "durable")
		if !s.keep {
			s.deleteCluster(one, false)
		}
	})
	t.Run("FiveToSeven", func(t *testing.T) {
		s := newSuite(t, true)
		name := s.create(s.spec(5))
		s.assertReplicaMembers(name, 5)
		s.put(name, "initial-sync", "durable")
		writer := s.startWriter(name)
		defer writer.finish()
		s.update(name, func(spec *api.ClusterSpec) { spec.Components.Engine.Replicas = 7 })
		s.assertReplicaMembers(name, 7)
		s.assertMemberData(name, "initial-sync", "durable")
		writer.finish()
		s.assertWriterData(name, writer)
	})
	t.Run("Scaling", func(t *testing.T) {
		s := newSuite(t, true)
		name := s.create(s.spec(3))
		s.assertReplicaMembers(name, 3)
		s.put(name, "before-scale", "durable")
		writer := s.startWriter(name)
		defer writer.finish()
		for _, replicas := range []int{5, 3} {
			s.update(name, func(spec *api.ClusterSpec) { spec.Components.Engine.Replicas = replicas })
			s.assertReplicaMembers(name, replicas)
			s.proof(name, "before-scale", "durable")
			s.assertMemberData(name, "before-scale", "durable")
			s.put(name, "after-scale", "durable")
		}
		var replace string
		for _, m := range s.members(name) {
			if m.Role == api.MemberRoleSecondary {
				replace = m.Name
				break
			}
		}
		if replace == "" {
			t.Fatal("no secondary available for replacement")
		}
		s.operation(s.client.ReplaceClusterMember(s.ctx, name, replace, &api.ReplaceClusterMemberParams{}))
		s.ready(name)
		s.assertReplicaMembers(name, 3)
		for _, m := range s.members(name) {
			if m.Name == replace {
				t.Fatalf("replaced member %s is still present", replace)
			}
		}
		s.proof(name, "before-scale", "durable")
		s.proof(name, "after-scale", "durable")
		s.assertMemberData(name, "before-scale", "durable")
		writer.finish()
		s.assertWriterData(name, writer)
		// KEEP controls final teardown, not the suspend/resume behavior under test.
		s.deleteCluster(name, true)
		s.phase(name, api.ClusterStatusPhaseSuspended)
		var resumed api.Cluster
		s.expect(&resumed, 202)(s.client.ResumeCluster(s.ctx, name, &api.ResumeClusterParams{}))
		s.ready(name)
		s.assertReplicaMembers(name, 3)
		s.proof(name, "before-scale", "durable")
		if !s.keep {
			s.deleteCluster(name, false)
		}
	})
}

func (s *suite) assertReplicaMembers(name string, want int) {
	s.t.Helper()
	members := s.members(name)
	if len(members) != want {
		s.t.Fatalf("cluster=%s members=%d want=%d", name, len(members), want)
	}
	primary := 0
	for _, m := range members {
		var individual api.Member
		s.expect(&individual)(s.client.GetClusterMember(s.ctx, name, m.Name, nil))
		if individual.Name != m.Name || !m.Health || m.State == "" || m.Set == "" || m.LagSeconds == nil || *m.LagSeconds < 0 {
			s.t.Fatalf("member=%s lacks healthy state/set/nonnegative lag", m.Name)
		}
		switch m.Role {
		case api.MemberRolePrimary:
			primary++
		case api.MemberRoleSecondary:
		default:
			s.t.Fatalf("member=%s unexpected role=%s", m.Name, m.Role)
		}
		s.t.Logf("member=%s set=%s role=%s state=%s health=%t lagSeconds=%g", m.Name, m.Set, m.Role, m.State, m.Health, *m.LagSeconds)
	}
	if primary != 1 {
		s.t.Fatalf("cluster=%s primaries=%d want=1", name, primary)
	}
	s.snapshot(name+"-members", members)
	// The connect may consume most of publicRoutingGrace; the read-only config
	// read gets its own bounded retry afterwards instead of a deadline that
	// started before the connect.
	admin := s.mongoAs(name, "databaseAdmin", "", "")
	var config struct {
		Config struct {
			Members []struct {
				Votes      *int `bson:"votes"`
				NewlyAdded bool `bson:"newlyAdded"`
			} `bson:"members"`
		} `bson:"config"`
	}
	if err := retryPublicOperation(s.ctx, func(attempt context.Context) error {
		return admin.Database("admin").RunCommand(attempt, bson.D{{Key: "replSetGetConfig", Value: 1}}).Decode(&config)
	}); err != nil {
		s.t.Fatal(err)
	}
	votes := 0
	for _, member := range config.Config.Members {
		if member.NewlyAdded {
			s.t.Fatal("Ready replica config still contains an initial-sync newlyAdded member")
		}
		if member.Votes == nil {
			votes++
		} else {
			votes += *member.Votes
		}
	}
	if len(config.Config.Members) != want || votes != want || votes/2+1 > len(members) {
		s.t.Fatalf("replica config members=%d votes=%d want=%d; quorum unsafe", len(config.Config.Members), votes, want)
	}
	s.snapshot(name+"-replica-config", config)
}

func (s *suite) assertMemberData(name, id, want string) {
	s.t.Helper()
	for _, member := range s.members(name) {
		connectCtx, cancel := context.WithTimeout(s.ctx, publicRoutingGrace)
		client := s.mongoMember(connectCtx, name, member, "databaseAdmin")
		cancel()
		var document struct {
			Value string `bson:"value"`
		}
		err := retryPublicOperation(s.ctx, func(attempt context.Context) error {
			return client.Database("e2e").Collection("proof").FindOne(attempt, bson.M{"_id": id}).Decode(&document)
		})
		_ = client.Disconnect(context.Background())
		if err != nil || document.Value != want {
			s.t.Fatalf("member=%s initial-sync proof value=%s want=%s err=%v", member.Name, document.Value, want, err)
		}
	}
}
