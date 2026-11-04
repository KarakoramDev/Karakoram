//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

// Phases share an ordered fixture, but selecting one phase provisions its prerequisites.
func TestWeaviateLifecycle(t *testing.T) {
	s := newSuite(t, true)
	cidr := requireCIDR(t)
	storage := s.storage() // Missing prerequisites skip before provisioning paid members.
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	t.Cleanup(cancel)
	s.ctx = ctx
	var provider api.Provider
	s.expect(&provider)(s.client.GetProvider(s.ctx, "weaviate"))
	version := "1.40.1"
	for _, v := range provider.Versions {
		if v.Version == "1.39.12" {
			version = v.Version
		}
	}
	f := &weaviateFixture{root: s, storage: storage, version: version, cidr: cidr}
	if name := os.Getenv("KARAKORAM_E2E_WEAVIATE_CLUSTER"); name != "" {
		c := s.cluster(name)
		if c.Spec.Provider != "weaviate" || c.Spec.Topology.Type != api.TopologyTypeCluster || c.Spec.Components.Engine.Replicas != 3 || c.Spec.Backup == nil || c.Spec.Backup.Storage == nil || *c.Spec.Backup.Storage != storage {
			t.Fatal("adopted fixture must be a seeded three-member Weaviate cluster bound to selected storage")
		}
		f.name, f.version, f.created, f.seeded = name, c.Spec.Version, true, true
	}
	phases := []struct {
		name string
		run  func()
	}{
		{"Create", func() {
			c := f.s.cluster(f.name)
			if c.Spec.Version != version || c.Spec.Tls == nil || c.Spec.Tls.Mode != api.TLSModeRequireTLS || c.Spec.Monitoring == nil || !c.Spec.Monitoring.Enabled {
				f.s.t.Fatal("created cluster version/TLS/monitoring mismatch")
			}
		}},
		{"Ready", func() { f.health(f.name, 3) }},
		{"TLSData", func() { f.leaf(f.connect(f.name, 0)); f.proof(f.name, 3) }},
		{"Access", f.access},
		{"Monitoring", f.monitoring},
		{"Restart", func() {
			f.s.operation(f.s.client.RestartCluster(f.s.ctx, f.name, &api.RestartClusterParams{}))
			f.health(f.name, 3)
			f.proof(f.name, 3)
		}},
		{"ReplaceMember", func() {
			members := f.s.members(f.name)
			// RF=3 permits replacement of a voter; the provider preserves its name/volume.
			f.s.operation(f.s.client.ReplaceClusterMember(f.s.ctx, f.name, members[0].Name, &api.ReplaceClusterMemberParams{}))
			f.health(f.name, 3)
			f.proof(f.name, 3)
		}},
		{"Backup", func() { f.fullBackup() }},
		{"Restore", func() {
			b := f.fullBackup()
			e := f.connect(f.name, 0)
			e.json("DELETE", "/v1/objects/"+weaviateClass+"/"+weaviateID(0)+"?consistency_level=ALL", nil, nil, 204)
			e.json("GET", "/v1/objects/"+weaviateClass+"/"+weaviateID(0), nil, nil, 404)
			f.s.restore(f.name, api.RestoreSource{SourceCluster: ptr(f.name), Backup: ptr(b.Name)})
			f.health(f.name, 3)
			f.proof(f.name, 3)
		}},
		{"Clone", func() {
			b := f.fullBackup()
			spec := weaviateSpec(f.storage, f.s.cluster(f.name).Spec.Version, f.cidr)
			spec.DataSource = &api.DataSource{Cluster: f.name, Backup: ptr(b.Name)}
			clone := f.s.create(spec)
			f.health(clone, 3)
			f.proof(clone, 3)
			f.s.expect(nil, 404)(f.s.client.GetWeaviateUser(f.s.ctx, clone, weaviateUser, &api.GetWeaviateUserParams{}))
			source, target := f.connect(f.name, 0), f.connect(clone, 0)
			if source.authorization == target.authorization {
				f.s.t.Fatal("clone reused source root identity credential")
			}
			f.s.snapshot("clone-identities", map[string]bool{"sourceUserAbsent": true, "freshRootKey": true})
		}},
		{"Upgrade", func() {
			if f.s.cluster(f.name).Spec.Version != "1.39.12" {
				f.s.t.Skip("1.39.12 source is not offered or adopted fixture already upgraded")
			}
			f.s.update(f.name, func(spec *api.ClusterSpec) { spec.Version = "1.40.1" })
			f.health(f.name, 3)
			f.proof(f.name, 3)
			for i := range 3 {
				var meta struct {
					Version string `json:"version"`
				}
				f.connect(f.name, i).json("GET", "/v1/meta", nil, &meta, 200)
				if meta.Version != "1.40.1" {
					f.s.t.Fatal("native upgrade version mismatch")
				}
			}
		}},
		{"ScaleOut", func() { f.scale(4) }},
		{"ScaleDown", func() {
			if f.s.cluster(f.name).Spec.Components.Engine.Replicas != 4 {
				f.scale(4)
			}
			f.scale(3)
		}},
		{"RotateSystemUser", func() {
			old := f.connect(f.name, 0)
			f.s.operation(f.s.client.RotateSystemUser(f.s.ctx, f.name, "karakoram-admin", &api.RotateSystemUserParams{}))
			fresh := f.connect(f.name, 0)
			if old.authorization == fresh.authorization {
				f.s.t.Fatal("system credential unchanged")
			}
			old.json("GET", "/v1/objects/"+weaviateClass+"/"+weaviateID(0), nil, nil, 401)
			f.proof(f.name, 3)
		}},
		{"RotateTLS", func() {
			old := f.leaf(f.connect(f.name, 0))
			f.s.operation(f.s.client.RotateClusterTLS(f.s.ctx, f.name, &api.RotateClusterTLSParams{}))
			fresh := f.leaf(f.connect(f.name, 0))
			if bytes.Equal(old, fresh) {
				f.s.t.Fatal("TLS rotation did not replace leaf")
			}
			f.proof(f.name, 3)
			f.s.snapshot("rotated-tls", map[string]bool{"newLeaf": true, "verifiedChainAndHostname": true})
		}},
		{"DeleteKeepBackups", func() { f.delete(true) }},
		{"Delete", func() { f.delete(false) }},
	}
	for _, phase := range phases {
		if !t.Run(phase.name, func(t *testing.T) { f.phase(t, phase.run) }) {
			return
		}
	}
}

func (f *weaviateFixture) scale(count int) {
	f.s.update(f.name, func(spec *api.ClusterSpec) { spec.Components.Engine.Replicas = count })
	f.health(f.name, count)
	f.proof(f.name, count)
	// Every RF=3 replica must remain served by a live member after the drain.
	var nodes struct {
		Nodes []struct {
			Name   string `json:"name"`
			Shards []struct {
				Class       string `json:"class"`
				Name        string `json:"name"`
				ObjectCount int    `json:"objectCount"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	f.connect(f.name, 0).json("GET", "/v1/nodes?output=verbose", nil, &nodes, 200)
	replicas := map[string]int{}
	for _, node := range nodes.Nodes {
		for _, shard := range node.Shards {
			if shard.Class == weaviateClass {
				if shard.ObjectCount != weaviateCount {
					f.s.t.Fatal("replica lost objects")
				}
				replicas[shard.Name]++
			}
		}
	}
	if len(replicas) == 0 {
		f.s.t.Fatal("proof shard replicas missing")
	}
	for _, n := range replicas {
		if n != 3 {
			f.s.t.Fatal("proof shard no longer has three served replicas")
		}
	}
	f.s.snapshot("replicas", replicas)
}

func (f *weaviateFixture) monitoring() {
	baseline := map[string]time.Time{}
	collect := func() api.ClusterMetrics {
		var m api.ClusterMetrics
		f.s.expect(&m)(f.s.client.GetClusterMetrics(f.s.ctx, f.name, &api.GetClusterMetricsParams{Range: ptr("15m")}))
		return m
	}
	for _, series := range collect().Series {
		for _, p := range series.Points {
			key := series.Member + "/" + series.Name
			if p.Timestamp.After(baseline[key]) {
				baseline[key] = p.Timestamp
			}
		}
	}
	started := time.Now()
	names := []string{"weaviate.up", "weaviate.objects", "weaviate.shards", "weaviate.collections", "weaviate.queries.total", "weaviate.requests.total", "weaviate.batch.objects.total", "weaviate.vectorIndex.capacity", "weaviate.vectorIndex.tombstones", "weaviate.lsm.bytes", "weaviate.asyncQueue.length"}
	f.s.poll("fresh Weaviate catalog metrics", 15*time.Minute, func() bool {
		m := collect()
		fresh := map[string]bool{}
		for _, series := range m.Series {
			if strings.HasPrefix(series.Name, "weaviate.") {
				for _, p := range series.Points {
					key := series.Member + "/" + series.Name
					if p.Timestamp.After(baseline[key]) && p.Timestamp.After(started) {
						fresh[key] = true
					}
				}
			}
		}
		for _, member := range f.s.members(f.name) {
			for _, name := range names {
				if !fresh[member.Name+"/"+name] {
					return false
				}
			}
		}
		f.s.snapshot("metrics", m)
		return true
	})
}

func (f *weaviateFixture) access() {
	s := f.s
	var created api.WeaviateUserCreated
	s.expect(&created, 201)(s.client.CreateWeaviateUser(s.ctx, f.name, &api.CreateWeaviateUserParams{}, api.WeaviateUserCreate{Name: weaviateUser, Roles: []string{"viewer"}}))
	if created.ApiKey == nil || *created.ApiKey == "" {
		s.t.Fatal("user create omitted key")
	}
	e := f.connect(f.name, 1)
	e.authorization = "Bearer " + *created.ApiKey
	path := "/v1/objects/" + weaviateClass + "/" + weaviateID(0)
	e.json("GET", path, nil, nil, 200)
	e.json("POST", "/v1/objects", map[string]any{"class": weaviateClass, "properties": map[string]any{"marker": "denied"}, "vector": []float64{1, 0, 0.5}}, nil, 403)
	s.expect(nil, 204)(s.client.UpdateWeaviateUser(s.ctx, f.name, weaviateUser, &api.UpdateWeaviateUserParams{}, api.WeaviateUserUpdate{Roles: []string{}}))
	e.json("GET", path, nil, nil, 403)
	s.expect(nil, 204)(s.client.UpdateWeaviateUser(s.ctx, f.name, weaviateUser, &api.UpdateWeaviateUserParams{}, api.WeaviateUserUpdate{Roles: []string{"viewer"}}))
	var rotated api.WeaviateAPIKey
	s.expect(&rotated, 200)(s.client.RotateWeaviateUserKey(s.ctx, f.name, weaviateUser, &api.RotateWeaviateUserKeyParams{}))
	e.json("GET", path, nil, nil, 401)
	if rotated.ApiKey == nil || *rotated.ApiKey == "" || *rotated.ApiKey == *created.ApiKey {
		s.t.Fatal("rotation omitted new key")
	}
	e.authorization = "Bearer " + *rotated.ApiKey
	e.json("GET", path, nil, nil, 200)
	role := api.WeaviateRole{Name: "e2e-role", Permissions: []api.WeaviatePermission{{Action: "read_collections", Collections: &api.WeaviatePermission_Collections{Collection: ptr(weaviateClass)}}}}
	s.expect(nil, 201)(s.client.CreateWeaviateRole(s.ctx, f.name, &api.CreateWeaviateRoleParams{}, role))
	role.Permissions = append(role.Permissions, api.WeaviatePermission{Action: "read_data", Data: &api.WeaviatePermission_Data{Collection: ptr(weaviateClass)}})
	s.expect(nil, 200)(s.client.UpdateWeaviateRole(s.ctx, f.name, role.Name, &api.UpdateWeaviateRoleParams{}, role))
	var got api.WeaviateRole
	s.expect(&got)(s.client.GetWeaviateRole(s.ctx, f.name, role.Name, &api.GetWeaviateRoleParams{}))
	if got.Name != role.Name || len(got.Permissions) != 2 {
		s.t.Fatal("role replacement not observed")
	}
	s.expect(nil, 204)(s.client.DeleteWeaviateRole(s.ctx, f.name, role.Name, &api.DeleteWeaviateRoleParams{}))
	s.expect(nil, 404)(s.client.GetWeaviateRole(s.ctx, f.name, role.Name, &api.GetWeaviateRoleParams{}))
	for _, protected := range []string{"karakoram-admin", "karakoram-cluster"} {
		s.expect(nil, 409)(s.client.DeleteWeaviateUser(s.ctx, f.name, protected, &api.DeleteWeaviateUserParams{}))
	}
	s.expect(nil, 409)(s.client.DeleteWeaviateRole(s.ctx, f.name, "viewer", &api.DeleteWeaviateRoleParams{}))
	s.snapshot("access", map[string]bool{"viewerRead": true, "writeDenied": true, "rolesUpdated": true, "oldKeyRejected": true, "customRoleReplacedAndDeleted": true, "protectedIdentitiesRejected": true})
}

func (f *weaviateFixture) delete(keep bool) {
	b := f.fullBackup()
	backups := f.s.inventory(f.name)
	f.s.deleteClusterOptions(f.name, false, keep)
	f.s.expect(nil, 404)(f.s.client.GetCluster(f.s.ctx, f.name, &api.GetClusterParams{}))
	if keep {
		var got api.Backup
		f.s.expect(&got)(f.s.client.GetBackup(f.s.ctx, f.name, b.Name, &api.GetBackupParams{}))
		if got.Location == nil || b.Location == nil || *got.Location != *b.Location || got.Phase != api.BackupPhaseSucceeded {
			f.s.t.Fatal("retained backup changed or disappeared")
		}
		f.s.snapshot("retained-backup", got)
		for _, backup := range backups {
			f.s.expect(nil, 204)(f.s.client.DeleteBackup(f.s.ctx, f.name, backup.Name, &api.DeleteBackupParams{}))
		}
	}
	for _, backup := range backups {
		f.s.expect(nil, 404)(f.s.client.GetBackup(f.s.ctx, f.name, backup.Name, &api.GetBackupParams{}))
	}
	f.created, f.seeded, f.backup = false, false, nil
}
