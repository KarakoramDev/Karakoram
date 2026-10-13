//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

// TestElasticLifecycle exercises the complete Elastic Stack on a private Zerops
// network. Each selected phase provisions its own missing prerequisites; phases
// in one invocation share their source cluster and successful snapshot.
func TestElasticLifecycle(t *testing.T) {
	s := newSuite(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	t.Cleanup(cancel)
	s.ctx = ctx
	storage := s.storage()
	f := &elasticFixture{root: s, name: s.name(), storage: storage}
	phases := []struct {
		name string
		run  func()
	}{
		{"Create", func() {
			c := f.s.cluster(f.name)
			if c.Spec.Provider != "elasticsearch" || c.Spec.Version != "9.5.4" || c.Spec.Topology.Type != api.TopologyTypeCluster || c.Spec.Components.Engine.Replicas != 3 {
				f.s.t.Fatal("created cluster does not match requested topology/version")
			}
			if c.Spec.Components.Engine.Resources.MinRam != 2 || c.Spec.Components.Engine.Resources.MaxRam != 2 {
				f.s.t.Fatal("Elasticsearch RAM is not fixed at 2 GB")
			}
			if c.Spec.Components.Extra == nil {
				f.s.t.Fatal("stack components missing")
			}
			for _, name := range []string{"kibana", "logstash", "apm"} {
				if (*c.Spec.Components.Extra)[name].Replicas != 1 {
					f.s.t.Fatalf("component %s replicas != 1", name)
				}
			}
		}},
		{"TLSData", func() {
			e := f.connect(f.name)
			req, err := http.NewRequestWithContext(f.s.ctx, http.MethodGet, e.base+"/", nil)
			if err != nil {
				f.s.t.Fatal("creating TLS probe failed")
			}
			req.SetBasicAuth(e.user, e.password)
			r, err := e.client.Do(req)
			if err != nil {
				f.s.t.Fatal("CA-verified TLS probe failed")
			}
			defer r.Body.Close()
			_, _ = io.Copy(io.Discard, r.Body)
			if r.StatusCode != 200 || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
				f.s.t.Fatal("TLS probe did not verify a trusted certificate chain")
			}
			leaf := r.TLS.PeerCertificates[0]
			if leaf.VerifyHostname(f.name+"n0") != nil {
				f.s.t.Fatal("HTTP certificate does not match node hostname")
			}
			f.s.snapshot("tls", map[string]interface{}{"verifiedChain": true, "verifiedHostname": true, "notBefore": leaf.NotBefore, "notAfter": leaf.NotAfter, "tlsVersion": r.TLS.Version})
			f.proof(f.name)
		}},
		{"Access", f.accessChecks},
		{"Kibana", f.kibanaChecks},
		{"Logstash", f.logstashChecks},
		{"APM", f.apmChecks},
		{"Monitoring", f.monitoringChecks},
		{"Restart", func() {
			f.operation(f.s.client.RestartCluster(f.s.ctx, f.name, &api.RestartClusterParams{Namespace: f.s.namespace}))
			f.health(f.name, 3)
			f.proof(f.name)
		}},
		{"ReplaceMember", f.replaceChecks},
		{"Backup", func() { f.snapshotBackup() }},
		{"Restore", f.restoreChecks},
		{"Clone", f.cloneChecks},
		{"Upgrade", f.upgradeChecks},
		{"Scale", f.scaleChecks},
		{"DeleteKeep", f.deleteKeepChecks},
		{"Delete", func() {
			f.snapshotBackup()
			backups := f.backupInventory()
			f.s.deleteClusterOptions(f.name, false, false)
			f.assertDeleted(f.name)
			f.assertBackupsDeleted(backups)
		}},
	}
	for _, phase := range phases {
		if !t.Run(phase.name, func(t *testing.T) { f.phase(t, phase.run) }) {
			// Do not mutate/delete a failed fixture before its evidence is collected.
			return
		}
	}
}

func TestElasticDedicatedMasters(t *testing.T) {
	s := newSuite(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	t.Cleanup(cancel)
	s.ctx = ctx
	f := &elasticFixture{root: s, s: s, name: s.name()}
	start := time.Now()
	t.Cleanup(func() {
		t.Logf("phase=%s elapsed=%s failed=%t", t.Name(), time.Since(start).Round(time.Second), t.Failed())
		s.snapshot("timing", map[string]interface{}{"phase": t.Name(), "elapsedSeconds": time.Since(start).Seconds(), "failed": t.Failed()})
		if t.Failed() {
			f.failureEvidence()
		}
	})
	spec := elasticSpec("", 2, false)
	spec.Backup = nil
	spec.Topology.Type = api.TopologyTypeDedicatedMasters
	(*spec.Components.Extra)["master"] = api.Component{Replicas: 3, Resources: spec.Components.Engine.Resources}
	f.create(f.name, spec)
	f.health(f.name, 5)
	var nodes []struct {
		Name string `json:"name"`
		Role string `json:"node.role"`
	}
	f.connect(f.name).json(http.MethodGet, "/_cat/nodes?format=json&h=name,node.role", nil, &nodes, 200)
	masters, data := 0, 0
	for _, node := range nodes {
		master := strings.Contains(node.Role, "m")
		// "d" is the generic data role; c/f/h/s/w are tier-specific data roles.
		hasData := strings.ContainsAny(node.Role, "dcfhsw")
		switch {
		case master && !hasData && node.Role == "m":
			masters++
		case hasData && !master:
			data++
		default:
			t.Fatalf("member=%s unexpected roles=%s", node.Name, node.Role)
		}
	}
	if len(nodes) != 5 || masters != 3 || data != 2 {
		t.Fatalf("node roles: master-only=%d data=%d total=%d want=3/2/5", masters, data, len(nodes))
	}
	s.snapshot("dedicated-master-nodes", nodes)
	f.seed()
	f.health(f.name, 5)
	f.proof(f.name)
	s.deleteClusterOptions(f.name, false, false)
	f.assertDeleted(f.name)
}

func (f *elasticFixture) accessChecks() {
	role, user := "e2e-reader", "e2e-user"
	password := elasticRandom(f.s.t, 24)
	roleBody := map[string]interface{}{"name": role, "cluster": []string{}, "indices": []interface{}{map[string]interface{}{"names": []string{elasticIndex}, "privileges": []string{"read", "view_index_metadata"}, "allowRestrictedIndices": false}}, "applications": []interface{}{}}
	f.access(http.MethodPost, "/roles", roleBody, nil, 201)
	var gotRole struct {
		Name string `json:"name"`
	}
	f.access(http.MethodGet, "/roles/"+role, nil, &gotRole, 200)
	if gotRole.Name != role {
		f.s.t.Fatal("role get returned wrong role")
	}
	f.access(http.MethodPost, "/users", map[string]interface{}{"username": user, "password": password, "roles": []string{role}, "fullName": "E2E reader", "enabled": true}, nil, 201)
	var gotUser struct {
		Username string   `json:"username"`
		Roles    []string `json:"roles"`
	}
	f.access(http.MethodGet, "/users/"+user, nil, &gotUser, 200)
	if gotUser.Username != user || len(gotUser.Roles) != 1 || gotUser.Roles[0] != role {
		f.s.t.Fatal("user get returned wrong roles")
	}
	var users struct {
		Items []struct {
			Username string `json:"username"`
		} `json:"items"`
	}
	f.access(http.MethodGet, "/users", nil, &users, 200)
	found := false
	for _, v := range users.Items {
		if v.Username == user {
			found = true
		}
	}
	if !found {
		f.s.t.Fatal("user list omitted created user")
	}
	var roles struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	f.access(http.MethodGet, "/roles", nil, &roles, 200)
	found = false
	for _, v := range roles.Items {
		if v.Name == role {
			found = true
		}
	}
	if !found {
		f.s.t.Fatal("role list omitted created role")
	}
	e := f.connect(f.name)
	e.user, e.password = user, password
	e.json(http.MethodGet, "/"+elasticIndex+"/_search", nil, nil, 200)
	e.json(http.MethodGet, "/"+elasticPrivateIndex+"/_search", nil, nil, 403)
	newPassword := elasticRandom(f.s.t, 24)
	f.access(http.MethodPut, "/users/"+user, map[string]interface{}{"username": user, "password": newPassword, "roles": []string{role}, "fullName": "Updated E2E reader", "enabled": true}, nil, 200)
	e.json(http.MethodGet, "/_security/_authenticate", nil, nil, 401)
	e.password = newPassword
	e.json(http.MethodGet, "/"+elasticIndex+"/_search", nil, nil, 200)
	roleBody["indices"] = []interface{}{}
	f.access(http.MethodPut, "/roles/"+role, roleBody, nil, 200)
	e.json(http.MethodGet, "/"+elasticIndex+"/_search", nil, nil, 403)
	var key struct {
		ID      string `json:"id"`
		Encoded string `json:"encoded"`
	}
	f.access(http.MethodPost, "/api-keys", map[string]interface{}{"name": "e2e-key", "expiration": "1h", "roleDescriptors": map[string]interface{}{"reader": map[string]interface{}{"cluster": []string{}, "indices": []interface{}{map[string]interface{}{"names": []string{elasticIndex}, "privileges": []string{"read"}}}}}}, &key, 201)
	if key.ID == "" || key.Encoded == "" {
		f.s.t.Fatal("API key creation omitted id/encoded credential")
	}
	var keys struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	f.access(http.MethodGet, "/api-keys", nil, &keys, 200)
	found = false
	for _, v := range keys.Items {
		if v.ID == key.ID {
			found = true
		}
	}
	if !found {
		f.s.t.Fatal("API key list omitted created key")
	}
	e.authorization = "ApiKey " + key.Encoded
	e.json(http.MethodGet, "/"+elasticIndex+"/_search", nil, nil, 200)
	e.json(http.MethodGet, "/"+elasticPrivateIndex+"/_search", nil, nil, 403)
	f.access(http.MethodDelete, "/api-keys/"+url.PathEscape(key.ID), nil, nil, 204)
	e.json(http.MethodGet, "/"+elasticIndex+"/_search", nil, nil, 401)
	f.access(http.MethodDelete, "/users/"+user, nil, nil, 204)
	e.authorization = ""
	e.json(http.MethodGet, "/_security/_authenticate", nil, nil, 401)
	f.access(http.MethodDelete, "/roles/"+role, nil, nil, 204)
	f.access(http.MethodGet, "/roles/"+role, nil, nil, 404)
	f.access(http.MethodGet, "/users/"+user, nil, nil, 404)
	f.s.snapshot("access", map[string]interface{}{"userScopedRead": true, "userDeniedOtherIndex": true, "passwordChanged": true, "roleRevoked": true, "apiKeyAuthenticated": true, "apiKeyInvalidated": true, "userDeleted": true, "roleDeleted": true})
}

func (f *elasticFixture) kibanaStatus(version string) {
	e := f.component("http://" + f.name + "kb:5601")
	c := f.s.connection(f.name)
	_, e.password = credential(c, "elastic")
	e.user = "elastic"
	var status struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	e.json(http.MethodGet, "/api/status", nil, &status, 200)
	if status.Version.Number != version {
		f.s.t.Fatalf("Kibana version=%s want=%s", status.Version.Number, version)
	}
	f.s.snapshot("kibana-status", map[string]interface{}{"available": true, "version": status.Version.Number})
}

func (f *elasticFixture) kibanaChecks() {
	f.kibanaStatus("9.5.4")
	c := f.s.connection(f.name)
	_, password := credential(c, "elastic")
	e := f.component(f.publicEndpoint("kibana"))
	body, err := json.Marshal(map[string]interface{}{"providerType": "basic", "providerName": "basic", "currentURL": "/", "params": map[string]string{"username": "elastic", "password": password}})
	if err != nil {
		f.s.t.Fatal("encoding Kibana login failed")
	}
	e.request(http.MethodPost, "/internal/security/login", body, "application/json", map[string]string{"kbn-xsrf": "true"}, nil, 200)
	f.s.snapshot("kibana-login", map[string]interface{}{"publicLogin": true})
}

func (f *elasticFixture) logstashChecks() {
	e := f.component("http://" + f.name + "ls:8080")
	for range 3 {
		marker := elasticRandom(f.s.t, 16)
		e.json(http.MethodPost, "/", map[string]interface{}{"message": marker, "@timestamp": time.Now().UTC().Format(time.RFC3339Nano)}, nil, 200)
		f.waitSearch("logs-*", map[string]interface{}{"match_phrase": map[string]interface{}{"message": marker}})
	}
	var streams struct {
		Streams []struct {
			Name string `json:"name"`
		} `json:"data_streams"`
	}
	f.connect(f.name).json(http.MethodGet, "/_data_stream/logs-*", nil, &streams, 200)
	if len(streams.Streams) == 0 {
		f.s.t.Fatal("Logstash did not create a logs data stream")
	}
	f.s.snapshot("logstash", map[string]interface{}{"documentsIndexed": 3, "dataStreams": streams.Streams})
}

func (f *elasticFixture) apmChecks() {
	c := f.s.connection(f.name)
	_, token := credential(c, "apm-secret-token")
	if token == "" {
		f.s.t.Fatal("connection details omitted APM secret token")
	}
	for _, target := range []struct{ name, base string }{{"private", "http://" + f.name + "apm:8200"}, {"public", f.publicEndpoint("apm")}} {
		trace := elasticRandom(f.s.t, 16)
		transaction := elasticRandom(f.s.t, 8)
		var body strings.Builder
		encoder := json.NewEncoder(&body)
		if err := encoder.Encode(map[string]interface{}{"metadata": map[string]interface{}{"service": map[string]interface{}{"name": f.name, "agent": map[string]string{"name": "go", "version": "1.0.0"}, "language": map[string]string{"name": "go"}}}}); err != nil {
			f.s.t.Fatal("encoding APM metadata failed")
		}
		if err := encoder.Encode(map[string]interface{}{"transaction": map[string]interface{}{"id": transaction, "trace_id": trace, "name": "E2E " + target.name, "type": "request", "duration": 1.0, "timestamp": time.Now().UnixMicro(), "sampled": true, "span_count": map[string]int{"started": 0}}}); err != nil {
			f.s.t.Fatal("encoding APM transaction failed")
		}
		e := f.component(target.base)
		e.authorization = "Bearer " + token
		e.request(http.MethodPost, "/intake/v2/events", []byte(body.String()), "application/x-ndjson", nil, nil, 202)
		f.waitSearch("traces-apm*", map[string]interface{}{"term": map[string]interface{}{"trace.id": trace}})
		f.s.t.Logf("APM %s intake accepted and trace indexed", target.name)
		f.s.snapshot("apm-"+target.name, map[string]interface{}{"accepted": true, "traceIndexed": true})
	}
}

func (f *elasticFixture) monitoringChecks() {
	f.s.poll("Elasticsearch monitoring samples", 10*time.Minute, func() bool {
		var metrics api.ClusterMetrics
		f.s.expect(&metrics, 200)(f.s.client.GetClusterMetrics(f.s.ctx, f.name, &api.GetClusterMetricsParams{Namespace: f.s.namespace, Range: ptr("15m")}))
		seen := map[string]bool{}
		for _, series := range metrics.Series {
			if strings.HasPrefix(series.Name, "elasticsearch.") && len(series.Points) > 0 {
				seen[series.Member] = true
			}
		}
		for _, member := range f.s.members(f.name) {
			if strings.HasPrefix(member.Name, f.name+"n") && !seen[member.Name] {
				return false
			}
		}
		if len(seen) < 3 {
			return false
		}
		f.s.snapshot("metrics", metrics)
		return true
	})
}

func (f *elasticFixture) replaceChecks() {
	old := f.name + "n1"
	before := map[string]bool{}
	for _, m := range f.s.members(f.name) {
		before[m.Name] = true
	}
	if !before[old] {
		f.s.t.Fatal("replacement target n1 missing")
	}
	f.operation(f.s.client.ReplaceClusterMember(f.s.ctx, f.name, old, &api.ReplaceClusterMemberParams{Namespace: f.s.namespace}))
	foundNew := false
	for _, m := range f.s.members(f.name) {
		if m.Name == old {
			f.s.t.Fatal("replaced member remains in status")
		}
		if strings.HasPrefix(m.Name, f.name+"n") && !before[m.Name] {
			foundNew = true
			f.s.t.Logf("replacement member=%s", m.Name)
		}
	}
	if !foundNew {
		f.s.t.Fatal("replacement did not allocate a new member name")
	}
	f.health(f.name, 3)
	f.proof(f.name)
}

func (f *elasticFixture) restoreChecks() {
	b := f.snapshotBackup()
	f.connect(f.name).json(http.MethodDelete, "/"+elasticIndex, nil, nil, 200)
	f.connect(f.name).json(http.MethodHead, "/"+elasticIndex, nil, nil, 404)
	name := f.s.name()
	var restore api.Restore
	f.s.expect(&restore, 202)(f.s.client.CreateRestore(f.s.ctx, f.name, &api.CreateRestoreParams{Namespace: f.s.namespace}, api.CreateRestoreRequest{Name: name, Source: api.RestoreSource{SourceCluster: ptr(f.name), Backup: ptr(b.Name)}}))
	f.s.poll("snapshot restore "+name, elasticTimeout, func() bool {
		f.s.expect(&restore, 200)(f.s.client.GetRestore(f.s.ctx, f.name, name, &api.GetRestoreParams{Namespace: f.s.namespace}))
		f.s.t.Logf("restore=%s phase=%s", name, restore.Phase)
		if restore.Phase == api.RestorePhaseFailed {
			f.s.snapshot("failed-restore", restore)
			f.s.t.Fatal("snapshot restore failed")
		}
		return restore.Phase == api.RestorePhaseSucceeded
	})
	f.s.snapshot("restore", restore)
	f.ready(f.name)
	f.health(f.name, 3)
	f.proof(f.name)
}

func (f *elasticFixture) cloneChecks() {
	b := f.snapshotBackup()
	clone := f.s.name()
	spec := elasticSpec(f.storage, 1, false)
	spec.DataSource = &api.DataSource{Cluster: f.name, Backup: ptr(b.Name)}
	f.create(clone, spec)
	f.proof(clone)
	var health struct {
		Status string `json:"status"`
		Nodes  int    `json:"number_of_nodes"`
	}
	f.connect(clone).json(http.MethodGet, "/_cluster/health", nil, &health, 200)
	// The restored proof index has one replica: a singleton is correctly yellow.
	if health.Nodes != 1 || (health.Status != "yellow" && health.Status != "green") {
		f.s.t.Fatal("singleton clone has invalid cluster health")
	}
	f.s.deleteClusterOptions(clone, false, false)
	f.assertDeleted(clone)
}

func (f *elasticFixture) upgradeChecks() {
	f.s.t.Log("rolling PATCH upgrade of Elastic Stack 9.5.4 -> 9.5.5")
	f.s.expect(nil, 200)(f.s.client.PatchClusterWithApplicationMergePatchPlusJSONBody(f.s.ctx, f.name, &api.PatchClusterParams{Namespace: f.s.namespace}, api.ClusterMergePatch{"spec": map[string]interface{}{"version": "9.5.5"}}))
	if c := f.ready(f.name); c.Spec.Version != "9.5.5" {
		f.s.t.Fatal("upgrade desired version is not 9.5.5")
	}
	f.health(f.name, 3)
	var nodes struct {
		Nodes map[string]struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"nodes"`
	}
	f.connect(f.name).json(http.MethodGet, "/_nodes", nil, &nodes, 200)
	if len(nodes.Nodes) != 3 {
		f.s.t.Fatal("upgrade node inventory is not three nodes")
	}
	for _, node := range nodes.Nodes {
		if node.Version != "9.5.5" {
			f.s.t.Fatalf("member=%s version=%s want=9.5.5", node.Name, node.Version)
		}
	}
	members := f.s.members(f.name)
	if len(members) != 6 {
		f.s.t.Fatalf("stack member count=%d want=6", len(members))
	}
	for _, member := range members {
		if member.Version == nil || *member.Version != "9.5.5" {
			f.s.t.Fatalf("member=%s does not report version 9.5.5", member.Name)
		}
	}
	f.s.snapshot("upgraded-members", members)
	f.kibanaStatus("9.5.5")
	f.proof(f.name)
}

func (f *elasticFixture) assertDeleted(name string) {
	f.s.expect(nil, 404)(f.s.client.GetCluster(f.s.ctx, name, &api.GetClusterParams{Namespace: f.s.namespace}))
	var cursor *string
	for {
		var list api.ClusterList
		f.s.expect(&list, 200)(f.s.client.ListClusters(f.s.ctx, &api.ListClustersParams{Namespace: f.s.namespace, Limit: ptr(100), Cursor: cursor}))
		for _, c := range list.Items {
			if c.Name == name {
				f.s.t.Fatalf("deleted cluster %s remains in list", name)
			}
		}
		if list.NextCursor == nil {
			break
		}
		cursor = list.NextCursor
	}
	f.s.snapshot(fmt.Sprintf("%s-deleted", name), map[string]interface{}{"status404": true, "absentFromClusterList": true, "keepVolumes": false, "keepBackups": false})
}

func (f *elasticFixture) scaleChecks() {
	for _, count := range []int{4, 3} {
		start := time.Now()
		before := map[string]bool{}
		for _, member := range f.s.members(f.name) {
			if strings.HasPrefix(member.Name, f.name+"n") {
				before[member.Name] = true
			}
		}
		f.s.t.Logf("scaling engine to %d", count)
		f.update(func(spec *api.ClusterSpec) { spec.Components.Engine.Replicas = count })
		f.health(f.name, count)
		f.proof(f.name)
		if count == 3 {
			for _, member := range f.s.members(f.name) {
				delete(before, member.Name)
			}
			if len(before) != 1 {
				f.s.t.Fatalf("scale-down removed %d nodes want=1", len(before))
			}
			f.assertScaleExclusionsCleared(before)
		}
		f.s.t.Logf("scale=%d elapsed=%s", count, time.Since(start).Round(time.Second))
	}
}

func (f *elasticFixture) assertScaleExclusionsCleared(removed map[string]bool) {
	e := f.connect(f.name)
	var state struct {
		Metadata struct {
			Coordination struct {
				Exclusions []json.RawMessage `json:"voting_config_exclusions"`
			} `json:"cluster_coordination"`
		} `json:"metadata"`
	}
	e.json(http.MethodGet, "/_cluster/state/metadata", nil, &state, 200)
	if state.Metadata.Coordination.Exclusions == nil {
		f.s.t.Fatal("cluster metadata omitted voting configuration exclusions")
	}
	if len(state.Metadata.Coordination.Exclusions) != 0 {
		f.s.t.Fatal("voting configuration exclusions remain after scale-down")
	}
	var settings struct {
		Persistent map[string]json.RawMessage `json:"persistent"`
		Transient  map[string]json.RawMessage `json:"transient"`
	}
	e.json(http.MethodGet, "/_cluster/settings?flat_settings=true", nil, &settings, 200)
	for scope, values := range map[string]map[string]json.RawMessage{"persistent": settings.Persistent, "transient": settings.Transient} {
		raw, exists := values["cluster.routing.allocation.exclude._name"]
		if !exists {
			continue
		}
		var excluded string
		if err := json.Unmarshal(raw, &excluded); err != nil {
			f.s.t.Fatalf("%s allocation exclusions have invalid shape", scope)
		}
		for _, name := range strings.Split(excluded, ",") {
			if removed[strings.TrimSpace(name)] {
				f.s.t.Fatalf("%s allocation exclusion still names removed node %s", scope, name)
			}
		}
	}
	f.s.snapshot("scale-exclusions", map[string]interface{}{"removedNodes": removed, "votingExclusions": state.Metadata.Coordination.Exclusions, "settings": settings})
}

func (f *elasticFixture) deleteKeepChecks() {
	b := f.snapshotBackup()
	backups := f.backupInventory()
	if backup, found := backups[b.Name]; !found || backup.Phase != api.BackupPhaseSucceeded {
		f.s.t.Fatal("successful snapshot missing from pre-suspension backup inventory")
	}
	f.s.deleteClusterOptions(f.name, true, true)
	f.s.phase(f.name, api.ClusterStatusPhaseSuspended)
	f.assertBackupsRetained(backups)
	var resumed api.Cluster
	f.s.expect(&resumed, 202)(f.s.client.ResumeCluster(f.s.ctx, f.name, &api.ResumeClusterParams{Namespace: f.s.namespace}))
	f.ready(f.name)
	f.health(f.name, 3)
	f.proof(f.name)
	f.assertBackupsRetained(backups)
	// Ready requires the configured Stack components to be reprovisioned too.
	if members := f.s.members(f.name); len(members) != 6 {
		f.s.t.Fatalf("resumed Stack member count=%d want=6", len(members))
	}
	f.kibanaStatus(f.s.cluster(f.name).Spec.Version)
}
