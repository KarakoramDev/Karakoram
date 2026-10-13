//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

const (
	elasticTimeout      = 60 * time.Minute
	elasticIndex        = "e2e-proof"
	elasticPrivateIndex = "e2e-private"
	elasticDocuments    = 12
)

// elasticFixture is provisioned lazily, so selecting any phase also provisions
// its prerequisites. An unfiltered run shares one cluster across all phases.
type elasticFixture struct {
	root, s         *suite
	name, storage   string
	created, seeded bool
	backup          *api.Backup
	targets         []string
}

func elasticSpec(storage string, replicas int, components bool) api.ClusterSpec {
	resources := smallResources()
	resources.MinRam, resources.MaxRam = 2, 2
	extra := map[string]api.Component{}
	for _, component := range []string{"kibana", "logstash", "apm"} {
		r := resources
		if component == "logstash" {
			r.MinRam, r.MaxRam = 1.5, 1.5
		}
		if component == "apm" {
			r.MinRam, r.MaxRam = 0.5, 0.5
		}
		r.MinDisk, r.MaxDisk = 1, 5
		n := 0
		if components {
			n = 1
		}
		extra[component] = api.Component{Replicas: n, Resources: r}
	}
	options := map[string]interface{}{"kibana": map[string]interface{}{"public": components}, "apm": map[string]interface{}{"public": components}}
	return api.ClusterSpec{
		Provider: "elasticsearch", Version: "9.5.4", Topology: api.Topology{Type: api.TopologyTypeCluster},
		Components:    api.Components{Engine: api.Component{Replicas: replicas, Resources: resources}, Extra: &extra},
		EngineOptions: &options, Backup: &api.BackupSpec{Storage: ptr(storage)},
		Tls: &api.TLS{Mode: api.TLSMode("requireTLS"), Ca: api.TLSCa("selfSigned")}, Exposure: &api.Exposure{Public: false}, Monitoring: &api.MonitoringSpec{Enabled: true},
	}
}

func (f *elasticFixture) phase(t *testing.T, action func()) {
	s := *f.root
	s.t = t
	f.s = &s
	start := time.Now()
	t.Logf("phase=%s starting cluster=%s", t.Name(), f.name)
	t.Cleanup(func() {
		t.Logf("phase=%s elapsed=%s failed=%t", t.Name(), time.Since(start).Round(time.Second), t.Failed())
		f.s.snapshot("timing", map[string]interface{}{"phase": t.Name(), "elapsedSeconds": time.Since(start).Seconds(), "failed": t.Failed()})
		if t.Failed() {
			f.failureEvidence()
		}
	})
	f.ensure()
	action()
}

func (f *elasticFixture) ensure() {
	if !f.created {
		f.create(f.name, elasticSpec(f.storage, 3, true))
		f.created = true
	}
	f.seed()
	f.health(f.name, 3)
	f.proof(f.name)
}

func (f *elasticFixture) seed() {
	if f.seeded {
		return
	}
	e := f.connect(f.name)
	for _, index := range []string{elasticIndex, elasticPrivateIndex} {
		e.json(http.MethodPut, "/"+index, map[string]interface{}{"settings": map[string]interface{}{"number_of_shards": 1, "number_of_replicas": 1}}, nil, 200)
	}
	for i := range elasticDocuments {
		e.json(http.MethodPut, fmt.Sprintf("/%s/_doc/%d?refresh=wait_for", elasticIndex, i), map[string]interface{}{"marker": f.name, "sequence": i}, nil, 201)
	}
	e.json(http.MethodPut, "/"+elasticPrivateIndex+"/_doc/1?refresh=wait_for", map[string]interface{}{"private": true}, nil, 201)
	f.seeded = true
}

func (f *elasticFixture) create(name string, spec api.ClusterSpec) {
	f.targets = append(f.targets, name)
	f.root.cleanupCluster(name)
	var cluster api.Cluster
	f.s.expect(&cluster, 201)(f.s.client.CreateClusterWithBody(f.s.ctx, &api.CreateClusterParams{Namespace: f.s.namespace}, "application/json", f.s.clusterBody(clusterWriteEnvelope{Name: name, Spec: spec})))
	f.s.t.Logf("created cluster=%s version=%s replicas=%d", name, spec.Version, spec.Components.Engine.Replicas)
	f.ready(name)
}

func (f *elasticFixture) ready(name string) api.Cluster {
	var c api.Cluster
	f.s.poll(name+" Ready", elasticTimeout, func() bool {
		c = f.s.cluster(name)
		f.s.t.Logf("cluster=%s phase=%s generation=%d observed=%d", name, c.Status.Phase, c.Generation, c.Status.ObservedGeneration)
		if c.Status.Phase == api.ClusterStatusPhaseError {
			f.s.t.Fatal("cluster entered Error")
		}
		return c.Status.Phase == api.ClusterStatusPhaseReady && c.Status.ObservedGeneration >= c.Generation
	})
	f.s.snapshot(name+"-status", c.Status)
	return c
}

func (f *elasticFixture) update(edit func(*api.ClusterSpec)) api.Cluster {
	c := f.s.cluster(f.name)
	edit(&c.Spec)
	f.s.expect(nil, 200)(f.s.client.ReplaceClusterWithBody(f.s.ctx, f.name, &api.ReplaceClusterParams{Namespace: f.s.namespace}, "application/json", f.s.clusterBody(clusterWriteEnvelope{Spec: c.Spec})))
	return f.ready(f.name)
}

func (f *elasticFixture) operation(r *http.Response, err error) {
	var op api.Operation
	f.s.expect(&op, 202)(r, err)
	if op.Id == "" {
		f.s.t.Fatal("operation omitted id")
	}
	f.s.poll("operation "+op.Id, elasticTimeout, func() bool {
		f.s.expect(&op, 200)(f.s.client.GetOperation(f.s.ctx, op.Id))
		f.s.t.Logf("operation=%s type=%s phase=%s", op.Id, op.Type, op.Phase)
		if op.Phase == api.OperationPhaseFailed {
			f.s.snapshot("failed-operation", op)
			f.s.t.Fatal("operation failed")
		}
		return op.Phase == api.OperationPhaseSucceeded
	})
	f.s.snapshot("operation", op)
	f.ready(f.name)
}

func (f *elasticFixture) snapshotBackup() api.Backup {
	if f.backup != nil {
		return *f.backup
	}
	name := f.s.name()
	var b api.Backup
	f.s.expect(&b, 202)(f.s.client.CreateBackup(f.s.ctx, f.name, &api.CreateBackupParams{Namespace: f.s.namespace}, api.CreateBackupRequest{Name: name, Storage: f.storage, Type: "snapshot"}))
	f.s.poll("snapshot backup "+name, elasticTimeout, func() bool {
		f.s.expect(&b, 200)(f.s.client.GetBackup(f.s.ctx, f.name, name, &api.GetBackupParams{Namespace: f.s.namespace}))
		f.s.t.Logf("backup=%s phase=%s", name, b.Phase)
		if b.Phase == api.BackupPhaseFailed {
			f.s.snapshot("failed-backup", b)
			f.s.t.Fatal("snapshot failed")
		}
		return b.Phase == api.BackupPhaseSucceeded
	})
	if b.Type != api.BackupTypeSnapshot {
		f.s.t.Fatal("backup is not a snapshot")
	}
	f.s.snapshot("backup", b)
	f.backup = &b
	return b
}

// failureEvidence never records credential responses or arbitrary HTTP bodies.
func (f *elasticFixture) failureEvidence() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, name := range f.targets {
		var c api.Cluster
		if r, err := f.s.client.GetCluster(ctx, name, &api.GetClusterParams{Namespace: f.s.namespace}); readJSON(&c, r, err) == nil {
			f.s.snapshot(name+"-failure-status", c.Status)
		}
		var events api.EventList
		if r, err := f.s.client.ListEvents(ctx, &api.ListEventsParams{Cluster: ptr(name), Namespace: f.s.namespace, Limit: ptr(100)}); readJSON(&events, r, err) == nil {
			f.s.snapshot(name+"-failure-events", events)
		}
		var ops api.OperationList
		if r, err := f.s.client.ListClusterOperations(ctx, name, &api.ListClusterOperationsParams{Namespace: f.s.namespace, Limit: ptr(100)}); readJSON(&ops, r, err) == nil {
			f.s.snapshot(name+"-failure-operations", ops)
		}
	}
}

type elasticHTTP struct {
	s                                   *suite
	client                              *http.Client
	base, user, password, authorization string
}

func (f *elasticFixture) connect(name string) *elasticHTTP {
	c := f.s.connection(name)
	_, password := credential(c, "elastic")
	if password == "" || c.CaCertificate == nil {
		f.s.t.Fatal("connection details omitted elastic password or CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(*c.CaCertificate)) {
		f.s.t.Fatal("connection details CA is not valid PEM")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 30 * time.Second}
	client := &http.Client{Transport: transport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	f.s.t.Cleanup(client.CloseIdleConnections)
	return &elasticHTTP{s: f.s, client: client, base: "https://" + name + "n0:9200", user: "elastic", password: password}
}

func (e *elasticHTTP) json(method, path string, body, out interface{}, codes ...int) {
	e.s.t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			e.s.t.Fatal("encoding JSON request failed")
		}
	}
	e.request(method, path, data, "application/json", nil, out, codes...)
}

func (e *elasticHTTP) request(method, path string, data []byte, contentType string, headers map[string]string, out interface{}, codes ...int) {
	e.s.t.Helper()
	req, err := http.NewRequestWithContext(e.s.ctx, method, strings.TrimRight(e.base, "/")+path, bytes.NewReader(data))
	if err != nil {
		e.s.t.Fatal("creating HTTP request failed")
	}
	req.Header.Set("Content-Type", contentType)
	if e.authorization != "" {
		req.Header.Set("Authorization", e.authorization)
	} else if e.user != "" {
		req.SetBasicAuth(e.user, e.password)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	r, err := e.client.Do(req)
	// Do not print transport errors: URLs and credential-bearing requests are private.
	if err != nil {
		e.s.t.Fatalf("%s data-plane request failed (transport/TLS)", method)
	}
	defer r.Body.Close()
	ok := false
	for _, code := range codes {
		if r.StatusCode == code {
			ok = true
		}
	}
	if !ok {
		// Report only the service's error classification, never the raw body.
		var failure struct {
			Error   json.RawMessage `json:"error"`
			Message string          `json:"message"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&failure)
		reason := failure.Message
		var es struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(failure.Error, &es) == nil && es.Type != "" {
			reason = es.Type
		}
		if len(reason) > 160 {
			reason = reason[:160]
		}
		e.s.t.Fatalf("%s data-plane HTTP %d, expected %v: %s", method, r.StatusCode, codes, reason)
	}
	if out != nil {
		if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(out); err != nil {
			e.s.t.Fatal("decoding data-plane JSON failed")
		}
	} else {
		_, _ = io.Copy(io.Discard, r.Body)
	}
}

func (f *elasticFixture) access(method, path string, body, out interface{}, codes ...int) {
	// Use the generated client's configured base URL and transport; the new
	// engine routes intentionally do not depend on concurrent code generation.
	base := strings.TrimRight(f.s.client.Server, "/") + "/clusters/" + url.PathEscape(f.name) + "/elasticsearch"
	if f.s.namespace != nil {
		separator := "?"
		if strings.Contains(path, "?") {
			separator = "&"
		}
		path += separator + "namespace=" + url.QueryEscape(*f.s.namespace)
	}
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			f.s.t.Fatal("encoding access request failed")
		}
	}
	req, err := http.NewRequestWithContext(f.s.ctx, method, base+path, bytes.NewReader(data))
	if err != nil {
		f.s.t.Fatal("creating access request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.s.token)
	f.s.expect(out, codes...)(f.s.client.Client.Do(req))
}

func (f *elasticFixture) component(base string) *elasticHTTP {
	client := &http.Client{Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &elasticHTTP{s: f.s, client: client, base: base}
}

func (f *elasticFixture) publicEndpoint(component string) string {
	c := f.s.connection(f.name)
	if c.Connection.Endpoints != nil {
		for _, endpoint := range *c.Connection.Endpoints {
			if endpoint.Name != component || !endpoint.Public {
				continue
			}
			u, err := url.Parse(endpoint.Url)
			if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				f.s.t.Fatal("public endpoint must use verified HTTPS")
			}
			return strings.TrimRight(endpoint.Url, "/")
		}
	}
	f.s.t.Fatalf("connection details omitted public %s endpoint", component)
	return ""
}

func (f *elasticFixture) health(name string, nodes int) {
	e := f.connect(name)
	var health struct {
		Status       string `json:"status"`
		Nodes        int    `json:"number_of_nodes"`
		Relocating   int    `json:"relocating_shards"`
		Initializing int    `json:"initializing_shards"`
		Unassigned   int    `json:"unassigned_shards"`
	}
	f.s.poll("green Elasticsearch health", elasticTimeout, func() bool {
		e.json(http.MethodGet, "/_cluster/health", nil, &health, 200)
		f.s.t.Logf("health=%s nodes=%d relocating=%d initializing=%d unassigned=%d", health.Status, health.Nodes, health.Relocating, health.Initializing, health.Unassigned)
		return health.Status == "green" && health.Nodes == nodes && health.Relocating == 0 && health.Initializing == 0 && health.Unassigned == 0
	})
	f.s.snapshot(name+"-health", health)
}

func (f *elasticFixture) proof(name string) {
	e := f.connect(name)
	var count struct {
		Count int `json:"count"`
	}
	e.json(http.MethodGet, "/"+elasticIndex+"/_count", nil, &count, 200)
	if count.Count != elasticDocuments {
		f.s.t.Fatalf("document count=%d want=%d", count.Count, elasticDocuments)
	}
	var result struct {
		Hits struct {
			Hits []struct {
				ID     string `json:"_id"`
				Source struct {
					Marker   string `json:"marker"`
					Sequence int    `json:"sequence"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	e.json(http.MethodPost, "/"+elasticIndex+"/_search", map[string]interface{}{"size": elasticDocuments, "query": map[string]interface{}{"match_all": map[string]interface{}{}}}, &result, 200)
	if len(result.Hits.Hits) != elasticDocuments {
		f.s.t.Fatal("search omitted proof documents")
	}
	seen := map[int]bool{}
	for _, hit := range result.Hits.Hits {
		n := hit.Source.Sequence
		if n < 0 || n >= elasticDocuments || seen[n] || hit.ID != fmt.Sprint(n) || hit.Source.Marker != f.name {
			f.s.t.Fatal("search returned altered proof document")
		}
		seen[n] = true
	}
	f.s.snapshot(name+"-proof", map[string]interface{}{"documents": count.Count, "exactContents": true})
}

func elasticRandom(t *testing.T, length int) string {
	t.Helper()
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		t.Fatal("generating test identifier failed")
	}
	return hex.EncodeToString(b)
}

func (f *elasticFixture) waitSearch(pattern string, query interface{}) {
	e := f.connect(f.name)
	f.s.poll("indexed "+pattern+" event", 15*time.Minute, func() bool {
		var result struct {
			Hits struct {
				Hits []json.RawMessage `json:"hits"`
			} `json:"hits"`
		}
		e.json(http.MethodPost, "/"+pattern+"/_search?ignore_unavailable=true&allow_no_indices=true", map[string]interface{}{"size": 1, "query": query}, &result, 200)
		return len(result.Hits.Hits) > 0
	})
	f.s.snapshot("ingestion", map[string]interface{}{"indexPattern": pattern, "found": true})
}

func (f *elasticFixture) backupInventory() map[string]api.Backup {
	f.s.t.Helper()
	backups := map[string]api.Backup{}
	var cursor *string
	for {
		var list api.BackupList
		f.s.expect(&list, 200)(f.s.client.ListBackups(f.s.ctx, f.name, &api.ListBackupsParams{Namespace: f.s.namespace, Limit: ptr(100), Cursor: cursor}))
		for _, backup := range list.Items {
			backups[backup.Name] = backup
		}
		if list.NextCursor == nil {
			break
		}
		cursor = list.NextCursor
	}
	return backups
}

func (f *elasticFixture) assertBackupsRetained(before map[string]api.Backup) {
	f.s.t.Helper()
	if after := f.backupInventory(); !reflect.DeepEqual(before, after) {
		f.s.t.Fatal("retained backup inventory changed")
	}
	for name, want := range before {
		var got api.Backup
		f.s.expect(&got, 200)(f.s.client.GetBackup(f.s.ctx, f.name, name, &api.GetBackupParams{Namespace: f.s.namespace}))
		if !reflect.DeepEqual(want, got) {
			f.s.t.Fatalf("retained backup %s changed", name)
		}
	}
	f.s.snapshot("backups-retained", before)
}

func (f *elasticFixture) assertBackupsDeleted(before map[string]api.Backup) {
	f.s.t.Helper()
	for name := range before {
		f.s.expect(nil, 404)(f.s.client.GetBackup(f.s.ctx, f.name, name, &api.GetBackupParams{Namespace: f.s.namespace}))
	}
	if len(f.backupInventory()) != 0 {
		f.s.t.Fatal("deleted cluster still has backup records")
	}
	f.s.snapshot("backups-deleted", map[string]interface{}{"recordsRemoved": len(before), "status404": true, "inventoryEmpty": true})
}
