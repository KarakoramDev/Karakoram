//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

const weaviateClass = "KarakoramProof"
const weaviateUser = "e2e-reader"
const weaviateCount = 8

type weaviateFixture struct {
	root, s                      *suite
	name, storage, version, cidr string
	created, seeded              bool
	backup                       *api.Backup
}

func weaviateSpec(storage, version, cidr string) api.ClusterSpec {
	r := smallResources()
	r.MinRam, r.MaxRam, r.MinDisk, r.MaxDisk = 2, 2, 10, 10
	return api.ClusterSpec{Provider: "weaviate", Version: version, Topology: api.Topology{Type: api.TopologyTypeCluster}, Components: api.Components{Engine: api.Component{Replicas: 3, Resources: r}}, Tls: &api.TLS{Mode: api.TLSModeRequireTLS, Ca: api.TLSCaSelfSigned}, Exposure: &api.Exposure{Public: true, IpType: ptr(api.ExposureIpTypeIPV6), Allowlist: ptr([]string{cidr})}, Monitoring: &api.MonitoringSpec{Enabled: true}, Backup: &api.BackupSpec{Storage: ptr(storage)}}
}

func (f *weaviateFixture) phase(t *testing.T, action func()) {
	s := *f.root
	s.t = t
	f.s = &s
	start := time.Now()
	t.Cleanup(func() {
		s.snapshot("timing", map[string]any{"elapsedSeconds": time.Since(start).Seconds(), "failed": t.Failed()})
	})
	if !f.created {
		f.name = f.root.name()
		f.root.cleanupCluster(f.name)
		var created api.Cluster
		f.s.expect(&created, 201)(f.s.client.CreateClusterWithBody(f.s.ctx, &api.CreateClusterParams{}, "application/json", f.s.clusterBody(clusterWriteEnvelope{Name: f.name, Spec: weaviateSpec(f.storage, f.version, f.cidr)})))
		f.created = true
	}
	count := f.s.cluster(f.name).Spec.Components.Engine.Replicas
	f.health(f.name, count)
	if !f.seeded {
		f.seed()
		f.seeded = true
	}
	f.proof(f.name, count)
	action()
}

// Connections and keys are deliberately never passed to snapshot or error logs.
func (f *weaviateFixture) connect(name string, member int) *weaviateHTTP {
	c := f.s.connection(name)
	_, key := credential(c, "karakoram-admin")
	if key == "" || c.CaCertificate == nil || c.Connection.Endpoints == nil {
		f.s.t.Fatal("Weaviate connection omitted protected credential, CA or endpoints")
	}
	var endpoints []string
	for _, ep := range *c.Connection.Endpoints {
		if ep.Name == "rest" && ep.Public {
			endpoints = append(endpoints, ep.Url)
		}
	}
	if member >= len(endpoints) {
		f.s.t.Fatal("Weaviate public REST member endpoint missing")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(*c.CaCertificate)) {
		f.s.t.Fatal("invalid Weaviate CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 30 * time.Second}
	h := &http.Client{Transport: transport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	f.s.t.Cleanup(h.CloseIdleConnections)
	return &weaviateHTTP{s: f.s, client: h, base: endpoints[member], authorization: "Bearer " + key}
}

func (f *weaviateFixture) health(name string, count int) {
	c := f.s.ready(name)
	members := f.s.members(name)
	if c.Spec.Provider != "weaviate" || len(members) != count || len(c.Status.Members) != count {
		f.s.t.Fatal("Weaviate member count/provider mismatch")
	}
	for _, m := range members {
		if !m.Health || m.Agent == nil || !m.Agent.Connected {
			f.s.t.Fatal("Weaviate member unhealthy or agent disconnected")
		}
	}
	var native struct {
		Nodes []wvapi.Node `json:"nodes"`
	}
	f.connect(name, 0).json("GET", "/v1/nodes?output=verbose", nil, &native, 200)
	if len(native.Nodes) != count {
		f.s.t.Fatal("native member count mismatch")
	}
	for _, n := range native.Nodes {
		if n.Status != "HEALTHY" || n.Version != c.Spec.Version {
			f.s.t.Fatal("native member health/version mismatch")
		}
	}
	f.s.snapshot(name+"-nodes", native)
}

func weaviateID(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1) }

func (f *weaviateFixture) seed() {
	e := f.connect(f.name, 0)
	e.json("POST", "/v1/schema", map[string]any{"class": weaviateClass, "vectorizer": "none", "replicationConfig": map[string]any{"factor": 3}, "shardingConfig": map[string]any{"desiredCount": 1}, "properties": []any{map[string]any{"name": "marker", "dataType": []string{"text"}}, map[string]any{"name": "sequence", "dataType": []string{"int"}}}}, nil, 200)
	objects := make([]any, weaviateCount)
	for i := range weaviateCount {
		objects[i] = map[string]any{"class": weaviateClass, "id": weaviateID(i), "properties": map[string]any{"marker": f.name, "sequence": i}, "vector": []float64{1, float64(i) / 10, 0.5}}
	}
	var result []struct {
		ID     string `json:"id"`
		Result struct {
			Status string          `json:"status"`
			Errors json.RawMessage `json:"errors"`
		} `json:"result"`
	}
	e.json("POST", "/v1/batch/objects?consistency_level=ALL", map[string]any{"objects": objects}, &result, 200)
	if len(result) != weaviateCount {
		f.s.t.Fatal("batch response count mismatch")
	}
	for _, v := range result {
		if v.Result.Status != "SUCCESS" || (len(v.Result.Errors) > 0 && string(v.Result.Errors) != "null") {
			f.s.t.Fatal("batch object insert failed")
		}
	}
}

func (f *weaviateFixture) proof(name string, count int) {
	for member := range count {
		e := f.connect(name, member)
		for i := range weaviateCount {
			var object struct {
				ID         string `json:"id"`
				Properties struct {
					Marker   string `json:"marker"`
					Sequence int    `json:"sequence"`
				} `json:"properties"`
			}
			e.json("GET", "/v1/objects/"+weaviateClass+"/"+weaviateID(i)+"?consistency_level=ALL", nil, &object, 200)
			if object.ID != weaviateID(i) || object.Properties.Marker != f.name || object.Properties.Sequence != i {
				f.s.t.Fatal("proof object changed or missing")
			}
		}
		var result struct {
			Errors []json.RawMessage `json:"errors"`
			Data   struct {
				Get map[string][]struct {
					Marker   string `json:"marker"`
					Sequence int    `json:"sequence"`
				} `json:"Get"`
			} `json:"data"`
		}
		e.json("POST", "/v1/graphql", map[string]any{"query": "{Get{" + weaviateClass + "(nearVector:{vector:[1,0,0.5]},limit:8,consistencyLevel:ALL){marker sequence}}}"}, &result, 200)
		rows := result.Data.Get[weaviateClass]
		values := make([]int, 0, len(rows))
		for _, row := range rows {
			if row.Marker != f.name {
				f.s.t.Fatal("nearVector marker mismatch")
			}
			values = append(values, row.Sequence)
		}
		if len(result.Errors) > 0 || !weaviateSequences(values, weaviateCount) {
			f.s.t.Fatal("nearVector result incomplete, duplicated or erroneous")
		}
	}
	f.s.snapshot(name+"-proof", map[string]any{"members": count, "objects": weaviateCount, "nearVector": true, "consistency": "ALL"})
}

func (f *weaviateFixture) fullBackup() api.Backup {
	if f.backup == nil {
		// Include an application identity in the snapshot so clone isolation is meaningful.
		var users api.WeaviateUserList
		f.s.expect(&users)(f.s.client.ListWeaviateUsers(f.s.ctx, f.name, &api.ListWeaviateUsersParams{}))
		found := false
		for _, u := range users.Items {
			found = found || u.Name == weaviateUser
		}
		if !found {
			f.s.expect(nil, 201)(f.s.client.CreateWeaviateUser(f.s.ctx, f.name, &api.CreateWeaviateUserParams{}, api.WeaviateUserCreate{Name: weaviateUser, Roles: []string{"viewer"}}))
		}
		b := f.s.backup(f.name, f.storage, "full")
		if b.Type != api.BackupTypeFull || b.Location == nil || *b.Location == "" {
			f.s.t.Fatal("full backup omitted immutable location")
		}
		f.backup = &b
	}
	return *f.backup
}

func (f *weaviateFixture) leaf(e *weaviateHTTP) []byte {
	req, err := http.NewRequestWithContext(f.s.ctx, "GET", e.base+"/v1/meta", nil)
	if err != nil {
		f.s.t.Fatal("creating TLS probe failed")
	}
	req.Header.Set("Authorization", e.authorization)
	r, err := e.client.Do(req)
	if err != nil {
		f.s.t.Fatal("CA-verified TLS probe failed")
	}
	defer r.Body.Close()
	_, _ = io.Copy(io.Discard, r.Body)
	if r.StatusCode != 200 || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		f.s.t.Fatal("native TLS chain/hostname not verified")
	}
	f.s.snapshot("tls", map[string]any{"verifiedChainAndHostname": true, "notBefore": r.TLS.PeerCertificates[0].NotBefore, "notAfter": r.TLS.PeerCertificates[0].NotAfter})
	return r.TLS.PeerCertificates[0].Raw
}

type weaviateHTTP struct {
	s                   *suite
	client              *http.Client
	base, authorization string
}

// Native diagnostics report status codes only: upstream errors can echo keys.
func (e *weaviateHTTP) json(method, path string, body, out any, codes ...int) {
	e.s.t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			e.s.t.Fatal("encoding native JSON failed")
		}
	}
	req, err := http.NewRequestWithContext(e.s.ctx, method, e.base+path, bytes.NewReader(data))
	if err != nil {
		e.s.t.Fatal("creating native request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", e.authorization)
	r, err := e.client.Do(req)
	if err != nil {
		e.s.t.Fatal("native transport/TLS request failed")
	}
	defer r.Body.Close()
	ok := false
	for _, code := range codes {
		ok = ok || r.StatusCode == code
	}
	if !ok {
		e.s.t.Fatalf("native %s HTTP %d; expected %v", method, r.StatusCode, codes)
	}
	if out != nil {
		if json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(out) != nil {
			e.s.t.Fatal("decoding native JSON failed")
		}
	} else {
		_, _ = io.Copy(io.Discard, r.Body)
	}
}
