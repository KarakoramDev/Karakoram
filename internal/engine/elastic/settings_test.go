// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestRestartToken(t *testing.T) {
	for _, parts := range [][]string{{}, {"a", "bc"}, {"ab", "c"}, {"a\"b", "c"}} {
		raw, err := json.Marshal(parts)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if got := RestartToken(parts...); got != hex.EncodeToString(sum[:]) {
			t.Fatal("canonical JSON hash")
		}
	}
	if RestartToken("a", "bc") == RestartToken("ab", "c") {
		t.Fatal("ambiguous parts")
	}
	if RestartToken() != RestartToken([]string{}...) {
		t.Fatal("empty input instability")
	}
}

func TestNodeRestartToken(t *testing.T) {
	provider := map[string]any{"s3.client.kk.endpoint": "https://s3.example.com", "discovery.seed_hosts": []string{"old"}, "cluster.initial_master_nodes": []string{"demon0"}}
	user := map[string]any{"indices.recovery.max_bytes_per_sec": "20mb", "node.attr.rack": "a"}
	base, err := NodeRestartToken("9.5.5", 768, []string{"master", "data"}, provider, user)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, version  string
		heap           int
		roles          []string
		provider, user map[string]any
		same           bool
	}{
		{"ordered role set", "9.5.5", 768, []string{"data", "master", "master"}, provider, user, true},
		{"nested settings", "9.5.5", 768, []string{"master", "data"}, provider, map[string]any{"node": map[string]any{"attr": map[string]any{"rack": "a"}}, "indices": map[string]any{"recovery": map[string]any{"max_bytes_per_sec": "20mb"}}}, true},
		{"bootstrap and seeds removed", "9.5.5", 768, []string{"master", "data"}, map[string]any{"s3.client.kk.endpoint": "https://s3.example.com"}, user, true},
		{"version", "9.5.4", 768, []string{"master", "data"}, provider, user, false}, {"heap", "9.5.5", 832, []string{"master", "data"}, provider, user, false},
		{"roles", "9.5.5", 768, []string{"data"}, provider, user, false}, {"provider setting", "9.5.5", 768, []string{"master", "data"}, map[string]any{"s3.client.kk.endpoint": "https://other.example.com"}, user, false},
		{"user setting", "9.5.5", 768, []string{"master", "data"}, provider, map[string]any{"node.attr.rack": "b"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NodeRestartToken(tt.version, tt.heap, tt.roles, tt.provider, tt.user)
			if err != nil {
				t.Fatal(err)
			}
			if (got == base) != tt.same {
				t.Fatal("restart token identity")
			}
		})
	}
	a, err := NodeRestartToken("9.5.5", 768, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NodeRestartToken("9.5.5", 768, []string{}, map[string]any{}, map[string]any{})
	if err != nil || a != b {
		t.Fatal("nil and empty inputs")
	}
	for _, bad := range []map[string]any{{"a": math.NaN()}, {"a": func() {}}, {"a.b": 1, "a": map[string]any{"b": 2}}} {
		if _, err := NodeRestartToken("9.5.5", 768, nil, bad, nil); err == nil {
			t.Fatal("invalid settings")
		}
	}
}

func TestS3Settings(t *testing.T) {
	for _, tt := range []struct{ namespace, storage string }{{"ns", "archive"}, {"n", "sarchive"}, {"ns", "other"}} {
		t.Run(tt.namespace+tt.storage, func(t *testing.T) {
			name := S3ClientName(tt.namespace, tt.storage)
			if len(name) != 12 || !strings.HasPrefix(name, "kk") || name != S3ClientName(tt.namespace, tt.storage) {
				t.Fatal("client name")
			}
			if _, err := hex.DecodeString(name[2:]); err != nil {
				t.Fatal(err)
			}
		})
	}
	if S3ClientName("ns", "archive") == S3ClientName("n", "sarchive") {
		t.Fatal("ambiguous client identity")
	}
	for _, region := range []string{"", "eu-central-1"} {
		wantRegion := region
		if wantRegion == "" {
			wantRegion = "us-east-1"
		}
		got := S3ClientSettings("kk123", "https://s3.example.com", region)
		want := map[string]any{"s3.client.kk123.endpoint": "https://s3.example.com", "s3.client.kk123.region": wantRegion, "s3.client.kk123.path_style_access": true}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("static S3 settings")
		}
	}
	if !reflect.DeepEqual(S3SecureSettings("kk123", "access", "secret"), map[string]string{"s3.client.kk123.access_key": "access", "s3.client.kk123.secret_key": "secret"}) {
		t.Fatal("secure S3 settings")
	}
}

func TestRepositoryPaths(t *testing.T) {
	if RepositoryName("archive") != "kk-archive" {
		t.Fatal("repository name")
	}
	for _, tt := range []struct{ prefix, namespace, cluster, uid, want string }{
		{"backups", "ns", "demo", "id", "backups/ns/demo/id"}, {"/backups///nested/", "/ns/", "demo/", "/id/", "backups/nested/ns/demo/id"}, {"", "ns", "demo", "id", "ns/demo/id"}, {"///", "", "", "", ""}, {"a/../b", "ns", "demo", "id", "a/../b/ns/demo/id"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			if got := RepositoryBasePath(tt.prefix, tt.namespace, tt.cluster, tt.uid); got != tt.want {
				t.Fatalf("got %q", got)
			}
		})
	}
}
