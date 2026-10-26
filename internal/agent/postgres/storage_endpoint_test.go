// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestS3EndpointRenderShapes(t *testing.T) {
	for _, tc := range []struct {
		endpoint, style string
		path            bool
		port            int
	}{
		{"https://s3.example.test", "host", false, 0},
		{"http://s3.example.test:9000", "path", true, 9000},
		{"http://192.0.2.1:9000", "path", true, 9000},
		{"https://[2001:db8::1]:9443", "path", true, 9443},
	} {
		r := pgspec.Repository{Stanza: "kk", Path: "/tenant/postgres", S3: pgspec.S3{Endpoint: tc.endpoint, Bucket: "objects", Region: "us-east-1", PathStyle: tc.path, Port: tc.port}}
		rendered := renderRepository(r, dataPaths(t.TempDir()), "ca.pem", "postgres")
		for _, want := range []string{"repo1-s3-endpoint=" + tc.endpoint + "\n", "repo1-s3-uri-style=" + tc.style + "\n", "repo1-path=/tenant/postgres\n"} {
			if !strings.Contains(rendered, want) {
				t.Fatalf("missing native repository endpoint/style/path field")
			}
		}
		if tc.port > 0 && !strings.Contains(rendered, fmt.Sprintf("repo1-storage-port=%d\n", tc.port)) {
			t.Fatal("native endpoint port missing")
		}
	}
}
