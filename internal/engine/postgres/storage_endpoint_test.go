// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRepositoryEndpointAddressing(t *testing.T) {
	for _, tc := range []struct {
		endpoint, bucket string
		force, path      bool
		port             int
	}{
		{"https://s3.example.test", "objects", false, false, 0},
		{"http://s3.example.test:9000", "objects", true, true, 9000},
		{"http://192.0.2.1:9000", "objects", false, true, 9000},
		{"https://[2001:db8::1]:9443", "objects", false, true, 9443},
		{"https://s3.example.test", "application.objects", false, true, 0},
		{"http://s3.example.test", "application.objects", false, false, 0},
	} {
		t.Run(tc.endpoint+"/"+tc.bucket, func(t *testing.T) {
			r, err := repository(&domain.BackupStorage{EndpointURL: tc.endpoint, Bucket: tc.bucket, ForcePathStyle: tc.force}, "tenant/postgres")
			if err != nil {
				t.Fatal(err)
			}
			if r.S3.Endpoint != tc.endpoint || r.S3.PathStyle != tc.path || r.S3.Port != tc.port || r.Path != "/tenant/postgres" {
				t.Fatalf("incorrect endpoint/style/port/path projection")
			}
		})
	}
	if _, err := repository(&domain.BackupStorage{EndpointURL: "https://s3.example.test/s3"}, "tenant"); err == nil || !strings.Contains(err.Error(), "path prefixes") {
		t.Fatalf("prefix: %v", err)
	}
}
