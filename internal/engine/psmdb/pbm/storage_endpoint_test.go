// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestPBMEndpointAddressing(t *testing.T) {
	for _, tc := range []struct {
		endpoint, bucket string
		force, path      bool
	}{
		{"https://s3.example.test", "objects", false, false},
		{"http://s3.example.test:9000", "objects", true, true},
		{"http://192.0.2.1:9000", "objects", false, true},
		{"https://[2001:db8::1]:9443", "objects", false, true},
		{"https://s3.example.test", "application.objects", false, true},
		{"http://s3.example.test", "application.objects", false, false},
	} {
		t.Run(tc.endpoint+"/"+tc.bucket, func(t *testing.T) {
			env := Env{Storage: domain.BackupStorage{Type: "s3", EndpointURL: tc.endpoint, Bucket: tc.bucket, ForcePathStyle: tc.force}, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			data, err := RenderConfig(env)
			if err != nil {
				t.Fatal(err)
			}
			var cfg struct {
				Storage struct {
					S3 struct {
						Endpoint string `yaml:"endpointUrl"`
						Path     bool   `yaml:"forcePathStyle"`
					} `yaml:"s3"`
				} `yaml:"storage"`
			}
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.Storage.S3.Endpoint != tc.endpoint || cfg.Storage.S3.Path != tc.path {
				t.Fatal("incorrect endpoint/style projection")
			}
		})
	}
	_, err := RenderConfig(Env{Storage: domain.BackupStorage{Type: "s3", EndpointURL: "https://s3.example.test/s3", Bucket: "objects"}, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}})
	if err == nil || !strings.Contains(err.Error(), "path prefixes") {
		t.Fatalf("prefix: %v", err)
	}
}
