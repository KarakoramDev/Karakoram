// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestClientEndpointAddressing(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, bucket, host, path string
		force                              bool
	}{
		{"dns-http", "http://s3.example.test", "objects", "objects.s3.example.test", "/probe", false},
		{"dns-https-port", "https://s3.example.test:9443", "objects", "objects.s3.example.test:9443", "/probe", false},
		{"forced-dns", "https://s3.example.test/", "objects", "s3.example.test", "/objects/probe", true},
		{"ipv4-http", "http://192.0.2.1:9000", "objects", "192.0.2.1:9000", "/objects/probe", false},
		{"ipv4-https", "https://192.0.2.1", "objects", "192.0.2.1", "/objects/probe", false},
		{"ipv6-http", "http://[2001:db8::1]:9000", "objects", "[2001:db8::1]:9000", "/objects/probe", false},
		{"ipv6-https", "https://[2001:db8::1]", "objects", "[2001:db8::1]", "/objects/probe", false},
		{"dotted-https", "https://s3.example.test", "application.objects", "s3.example.test", "/application.objects/probe", false},
		{"dotted-http", "http://s3.example.test", "application.objects", "application.objects.s3.example.test", "/probe", false},
		{"forced-ipv4", "http://192.0.2.1:9000", "objects", "192.0.2.1:9000", "/objects/probe", true},
		{"forced-ipv6", "https://[2001:db8::1]:9443", "objects", "[2001:db8::1]:9443", "/objects/probe", true},
		{"forced-dotted-https", "https://s3.example.test", "application.objects", "s3.example.test", "/application.objects/probe", true},
		{"dotted-forced", "http://s3.example.test", "application.objects", "s3.example.test", "/application.objects/probe", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := domain.BackupStorage{Type: "s3", EndpointURL: tc.endpoint, Bucket: tc.bucket, Region: "us-east-1", ForcePathStyle: tc.force}
			client, closeClient, err := NewClient(s, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			defer closeClient()
			target, err := client.PresignedHeadObject(t.Context(), s.Bucket, "probe", time.Minute, nil)
			if err != nil {
				t.Fatal(err)
			}
			if target.Host != tc.host || target.Path != tc.path {
				t.Fatalf("target host/path = %q %q; want %q %q", target.Host, target.Path, tc.host, tc.path)
			}
		})
	}
}

func TestVerifyUnsupportedEndpointPrefix(t *testing.T) {
	for _, endpoint := range []string{"https://s3.example.test/s3", "https://s3.example.test/s3/", "https://s3.example.test/%2f"} {
		err := Verify(t.Context(), domain.BackupStorage{Type: "s3", EndpointURL: endpoint, Bucket: "objects"}, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
		reason, message := Failure(err)
		if err == nil || reason != "endpoint_not_allowed" || !strings.Contains(message, "path prefixes") || !strings.Contains(message, "prefix") {
			t.Fatalf("reason=%q message=%q err=%v", reason, message, err)
		}
	}
}
