// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package s3endpoint

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for _, endpoint := range []string{"https://s3.example.test", "http://s3.example.test:9000/", "http://192.0.2.1:9000", "https://[2001:db8::1]:9443", "https://legacy_storage.example.test", "https://[::ffff:c000:201]"} {
		if _, err := Parse(endpoint); err != nil {
			t.Errorf("valid origin rejected: %v", err)
		}
	}
	for _, endpoint := range []string{"s3.example.test", "s3.example.test:9000", "ftp://s3.example.test", "https://user:secret@s3.example.test", "https://s3.example.test?token=secret", "https://s3.example.test?", "https://s3.example.test#secret", "https://s3.example.test#", "https://s3.example.test:0", "https://s3.example.test:65536", "https://s3.example.test:", "https://s3.example.test:080", "https://[fe80::1%25eth0]", "https://[::ffff:192.0.2.1]", "https://-bad.example.test"} {
		if _, err := Parse(endpoint); err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("invalid origin accepted or leaked input: %v", err)
		}
	}
	for _, endpoint := range []string{"https://s3.example.test/s3", "https://s3.example.test/s3/", "https://s3.example.test/%2f", "https://s3.example.test//"} {
		if _, err := Parse(endpoint); !errors.Is(err, ErrPathPrefix) {
			t.Errorf("missing path-prefix error: %v", err)
		}
	}
}

func TestPathStyle(t *testing.T) {
	for _, tc := range []struct {
		endpoint, bucket string
		path             bool
	}{
		{"https://s3.example.test", "objects", false},
		{"http://s3.example.test:9000", "application.objects", false},
		{"https://s3.example.test", "application.objects", true},
		{"http://192.0.2.1:9000", "objects", true},
		{"https://[2001:db8::1]:9443", "objects", true},
		{"https://s3.example.test", "Legacy_Bucket", true},
	} {
		u, err := url.Parse(tc.endpoint)
		if err != nil {
			t.Fatal(err)
		}
		if got := PathStyle(u, tc.bucket, false); got != tc.path {
			t.Errorf("%s %s: got path=%t", tc.endpoint, tc.bucket, got)
		}
		if !PathStyle(u, tc.bucket, true) {
			t.Error("explicit force path ignored")
		}
	}
}
