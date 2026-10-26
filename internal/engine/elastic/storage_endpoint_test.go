// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import "testing"

func TestS3EndpointRenderShapes(t *testing.T) {
	for _, endpoint := range []string{"https://s3.example.test", "http://s3.example.test:9000/", "http://192.0.2.1:9000", "https://[2001:db8::1]:9443"} {
		settings := S3ClientSettings("endpointprobe", endpoint, "us-east-1")
		if settings["s3.client.endpointprobe.endpoint"] != endpoint || settings["s3.client.endpointprobe.path_style_access"] != true {
			t.Fatalf("endpoint origin/path-style not preserved")
		}
	}
}
