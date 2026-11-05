// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nativeartifacts

import "testing"

func TestWeaviateOfficialArtifacts(t *testing.T) {
	for name, version := range map[string]string{"weaviate-1.39": "1.39.15", "weaviate-1.40": "1.40.3", "weaviate-1.39.12": "1.39.12", "weaviate-1.40.1": "1.40.1"} {
		a, err := Get(name)
		if err != nil {
			t.Fatal(err)
		}
		file := "weaviate-v" + version + "-linux-amd64.tar.gz"
		if a.Version != version || a.File != file || a.UpstreamURL != "https://github.com/weaviate/weaviate/releases/download/v"+version+"/"+file || a.License != "BSD-3-Clause AND LicenseRef-Weaviate-Enterprise" || a.Build != "upstream-release" {
			t.Fatalf("incorrect official Weaviate artifact: %+v", a)
		}
	}
}
