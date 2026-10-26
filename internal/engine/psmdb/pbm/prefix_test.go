// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestWriterPrefixMatchesClusterDeletion(t *testing.T) {
	for _, prefix := range []string{"/x/y", "x/y/", "/x/y/", "/", ""} {
		t.Run(prefix, func(t *testing.T) {
			env, _, _, _ := setup(t)
			env.Storage.Prefix = prefix
			// Include the old persisted form, before writers normalized boundary slashes.
			env.Prefix = strings.TrimRight(prefix, "/") + "/karakoram/demo-uid"
			raw, err := RenderConfig(env)
			if err != nil {
				t.Fatal(err)
			}
			var config struct {
				Storage struct {
					S3 struct {
						Prefix string `yaml:"prefix"`
					} `yaml:"s3"`
				} `yaml:"storage"`
			}
			if err := yaml.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
			directory, err := backupstorage.ClusterDirectory(env.Storage, "demo", "uid")
			if err != nil || directory != config.Storage.S3.Prefix+"/" {
				t.Fatalf("delete = %q, writer = %q, error = %v", directory, config.Storage.S3.Prefix, err)
			}
			backup := domain.Backup{Storage: env.Storage.Name, Namespace: env.Storage.Namespace, PBMPrefix: env.Prefix, PBMName: "snapshot"}
			data, metadata, err := backupstorage.SnapshotKeys(env.Storage, backup)
			if err != nil || data != directory+"snapshot/" || metadata != directory+"snapshot.pbm.json" {
				t.Fatal(data, metadata, err)
			}
		})
	}
}
