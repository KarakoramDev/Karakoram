// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestElasticsearchDirectoryPreservesHistoricalBindingMapping(t *testing.T) {
	for _, test := range []struct{ prefix, base string }{
		{"a//b", "a/b/default/demo/uid"},
		{"/backups///nested/", "backups/nested/default/demo/uid"},
		{"/x/y/", "x/y/default/demo/uid"},
		{"/", "default/demo/uid"},
	} {
		storage := domain.BackupStorage{Prefix: test.prefix}
		directory, err := ElasticsearchDirectory(storage, "default", "demo", "uid", test.base)
		if err != nil || directory != test.base+"/" {
			t.Fatal(test.prefix, directory, err)
		}
		for _, base := range []string{test.base + "-other", test.base + "/..", "default/demo"} {
			if _, err := ElasticsearchDirectory(storage, "default", "demo", "uid", base); err == nil {
				t.Fatal("accepted another ownership scope", test.prefix, base)
			}
		}
	}
	for _, prefix := range []string{"a/./b", "a/../b", "a\\b"} {
		base := domain.ElasticsearchObjectPrefix(prefix, "default/demo/uid")
		if _, err := ElasticsearchDirectory(domain.BackupStorage{Prefix: prefix}, "default", "demo", "uid", base); err == nil {
			t.Fatal("unsafe historical namespace became deletable", prefix)
		}
	}
}
