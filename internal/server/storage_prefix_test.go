// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStoragePrefixValidationOnCreateAndReplace(t *testing.T) {
	for _, create := range []bool{true, false} {
		for _, test := range []struct {
			prefix string
			valid  bool
		}{
			{"/x/y", true}, {"x/y/", true}, {"/x/y/", true}, {"/", true},
			{"a//b", false}, {"..", false}, {"a/../b", false}, {"a/./b", false}, {"a\\b", false},
		} {
			storage := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test", Region: "region", Bucket: "objects", Prefix: test.prefix, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			if err := validateStorage(storage, create); (err == nil) != test.valid {
				t.Fatalf("create=%t prefix=%q error=%v", create, test.prefix, err)
			}
		}
	}
}
