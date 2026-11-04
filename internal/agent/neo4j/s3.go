// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"errors"
	"path"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/minio/minio-go/v7"
)

// Tests may replace this factory before starting an agent; production always
// enforces the shared dial-time endpoint policy, including loopback denial.
var newJobS3 = func(s neo4jspec.S3) (*minio.Client, func(), error) {
	return backupstorage.NewClientWithAllowlist(domain.BackupStorage{Type: "s3", EndpointURL: s.Endpoint, Bucket: s.Bucket, Region: s.Region, ForcePathStyle: s.PathStyle, VerifyTLS: s.VerifyTLS}, domain.StorageCredentials{AccessKeyID: s.AccessKey, SecretAccessKey: s.SecretKey}, s.EndpointAllowlist)
}

func validateObjectPrefix(prefix string) error {
	if prefix == "" || strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix || strings.ContainsAny(prefix, "\\\x00\r\n") {
		return errors.New("invalid object prefix")
	}
	return nil
}
