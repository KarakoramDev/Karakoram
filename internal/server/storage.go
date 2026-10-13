// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"net/url"
	"time"
)

func storageAPI(b domain.BackupStorage) api.BackupStorage {
	configured := b.Credentials.AccessKeyID != "" && b.Credentials.SecretAccessKey != ""
	return api.BackupStorage{Name: b.Name, Type: api.BackupStorageType(b.Type), EndpointURL: b.EndpointURL, Region: b.Region, Bucket: b.Bucket, Prefix: &b.Prefix, ForcePathStyle: b.ForcePathStyle, VerifyTLS: b.VerifyTLS, Description: &b.Description, CredentialsConfigured: &configured}
}
func credentialsDomain(c api.S3Credentials) domain.StorageCredentials {
	return domain.StorageCredentials{AccessKeyID: value(c.AccessKeyId), SecretAccessKey: value(c.SecretAccessKey)}
}
func validStorage(b domain.BackupStorage) bool {
	u, err := url.Parse(b.EndpointURL)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && b.Name != "" && b.Type == "s3" && b.Region != "" && b.Bucket != "" && b.Credentials.AccessKeyID != "" && b.Credentials.SecretAccessKey != ""
}
func (s *Server) storage(ctx context.Context, name string, in input) response {
	switch name {
	case "ListBackupStorages":
		items, err := s.Store.ListBackupStorages(ctx)
		if err != nil {
			return s.failure(err)
		}
		out := make([]api.BackupStorage, 0, len(items))
		for _, b := range items {
			full, err := s.Store.GetBackupStorage(ctx, b.Name)
			if err != nil {
				return s.failure(err)
			}
			out = append(out, storageAPI(*full))
		}
		return listResponse(s, ctx, out, nil)
	case "CreateBackupStorage":
		b := in.Body.(*api.CreateBackupStorageRequest)
		v := domain.BackupStorage{Name: b.Name, Type: string(b.Type), EndpointURL: b.EndpointURL, Region: b.Region, Bucket: b.Bucket, Prefix: value(b.Prefix), ForcePathStyle: b.ForcePathStyle == nil || *b.ForcePathStyle, VerifyTLS: b.VerifyTLS == nil || *b.VerifyTLS, Description: value(b.Description), Credentials: credentialsDomain(b.Credentials)}
		if !validStorage(v) {
			return invalid()
		}
		if err := s.Store.CreateBackupStorage(ctx, &v); err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: storageAPI(v), noStore: true}
	}
	b, err := s.Store.GetBackupStorage(ctx, in.Storage)
	if err != nil {
		return s.failure(err)
	}
	switch name {
	case "GetBackupStorage":
		return ok(storageAPI(*b))
	case "ReplaceBackupStorage":
		v := in.Body.(*api.UpdateBackupStorageRequest)
		b.Type = string(v.Type)
		b.EndpointURL = v.EndpointURL
		b.Region = v.Region
		b.Bucket = v.Bucket
		b.Prefix = value(v.Prefix)
		b.ForcePathStyle = v.ForcePathStyle
		b.VerifyTLS = v.VerifyTLS
		b.Description = value(v.Description)
		if v.Credentials != nil {
			if v.Credentials.AccessKeyId != nil {
				b.Credentials.AccessKeyID = *v.Credentials.AccessKeyId
			}
			if v.Credentials.SecretAccessKey != nil {
				b.Credentials.SecretAccessKey = *v.Credentials.SecretAccessKey
			}
		}
		if !validStorage(*b) {
			return invalid()
		}
		if err = s.Store.UpdateBackupStorage(ctx, b); err != nil {
			return s.failure(err)
		}
		return response{status: 200, body: storageAPI(*b), noStore: true}
	case "DeleteBackupStorage":
		clusters, err := s.Store.ListClusters(ctx)
		if err != nil {
			return s.failure(err)
		}
		for _, c := range clusters {
			if c.Spec.Backup.Storage == b.Name {
				return fail(409, "conflict", "Storage is used by a cluster")
			}
			for _, schedule := range c.Spec.Backup.Schedules {
				if schedule.Storage == b.Name {
					return fail(409, "conflict", "Storage is used by a backup schedule")
				}
			}
		}
		backups, err := s.Store.ListBackups(ctx, "*")
		if err != nil {
			return s.failure(err)
		}
		for _, backup := range backups {
			if backup.Storage == b.Name {
				return fail(409, "conflict", "Storage contains a backup record; delete retained backups using their original cluster name before removing the storage")
			}
		}
		if err = s.Store.DeleteBackupStorage(ctx, b.Name); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "VerifyBackupStorage":
		err := backupstorage.Verify(ctx, *b, b.Credentials)
		message := "Read, write and delete access verified"
		if err != nil {
			message = "Storage verification failed"
		}
		return ok(api.StorageVerification{Verified: err == nil, CheckedAt: time.Now().UTC(), Message: message})
	}
	return fail(500, "internal_error", "Unknown storage operation")
}
