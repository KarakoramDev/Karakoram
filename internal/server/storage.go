// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net/url"
	"strings"
	"time"
)

func storageAPI(b domain.BackupStorage) api.BackupStorage {
	configured := b.Credentials.AccessKeyID != "" && b.Credentials.SecretAccessKey != ""
	out := api.BackupStorage{Namespace: ptr(domain.NormalizedNamespace(b.Namespace)), Name: b.Name, Type: api.BackupStorageType(b.Type), EndpointURL: b.EndpointURL, Region: b.Region, Bucket: b.Bucket, Prefix: &b.Prefix, ForcePathStyle: b.ForcePathStyle, VerifyTLS: b.VerifyTLS, Description: &b.Description, CredentialsConfigured: &configured}
	if v := b.Verification; v != nil {
		out.Verification = &api.StorageVerification{Verified: v.Verified, CheckedAt: v.CheckedAt, Message: v.Message}
		if v.FailureReason != "" {
			out.Verification.FailureReason = ptr(api.StorageVerificationFailureReason(v.FailureReason))
		}
	}
	return out
}
func credentialsDomain(c api.S3Credentials) domain.StorageCredentials {
	return domain.StorageCredentials{AccessKeyID: value(c.AccessKeyId), SecretAccessKey: value(c.SecretAccessKey)}
}
func validateStorage(b domain.BackupStorage, create bool) error {
	if create {
		if err := store.ValidateBackupStorageName(b.Name); err != nil {
			return err
		}
	}
	for _, field := range []struct{ name, value string }{
		{"type", b.Type}, {"endpointURL", b.EndpointURL}, {"region", b.Region}, {"bucket", b.Bucket},
		{"credentials.accessKeyId", b.Credentials.AccessKeyID}, {"credentials.secretAccessKey", b.Credentials.SecretAccessKey},
	} {
		if strings.TrimSpace(field.value) == "" {
			return domain.InvalidField(field.name, domain.ReasonRequired, "%s is required", field.name)
		}
	}
	if b.Type != "s3" {
		return domain.InvalidField("type", domain.ReasonUnsupported, "type must be s3")
	}
	u, err := url.Parse(b.EndpointURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return domain.InvalidField("endpointURL", domain.ReasonInvalid, "endpointURL must be an HTTP(S) URL without credentials, query or fragment")
	}
	return nil
}
func (s *Server) storage(ctx context.Context, name string, in input) response {
	namespace := domain.NormalizedNamespace(namespaceFilter(ctx))
	switch name {
	case "ListBackupStorages":
		items, err := s.Store.ListBackupStorages(ctx, namespaceFilter(ctx))
		if err != nil {
			return s.failure(err)
		}
		out := make([]api.BackupStorage, 0, len(items))
		for _, b := range items {
			if !s.allowed(ctx, "backup-storages", "read", clusterObject(b.Namespace, b.Name)) {
				continue
			}
			full, err := s.Store.GetBackupStorage(ctx, b.Namespace, b.Name)
			if err != nil {
				return s.failure(err)
			}
			out = append(out, storageAPI(*full))
		}
		return listResponse(s, ctx, out, nil)
	case "CreateBackupStorage":
		b := in.Body.(*api.CreateBackupStorageRequest)
		if failure := s.usableNamespace(namespace); failure != nil {
			return *failure
		}
		v := domain.BackupStorage{Namespace: namespace, Name: b.Name, Type: string(b.Type), EndpointURL: b.EndpointURL, Region: b.Region, Bucket: b.Bucket, Prefix: value(b.Prefix), ForcePathStyle: b.ForcePathStyle == nil || *b.ForcePathStyle, VerifyTLS: b.VerifyTLS == nil || *b.VerifyTLS, Description: value(b.Description), Credentials: credentialsDomain(b.Credentials)}
		if err := validateStorage(v, true); err != nil {
			return s.failure(err)
		}
		if err := s.Store.CreateBackupStorage(ctx, &v); err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: storageAPI(v), noStore: true}
	}
	b, err := s.Store.GetBackupStorage(ctx, namespace, in.Storage)
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
		if err := validateStorage(*b, false); err != nil {
			return s.failure(err)
		}
		if err = s.Store.UpdateBackupStorage(ctx, b); err != nil {
			return s.failure(err)
		}
		return response{status: 200, body: storageAPI(*b), noStore: true}
	case "DeleteBackupStorage":
		clusters, err := s.Store.ListClusters(ctx, namespace)
		if err != nil {
			return s.failure(err)
		}
		for _, c := range clusters {
			if c.Spec.Backup.Storage == b.Name {
				return s.failure(domain.Conflict("backupStorage", b.Name, domain.ReasonInvalidState, store.ErrConflict, "backupStorage %q is used by cluster %q", b.Name, c.Name))
			}
			for _, schedule := range c.Spec.Backup.Schedules {
				if schedule.Storage == b.Name {
					return s.failure(domain.Conflict("backupStorage", b.Name, domain.ReasonInvalidState, store.ErrConflict, "backupStorage %q is used by a backup schedule on cluster %q", b.Name, c.Name))
				}
			}
		}
		backups, err := s.Store.ListBackups(ctx, domain.ClusterRef{Namespace: namespace})
		if err != nil {
			return s.failure(err)
		}
		for _, backup := range backups {
			if backup.Storage == b.Name {
				return s.failure(domain.Conflict("backupStorage", b.Name, domain.ReasonInvalidState, store.ErrConflict, "backupStorage %q contains backup %q on cluster %q; delete retained backups using their original cluster name before removing the storage", b.Name, backup.Name, backup.Cluster))
			}
		}
		if err = s.Store.DeleteBackupStorage(ctx, namespace, b.Name); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "VerifyBackupStorage":
		err := backupstorage.Verify(ctx, *b, b.Credentials)
		reason, message := backupstorage.Failure(err)
		result := api.StorageVerification{Verified: err == nil, CheckedAt: time.Now().UTC(), Message: message}
		if reason != "" {
			category := api.StorageVerificationFailureReason(reason)
			result.FailureReason = &category
		}
		b.Verification = &domain.StorageVerification{Verified: result.Verified, CheckedAt: result.CheckedAt, Message: result.Message, FailureReason: reason}
		if err := s.Store.SaveBackupStorageVerification(ctx, b); err != nil {
			return s.failure(err)
		}
		return ok(result)
	}
	return fail(500, "internal_error", "Unknown storage operation")
}
