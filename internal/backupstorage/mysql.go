// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package backupstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

// MySQLDirectory validates recorded provenance independently of a mutable storage prefix.
func MySQLDirectory(s domain.BackupStorage, provider, namespace, cluster, repository string) (string, error) {
	ns := domain.NormalizedNamespace(namespace)
	if domain.NormalizedNamespace(s.Namespace) != ns {
		return "", errors.New("MySQL repository belongs to another storage namespace")
	}
	parts := strings.Split(repository, "/")
	if (provider != "mysql" && provider != "pxc") || len(parts) < 3 || parts[len(parts)-3] != provider || parts[len(parts)-2] != ns {
		return "", errors.New("recorded MySQL repository is outside its storage namespace")
	}
	for _, p := range parts {
		if p == "" || p == "." || p == ".." || strings.Contains(p, "\\") {
			return "", errors.New("unsafe MySQL repository")
		}
	}
	identity := parts[len(parts)-1]
	if cluster == "" || strings.ContainsAny(cluster, "/\\") || !strings.HasPrefix(identity, cluster+"-") || len(identity) <= len(cluster)+1 {
		return "", errors.New("MySQL repository lacks cluster incarnation")
	}
	return repository + "/", nil
}

// ValidateMySQLBackup bounds every object path by the immutable incarnation.
func ValidateMySQLBackup(s domain.BackupStorage, b domain.Backup) error {
	m := b.MySQL
	if (b.Provider != "mysql" && b.Provider != "pxc") || m == nil || m.Label == "" || m.Label == "." || m.Label == ".." || m.ClusterUID == "" || strings.ContainsAny(m.Label, "/\\\x00\r\n") || m.Generation < 0 {
		return errors.New("backup lacks immutable MySQL provenance")
	}
	if _, err := MySQLDirectory(s, b.Provider, b.Namespace, b.Cluster, m.Repository); err != nil {
		return err
	}
	identity := b.Cluster + "-" + m.ClusterUID
	if m.Generation > 0 {
		identity += fmt.Sprintf("-g%d", m.Generation)
	}
	if path.Base(m.Repository) != identity {
		return errors.New("MySQL repository incarnation mismatch")
	}
	if len(m.ObjectPaths) != 1 || m.ObjectPaths[0] != m.Repository+"/base/"+m.Label {
		return errors.New("MySQL backup objects are outside their recorded set")
	}
	return nil
}

// MySQLDeletionClosure includes all incrementals depending transitively on the selected set.
func MySQLDeletionClosure(backups []domain.Backup, b domain.Backup) []domain.Backup {
	if b.MySQL == nil {
		return nil
	}
	removed := map[string]bool{b.MySQL.Label: true}
	for changed := true; changed; {
		changed = false
		for _, item := range backups {
			m := item.MySQL
			if m == nil || item.Provider != b.Provider || item.Storage != b.Storage || m.Repository != b.MySQL.Repository || m.ClusterUID != b.MySQL.ClusterUID {
				continue
			}
			if !removed[m.Label] && removed[m.ParentLabel] {
				removed[m.Label] = true
				changed = true
			}
		}
	}
	result := []domain.Backup{}
	found := false
	for _, item := range backups {
		if item.MySQL != nil && item.Provider == b.Provider && item.Storage == b.Storage && item.MySQL.Repository == b.MySQL.Repository && removed[item.MySQL.Label] {
			result = append(result, item)
			found = found || item.Name == b.Name
		}
	}
	if !found {
		result = append(result, b)
	}
	return result
}
func DeleteMySQLRepository(ctx context.Context, s domain.BackupStorage, provider, namespace, cluster, repository string) error {
	directory, err := MySQLDirectory(s, provider, namespace, cluster, repository)
	if err != nil {
		return err
	}
	client, closeClient, err := NewClient(s, s.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	pass, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	progress, err := deleteStoredObjects(pass, client, s.Bucket, directory, false)
	if progress && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return ErrDeletionInProgress
	}
	return err
}

// DeleteMySQLBackup removes a chain closure and binlogs no longer needed by any
// surviving base. The last base retires the complete shared binlog namespace.
func DeleteMySQLBackup(ctx context.Context, s domain.BackupStorage, b domain.Backup, backups []domain.Backup) error {
	if err := ValidateMySQLBackup(s, b); err != nil {
		return err
	}
	removed := MySQLDeletionClosure(backups, b)
	for _, item := range removed {
		if err := ValidateMySQLBackup(s, item); err != nil {
			return err
		}
	}
	client, closeClient, err := NewClient(s, s.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	pass, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for _, item := range removed {
		for _, prefix := range []string{item.MySQL.ObjectPaths[0] + "/", item.MySQL.ObjectPaths[0] + ".meta/"} {
			progress, e := deleteStoredObjects(pass, client, s.Bucket, prefix, false)
			if progress && ctx.Err() == nil && errors.Is(e, context.DeadlineExceeded) {
				return ErrDeletionInProgress
			}
			if e != nil {
				return e
			}
		}
	}
	var surviving []domain.MySQLBackup
	for _, item := range backups {
		if item.MySQL == nil || item.Provider != b.Provider || item.Storage != b.Storage || item.MySQL.Repository != b.MySQL.Repository {
			continue
		}
		deleted := false
		for _, r := range removed {
			deleted = deleted || item.MySQL.Label == r.MySQL.Label
		}
		if !deleted && item.Phase == domain.PhaseSucceeded {
			surviving = append(surviving, *item.MySQL)
		}
	}
	if len(surviving) == 0 {
		_, err = deleteStoredObjects(pass, client, s.Bucket, b.MySQL.Repository+"/binlogs/", false)
	} else {
		err = pruneMySQLBinlogs(pass, client, s.Bucket, b.MySQL.Repository+"/binlogs/", surviving)
	}
	return err
}

// A binlog is unnecessary only if every remaining backup already contains all
// of its transactions. Wall-clock backup completion alone is not sufficient:
// XtraBackup's consistent snapshot can precede its completion timestamp.
func pruneMySQLBinlogs(ctx context.Context, client *minio.Client, bucket, prefix string, surviving []domain.MySQLBackup) error {
	for object := range client.ListObjectsIter(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return object.Err
		}
		if !strings.HasPrefix(object.Key, prefix) {
			return errors.New("S3 returned a binlog outside the selected MySQL repository")
		}
		if !strings.HasSuffix(object.Key, ".json") {
			continue
		}
		reader, err := client.GetObject(ctx, bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, 16<<20+1))
		closeErr := reader.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if len(raw) > 16<<20 {
			return errors.New("MySQL binlog sidecar exceeds supported size")
		}
		var sidecar struct {
			GTIDSet *string `json:"gtid_set"`
		}
		if err = json.Unmarshal(raw, &sidecar); err != nil {
			return err
		}
		if sidecar.GTIDSet == nil {
			return errors.New("MySQL binlog sidecar lacks transaction provenance")
		}
		redundant := true
		for _, backup := range surviving {
			covered, e := myspec.GTIDContains(backup.GTIDSet, *sidecar.GTIDSet)
			if e != nil {
				return e
			}
			if !covered {
				redundant = false
				break
			}
		}
		if !redundant {
			continue
		}
		for _, key := range []string{strings.TrimSuffix(object.Key, ".json"), object.Key} {
			if _, err = deleteStoredObjects(ctx, client, bucket, key, true); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
