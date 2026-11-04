// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/minio/minio-go/v7"
)

var postgresLabel = regexp.MustCompile(`^[0-9]{8}-[0-9]{6}F(_[0-9]{8}-[0-9]{6}[DI])?$`)
var postgresExpireMu sync.Mutex
var postgresWAL = regexp.MustCompile(`^[0-9A-F]{24}(-[0-9a-f]{40})?(\.[a-z0-9]+)?$`)
var postgresArchiveID = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?-[1-9][0-9]*$`)
var postgresGroupDirectory = regexp.MustCompile(`^g[0-8](-i[1-9][0-9]*)?$`)

// PostgresDirectory bounds deletion by immutable incarnation provenance, never
// just a cluster name or a mutable registered-storage prefix.
func PostgresDirectory(s domain.BackupStorage, namespace, cluster, repository string) (string, error) {
	ns := domain.NormalizedNamespace(namespace)
	if domain.NormalizedNamespace(s.Namespace) != ns {
		return "", errors.New("repository belongs to another storage namespace")
	}
	// The repository recorded on the backup is its immutable provenance.
	// Registered storage prefixes can change after that repository was created.
	parts := strings.Split(repository, "/")
	ownership := parts
	if len(parts) > 0 && postgresGroupDirectory.MatchString(parts[len(parts)-1]) {
		ownership = parts[:len(parts)-1]
	}
	if len(ownership) < 3 || ownership[len(ownership)-3] != "postgres" || ownership[len(ownership)-2] != ns {
		return "", errors.New("recorded PostgreSQL repository is outside its storage namespace")
	}
	for _, part := range strings.Split(repository, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
			return "", errors.New("unsafe PostgreSQL repository namespace")
		}
	}
	identity := ownership[len(ownership)-1]
	if cluster == "" || strings.ContainsAny(cluster, "/\\") || !strings.HasPrefix(identity, cluster+"-") || strings.Contains(identity, "/") || len(identity) <= len(cluster)+1 {
		return "", errors.New("repository lacks a cluster incarnation identity")
	}
	return repository + "/", nil
}

// DeletePostgresRepository is called only after every writer has stopped.
func DeletePostgresRepository(ctx context.Context, s domain.BackupStorage, namespace, cluster, repository string) error {
	directory, err := PostgresDirectory(s, namespace, cluster, repository)
	if err != nil {
		return err
	}
	pass, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := NewClient(s, s.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	progress, err := deleteStoredObjects(pass, client, s.Bucket, directory, false)
	if progress && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return ErrDeletionInProgress
	}
	return err
}

type infoEntry struct{ section, key, value string }

func parsePostgresInfo(data []byte) ([]infoEntry, error) {
	var entries []infoEntry
	section := ""
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line[1 : len(line)-1]
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || section == "" || key == "" || !json.Valid([]byte(value)) {
			return nil, errors.New("invalid pgBackRest info file")
		}
		entries = append(entries, infoEntry{section, key, value})
	}
	expected := ""
	for _, entry := range entries {
		if entry.section == "backrest" && entry.key == "backrest-checksum" {
			if err := json.Unmarshal([]byte(entry.value), &expected); err != nil {
				return nil, err
			}
		}
	}
	if expected == "" || expected != postgresInfoChecksum(entries) {
		return nil, errors.New("pgBackRest info checksum mismatch")
	}
	return entries, nil
}

// pgBackRest hashes the section/key stream as JSON, preserving raw JSON values
// and input order, excluding only backrest-checksum (upstream src/info/info.c).
func postgresInfoChecksum(entries []infoEntry) string {
	var canonical strings.Builder
	canonical.WriteByte('{')
	section := ""
	for _, entry := range entries {
		if entry.section == "backrest" && entry.key == "backrest-checksum" {
			continue
		}
		if section != entry.section {
			if section != "" {
				canonical.WriteString("},")
			}
			b, _ := json.Marshal(entry.section)
			canonical.Write(b)
			canonical.WriteString(":{")
			section = entry.section
		} else {
			canonical.WriteByte(',')
		}
		key, _ := json.Marshal(entry.key)
		canonical.Write(key)
		canonical.WriteByte(':')
		canonical.WriteString(entry.value)
	}
	canonical.WriteString("}}")
	sum := sha1.Sum([]byte(canonical.String()))
	return hex.EncodeToString(sum[:])
}
func renderPostgresInfo(entries []infoEntry) []byte {
	checksum := postgresInfoChecksum(entries)
	var out strings.Builder
	section := ""
	for _, entry := range entries {
		if entry.section != section {
			if section != "" {
				out.WriteByte('\n')
			}
			fmt.Fprintf(&out, "[%s]\n", entry.section)
			section = entry.section
		}
		value := entry.value
		if entry.section == "backrest" && entry.key == "backrest-checksum" {
			encoded, _ := json.Marshal(checksum)
			value = string(encoded)
		}
		fmt.Fprintf(&out, "%s=%s\n", entry.key, value)
	}
	return []byte(out.String())
}

type postgresInfoBackup struct {
	Prior        string   `json:"backup-prior"`
	References   []string `json:"backup-reference"`
	ArchiveStart string   `json:"backup-archive-start"`
	DatabaseID   int      `json:"db-id"`
}
type postgresExpirePlan struct {
	Removed   []string       `json:"removed"`
	Info      []byte         `json:"info"`
	Oldest    map[int]string `json:"oldest"`
	Surviving map[int]bool   `json:"surviving"`
	Empty     bool           `json:"empty"`
}

func planPostgresExpire(data []byte, labels ...string) (postgresExpirePlan, error) {
	entries, err := parsePostgresInfo(data)
	if err != nil {
		return postgresExpirePlan{}, err
	}
	backups := map[string]postgresInfoBackup{}
	for _, entry := range entries {
		if entry.section == "backup:current" {
			var b postgresInfoBackup
			if err = json.Unmarshal([]byte(entry.value), &b); err != nil {
				return postgresExpirePlan{}, err
			}
			backups[entry.key] = b
		}
	}
	removed := make(map[string]bool, len(labels))
	for _, label := range labels {
		if !postgresLabel.MatchString(label) {
			return postgresExpirePlan{}, errors.New("invalid expiration plan label")
		}
		removed[label] = true
	}
	for changed := true; changed; {
		changed = false
		for name, b := range backups {
			if removed[name] {
				continue
			}
			dependent := removed[b.Prior]
			for _, ref := range b.References {
				dependent = dependent || removed[ref]
			}
			if dependent {
				removed[name] = true
				changed = true
			}
		}
	}
	plan := postgresExpirePlan{Oldest: map[int]string{}, Surviving: map[int]bool{}}
	kept := make([]infoEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.section == "backup:current" && removed[entry.key] {
			plan.Removed = append(plan.Removed, entry.key)
			continue
		}
		kept = append(kept, entry)
	}
	count := 0
	for name, b := range backups {
		if removed[name] {
			continue
		}
		count++
		plan.Surviving[b.DatabaseID] = true
		if b.ArchiveStart != "" && (plan.Oldest[b.DatabaseID] == "" || b.ArchiveStart < plan.Oldest[b.DatabaseID]) {
			plan.Oldest[b.DatabaseID] = b.ArchiveStart
		}
	}
	// Also remove an already absent set's remnants on an idempotent retry.
	for label := range removed {
		if _, exists := backups[label]; !exists {
			plan.Removed = append(plan.Removed, label)
		}
	}
	plan.Info = renderPostgresInfo(kept)
	plan.Empty = count == 0
	return plan, nil
}
func getPostgresObject(ctx context.Context, client *minio.Client, bucket, key string) ([]byte, error) {
	object, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, 16<<20+1))
	if len(data) > 16<<20 {
		return nil, errors.New("pgBackRest metadata exceeds supported size")
	}
	return data, err
}
func putPostgresObject(ctx context.Context, client *minio.Client, bucket, key string, data []byte) error {
	_, err := client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true})
	return err
}

// DeletePostgresBackup performs retained expiration without a PostgreSQL host or
// native executable on the control plane. A repository-local plan makes partial
// metadata/object deletion resumable. Callers must establish no live writers.
func DeletePostgresBackup(ctx context.Context, s domain.BackupStorage, b domain.Backup) error {
	return deletePostgresBackup(ctx, s, b, nil)
}

func deletePostgresBackup(ctx context.Context, s domain.BackupStorage, b domain.Backup, transport http.RoundTripper) error {
	postgresExpireMu.Lock()
	defer postgresExpireMu.Unlock()
	if b.Postgres == nil || b.Storage != s.Name || !postgresLabel.MatchString(b.Postgres.Label) {
		return errors.New("backup lacks pgBackRest snapshot provenance")
	}
	directory, err := PostgresDirectory(s, b.Namespace, b.Cluster, b.Postgres.Repository)
	if err != nil {
		return err
	}
	pass, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := newClient(s, s.Credentials, transport)
	if err != nil {
		return err
	}
	defer closeClient()
	planKey := directory + ".karakoram-expire/" + b.Postgres.Label + ".json"
	data, err := getPostgresObject(pass, client, s.Bucket, planKey)
	var plan postgresExpirePlan
	if err == nil {
		if err = json.Unmarshal(data, &plan); err != nil {
			return errors.New("invalid retained expiration plan")
		}
	} else if minio.ToErrorResponse(err).Code != "NoSuchKey" && minio.ToErrorResponse(err).Code != "NoSuchObject" {
		return err
	}
	// A per-label plan may predate a completed expiration of another label.
	// Rebase its removal closure onto current inventory, never replay its saved
	// replacement info file: that would resurrect the other plan's deleted sets.
	data, err = getPostgresObject(pass, client, s.Bucket, directory+"backup/kk/backup.info")
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		data, err = getPostgresObject(pass, client, s.Bucket, directory+"backup/kk/backup.info.copy")
		if minio.ToErrorResponse(err).Code == "NoSuchKey" && !plan.Empty {
			for object := range client.ListObjectsIter(pass, s.Bucket, minio.ListObjectsOptions{Prefix: directory, Recursive: true, MaxKeys: 1}) {
				if object.Err != nil {
					return object.Err
				}
				return errors.New("repository objects remain but pgBackRest inventory is unavailable; preserve the backup record and repair metadata")
			}
			return nil
		}
	}
	if err != nil {
		// Last-set deletion can already have removed both inventory copies.
		// Its durable empty plan still owns removal of the repository remnants.
		if !plan.Empty || minio.ToErrorResponse(err).Code != "NoSuchKey" {
			return err
		}
	} else {
		labels := append(plan.Removed, b.Postgres.Label)
		plan, err = planPostgresExpire(data, labels...)
		if err != nil {
			return err
		}
		data, err = json.Marshal(plan)
		if err != nil {
			return err
		}
		if err = putPostgresObject(pass, client, s.Bucket, planKey, data); err != nil {
			return err
		}
	}
	if plan.Empty {
		// Keep the durable plan until all data and both metadata copies are
		// absent. Removing it with the first bulk page would make a partial
		// last-set deletion impossible to resume after a process restart.
		for _, prefix := range []string{"archive/", "backup/"} {
			if _, err = deleteStoredObjects(pass, client, s.Bucket, directory+prefix, false); err != nil {
				return err
			}
		}
		for object := range client.ListObjectsIter(pass, s.Bucket, minio.ListObjectsOptions{Prefix: directory, Recursive: true, WithVersions: true}) {
			if object.Err != nil {
				return object.Err
			}
			if !strings.HasPrefix(object.Key, directory) {
				return errors.New("repository listing escaped recorded provenance")
			}
			if object.Key == planKey {
				continue
			}
			if _, err = deleteStoredObjects(pass, client, s.Bucket, object.Key, true); err != nil {
				return err
			}
		}
		_, err = deleteStoredObjects(pass, client, s.Bucket, planKey, true)
		return err
	}
	// Publish identical checksummed copies before destroying files. A crash may
	// leave an old copy, but never one that refers to already destroyed sets.
	for _, file := range []string{"backup.info", "backup.info.copy"} {
		if err = putPostgresObject(pass, client, s.Bucket, directory+"backup/kk/"+file, plan.Info); err != nil {
			return err
		}
	}
	for _, label := range plan.Removed {
		if !postgresLabel.MatchString(label) {
			return errors.New("invalid expiration plan label")
		}
		if _, err = deleteStoredObjects(pass, client, s.Bucket, directory+"backup/kk/"+label+"/", false); err != nil {
			return err
		}
	}
	archivePrefix := directory + "archive/kk/"
	prunedHistories := map[string]bool{}
	for object := range client.ListObjectsIter(pass, s.Bucket, minio.ListObjectsOptions{Prefix: archivePrefix, Recursive: true}) {
		if object.Err != nil {
			return object.Err
		}
		if !strings.HasPrefix(object.Key, archivePrefix) {
			return errors.New("archive listing escaped repository")
		}
		relative := strings.TrimPrefix(object.Key, archivePrefix)
		parts := strings.Split(relative, "/")
		if len(parts) < 2 || !postgresArchiveID.MatchString(parts[0]) {
			continue
		}
		_, idText, _ := strings.Cut(parts[0], "-")
		id, err := strconv.Atoi(idText)
		if err != nil {
			continue
		}
		if !plan.Surviving[id] {
			if !prunedHistories[parts[0]] {
				if _, err = deleteStoredObjects(pass, client, s.Bucket, archivePrefix+parts[0]+"/", false); err != nil {
					return err
				}
				prunedHistories[parts[0]] = true
			}
			continue
		}
		if len(parts) != 3 || !postgresWAL.MatchString(parts[2]) {
			continue
		}
		oldest := plan.Oldest[id]
		if oldest != "" && parts[2][:24] < oldest {
			if _, err = deleteStoredObjects(pass, client, s.Bucket, object.Key, true); err != nil {
				return err
			}
		}
	}
	_, err = deleteStoredObjects(pass, client, s.Bucket, planKey, true)
	return err
}
