// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

const physicalManifestName = "physical-manifest.json"

func createBackupManifest(ctx context.Context, client *minio.Client, source myspec.Repository, manifest myspec.BackupObjectManifest) (string, int64, error) {
	if err := manifest.Validate(); err != nil {
		return "", 0, err
	}
	if err := verifyPhysicalObjectSet(ctx, client, source, manifest); err != nil {
		return "", 0, err
	}
	for i, expected := range manifest.Objects {
		object, err := client.GetObject(ctx, source.S3.Bucket, expected.Key, minio.GetObjectOptions{})
		if err != nil {
			return "", 0, err
		}
		info, err := object.Stat()
		if err != nil {
			object.Close()
			return "", 0, err
		}
		hash := sha256.New()
		size, err := io.Copy(hash, object)
		object.Close()
		if err != nil || size != expected.Size || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
			return "", 0, errors.New("uploaded_physical_bytes_differ_from_source")
		}
		manifest.Objects[i].VersionID = info.VersionID
	}
	if err := verifyPhysicalObjectSet(ctx, client, source, manifest); err != nil {
		return "", 0, err
	}
	raw, digest, err := manifest.Canonical()
	if err != nil {
		return "", 0, err
	}
	_, err = client.PutObject(ctx, source.S3.Bucket, metadataObject(manifest.ObjectPrefix, physicalManifestName), bytes.NewReader(raw), int64(len(raw)), minio.PutObjectOptions{ContentType: "application/json"})
	return digest, int64(len(raw)), err
}

func readBackupManifest(ctx context.Context, client *minio.Client, source myspec.Repository, ref myspec.BackupManifestRef) (myspec.BackupObjectManifest, error) {
	var manifest myspec.BackupObjectManifest
	if !myspec.ValidDigest(ref.Digest) || ref.Size <= 0 || ref.Size > int64(int(^uint(0)>>1)-1) {
		return manifest, errors.New("physical_manifest_provenance_missing")
	}
	object, err := client.GetObject(ctx, source.S3.Bucket, metadataObject(ref.ObjectPrefix, physicalManifestName), minio.GetObjectOptions{})
	if err != nil {
		return manifest, err
	}
	raw := make([]byte, int(ref.Size)+1)
	n, readErr := io.ReadFull(object, raw)
	object.Close()
	if n != int(ref.Size) || readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		return manifest, errors.New("physical_manifest_size_mismatch")
	}
	raw = raw[:n]
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != ref.Digest {
		return manifest, errors.New("physical_manifest_digest_mismatch")
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return manifest, err
	}
	if err = manifest.Validate(); err != nil || manifest.ObjectPrefix != ref.ObjectPrefix {
		return manifest, errors.New("physical_manifest_invalid")
	}
	return manifest, nil
}

func verifyPhysicalObjectSet(ctx context.Context, client *minio.Client, source myspec.Repository, manifest myspec.BackupObjectManifest) error {
	keys := make([]string, 0, len(manifest.Objects))
	for object := range client.ListObjects(ctx, source.S3.Bucket, minio.ListObjectsOptions{Prefix: manifest.ObjectPrefix + "/", Recursive: true}) {
		if object.Err != nil {
			return object.Err
		}
		keys = append(keys, object.Key)
	}
	sort.Strings(keys)
	if len(keys) != len(manifest.Objects) {
		return errors.New("physical_object_set_changed")
	}
	for i, key := range keys {
		if key != manifest.Objects[i].Key {
			return errors.New("physical_object_set_changed")
		}
	}
	return nil
}

// xbcloud objects contain raw XBStream chunks. Its native downloader writes the
// downloaded chunk contents directly to stdout, in per-file chunk order. Feed
// only verified local chunks to xbstream, never a second mutable S3 discovery.
func extractVerifiedBackup(ctx context.Context, s myspec.Spec, source myspec.Repository, ref myspec.BackupManifestRef, dir string, logicalFiles []string) error {
	client, err := s3Client(source)
	if err != nil {
		return err
	}
	manifest, err := readBackupManifest(ctx, client, source, ref)
	if err != nil {
		return err
	}
	if err = verifyPhysicalObjectSet(ctx, client, source, manifest); err != nil {
		return err
	}
	chunks, err := os.MkdirTemp(filepath.Dir(dir), "verified-chunks-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(chunks)
	var files []string
	found := map[string]bool{}
	for _, object := range manifest.Objects {
		if len(logicalFiles) > 0 {
			relative := strings.TrimPrefix(strings.TrimPrefix(object.Key, ref.ObjectPrefix+"/"), "./")
			logical := relative
			if dot := strings.LastIndexByte(relative, '.'); dot >= 0 {
				logical = relative[:dot]
			}
			match := false
			for _, wanted := range logicalFiles {
				if logical == wanted {
					match, found[wanted] = true, true
					break
				}
			}
			if !match {
				continue
			}
		}
		name := filepath.Join(chunks, fmt.Sprintf("%020d", len(files)))
		if err = client.FGetObject(ctx, source.S3.Bucket, object.Key, name, minio.GetObjectOptions{VersionID: object.VersionID}); err != nil {
			return err
		}
		hash, size, err := hashFile(name)
		if err != nil || hash != object.SHA256 || size != object.Size {
			return errors.New("physical_object_checksum_mismatch")
		}
		files = append(files, name)
	}
	for _, wanted := range logicalFiles {
		if !found[wanted] {
			return errors.New("physical_metadata_missing")
		}
	}
	if err = verifyPhysicalObjectSet(ctx, client, source, manifest); err != nil {
		return err
	}
	reader := &backupChunkReader{files: files}
	defer reader.Close()
	cmd := backupCommand(ctx, s, "xbstream", "-x", "-C", dir)
	cmd.Stdin = reader
	return cmd.Run()
}

type backupChunkReader struct {
	files   []string
	next    int
	current *os.File
}

func (r *backupChunkReader) Read(buffer []byte) (int, error) {
	for {
		if r.current == nil {
			if r.next == len(r.files) {
				return 0, io.EOF
			}
			file, err := os.Open(r.files[r.next])
			if err != nil {
				return 0, err
			}
			r.current = file
			r.next++
		}
		n, err := r.current.Read(buffer)
		if err == io.EOF {
			name := r.current.Name()
			closeErr := r.current.Close()
			r.current = nil
			if closeErr != nil {
				return n, closeErr
			}
			if err = os.Remove(name); err != nil {
				return n, err
			}
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (r *backupChunkReader) Close() error {
	if r.current != nil {
		err := r.current.Close()
		r.current = nil
		return err
	}
	return nil
}
