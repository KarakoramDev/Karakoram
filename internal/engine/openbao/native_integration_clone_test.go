// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package openbao

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Only infrastructure and the signed S3 endpoint are fake: agents, Raft
// snapshot export/force-import, seal transitions and authentication are native.
func TestNativeControlPlaneCloneDeletedSource(t *testing.T) {
	binary := os.Getenv("KARAKORAM_OPENBAO_BIN")
	if binary == "" {
		t.Skip("set KARAKORAM_OPENBAO_BIN to an installed OpenBao binary")
	}
	ctx := t.Context()
	s3 := &snapshotFakeS3{objects: map[string][]byte{}}
	server := httptest.NewServer(s3)
	t.Cleanup(server.Close)
	original := snapshotS3Client
	t.Cleanup(func() { snapshotS3Client = original })
	snapshotS3Client = func(s domain.BackupStorage, c domain.StorageCredentials) (*minio.Client, func(), error) {
		u, err := url.Parse(s.EndpointURL)
		if err != nil {
			return nil, nil, err
		}
		client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: server.Client().Transport})
		return client, func() {}, err
	}
	cleanup := func(h *nativeCP) {
		t.Helper()
		h.loop(t, "delete native clone fixture", func() (bool, error) {
			return h.p.Cleanup(ctx, h.rc, domain.DeleteOptions{KeepVolumes: false, KeepBackups: true})
		})
		for name, n := range h.nodes {
			if n.cancel != nil {
				t.Fatal("agent remains after deletion", name)
			}
			if _, err := os.Stat(n.dir); !os.IsNotExist(err) {
				t.Fatal("member directory remains after deletion", name, err)
			}
		}
		if len(h.services) != 0 {
			t.Fatal("native fixture services remain after deletion")
		}
		for _, key := range []string{SecretUnseal, SecretManagementToken, SecretInitKey, SecretConnections, "tls/ca.crt", "tls/ca.key"} {
			if _, err := secret(ctx, h.rc, key); !missing(err) {
				t.Fatal("cluster secret remains after deletion", key, err)
			}
		}
	}

	source := newNativeCP(t, binary)
	sourceStore := source.p.options.Store.(*store.Store)
	if err := sourceStore.DeleteCluster(ctx, source.rc.Cluster.Ref()); err != nil {
		t.Fatal(err)
	}
	source.rc.Cluster.Name = "offline"
	source.rc.Cluster.Spec.Components.Engine.Replicas = 1
	source.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"unseal":{"shares":3,"threshold":2}}`)
	if err := sourceStore.CreateCluster(ctx, source.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	source.ready(t)
	offline, err := source.p.RevealUnsealKeys(ctx, source.rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(offline.Shares) != 3 || offline.Threshold != 2 {
		t.Fatal("source did not initialize with 2-of-3 shares")
	}
	root := source.root(t)
	if err = root.Do(ctx, "POST", "sys/mounts/clone-proof", map[string]any{"type": "kv"}, nil); err != nil {
		t.Fatal(err)
	}
	if err = root.Do(ctx, "POST", "clone-proof/retained", map[string]string{"value": "recovered-from-deleted-source"}, nil); err != nil {
		t.Fatal(err)
	}
	archive := &snapshotTestStore{
		ClusterStore: source.rc.Store,
		storage:      domain.BackupStorage{Namespace: source.rc.Cluster.Ref().Namespace, Name: "s3", Bucket: "bucket", Prefix: "native-clone", EndpointURL: server.URL, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}},
		backup:       &domain.Backup{Namespace: source.rc.Cluster.Ref().Namespace, Cluster: source.rc.Cluster.Name, Name: "retained", Storage: "s3", Type: "raft-snapshot", Provider: "openbao"},
	}
	source.rc.Store = archive
	source.operation(t, domain.OperationBackup, `{"backup":"retained"}`)
	if archive.backup.Phase != domain.PhaseSucceeded || archive.backup.Size == 0 || !SnapshotBelongsToCluster(source.rc.Cluster, archive.backup) {
		t.Fatal("source snapshot missing successful native export or immutable provenance")
	}
	objectKey := "/bucket/" + archive.backup.Location
	s3.mu.Lock()
	retained := bytes.Clone(s3.objects[objectKey])
	puts := s3.puts
	s3.mu.Unlock()
	if len(retained) == 0 || puts == 0 {
		t.Fatal("signed S3 endpoint did not receive the native snapshot")
	}
	sourceRef := source.rc.Cluster.Ref()
	cleanup(source)
	if err = sourceStore.DeleteCluster(ctx, sourceRef); err != nil {
		t.Fatal(err)
	}
	if _, err = sourceStore.GetCluster(ctx, sourceRef); !missing(err) {
		t.Fatal("source cluster still exists", err)
	}

	clone := newNativeCP(t, binary)
	clone.rc.Cluster.Spec.DataSource = &domain.DataSource{Namespace: sourceRef.Namespace, Cluster: sourceRef.Name, Backup: archive.backup.Name}
	cloneArchive := &snapshotTestStore{ClusterStore: clone.rc.Store, storage: archive.storage, backup: archive.backup}
	clone.rc.Store = cloneArchive
	if _, err = clone.rc.Store.GetCluster(ctx, sourceRef); !missing(err) {
		t.Fatal("clone can still resolve a live source", err)
	}
	if _, err = clone.rc.Secrets.GetClusterSecret(ctx, sourceRef, SecretUnseal); !missing(err) {
		t.Fatal("clone can still fetch source keys instead of using supplied shares", err)
	}
	supplied, err := json.Marshal(RestoreUnsealShares{Shares: offline.Shares, Threshold: offline.Threshold, SealType: "shamir"})
	if err != nil {
		t.Fatal(err)
	}
	if err = putSecret(ctx, clone.rc, RestoreUnsealSecret("clone"), supplied); err != nil {
		t.Fatal(err)
	}
	// Stop before foreign-key replacement to prove the target really began
	// with a different native 3-of-5 split, not the supplied source material.
	clone.loop(t, "clone initial native formation", func() (bool, error) {
		r, err := clone.p.Sync(ctx, clone.rc)
		if err != nil {
			return false, err
		}
		if r.ConvergenceError != nil {
			return false, r.ConvergenceError
		}
		return state(t, clone.rc).CloneRestoreStep == "prepare", nil
	})
	initial, err := readUnseal(ctx, clone.rc)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Threshold != 3 || len(initial.Shares) != 5 || slices.Equal(initial.Shares, offline.Shares) {
		t.Fatal("target did not initialize with independent 3-of-5 material")
	}
	clone.verifyNativeRecoveryTLS(t)
	clone.ready(t)
	s := state(t, clone.rc)
	if s.CloneRestoreStep != "done" || len(s.Members) != 3 {
		t.Fatal("clone did not complete three-member native recovery")
	}
	material, err := readUnseal(ctx, clone.rc)
	if err != nil {
		t.Fatal(err)
	}
	if material.Threshold != 2 || !slices.Equal(material.Shares, offline.Shares) {
		t.Fatal("clone did not replace target material with source snapshot keys")
	}
	client, _, err := clone.p.activeClient(ctx, clone.rc, s, offline.InitialRootToken)
	if err != nil {
		t.Fatal(err)
	}
	var recovered struct {
		Data struct {
			Value string `json:"value"`
		} `json:"data"`
	}
	if err = client.Do(ctx, "GET", "clone-proof/retained", nil, &recovered); err != nil || recovered.Data.Value != "recovered-from-deleted-source" {
		t.Fatal("native clone did not recover snapshot secret", err)
	}
	// Explicitly seal a standby, then unseal it with offline source shares.
	// This cannot succeed merely because an imported process stayed unsealed.
	standby := ""
	for _, m := range s.Members {
		if m.Standby {
			standby = m.Name
			break
		}
	}
	if standby == "" {
		t.Fatal("clone has no standby for native unseal proof")
	}
	memberClient, err := clone.p.nativeClient(ctx, clone.rc, standby, offline.InitialRootToken)
	if err != nil {
		t.Fatal(err)
	}
	if err = memberClient.Do(ctx, "PUT", "sys/seal", nil, nil); err != nil {
		t.Fatal(err)
	}
	sealed, err := memberClient.SealStatus(ctx)
	if err != nil || !sealed.Sealed || sealed.T != 2 || sealed.N != 3 {
		t.Fatal("clone member does not have source snapshot seal configuration", err)
	}
	unsealed, err := memberClient.UnsealWith(ctx, offline.Shares[:offline.Threshold])
	if err != nil || unsealed.Sealed {
		t.Fatal("offline source snapshot shares cannot unseal clone", err)
	}
	clone.ready(t)
	token, err := secret(ctx, clone.rc, SecretManagementToken)
	if err != nil {
		t.Fatal(err)
	}
	manager, _, err := clone.p.activeClient(ctx, clone.rc, state(t, clone.rc), string(token))
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Do(ctx, "GET", "auth/token/lookup-self", nil, nil); err != nil {
		t.Fatal("clone management authentication failed", err)
	}
	for _, key := range []string{RestoreUnsealSecret("clone"), RestoreUnsealSecret("clone") + "/previous", RestoreUnsealSecret("clone") + "/marker", SecretInitKey} {
		if _, err = secret(ctx, clone.rc, key); !missing(err) {
			t.Fatal("temporary clone or initialization material remains", key, err)
		}
	}
	for _, n := range clone.nodes {
		if _, err = os.Stat(filepath.Join(n.dir, "karakoram/openbao/init-receipt.enc")); !os.IsNotExist(err) {
			t.Fatal("original target initialization receipt not acknowledged", err)
		}
	}
	clone.scan(t, append(slices.Clone(offline.Shares), offline.InitialRootToken))
	clone.scan(t, append(slices.Clone(initial.Shares), initial.RootToken))
	s3.mu.Lock()
	unchanged := bytes.Equal(retained, s3.objects[objectKey]) && s3.puts == puts && s3.deletes == 0
	s3.mu.Unlock()
	if !unchanged {
		t.Fatal("clone modified or deleted retained source snapshot")
	}
	cleanup(clone)
}
