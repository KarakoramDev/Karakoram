// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/minio/minio-go/v7"
	s3credentials "github.com/minio/minio-go/v7/pkg/credentials"
)

func assertRecoveryClientAccess(t *testing.T, s pgspec.Spec, allowed bool) {
	t.Helper()
	config, err := pgx.ParseConfig("")
	if err != nil {
		t.Fatal(err)
	}
	config.Host, config.Port, config.Database = "127.0.0.1", 5432, "postgres"
	config.User, config.Password = s.Superuser.Username, s.Superuser.Password
	config.TLSConfig, config.Fallbacks = nil, nil
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, config)
	if conn != nil {
		conn.Close(ctx)
	}
	if allowed {
		if err != nil {
			t.Fatal("client access did not reopen after source credential cleanup")
		}
		return
	}
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "28000" || !strings.Contains(pgerr.Message, "no pg_hba.conf entry") {
		t.Fatal("remote clients were not fenced by recovery HBA")
	}
}

// TestNativeCrossSourceRepositoryGenerations reproduces two recoveries from a
// timeline-1 source into the same existing target. Both promotions may select
// timeline 2, but each target generation must archive and take a new full safely.
func TestNativeCrossSourceRepositoryGenerations(t *testing.T) {
	config := os.Getenv("KARAKORAM_PG_TEST_REPOSITORY")
	if config == "" || os.Getenv("KARAKORAM_PG_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_PG_TEST_REPOSITORY and KARAKORAM_PG_BIN_DIR for native pgBackRest/S3 recovery")
	}
	if os.Geteuid() == 0 {
		t.Fatal("run native PostgreSQL tests as an unprivileged runtime user")
	}
	data, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	var repo pgspec.Repository
	if err = json.Unmarshal(data, &repo); err != nil {
		t.Fatal("decode test repository")
	}
	u, err := url.Parse(repo.S3.Endpoint)
	if err != nil {
		t.Fatal("parse test endpoint")
	}
	client, err := minio.New(u.Host, &minio.Options{Creds: s3credentials.NewStaticV4(repo.S3.AccessKey, repo.S3.SecretKey, ""), Secure: u.Scheme == "https", Region: repo.S3.Region})
	if err != nil {
		t.Fatal("create test object client")
	}
	root := fmt.Sprintf("%s/lineage-%d", repo.Path, time.Now().UnixNano())
	repo.Path = root + "-source"
	t.Cleanup(func() {
		objects := make(chan minio.ObjectInfo)
		go func() {
			defer close(objects)
			for object := range client.ListObjects(context.Background(), repo.S3.Bucket, minio.ListObjectsOptions{Prefix: strings.TrimPrefix(root, "/"), Recursive: true}) {
				if object.Err == nil {
					objects <- object
				}
			}
		}()
		for failure := range client.RemoveObjects(context.Background(), repo.S3.Bucket, objects, minio.RemoveObjectsOptions{}) {
			t.Errorf("remove test object: %v", failure.Err)
		}
	})
	call := func(h *nativeAgent, route string, request, response any) {
		t.Helper()
		r, err := h.request(http.MethodPost, route, request)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("%s HTTP %d", route, r.StatusCode)
		}
		if response != nil {
			if err = json.NewDecoder(r.Body).Decode(response); err != nil {
				t.Fatal(err)
			}
		}
	}
	apply := func(h *nativeAgent, s pgspec.Spec) {
		t.Helper()
		raw, _ := json.Marshal(s)
		h.mu.Lock()
		h.config.EngineSpec = raw
		h.config.ConfigVersion++
		h.mu.Unlock()
		call(h, "/v1/reload", struct{}{}, nil)
	}
	sql := func(h *nativeAgent, s pgspec.Spec, statement string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		c, err := localConnect(ctx, s, dataPaths(h.dir))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		if _, err = c.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	full := func(h *nativeAgent, receipt string) string {
		t.Helper()
		// Avoid the normal spread-out backup checkpoint extending this small
		// native regression to the production checkpoint completion interval.
		h.mu.Lock()
		var current pgspec.Spec
		err := json.Unmarshal(h.config.EngineSpec, &current)
		h.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		sql(h, current, "CHECKPOINT")
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			var response pgspec.BackupResponse
			call(h, "/v1/postgres/backup", pgspec.BackupRequest{Receipt: receipt, Type: "full"}, &response)
			if response.State == "done" {
				return response.Label
			}
			if response.State == "failed" {
				t.Fatal("native full backup failed")
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatal("native full backup timed out")
		return ""
	}
	snapshot := func() map[string]string {
		t.Helper()
		result := map[string]string{}
		for object := range client.ListObjects(t.Context(), repo.S3.Bucket, minio.ListObjectsOptions{Prefix: strings.TrimPrefix(repo.Path, "/") + "/", Recursive: true}) {
			if object.Err != nil {
				t.Fatal("read source inventory")
			}
			result[object.Key] = object.ETag
		}
		if len(result) == 0 {
			t.Fatal("source repository is empty")
		}
		return result
	}
	s := testSpec()
	s.BinDir = os.Getenv("KARAKORAM_PG_BIN_DIR")
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	s.Backup = &pgspec.Backup{Repository: repo}
	source := startNativeAgent(t, s)
	call(source, "/v1/postgres/stanza-create", pgspec.RepositoryRequest{}, nil)
	sql(source, s, "CREATE TABLE public.lineage_probe(id integer PRIMARY KEY); INSERT INTO public.lineage_probe SELECT generate_series(1,10)")
	label := full(source, "source-full")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	c, err := localConnect(ctx, s, dataPaths(source.dir))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var target time.Time
	if err = c.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&target); err != nil {
		t.Fatal(err)
	}
	c.Close(ctx)
	cancel()
	time.Sleep(20 * time.Millisecond)
	sql(source, s, "INSERT INTO public.lineage_probe VALUES (11)")
	sql(source, s, "SELECT pg_switch_wal(); CHECKPOINT")
	// The backup waits for archive completion through the segment containing target.
	full(source, "source-after-target")
	sourceID := source.status(t).SystemID
	source.stop(t)
	before := snapshot()
	targetSpec := s
	targetSpec.Superuser.Password = "target-superuser-secret"
	targetSpec.Backup = &pgspec.Backup{Repository: repo}
	targetSpec.Backup.Repository.Path = root + "-target"
	h := startNativeAgent(t, targetSpec)
	defer h.stop(t)
	if h.status(t).SystemID == sourceID {
		t.Fatal("fixture did not create an independent target system identifier")
	}
	call(h, "/v1/postgres/stanza-create", pgspec.RepositoryRequest{}, nil)
	full(h, "target-original-full")
	for generation := 1; generation <= 2; generation++ {
		receipt := fmt.Sprintf("cross-source-%d", generation)
		call(h, "/v1/postgres/prepare-restore", pgspec.PrepareRestoreRequest{Receipt: receipt + "/stop"}, nil)
		restore := pgspec.Restore{Source: repo, Set: label}
		if generation == 2 {
			restore.TargetTime = target.UTC().Format(time.RFC3339Nano)
		}
		targetSpec.Backup.Repository.Path = fmt.Sprintf("%s-target-g%d", root, generation)
		targetSpec.Postgres.RecoveryClientsHeld = true
		targetSpec.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &restore, RecoverySuperuser: &s.Superuser, Generation: generation + 1}
		apply(h, targetSpec)
		call(h, "/v1/postgres/restore", pgspec.RestoreRequest{Receipt: receipt, Restore: restore, Generation: generation + 1}, nil)
		call(h, "/v1/processes/postgres/start", nil, nil)
		deadline := time.Now().Add(45 * time.Second)
		status := h.status(t)
		for time.Now().Before(deadline) && (!status.Running || status.Role != "standalone" || status.Timeline != 2) {
			time.Sleep(100 * time.Millisecond)
			status = h.status(t)
		}
		if !status.Running || status.Role != "standalone" || status.SystemID != sourceID || status.Timeline != 2 {
			t.Fatalf("generation %d: expected restored source promoted on timeline 2; role=%s running=%v timeline=%d", generation, status.Role, status.Running, status.Timeline)
		}
		call(h, "/v1/postgres/rekey", struct{}{}, nil)
		assertRecoveryClientAccess(t, targetSpec, false)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		c, err := localConnect(ctx, targetSpec, dataPaths(h.dir))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		var count, sum int
		err = c.QueryRow(ctx, "SELECT count(*), sum(id) FROM public.lineage_probe").Scan(&count, &sum)
		c.Close(ctx)
		cancel()
		if err != nil || count != 10 || sum != 55 {
			t.Fatalf("generation %d: restored count=%d sum=%d err=%v", generation, count, sum, err)
		}
		targetSpec.Bootstrap = nil
		apply(h, targetSpec)
		call(h, "/v1/postgres/stanza-create", pgspec.RepositoryRequest{}, nil)
		call(h, "/v1/postgres/finish-restore", struct{}{}, nil)
		// Reapplying the completed config must never bring source keys back.
		apply(h, targetSpec)
		for _, name := range []string{"pgbackrest-restore.conf", "restore-s3-ca.pem", "bootstrap-restore.sh", "pgbackrest-action.conf", "action-s3-ca.pem"} {
			if _, err := os.Stat(filepath.Join(dataPaths(h.dir).root, name)); !os.IsNotExist(err) {
				t.Fatalf("source recovery file survived cutover: %s", name)
			}
		}
		cached, err := os.ReadFile(filepath.Join(h.dir, "karakoram", "config.json"))
		if err != nil {
			t.Fatal(err)
		}
		var saved struct {
			EngineSpec pgspec.Spec `json:"engineSpec"`
		}
		if err = json.Unmarshal(cached, &saved); err != nil || saved.EngineSpec.Bootstrap != nil {
			t.Fatal("cached recovery credentials survived cutover")
		}
		if strings.Contains(string(cached), repo.Path) || strings.Contains(string(cached), s.Superuser.Password) {
			t.Fatal("cached configuration still references the source repository")
		}
		assertRecoveryClientAccess(t, targetSpec, false)
		targetSpec.Postgres.RecoveryClientsHeld = false
		apply(h, targetSpec)
		assertRecoveryClientAccess(t, targetSpec, true)
		sql(h, targetSpec, fmt.Sprintf("INSERT INTO public.lineage_probe VALUES (%d)", 100+generation))
		sql(h, targetSpec, "SELECT pg_switch_wal(); CHECKPOINT")
		full(h, receipt+"/full")
		if h.status(t).ArchiveFailing {
			t.Fatalf("generation %d: archive failing after full backup", generation)
		}
		if !reflect.DeepEqual(before, snapshot()) {
			t.Fatal("cross-source recovery changed source repository objects")
		}
	}
	t.Log("independent system ID, repeated timeline 2 promotions, exact rows, archive + full per generation, source immutability and source credential cleanup passed")
}
