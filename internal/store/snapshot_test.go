// SPDX-License-Identifier: Apache-2.0
package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestSnapshotWALConsistency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sealer, _ := secrets.New(strings.Repeat("s", 32))
	st, err := Open(filepath.Join(dir, "karakoram.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err = st.writer.ExecContext(ctx, "CREATE TABLE snapshot_pairs (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err = st.writer.ExecContext(ctx, "INSERT INTO snapshot_pairs VALUES (1,0,0)"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			if _, err := st.writer.ExecContext(ctx, "UPDATE snapshot_pairs SET a=a+1,b=b+1"); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	dst := filepath.Join(dir, "snapshot.db")
	if err = st.Snapshot(ctx, dst); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err = CheckSnapshot(ctx, dst); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot permissions %o", info.Mode().Perm())
	}
	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var a, b int
	if err = db.QueryRow("SELECT a,b FROM snapshot_pairs").Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("inconsistent snapshot: %d != %d", a, b)
	}
	var schema string
	if err = db.QueryRow("SELECT MAX(name) FROM schema_migrations").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if schema != LatestSchemaVersion() {
		t.Fatalf("schema=%s", schema)
	}
}
func TestInstallationIdentityConcurrent(t *testing.T) {
	ctx := context.Background()
	sealer, _ := secrets.New(strings.Repeat("s", 32))
	st, err := Open(filepath.Join(t.TempDir(), "karakoram.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ids := make(chan string, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := st.EnsureInstallationID(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	var want string
	for id := range ids {
		if len(id) != 16 {
			t.Fatalf("identity=%q", id)
		}
		if want == "" {
			want = id
		}
		if id != want {
			t.Fatalf("identity changed %s -> %s", want, id)
		}
	}
	raw, err := st.GetSetting(ctx, "server/installation-id")
	if err != nil {
		t.Fatal(err)
	}
	var saved string
	if err = json.Unmarshal(raw, &saved); err != nil || saved != want {
		t.Fatalf("persisted identity: %s %v", raw, err)
	}
}

func TestSnapshotPreservesRestrictedTokensAndSealedDataMCP(t *testing.T) {
	ctx := t.Context()
	sealer, err := secrets.New(strings.Repeat("s", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(filepath.Join(t.TempDir(), "source.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	token := domain.ApiToken{ID: "restricted", Name: "data-client", Owner: "owner", Hash: secrets.Hash("bearer-token-not-persisted"), Restrictions: &domain.TokenRestrictions{
		ReadOnly: true, Namespaces: []string{"default"}, Clusters: []string{"default/demo"}, Permissions: []string{"cluster-data:read"},
	}}
	if err := st.CreateApiToken(ctx, &token); err != nil {
		t.Fatal(err)
	}
	ref := domain.ClusterRef{Namespace: "default", Name: "demo"}
	material := []byte(`{"password":"sealed-data-mcp-credential"}`)
	for _, key := range []string{"datamcp/config", "datamcp/state"} {
		if err := st.SetClusterSecret(ctx, ref, key, material); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AuthenticateSealer(ctx, sealer); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "snapshot.db")
	if err := st.Snapshot(ctx, dst); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, plaintext := range [][]byte{material, []byte("sealed-data-mcp-credential"), []byte("bearer-token-not-persisted")} {
		if bytes.Contains(raw, plaintext) {
			t.Fatalf("snapshot contains plaintext sensitive material %q", plaintext)
		}
	}
	restored, err := Open(dst, sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, err := restored.GetApiToken(ctx, token.ID)
	if err != nil || got.Hash != token.Hash || !reflect.DeepEqual(got.Restrictions, token.Restrictions) {
		t.Fatalf("restricted token not preserved: %+v %v", got, err)
	}
	for _, key := range []string{"datamcp/config", "datamcp/state"} {
		got, err := restored.GetClusterSecret(ctx, ref, key)
		if err != nil || !bytes.Equal(got, material) {
			t.Fatalf("sealed material %s not preserved: %v", key, err)
		}
	}
}
