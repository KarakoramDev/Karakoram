// SPDX-License-Identifier: Apache-2.0
package cpbackup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestExportAuthenticatesPersistedMasterKey(t *testing.T) {
	for _, kind := range []string{"cluster", "storage", "relay-pki", "internal-pki", "namespace", "notification", "agent", "idempotency"} {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()
			original, err := secrets.New(strings.Repeat("k", 32))
			if err != nil {
				t.Fatal(err)
			}
			wrong, err := secrets.New(strings.Repeat("w", 32))
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			path := filepath.Join(dir, "karakoram.db")
			st, err := store.Open(path, original)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "cluster":
				err = st.SetClusterSecret(ctx, domain.ClusterRef{Name: "example"}, "password", []byte("private"))
			case "storage":
				err = st.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "archive", Type: "s3", Credentials: domain.StorageCredentials{SecretAccessKey: "private"}})
			case "relay-pki":
				err = st.PutRelayPKI(ctx, &store.RelayPKI{CAKey: []byte("private")})
			case "internal-pki":
				var sealed string
				sealed, err = original.SealFor("server/internal-pki", []byte(`{"key":"private"}`))
				if err == nil {
					var raw []byte
					raw, err = json.Marshal(sealed)
					if err == nil {
						err = st.SetSetting(ctx, "server/internal-pki", raw)
					}
				}
			case "namespace":
				err = st.CreateNamespaceCredentials(ctx, &domain.Namespace{Name: "testing", Backend: "zerops"}, "private", nil)
			case "notification":
				err = st.SaveNotificationChannel(ctx, &domain.NotificationChannel{Name: "alerts", Secrets: domain.NotificationChannelSecrets{WebhookURL: "https://example.invalid/private"}}, true)
			case "agent":
				err = st.SetAgentToken(ctx, &domain.AgentToken{Cluster: "example", Member: "one", Token: "private"})
			case "idempotency":
				var record store.IdempotencyRecord
				record, _, err = st.BeginIdempotency(ctx, "request", "body-hash", time.Now())
				if err == nil {
					err = st.CompleteIdempotency(ctx, "request", record.Owner, store.IdempotentResponse{Status: 200, Body: []byte("private")}, time.Now())
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(t.TempDir(), "state.db.gz")
			// The controller's already-open store and sealer remain supported.
			m, err := Export(ctx, st, original, out, "test", "commit")
			if err != nil || m.KeyFingerprint != original.Fingerprint() {
				t.Fatalf("original key export: %+v: %v", m, err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, wrong)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			before := map[string][]byte{}
			for _, name := range []string{out, out + ".json"} {
				before[name], err = os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
			}
			m, err = Export(ctx, st, wrong, out, "test", "commit")
			if err == nil || !strings.Contains(err.Error(), "authenticate backup master key") || m.KeyFingerprint != "" {
				t.Fatalf("wrong key exported misleading receipt: %+v: %v", m, err)
			}
			for name, want := range before {
				got, err := os.ReadFile(name)
				if err != nil || string(got) != string(want) {
					t.Fatalf("wrong key changed published %s: %v", name, err)
				}
			}
			missing := filepath.Join(t.TempDir(), "not-published.db.gz")
			if _, err = Export(ctx, st, wrong, missing, "test", "commit"); err == nil {
				t.Fatal("wrong key export succeeded")
			}
			for _, name := range []string{missing, missing + ".json"} {
				if _, err = os.Stat(name); !os.IsNotExist(err) {
					t.Fatalf("published unusable archive %s: %v", name, err)
				}
			}
			stages, err := filepath.Glob(filepath.Join(dir, ".state-export-*"))
			if err != nil || len(stages) != 0 {
				t.Fatalf("unexpected snapshot staging: %v: %v", stages, err)
			}
		})
	}
}

func TestExportFreshAndUnsealedState(t *testing.T) {
	for _, withSetting := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "unsealed"}[withSetting], func(t *testing.T) {
			sealer, err := secrets.New(strings.Repeat("k", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(filepath.Join(t.TempDir(), "karakoram.db"), sealer)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if withSetting {
				if err = st.SetSetting(t.Context(), "public", []byte(`true`)); err != nil {
					t.Fatal(err)
				}
			}
			m, err := Export(t.Context(), st, sealer, filepath.Join(t.TempDir(), "state.db.gz"), "test", "commit")
			if err != nil || m.KeyFingerprint != sealer.Fingerprint() {
				t.Fatalf("unsealed export: %+v: %v", m, err)
			}
		})
	}
}
