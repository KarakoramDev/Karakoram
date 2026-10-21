// SPDX-License-Identifier: Apache-2.0
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// AuthenticateSealer checks existing encrypted state before an export claims key
// custody. Empty/unsealed stores have no original ciphertext to authenticate.
func (s *Store) AuthenticateSealer(ctx context.Context, sealer *secrets.Sealer) error {
	if sealer == nil || sealer.Fingerprint() != s.sealer.Fingerprint() {
		return errors.New("backup master key does not match the store sealer")
	}
	rows, err := s.reader.QueryContext(ctx, `SELECT kind,scope,key,data,secret FROM records
WHERE secret<>'' OR (kind='setting' AND key IN ('server/internal-pki','server/relay-pki'))`)
	if err != nil {
		return err
	}
	defer rows.Close()
	authenticate := func(identity, ciphertext string) error {
		if _, err := sealer.OpenFor(identity, ciphertext); err != nil {
			return fmt.Errorf("authenticate backup master key against persisted state: %w", err)
		}
		return nil
	}
	for rows.Next() {
		var kind, scope, key, ciphertext string
		var data []byte
		if err = rows.Scan(&kind, &scope, &key, &data, &ciphertext); err != nil {
			return err
		}
		var identity string
		switch kind {
		case "secret":
			identity = "cluster/" + scope + "/" + key
		case "namespace-secret":
			identity = "namespace/" + scope + "/" + key
		case "agent-token":
			identity = "agent/" + scope + "/" + key
		case "storage":
			identity = storageAAD(scope, key)
		case "setting":
			if key != "server/internal-pki" && key != "server/relay-pki" {
				continue
			}
			identity = key
			if err = json.Unmarshal(data, &ciphertext); err != nil {
				return err
			}
		case "notification-channel":
			var v domain.NotificationChannelSecrets
			if err = json.Unmarshal([]byte(ciphertext), &v); err != nil {
				return err
			}
			for _, field := range []struct{ name, value string }{
				{"webhook-url", v.WebhookURL}, {"webhook-signing-secret", v.WebhookSigningSecret},
				{"slack-webhook-url", v.SlackWebhookURL}, {"email-password", v.EmailPassword},
			} {
				if field.value != "" {
					if err = authenticate("notification/"+key+"/"+field.name, field.value); err != nil {
						return err
					}
				}
			}
			continue
		default:
			continue
		}
		if err = authenticate(identity, ciphertext); err != nil {
			return err
		}
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	responses, err := s.reader.QueryContext(ctx, "SELECT identity,response FROM idempotency_requests WHERE response<>''")
	if err != nil {
		return err
	}
	defer responses.Close()
	for responses.Next() {
		var identity, ciphertext string
		if err = responses.Scan(&identity, &ciphertext); err != nil {
			return err
		}
		if err = authenticate("idempotency/"+identity, ciphertext); err != nil {
			return err
		}
	}
	return responses.Err()
}

// Snapshot exports committed WAL state through SQLite, never by copying live files.
func (s *Store) Snapshot(ctx context.Context, dstPath string) error {
	var source string
	var seq int
	var name string
	rows, err := s.writer.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return err
	}
	for rows.Next() {
		if err = rows.Scan(&seq, &name, &source); err != nil {
			rows.Close()
			return err
		}
		if name == "main" {
			break
		}
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	dir := filepath.Dir(source)
	if source == "" {
		dir = filepath.Dir(dstPath)
	}
	f, err := os.CreateTemp(dir, ".karakoram-snapshot-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Close(); err != nil {
		return err
	}
	// VACUUM INTO accepts an existing empty file; CreateTemp fixes permissions before SQLite writes.
	if _, err = s.writer.ExecContext(ctx, "VACUUM INTO ?", tmp); err != nil {
		return err
	}
	if err = CheckSnapshot(ctx, tmp); err != nil {
		return err
	}
	f, err = os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = errors.Join(f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, dstPath); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(dstPath))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// CheckSnapshot performs a read-only SQLite integrity check without migrations.
func CheckSnapshot(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var result string
		if err = rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return errors.New("snapshot SQLite integrity check failed")
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return errors.New("snapshot SQLite integrity check returned no result")
	}
	return nil
}

func (s *Store) SchemaVersion(ctx context.Context) (string, error) {
	var v string
	err := s.reader.QueryRowContext(ctx, "SELECT COALESCE(MAX(name),'') FROM schema_migrations").Scan(&v)
	return v, err
}
func LatestSchemaVersion() string {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		panic(err)
	}
	var latest string
	for _, entry := range entries {
		if entry.Name() > latest {
			latest = entry.Name()
		}
	}
	return latest
}

// DataDir identifies the directory for non-authoritative snapshot staging.
func (s *Store) DataDir() string {
	if s.path == ":memory:" {
		return os.TempDir()
	}
	return filepath.Dir(s.path)
}

// EnsureInstallationID atomically creates an immutable installation identity.
func (s *Store) EnsureInstallationID(ctx context.Context) (string, error) {
	value, err := s.GetSetting(ctx, "server/installation-id")
	var id string
	if errors.Is(err, ErrNotFound) {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		raw, _ := json.Marshal(hex.EncodeToString(random[:]))
		if err := put(ctx, s.writer, "setting", "", "server/installation-id", json.RawMessage(raw), "", "", "", true); err != nil && !errors.Is(err, ErrConflict) {
			return "", err
		}
		value, err = s.GetSetting(ctx, "server/installation-id")
	}
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(value, &id); err != nil {
		return "", err
	}
	if len(id) != 16 {
		return "", fmt.Errorf("invalid installation identity")
	}
	return id, nil
}
