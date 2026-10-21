// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
)

// OAuthPut persists an OAuth record in the control-plane database. Secrets must
// be hashed before being passed here; records survive control-plane restarts.
func (s *Store) OAuthPut(ctx context.Context, kind, id string, value any) error {
	return put(ctx, s.writer, "oauth-"+kind, "", id, value, "", "", "", false)
}

func (s *Store) OAuthGet(ctx context.Context, kind, id string, value any) error {
	_, _, err := s.get(ctx, "oauth-"+kind, "", id, value)
	return err
}

func (s *Store) OAuthList(ctx context.Context, kind string) ([]json.RawMessage, error) {
	return list[json.RawMessage](ctx, s, "oauth-"+kind, "")
}

// OAuthFind locates a grant by a hashed code, access or refresh credential.
// The JSON member names are fixed internally, never supplied by a caller.
func (s *Store) OAuthFind(ctx context.Context, hash string, value any) error {
	var raw []byte
	err := s.reader.QueryRowContext(ctx, `SELECT data FROM records WHERE kind='oauth-grant' AND (json_extract(data,'$.codeHash')=? OR EXISTS (SELECT 1 FROM json_each(records.data,'$.tokens') WHERE json_extract(value,'$.hash')=?)) LIMIT 1`, hash, hash).Scan(&raw)
	if err != nil {
		return typed(err)
	}
	return json.Unmarshal(raw, value)
}
