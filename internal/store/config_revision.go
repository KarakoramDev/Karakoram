// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ConfigSecretRevision fingerprints cluster secrets and all storage definitions.
// Storage scopes are included: Elasticsearch can reference another namespace.
func (s *Store) ConfigSecretRevision(ctx context.Context, ref domain.ClusterRef) ([32]byte, error) {
	var result [32]byte
	if err := ref.Validate(); err != nil {
		return result, err
	}
	rows, err := s.reader.QueryContext(ctx, "SELECT kind,scope,key,secret,data FROM records WHERE (scope=? AND kind IN ('secret','agent-token')) OR kind='storage' ORDER BY kind,scope,key", ref.Key())
	if err != nil {
		return result, err
	}
	defer rows.Close()
	h := sha256.New()
	var size [8]byte
	for rows.Next() {
		var kind, scope, key, sealed, data string
		if err := rows.Scan(&kind, &scope, &key, &sealed, &data); err != nil {
			return result, err
		}
		for _, value := range [...]string{kind, scope, key, sealed, data} {
			binary.BigEndian.PutUint64(size[:], uint64(len(value)))
			h.Write(size[:])
			io.WriteString(h, value)
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	h.Sum(result[:0])
	return result, nil
}
