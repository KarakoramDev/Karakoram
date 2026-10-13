// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func TestSetClusterSecretsAtomicRoundTrip(t *testing.T) {
	sealer, err := secrets.New("0123456789abcdef0123456789abcdef-test-key")
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(filepath.Join(t.TempDir(), "k.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	in := map[string][]byte{"tls/ca.crt": []byte("cert"), "tls/ca.key": []byte("key")}
	if err := st.SetClusterSecrets(ctx, domain.ParseClusterKey("demo"), in); err != nil {
		t.Fatal(err)
	}
	for k, want := range in {
		got, err := st.GetClusterSecret(ctx, domain.ParseClusterKey("demo"), k)
		if err != nil || string(got) != string(want) {
			t.Fatalf("%s: got %q, %v", k, got, err)
		}
	}
}
