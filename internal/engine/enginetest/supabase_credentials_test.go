// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
)

func TestSupabaseFakeOpaqueKeys(t *testing.T) {
	keys, err := sbkeys.Generate(SupabaseKeyRandom{}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	SupabaseCredentialContracts(t, keys)
	for _, key := range []string{keys.PublishableKey, keys.SecretKey} {
		if !strings.Contains(key, "_TESTONLYnotarealkey000_") {
			t.Fatal("fake opaque key must carry the visible test-only body")
		}
	}
}
