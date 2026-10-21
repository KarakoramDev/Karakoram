// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestCoreKnownIdentityGuard(t *testing.T) {
	p := dataPaths(t.TempDir())
	for _, dir := range []string{p.root, filepath.Join(p.data, "mysql")} {
		if err := privateDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	id := myspec.DataIdentity{Initialized: true, ServerUUID: "11111111-2222-3333-4444-555555555555"}
	if err := durableJSON(filepath.Join(p.root, "initialized.json"), id); err != nil {
		t.Fatal(err)
	}
	s := coreTestSpec()
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("accepted missing auto.cnf for known identity")
	}
	auto := filepath.Join(p.data, "auto.cnf")
	if err := os.WriteFile(auto, []byte("[auto]\nserver-uuid=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("accepted replacement identity")
	}
	if err := os.WriteFile(auto, []byte("[auto]\nserver-uuid="+id.ServerUUID+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	fresh, err := guardInitialization(s, p)
	if err != nil || fresh {
		t.Fatalf("known identity fresh=%v err=%v", fresh, err)
	}
}
