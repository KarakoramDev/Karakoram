// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPhysicalIdentityIncarnations(t *testing.T) {
	p := dataPaths(t.TempDir())
	for _, dir := range []string{p.root, p.data} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	uuid := "11111111-2222-3333-4444-555555555555"
	first, err := physicalIdentity(p, uuid)
	if err != nil {
		t.Fatal(err)
	}
	again, err := physicalIdentity(p, uuid)
	if err != nil || again != first {
		t.Fatalf("same source changed: %q %v", again, err)
	}
	changed, err := physicalIdentity(p, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	if err != nil || changed == first {
		t.Fatalf("UUID replacement reused identity: %q %v", changed, err)
	}
	// A clone may carry the donor's data marker and even its initial UUID,
	// but never the donor's agent-owned token outside the datadir.
	recipient := dataPaths(t.TempDir())
	for _, dir := range []string{recipient.root, recipient.data} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	marker, err := os.ReadFile(filepath.Join(p.data, "karakoram-physical.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(recipient.data, "karakoram-physical.json"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	cloned, err := physicalIdentity(recipient, uuid)
	if err != nil || cloned == first {
		t.Fatalf("clone reused donor identity: %q %v", cloned, err)
	}
	if err = os.RemoveAll(p.data); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(p.data, 0700); err != nil {
		t.Fatal(err)
	}
	fresh, err := physicalIdentity(p, uuid)
	if err != nil || fresh == first {
		t.Fatalf("new datadir reused identity: %q %v", fresh, err)
	}
	if err = os.WriteFile(filepath.Join(p.data, "karakoram-physical.json"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = physicalIdentity(p, uuid); err == nil {
		t.Fatal("corrupt provenance silently replaced")
	}
}
