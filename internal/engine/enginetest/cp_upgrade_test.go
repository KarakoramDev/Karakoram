// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestRestoreFixtureKeysPreservesBytes(t *testing.T) {
	// Assemble labels so this regression test does not itself commit a PEM key.
	key := "-----BEGIN " + "PRIVATE KEY-----\nfixture-only\n-----END " + "PRIVATE KEY-----\n"
	nested, err := json.MarshalIndent(map[string]string{"keyPEM": key}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	original, err := json.MarshalIndent(map[string]string{
		"keyPEM":     key,
		"rawSecret":  base64.StdEncoding.EncodeToString([]byte(key)),
		"jsonSecret": base64.StdEncoding.EncodeToString(nested),
		"unrelated":  "unchanged",
	}, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	mark := func(raw []byte) []byte {
		return bytes.ReplaceAll(raw, []byte("PRIVATE KEY-----"), []byte("KARAKORAM TEST ONLY PRIVATE KEY-----"))
	}
	var marked map[string]string
	if err := json.Unmarshal(original, &marked); err != nil {
		t.Fatal(err)
	}
	marked["keyPEM"] = string(mark([]byte(key)))
	marked["rawSecret"] = base64.StdEncoding.EncodeToString(mark([]byte(key)))
	marked["jsonSecret"] = base64.StdEncoding.EncodeToString(mark(nested))
	input, err := json.MarshalIndent(marked, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(RestoreFixtureKeys(t, input), original) {
		t.Fatal("restoration must preserve exact original fixture bytes")
	}
	if !bytes.Equal(RestoreFixtureKeys(t, original), original) {
		t.Fatal("unmarked fixtures must remain unchanged")
	}
	if strings.Contains(string(RestoreFixtureKeys(t, input)), "KARAKORAM TEST ONLY") {
		t.Fatal("test-only label was not restored")
	}
}
