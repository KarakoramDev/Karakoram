// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nativeartifacts

import (
	"strings"
	"testing"
)

func TestMongoshOfficialArtifact(t *testing.T) {
	a, err := Get("mongodb-mongosh-2.14.1")
	if err != nil {
		t.Fatal(err)
	}
	if a.Version != "2.14.1" || a.File != "mongosh-2.14.1-linux-x64.tgz" || a.Size != 94615134 || a.SHA256 != "8d24940ebe3675959b518fd41ea16b4e2182d6a75c09777db09cfa64e18834a1" || a.License != "Apache-2.0" || a.Source.Repo != "https://github.com/mongodb-js/mongosh" || a.Source.Commit != "456a58b13c1d9dd83bd4e0985c10237ba83c18e1" || a.Build != "upstream-release" {
		t.Fatalf("incorrect mongosh artifact: %+v", a)
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	url, err := ResolveURL(a)
	if err != nil || url != "https://downloads.mongodb.com/compass/"+a.File {
		t.Fatal(url, err)
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://mirror.example.test/native/")
	url, err = ResolveURL(a)
	if err != nil || url != "https://mirror.example.test/native/"+a.File {
		t.Fatal(url, err)
	}
	script := prepareScript(t, PrepareCommands(a, url, "/opt/karakoram/mongosh-2.14.1", "root")[0])
	install := "tar --no-same-owner --no-same-permissions --delay-directory-restore -xzf"
	if !strings.Contains(script, install) || !strings.Contains(script, url) || strings.Contains(script, "downloads.mongodb.com") || strings.Index(script, "sha256sum -c -") > strings.Index(script, install) {
		t.Fatal("mirror or verified archive extraction lost", script)
	}
	if strings.Contains(script, "apt-get install") {
		t.Fatal("standalone shell must not replace package dependencies", script)
	}
}
