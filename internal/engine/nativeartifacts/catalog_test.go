// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nativeartifacts

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/version"
)

var updateGolden = flag.Bool("update", false, "update native artifact command goldens")

func TestSupabasePrepareCommandsGolden(t *testing.T) {
	a, err := Get("supabase-pg17-extensions")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(PrepareCommands(a, "https://example.test/"+a.File, "/", "postgres"), "\n") + "\n"
	const filename = "testdata/supabase-pg17-prepare.golden"
	if *updateGolden {
		if err := os.WriteFile(filename, []byte(got), 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatal("Supabase prepare command bytes changed")
	}
}

func TestLock(t *testing.T) {
	var entries []Artifact
	if err := json.Unmarshal(lock, &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("empty lock")
	}
	seen := make(map[string]bool)
	for _, a := range entries {
		if a.Name == "" || a.Version == "" || a.File == "" || a.Size <= 0 || a.License == "" || a.Build == "" {
			t.Fatalf("incomplete artifact: %+v", a)
		}
		switch a.Kind {
		case "":
			if a.PGMajor != 0 || (len(a.Extensions) != 0 && !legacyExtensionArtifact(a)) {
				t.Fatalf("extension metadata requires kind and major: %+v", a)
			}
		case "pg-extension":
			if !validPGMajor(a.PGMajor) || !(strings.HasSuffix(a.File, ".tar.gz") || strings.HasSuffix(a.File, ".tar.xz")) || len(a.Extensions) == 0 {
				t.Fatalf("invalid PostgreSQL extension artifact: %+v", a)
			}
		default:
			t.Fatalf("unknown artifact kind %q for %s", a.Kind, a.Name)
		}
		if path.Base(a.File) != a.File {
			t.Fatalf("non-basename filename: %s", a.File)
		}
		if digest, err := hex.DecodeString(a.SHA256); err != nil || len(digest) != 32 {
			t.Fatalf("invalid SHA256 for %s", a.Name)
		}
		if commit, err := hex.DecodeString(a.Source.Commit); err != nil || len(commit) != 20 || !strings.HasPrefix(a.Source.Repo, "https://github.com/") {
			t.Fatalf("invalid source for %s", a.Name)
		}
		if a.UpstreamURL == "" && len(a.RuntimeDeps) == 0 {
			t.Fatalf("missing qualified runtime dependencies for %s", a.Name)
		}
		if len(a.ExtensionFlags) != len(a.Extensions) {
			t.Fatalf("extension flags differ from qualified names for %s", a.Name)
		}
		for _, extension := range a.Extensions {
			flags, exists := a.ExtensionFlags[extension]
			if !exists {
				t.Fatalf("missing runtime flags for %s", extension)
			}
			if extension == "pgsodium" && (flags.Preload || !reflect.DeepEqual(flags.UnavailableFeatures, []string{"server-key-management", "key-id", "tce"})) {
				t.Fatalf("pgsodium must be crypto-only without preload: %+v", flags)
			}
		}
		if seen[a.Name] {
			t.Fatalf("duplicate artifact %s", a.Name)
		}
		seen[a.Name] = true
		got, err := Get(a.Name)
		if err != nil || !reflect.DeepEqual(got, a) {
			t.Fatalf("Get(%s) = %+v, %v", a.Name, got, err)
		}
		for _, member := range a.ExecutableMembers {
			if member == "" || path.IsAbs(member) || path.Clean(member) != member || member == "." || member == ".." || strings.HasPrefix(member, "../") {
				t.Fatalf("invalid executable member for %s: %q", a.Name, member)
			}
		}
	}
	if _, err := Get("unknown"); err == nil {
		t.Fatal("unknown artifact accepted")
	}
}

func TestResolveURL(t *testing.T) {
	old := version.Version
	t.Cleanup(func() { version.Version = old })
	t.Setenv("KARAKORAM_ARTIFACT_RELEASE", "")
	a := Artifact{Name: "example", File: "example.tar.gz", UpstreamURL: "https://upstream.example/example.tar.gz"}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://mirror.example/native///")
	version.Version = "dev"
	if got, err := ResolveURL(a); err != nil || got != "https://mirror.example/native/example.tar.gz" {
		t.Fatalf("mirror: %q, %v", got, err)
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	if got, err := ResolveURL(a); err != nil || got != a.UpstreamURL {
		t.Fatalf("upstream: %q, %v", got, err)
	}
	a.UpstreamURL = ""
	for _, v := range []string{"dev", "", "unknown", "v1.2.3/development", "v0.4.0-36-gf8a5dd5b", "v1.2.3-rc.1-2-gabcdef0", "v1.2.3-0-gabcdef0", "v1.2.3-2-gabcdef0-dirty"} {
		version.Version = v
		_, err := ResolveURL(a)
		var missing *MissingMirrorError
		if !errors.As(err, &missing) || missing.Name != a.Name {
			t.Fatalf("%s: expected typed missing mirror error, got %v", v, err)
		}
	}
	for _, v := range []string{"v1.2.3", "v1.2.3-rc.1"} {
		version.Version = v
		want := "https://github.com/KarakoramDev/Karakoram/releases/download/" + v + "/example.tar.gz"
		if got, err := ResolveURL(a); err != nil || got != want {
			t.Fatalf("release: %q, %v", got, err)
		}
	}
}

func TestDescribeBuildResolvesExplicitArtifactSources(t *testing.T) {
	old := version.Version
	t.Cleanup(func() { version.Version = old })
	t.Setenv("KARAKORAM_ARTIFACT_RELEASE", "")
	version.Version = "v0.4.0-36-gf8a5dd5b"
	a, err := Get("timescaledb-pg16")
	if err != nil {
		t.Fatal(err)
	}
	const mirror = "https://github.com/KarakoramDev/Karakoram/releases/download/v0.4.0"
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", mirror)
	if got, err := ResolveURL(a); err != nil || got != mirror+"/"+a.File {
		t.Fatalf("source build mirror: %q, %v", got, err)
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	a.UpstreamURL = "https://upstream.example/" + a.File
	if got, err := ResolveURL(a); err != nil || got != a.UpstreamURL {
		t.Fatalf("source build upstream: %q, %v", got, err)
	}
}

func TestArtifactReleaseOverride(t *testing.T) {
	old := version.Version
	t.Cleanup(func() { version.Version = old })
	version.Version = "v0.4.0-36-gf8a5dd5b"
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	a, err := Get("timescaledb-pg16")
	if err != nil {
		t.Fatal(err)
	}
	for _, release := range []string{"v0.4.0", "v0.4.1-rc.1"} {
		t.Setenv("KARAKORAM_ARTIFACT_RELEASE", release)
		want := "https://github.com/KarakoramDev/Karakoram/releases/download/" + release + "/" + a.File
		if got, err := ResolveURL(a); err != nil || got != want {
			t.Fatalf("artifact release %s: %q, %v", release, got, err)
		}
	}
	for _, release := range []string{"dev", "v0.4.0-36-gf8a5dd5b", "../v0.4.0"} {
		t.Setenv("KARAKORAM_ARTIFACT_RELEASE", release)
		if _, err := ResolveURL(a); err == nil || !strings.Contains(err.Error(), "KARAKORAM_ARTIFACT_RELEASE") {
			t.Fatalf("invalid artifact release %s: %v", release, err)
		}
	}
	// Explicit mirrors and official downloads retain precedence, even if an
	// unused release override is invalid.
	a.UpstreamURL = "https://upstream.example/" + a.File
	if got, err := ResolveURL(a); err != nil || got != a.UpstreamURL {
		t.Fatalf("official upstream: %q, %v", got, err)
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://mirror.example/native")
	if got, err := ResolveURL(a); err != nil || got != "https://mirror.example/native/"+a.File {
		t.Fatalf("mirror: %q, %v", got, err)
	}
}

func TestPrepareCommands(t *testing.T) {
	for _, file := range []string{"example.tar.xz", "example.tar.gz", "example.deb", "example-linux-x86_64"} {
		a := Artifact{Name: "example", File: file, SHA256: strings.Repeat("a", 64)}
		commands := PrepareCommands(a, "https://example.test/"+file, "/opt/karakoram/example-1", "runtime")
		if len(commands) != 1 {
			t.Fatalf("commands: %v", commands)
		}
		command := prepareScript(t, commands[0])
		for _, want := range []string{
			"set -eu\numask 077\nstage=$(mktemp -d /tmp/karakoram-artifact.XXXXXXXXXX)",
			"trap 'rm -rf -- \"$stage\"' EXIT HUP INT TERM",
			"cd \"$stage\"\nset -C", "test ! -e \"$file\" && test ! -L \"$file\"",
			"curl -fsSL", a.SHA256 + "  " + file, "sha256sum -c -",
		} {
			if !strings.Contains(command, want) {
				t.Fatalf("missing %q in %s", want, command)
			}
		}
		var install string
		if strings.HasSuffix(file, ".deb") {
			install = "DPKG_DEB_THREADS_MAX=1 apt-get install -y \"$stage/$file\""
		} else if strings.Contains(file, ".tar.") {
			install = "tar --no-same-owner --no-same-permissions --delay-directory-restore"
			for _, want := range []string{"python3 -c", legacyPG17Validator, "\"$file\" 'application'", "sudo -u 'runtime' -- sh -c", " < \"$file\""} {
				if !strings.Contains(command, want) {
					t.Fatalf("missing safe extraction %q in %s", want, command)
				}
			}
		} else {
			install = "cat > \"$binary\""
		}
		if !strings.HasSuffix(file, ".deb") && (strings.Contains(command, "chown -R") || strings.Contains(command, "cp -a")) {
			t.Fatal("must not perform privileged writes to runtime-owned installation paths")
		}
		if !strings.Contains(command, install) || strings.Index(command, "sha256sum -c -") > strings.Index(command, install) {
			t.Fatal("install missing or precedes verification: " + command)
		}
	}
}

func TestPrepareDebSerialDecompressionAndFailure(t *testing.T) {
	for _, status := range []int{0, 100, 137} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			dir := t.TempDir()
			file := filepath.Join(dir, "fixture.deb")
			data := []byte("verified package fixture")
			if err := os.WriteFile(file, data, 0600); err != nil {
				t.Fatal(err)
			}
			// Only apt is replaced: run the real download, checksum, privileged
			// shell body and cleanup without requiring root or installing packages.
			apt := "#!/bin/sh\n[ \"$DPKG_DEB_THREADS_MAX\" = 1 ] || exit 98\n[ \"$1\" = install ] && [ \"$2\" = -y ] && [ -f \"$3\" ] || exit 99\nprintf '%s\\n' serial-apt-reached\nexit " + fmt.Sprint(status) + "\n"
			if err := os.WriteFile(filepath.Join(dir, "apt-get"), []byte(apt), 0700); err != nil {
				t.Fatal(err)
			}
			a := Artifact{Name: "example", File: "fixture.deb", SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
			script := prepareScript(t, PrepareCommands(a, "file://"+file, "", "")[0])
			cmd := exec.Command("sh", "-c", script)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "DPKG_DEB_THREADS_MAX=64")
			output, err := cmd.CombinedOutput()
			got := 0
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				got = exit.ExitCode()
			}
			if got != status || !strings.Contains(string(output), "serial-apt-reached") {
				t.Fatalf("exit=%d, want=%d: %s", got, status, output)
			}
			diagnostic := "package installation failed (exit " + fmt.Sprint(status) + "; serial decompression enabled)"
			if status != 0 && (!strings.Contains(string(output), diagnostic) || !strings.Contains(string(output), "allocation memory limits")) {
				t.Fatalf("missing truthful preparation diagnostic: %s", output)
			}
			if status == 0 && strings.Contains(string(output), "installation failed") {
				t.Fatalf("successful preparation reported failure: %s", output)
			}
		})
	}
}

func TestPrepareProvisionsFreshApplicationDirectory(t *testing.T) {
	for _, file := range []string{"example.tar.xz", "example.tar.gz", "example-linux-x86_64"} {
		a := Artifact{Name: "example", File: file, SHA256: strings.Repeat("a", 64)}
		command := prepareScript(t, PrepareCommands(a, "https://example.test/"+file, "/opt/karakoram/example-1", "runtime")[0])
		provision := "mkdir -p -m 0755 '/opt/karakoram'\nif mkdir -m 0755 '/opt/karakoram/example-1' 2>/dev/null; then\nchown -h 'runtime:runtime' '/opt/karakoram/example-1'\nfi\n"
		if !strings.Contains(command, provision) {
			t.Fatal("fresh application directory must be provisioned with a no-follow, new-leaf-only ownership handoff")
		}
		if strings.Index(command, provision) < strings.Index(command, "sha256sum -c -") || strings.Index(command, provision) > strings.Index(command, "sudo -u 'runtime'") {
			t.Fatal("fresh-directory provisioning must follow verification and precede unprivileged publication")
		}
	}
}

// Undo only the quoting emitted by quote, keeping assertions readable while
// ensuring that the complete script runs in a single privileged shell.
func prepareScript(t *testing.T, command string) string {
	t.Helper()
	const prefix = "sudo sh -c '"
	if !strings.HasPrefix(command, prefix) || !strings.HasSuffix(command, "'") {
		t.Fatal("prepare must use one privileged shell: " + command)
	}
	return strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(command, prefix), "'"), "'\"'\"'", "'")
}

func TestShellQuoting(t *testing.T) {
	got := prepareScript(t, PrepareCommands(Artifact{Name: "example", File: "example", SHA256: "abc"}, "https://example.test/a'b; echo bad", "/opt/has space", "runtime")[0])
	if !strings.Contains(got, `'https://example.test/a'"'"'b; echo bad'`) || !strings.Contains(got, "'/opt/has space'") {
		t.Fatal(got)
	}
}

func TestFilesystemRootArtifact(t *testing.T) {
	a := Artifact{Name: "supabase-pg17-extensions", File: "extensions.tar.gz", SHA256: strings.Repeat("a", 64)}
	command := prepareScript(t, PrepareCommands(a, "https://example.test/extensions.tar.gz", "/", "runtime")[0])
	for _, want := range []string{"python3 -c", legacyPG17Validator, "\"$file\" 'extensions'", "--no-same-owner --no-same-permissions --keep-old-files --delay-directory-restore", "cp -a --no-preserve=ownership unpacked/usr/lib/postgresql/17/. '/usr/lib/postgresql/17/'", "cp -a --no-preserve=ownership unpacked/usr/share/postgresql/17/. '/usr/share/postgresql/17/'"} {
		if !strings.Contains(command, want) {
			t.Fatalf("missing restricted extension installation %q in %s", want, command)
		}
	}
	if strings.Contains(command, "-C '/'") {
		t.Fatal("must not extract into system root")
	}
	if strings.Contains(command, "chown") {
		t.Fatal("filesystem-root artifact must not recursively change ownership")
	}
}

func TestPrepareRejectsUnsafeArchiveMembers(t *testing.T) {
	for _, tc := range []struct {
		name, member, artifact string
		kind                   byte
	}{
		{"absolute", "/etc/unsafe", "example", tar.TypeReg},
		{"parent", "../unsafe", "example", tar.TypeReg},
		{"nested-parent", "app/../../unsafe", "example", tar.TypeReg},
		{"extension-path", "./etc/unsafe", "supabase-pg17-extensions", tar.TypeReg},
		{"extension-symlink", "./usr/lib/postgresql/17/unsafe", "supabase-pg17-extensions", tar.TypeSymlink},
		{"extension-hardlink", "./usr/lib/postgresql/17/unsafe", "supabase-pg17-extensions", tar.TypeLink},
		{"extension-device", "./usr/lib/postgresql/17/unsafe", "supabase-pg17-extensions", tar.TypeChar},
		{"new-extension-absolute", "/etc/unsafe", "fixture-pg17", tar.TypeReg},
		{"new-extension-parent", "../unsafe", "fixture-pg17", tar.TypeReg},
		{"new-extension-nested-parent", "usr/lib/postgresql/17/../../unsafe", "fixture-pg17", tar.TypeReg},
		{"new-extension-path", "./etc/unsafe", "fixture-pg17", tar.TypeReg},
		{"new-extension-symlink", "./usr/lib/postgresql/17/unsafe", "fixture-pg17", tar.TypeSymlink},
		{"new-extension-hardlink", "./usr/lib/postgresql/17/unsafe", "fixture-pg17", tar.TypeLink},
		{"new-extension-device", "./usr/lib/postgresql/17/unsafe", "fixture-pg17", tar.TypeChar},
		{"new-extension-wrong-major", "./usr/lib/postgresql/16/unsafe", "fixture-pg17", tar.TypeReg},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := writeArchiveFixture(t, []tar.Header{{Name: tc.member, Typeflag: tc.kind, Mode: 0644, Linkname: "/etc/unsafe"}})
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			a := Artifact{Name: tc.artifact, File: "fixture.tar.gz", SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
			if tc.artifact == "fixture-pg17" {
				a.Kind, a.PGMajor = "pg-extension", 17
			}
			dest := filepath.Join(t.TempDir(), "install")
			script := prepareScript(t, PrepareCommands(a, "file://"+file, dest, "unused")[0])
			output, err := exec.Command("sh", "-c", script).CombinedOutput()
			if err == nil || (!strings.Contains(string(output), "unsafe archive member") && !strings.Contains(string(output), "unexpected extension archive")) {
				t.Fatalf("unsafe archive not rejected by validation: %v\n%s", err, output)
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatal("rejected archive reached installation")
			}
		})
	}
}

func writeArchiveFixture(t *testing.T, members []tar.Header) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "fixture.tar.gz")
	f, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	for _, member := range members {
		if err := tw.WriteHeader(&member); err != nil {
			t.Fatal(err)
		}
	}
	for _, closer := range []interface{ Close() error }{tw, gz, f} {
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return file
}

func TestArchiveValidatorRealStorageMembersInCLocale(t *testing.T) {
	var fixture struct {
		Artifact string
		SHA256   string
		Members  []struct{ Name, Linkname, Type string }
	}
	data, err := os.ReadFile("testdata/storage-members.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	a, err := Get("supabase-storage")
	if err != nil || fixture.Artifact != a.File || fixture.SHA256 != a.SHA256 {
		t.Fatal("Storage member fixture must identify the actual locked archive", err)
	}
	var members []tar.Header
	for _, member := range fixture.Members {
		members = append(members, tar.Header{Name: member.Name, Linkname: member.Linkname, Typeflag: member.Type[0], Mode: 0755})
	}
	file := writeArchiveFixture(t, members)
	cmd := exec.Command("python3", "-c", archiveValidator, file, "application")
	cmd.Env = append(os.Environ(), "LC_ALL=C", "PYTHONCOERCECLOCALE=0", "PYTHONUTF8=0")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("real Storage Unicode names and internal relative symlinks rejected in C locale: %v\n%s", err, output)
	}
}

func TestArchiveValidatorLinkBoundaries(t *testing.T) {
	regular := tar.Header{Name: "app/file", Typeflag: tar.TypeReg, Mode: 0644}
	for _, tc := range []struct {
		name    string
		members []tar.Header
		allowed bool
	}{
		{"internal-symlink", []tar.Header{regular, {Name: "app/sub/link", Typeflag: tar.TypeSymlink, Linkname: "../file"}}, true},
		{"internal-hardlink", []tar.Header{regular, {Name: "copy", Typeflag: tar.TypeLink, Linkname: "app/file"}}, true},
		{"absolute-symlink", []tar.Header{{Name: "app/link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}}, false},
		{"escaping-symlink", []tar.Header{{Name: "app/link", Typeflag: tar.TypeSymlink, Linkname: "../../outside"}}, false},
		{"escaping-hardlink", []tar.Header{{Name: "app/link", Typeflag: tar.TypeLink, Linkname: "../outside"}}, false},
		{"symlink-ancestor", []tar.Header{{Name: "alias", Typeflag: tar.TypeSymlink, Linkname: "."}, {Name: "alias/link", Typeflag: tar.TypeSymlink, Linkname: "../outside"}}, false},
		{"symlink-cycle", []tar.Header{{Name: "a", Typeflag: tar.TypeSymlink, Linkname: "b"}, {Name: "b", Typeflag: tar.TypeSymlink, Linkname: "a"}}, false},
		{"hardlink-to-symlink", []tar.Header{regular, {Name: "app/link", Typeflag: tar.TypeSymlink, Linkname: "file"}, {Name: "copy", Typeflag: tar.TypeLink, Linkname: "app/link"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := writeArchiveFixture(t, tc.members)
			output, err := exec.Command("python3", "-c", archiveValidator, file, "application").CombinedOutput()
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v, error=%v\n%s", tc.allowed, err, output)
			}
		})
	}
}

func TestExtensionArtifact(t *testing.T) {
	const name = "test-extension-pg17"
	t.Cleanup(func() { delete(artifacts, name) })
	for _, a := range []Artifact{
		{Name: name},
		{Name: name, Kind: "pg-extension", PGMajor: 16},
		{Name: name, Kind: "application", PGMajor: 17},
		{Name: name, Kind: "pg-extension", PGMajor: 17},
	} {
		artifacts[name] = a
		got, err := ExtensionArtifact("test-extension", 17)
		allowed := a.Kind == "pg-extension" && a.PGMajor == 17
		if (err == nil) != allowed || (allowed && !reflect.DeepEqual(got, a)) {
			t.Fatalf("artifact=%+v, got=%+v, error=%v", a, got, err)
		}
	}
	for _, major := range []int{0, 15, 16, 18, 19} {
		if _, err := ExtensionArtifact("test-extension", major); err == nil {
			t.Fatalf("unavailable major %d accepted", major)
		}
	}
}

func TestPrepareExtensionMajors(t *testing.T) {
	for _, major := range []int{16, 17, 18} {
		t.Run(fmt.Sprint(major), func(t *testing.T) {
			a := Artifact{Name: fmt.Sprintf("example-pg%d", major), Kind: "pg-extension", PGMajor: major, File: "extensions.tar.xz", SHA256: strings.Repeat("a", 64)}
			command := prepareScript(t, PrepareCommands(a, "https://example.test/"+a.File, "/", "runtime")[0])
			for _, want := range []string{
				archiveValidator, fmt.Sprintf("\"$file\" 'pg-extension' '%d'", major),
				"umask 077", "mkdir unpacked", "--keep-old-files",
				fmt.Sprintf("cp -a --no-preserve=ownership unpacked/usr/lib/postgresql/%d/. '/usr/lib/postgresql/%d/'", major, major),
				fmt.Sprintf("cp -a --no-preserve=ownership unpacked/usr/share/postgresql/%d/. '/usr/share/postgresql/%d/'", major, major),
			} {
				if !strings.Contains(command, want) {
					t.Fatalf("missing %q in %s", want, command)
				}
			}
			if strings.Contains(command, "chown") || strings.Contains(command, "-C '/'") || strings.Contains(command, "sudo -u") {
				t.Fatal("extension installation must use restricted root publication without ownership handoff")
			}
		})
	}
}

func TestPrepareExtensionInvalidMetadata(t *testing.T) {
	for _, a := range []Artifact{
		{Kind: "pg-extension"},
		{Kind: "pg-extension", PGMajor: 15},
		{Kind: "pg-extension", PGMajor: 19},
		{Kind: "pg-extension", PGMajor: 17, File: "extension.deb"},
		{Kind: "pg-extension", PGMajor: 17, File: "extension"},
		{PGMajor: 17},
		{Kind: "unknown", PGMajor: 17},
	} {
		script := prepareScript(t, PrepareCommands(a, "https://example.test/", "/", "runtime")[0])
		output, err := exec.Command("sh", "-c", script).CombinedOutput()
		if err == nil || !strings.Contains(string(output), "invalid native artifact") || strings.Contains(script, "curl") {
			t.Fatalf("invalid metadata did not fail closed: %+v: %v\n%s", a, err, output)
		}
	}
}

func TestArchiveValidatorExtensionMajors(t *testing.T) {
	for _, major := range []string{"16", "17", "18"} {
		file := writeArchiveFixture(t, []tar.Header{
			{Name: "usr/lib/postgresql/" + major + "/extension.so", Typeflag: tar.TypeReg, Mode: 0644},
			{Name: "usr/share/postgresql/" + major + "/extension.control", Typeflag: tar.TypeReg, Mode: 0644},
		})
		for _, requested := range []string{"", "0", "15", "16", "17", "18", "19", "../17", "017"} {
			args := []string{"-c", archiveValidator, file, "pg-extension"}
			if requested != "" {
				args = append(args, requested)
			}
			output, err := exec.Command("python3", args...).CombinedOutput()
			if (err == nil) != (requested == major) {
				t.Fatalf("archive PG%s, requested %q: %v\n%s", major, requested, err, output)
			}
			if err != nil && !strings.Contains(string(output), "unsafe archive member") {
				t.Fatalf("validation did not fail closed: %s", output)
			}
		}
	}
}

func TestPrepareExecutableMembers(t *testing.T) {
	member := "bin/plugin's executable"
	file := writeArchiveFixture(t, []tar.Header{{Name: member, Typeflag: tar.TypeReg, Mode: 0644}})
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{Name: "example", File: "fixture.tar.gz", SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), ExecutableMembers: []string{member}}
	dest := t.TempDir()
	command := prepareScript(t, PrepareCommands(a, "file://"+file, dest, "runtime")[0])
	chmod := "chmod 0755 -- " + quote(dest+"/"+member)
	if !strings.Contains(command, strings.ReplaceAll(chmod, "'", "'\"'\"'")) || strings.Index(command, "sudo -u 'runtime'") > strings.Index(command, "chmod 0755") {
		t.Fatal("chmod must be quoted inside unprivileged publication")
	}
	runUnprivilegedPrepare(t, a, file, dest)
	info, err := os.Stat(filepath.Join(dest, member))
	if err != nil || info.Mode().Perm() != 0755 {
		t.Fatalf("executable mode: %v, %v", info, err)
	}
}

func TestPrepareRejectsUnsafeExecutableMembers(t *testing.T) {
	file := writeArchiveFixture(t, []tar.Header{
		{Name: "plugin", Typeflag: tar.TypeReg, Mode: 0644},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "plugin"},
		{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "plugin"},
		{Name: "directory", Typeflag: tar.TypeDir, Mode: 0755},
	})
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{"", ".", "/plugin", "../plugin", "bin/../../plugin", "./plugin", "bin/../plugin", "bin//plugin", "plugin\n", "missing", "link", "hardlink", "directory"} {
		t.Run(fmt.Sprintf("%q", member), func(t *testing.T) {
			a := Artifact{Name: "example", File: "fixture.tar.gz", SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), ExecutableMembers: []string{member}}
			dest := filepath.Join(t.TempDir(), "install")
			script := prepareScript(t, PrepareCommands(a, "file://"+file, dest, "unused")[0])
			output, err := exec.Command("sh", "-c", script).CombinedOutput()
			if err == nil || !strings.Contains(string(output), "unsafe archive member") {
				t.Fatalf("unsafe executable accepted: %v\n%s", err, output)
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatal("rejected executable reached installation")
			}
		})
	}
	for _, a := range []Artifact{
		{Name: "example", File: "example.deb", ExecutableMembers: []string{"plugin"}},
		{Name: "example", File: "example", ExecutableMembers: []string{"plugin"}},
		{Name: "supabase-pg17-extensions", File: "example.tar.gz", ExecutableMembers: []string{"plugin"}},
	} {
		output, err := exec.Command("sh", "-c", PrepareCommands(a, "", "", "")[0]).CombinedOutput()
		if err == nil || !strings.Contains(string(output), "require an application archive") {
			t.Fatalf("nonapplication executable members accepted: %v\n%s", err, output)
		}
	}
}

// Exercise the emitted publication commands as the current unprivileged user.
// The destination already exists, so no ownership handoff is necessary.
func runUnprivilegedPrepare(t *testing.T, a Artifact, file, dest string) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "sudo"), []byte("#!/bin/sh\n[ \"$1\" = '-u' ] || exit 1\nshift 3\nexec \"$@\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	script := prepareScript(t, PrepareCommands(a, "file://"+file, dest, "runtime")[0])
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("prepare: %v\n%s", err, output)
	}
}

// KARAKORAM_OPENBAO_ARTIFACT_DIR names a directory holding the locked upstream
// OpenBao archives; the test is skipped when it is unset.
func TestPrepareRealOpenBaoArchives(t *testing.T) {
	dir := os.Getenv("KARAKORAM_OPENBAO_ARTIFACT_DIR")
	if dir == "" {
		t.Skip("KARAKORAM_OPENBAO_ARTIFACT_DIR unset")
	}
	for _, name := range []string{"openbao", "openbao-plugin-database-mongodb", "openbao-plugin-kms-aws"} {
		t.Run(name, func(t *testing.T) {
			a, err := Get(name)
			if err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, a.File)
			info, err := os.Stat(file)
			if os.IsNotExist(err) {
				t.Fatalf("locked archive %s missing from KARAKORAM_OPENBAO_ARTIFACT_DIR", a.File)
			}
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != a.Size {
				t.Fatalf("cached archive size = %d, want %d", info.Size(), a.Size)
			}
			dest := t.TempDir()
			runUnprivilegedPrepare(t, a, file, dest)
			member := "bao"
			if name != "openbao" {
				member = a.ExecutableMembers[0]
			}
			info, err = os.Stat(filepath.Join(dest, member))
			if err != nil || info.Mode().Perm() != 0755 {
				t.Fatalf("installed %s mode: %v, %v", member, info, err)
			}
		})
	}
}
