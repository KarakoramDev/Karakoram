// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package nativeartifacts pins the native programs installed by engine members.
package nativeartifacts

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/version"
)

// Source identifies the immutable upstream source revision.
type Source struct {
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// ExtensionFlags records the runtime mode actually qualified for an extension.
type ExtensionFlags struct {
	Preload             bool     `json:"preload"`
	UnavailableFeatures []string `json:"unavailableFeatures,omitempty"`
}

// Artifact describes verified Linux amd64 bytes, not a floating release channel.
type Artifact struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	File        string `json:"file"`
	SHA256      string `json:"sha256"`
	Size        int64  `json:"size"`
	UpstreamURL string `json:"upstreamURL"`
	License     string `json:"license"`
	Source      Source `json:"source"`
	Build       string `json:"build"`
	// RuntimeDeps names the Ubuntu packages installed in the clean smoke runner.
	RuntimeDeps []string `json:"runtimeDeps,omitempty"`
	// Extensions lists the smoke-qualified extensions in a filesystem-root
	// bundle. The embedded catalog's slice must be treated as read-only.
	Extensions []string `json:"extensions,omitempty"`
	// ExtensionFlags qualifies preload and feature restrictions for Extensions.
	// Like Extensions, the embedded map and its values are read-only.
	ExtensionFlags map[string]ExtensionFlags `json:"extensionFlags,omitempty"`
}

//go:embed lock.json
var lock []byte

//go:embed validate_archive.py
var archiveValidator string

var artifacts = func() map[string]Artifact {
	var entries []Artifact
	if err := json.Unmarshal(lock, &entries); err != nil {
		panic(fmt.Sprintf("native artifacts lock: %v", err))
	}
	result := make(map[string]Artifact, len(entries))
	for _, a := range entries {
		if _, exists := result[a.Name]; exists {
			panic("duplicate native artifact: " + a.Name)
		}
		result[a.Name] = a
	}
	return result
}()

// Get returns the pinned artifact, or an error when the build does not supply it.
func Get(name string) (Artifact, error) {
	a, ok := artifacts[name]
	if !ok {
		return Artifact{}, fmt.Errorf("native artifact %q is unavailable in this build", name)
	}
	return a, nil
}

// MissingMirrorError is an admission failure for a source build that cannot
// locate a Karakoram-built artifact. Official upstream artifacts need no mirror.
type MissingMirrorError struct{ Name string }

func (e *MissingMirrorError) Error() string {
	return fmt.Sprintf("native artifact %q requires KARAKORAM_ARTIFACT_MIRROR in a development build", e.Name)
}

var releaseVersion = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`)

// ResolveURL applies the operator mirror, official upstream and release fallback
// in that order. Resolution happens in the control plane, before deployment.
func ResolveURL(a Artifact) (string, error) {
	if mirror := strings.TrimRight(os.Getenv("KARAKORAM_ARTIFACT_MIRROR"), "/"); mirror != "" {
		return mirror + "/" + a.File, nil
	}
	if a.UpstreamURL != "" {
		return a.UpstreamURL, nil
	}
	if releaseVersion.MatchString(version.Version) {
		return "https://github.com/KarakoramDev/Karakoram/releases/download/" + version.Version + "/" + a.File, nil
	}
	return "", &MissingMirrorError{Name: a.Name}
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

// PrepareCommands downloads and verifies bytes before installing them. destDir
// is the immutable installation root (normally /opt/karakoram/name-version).
// Tar archives preserve their full layout; debs use apt to resolve dependencies.
func PrepareCommands(a Artifact, resolvedURL, destDir, owner string) []string {
	// Keep download, verification and every privileged reopen inside one
	// root-owned private directory, including when prepare runs as a user.
	script := "set -eu\numask 077\nstage=$(mktemp -d /tmp/karakoram-artifact.XXXXXXXXXX)\ntrap 'rm -rf -- \"$stage\"' EXIT HUP INT TERM\ncd \"$stage\"\nset -C\nfile=" + quote(a.File) + "\ntest ! -e \"$file\" && test ! -L \"$file\"\ncurl -fsSL " + quote(resolvedURL) + " > \"$file\"\nprintf '%s\\n' " + quote(a.SHA256+"  "+a.File) + " | sha256sum -c -\n"
	isTar := strings.HasSuffix(a.File, ".tar.gz") || strings.HasSuffix(a.File, ".tar.xz")
	if isTar {
		kind := "application"
		if a.Name == "supabase-pg17-extensions" {
			kind = "extensions"
		}
		script += "python3 -c " + quote(archiveValidator) + " \"$file\" " + quote(kind) + "\n"
	}
	if !strings.HasSuffix(a.File, ".deb") && a.Name != "supabase-pg17-extensions" {
		// Fresh hosts may not have an application hierarchy yet. Only grant
		// ownership when mkdir created the leaf; never chown an existing tree
		// or follow a destination symlink when doing this initial handoff.
		script += "mkdir -p -m 0755 " + quote(path.Dir(destDir)) + "\nif mkdir -m 0755 " + quote(destDir) + " 2>/dev/null; then\nchown -h " + quote(owner+":"+owner) + " " + quote(destDir) + "\nfi\n"
	}
	switch {
	case strings.HasSuffix(a.File, ".deb"):
		script += "apt-get install -y \"$stage/$file\"\n"
	case a.Name == "supabase-pg17-extensions":
		// Never extract into / or copy the archive's ancestor directories.
		script += "umask 022\nmkdir unpacked\ntar --no-same-owner --no-same-permissions --keep-old-files --delay-directory-restore -xf \"$file\" -C unpacked\n"
		for _, dir := range []string{"usr/lib/postgresql/17", "usr/share/postgresql/17"} {
			dest := path.Join(destDir, dir)
			script += "test -d unpacked/" + dir + " && test ! -L unpacked/" + dir + "\nmkdir -p " + quote(dest) + "\ncp -a --no-preserve=ownership unpacked/" + dir + "/. " + quote(dest+"/") + "\n"
		}
	case isTar:
		// The runtime owns the final tree and may have changed its paths.
		// Open the protected archive as root, then drop privilege before any
		// destination traversal. Never copy or recursively chown user paths as root.
		extractFlag := "-xzf"
		if strings.HasSuffix(a.File, ".tar.xz") {
			extractFlag = "-xJf"
		}
		publish := "set -eu\numask 022\nmkdir -p " + quote(destDir) + "\ntar --no-same-owner --no-same-permissions --delay-directory-restore " + extractFlag + " - -C " + quote(destDir)
		script += "sudo -u " + quote(owner) + " -- sh -c " + quote(publish) + " < \"$file\"\n"
	default:
		publish := "set -eu\numask 022\nmkdir -p " + quote(destDir) + "\nbinary=$(mktemp " + quote(path.Join(destDir, ".artifact.XXXXXXXXXX")) + ")\ntrap 'rm -f -- \"$binary\"' EXIT HUP INT TERM\ncat > \"$binary\"\nchmod 0755 \"$binary\"\nmv -fT -- \"$binary\" " + quote(path.Join(destDir, a.Name))
		script += "sudo -u " + quote(owner) + " -- sh -c " + quote(publish) + " < \"$file\"\n"
	}
	return []string{"sudo sh -c " + quote(script)}
}
