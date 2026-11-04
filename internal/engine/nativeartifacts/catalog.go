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
	Name string `json:"name"`
	// Kind selects restricted installation for PostgreSQL extension archives.
	Kind        string `json:"kind,omitempty"`
	PGMajor     int    `json:"pgMajor,omitempty"`
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
	// ExecutableMembers lists regular archive files to chmod 0755 after
	// extraction as the runtime owner. Paths must be canonical and relative.
	// Like Extensions, the embedded slice is read-only.
	ExecutableMembers []string `json:"executableMembers,omitempty"`
}

//go:embed lock.json
var lock []byte

//go:embed validate_archive.py
var archiveValidator string

// Frozen for deployed application and Supabase prepare-command contracts.
// New extension artifacts use archiveValidator and explicit kind/major metadata.
//
//go:embed validate_archive_pg17.py
var legacyPG17Validator string
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

// ExtensionArtifact returns a locked per-major extension artifact. Recipes
// alone never establish availability; the lock must declare the matching ABI.
func ExtensionArtifact(product string, major int) (Artifact, error) {
	a, err := Get(fmt.Sprintf("%s-pg%d", product, major))
	if err != nil {
		return Artifact{}, err
	}
	if a.Kind != "pg-extension" || a.PGMajor != major || !validPGMajor(major) {
		return Artifact{}, fmt.Errorf("native artifact %q is not a PostgreSQL %d extension artifact", a.Name, major)
	}
	return a, nil
}

func validPGMajor(major int) bool { return major == 16 || major == 17 || major == 18 }

// The pre-profile bundle has no kind/major fields in its lock entry. Keep its
// original validator invocation and bytes so existing members do not redeploy.
func legacyExtensionArtifact(a Artifact) bool {
	return a.Name == "supabase-pg17-extensions" && a.Kind == "" && a.PGMajor == 0
}

// MissingMirrorError is an admission failure for a source build that cannot
// locate a Karakoram-built artifact. Official upstream artifacts need no mirror.
type MissingMirrorError struct{ Name string }

func (e *MissingMirrorError) Error() string {
	return fmt.Sprintf("native artifact %q requires KARAKORAM_ARTIFACT_MIRROR or KARAKORAM_ARTIFACT_RELEASE in a development build", e.Name)
}

var releaseVersion = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`)

// git describe identifies source builds, not published release tags, even though
// its distance/hash suffix also happens to be valid SemVer prerelease syntax.
var describedVersion = regexp.MustCompile(`-[0-9]+-g[0-9a-f]+(?:-dirty)?$`)

// ResolveURL applies the operator mirror, official upstream and release fallback
// in that order. An explicit artifact release overrides only the release fallback.
// Resolution happens in the control plane, before deployment.
func ResolveURL(a Artifact) (string, error) {
	if mirror := strings.TrimRight(os.Getenv("KARAKORAM_ARTIFACT_MIRROR"), "/"); mirror != "" {
		return mirror + "/" + a.File, nil
	}
	if a.UpstreamURL != "" {
		return a.UpstreamURL, nil
	}
	release := os.Getenv("KARAKORAM_ARTIFACT_RELEASE")
	if release != "" {
		if !releaseVersion.MatchString(release) || describedVersion.MatchString(release) {
			return "", fmt.Errorf("native artifact %q: KARAKORAM_ARTIFACT_RELEASE must name a release tag, not a development build", a.Name)
		}
	} else {
		release = version.Version
	}
	if releaseVersion.MatchString(release) && !describedVersion.MatchString(release) {
		return "https://github.com/KarakoramDev/Karakoram/releases/download/" + release + "/" + a.File, nil
	}
	return "", &MissingMirrorError{Name: a.Name}
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

// PrepareCommands downloads and verifies bytes before installing them. destDir
// is the immutable installation root (normally /opt/karakoram/name-version).
// Tar archives preserve their full layout; debs use apt to resolve dependencies.
func PrepareCommands(a Artifact, resolvedURL, destDir, owner string) []string {
	legacy := legacyExtensionArtifact(a)
	isExtension := a.Kind == "pg-extension" || legacy
	major := a.PGMajor
	if legacy {
		major = 17
	}
	isTar := strings.HasSuffix(a.File, ".tar.gz") || strings.HasSuffix(a.File, ".tar.xz")
	if (a.Kind != "" && a.Kind != "pg-extension") || (a.Kind == "" && a.PGMajor != 0) || (isExtension && (!validPGMajor(major) || !isTar)) {
		return []string{"sudo sh -c " + quote("echo 'invalid native artifact kind or PostgreSQL major' >&2\nexit 1")}
	}
	// Keep download, verification and every privileged reopen inside one
	// root-owned private directory, including when prepare runs as a user.
	script := "set -eu\numask 077\nstage=$(mktemp -d /tmp/karakoram-artifact.XXXXXXXXXX)\ntrap 'rm -rf -- \"$stage\"' EXIT HUP INT TERM\ncd \"$stage\"\nset -C\nfile=" + quote(a.File) + "\ntest ! -e \"$file\" && test ! -L \"$file\"\ncurl -fsSL " + quote(resolvedURL) + " > \"$file\"\nprintf '%s\\n' " + quote(a.SHA256+"  "+a.File) + " | sha256sum -c -\n"
	if len(a.ExecutableMembers) != 0 && (!isTar || isExtension) {
		return []string{"printf '%s\\n' 'executable members require an application archive' >&2; exit 1"}
	}
	if isTar {
		kind := "application"
		validator := legacyPG17Validator
		args := ""
		if legacy {
			kind = "extensions"
		} else if isExtension {
			kind = a.Kind
			validator = archiveValidator
			args = fmt.Sprintf(" '%d'", major)
		}
		if len(a.ExecutableMembers) != 0 {
			// Executable-member validation is newer than the frozen validator;
			// only artifacts that declare members use the generic one.
			validator = archiveValidator
			for _, member := range a.ExecutableMembers {
				args += " " + quote(member)
			}
		}
		script += "python3 -c " + quote(validator) + " \"$file\" " + quote(kind) + args + "\n"
	}
	if !strings.HasSuffix(a.File, ".deb") && !isExtension {
		// Fresh hosts may not have an application hierarchy yet. Only grant
		// ownership when mkdir created the leaf; never chown an existing tree
		// or follow a destination symlink when doing this initial handoff.
		script += "mkdir -p -m 0755 " + quote(path.Dir(destDir)) + "\nif mkdir -m 0755 " + quote(destDir) + " 2>/dev/null; then\nchown -h " + quote(owner+":"+owner) + " " + quote(destDir) + "\nfi\n"
	}
	switch {
	case strings.HasSuffix(a.File, ".deb"):
		// dpkg's automatic XZ parallelism budgets against host memory, not
		// necessarily the allocation's cgroup. Serial decoding avoids buffering
		// multiple expanded blocks while keeping apt's dependency/maintainer
		// script semantics. Set this inside the privileged shell: sudo may
		// discard an environment variable set by the caller.
		script += "if DPKG_DEB_THREADS_MAX=1 apt-get install -y \"$stage/$file\"; then\n:\nelse\nstatus=$?\nprintf '%s\\n' \"native artifact $file: package installation failed (exit $status; serial decompression enabled); inspect apt/dpkg diagnostics and allocation memory limits\" >&2\nexit \"$status\"\nfi\n"
	case isExtension:
		// Never extract into / or copy the archive's ancestor directories.
		script += "umask 022\nmkdir unpacked\ntar --no-same-owner --no-same-permissions --keep-old-files --delay-directory-restore -xf \"$file\" -C unpacked\n"
		for _, dir := range []string{fmt.Sprintf("usr/lib/postgresql/%d", major), fmt.Sprintf("usr/share/postgresql/%d", major)} {
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
		for _, member := range a.ExecutableMembers {
			publish += "\nchmod 0755 -- " + quote(destDir+"/"+member)
		}
		script += "sudo -u " + quote(owner) + " -- sh -c " + quote(publish) + " < \"$file\"\n"
	default:
		publish := "set -eu\numask 022\nmkdir -p " + quote(destDir) + "\nbinary=$(mktemp " + quote(path.Join(destDir, ".artifact.XXXXXXXXXX")) + ")\ntrap 'rm -f -- \"$binary\"' EXIT HUP INT TERM\ncat > \"$binary\"\nchmod 0755 \"$binary\"\nmv -fT -- \"$binary\" " + quote(path.Join(destDir, a.Name))
		script += "sudo -u " + quote(owner) + " -- sh -c " + quote(publish) + " < \"$file\"\n"
	}
	return []string{"sudo sh -c " + quote(script)}
}
