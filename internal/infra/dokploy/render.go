// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Official multi-architecture index, resolved with docker buildx imagetools
// inspect ubuntu:24.04. To update, resolve that tag again and replace the index
// digest (not an architecture-specific manifest), then run the engine image tests.
const baseImage = "ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55"

var layout = infra.Layout{AppDir: "/opt/karakoram", VolumeRoot: "/var/lib/karakoram/volumes", User: "karakoram", PrivateAddressFamily: infra.AddressFamilyIPv4}
var renderEnvKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var renderFileName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)

// renderDeployment consumes the artifact once, streaming it into a private zip.
// The caller owns the returned file and must remove it after uploading.
func renderDeployment(d infra.Deployment) (zipPath string, err error) {
	if err := d.Validate(); err != nil {
		return "", &infra.RejectedError{Code: "deployment", Message: err.Error()}
	}
	reject := func(message string) (string, error) {
		return "", &infra.RejectedError{Code: "deployment", Message: message}
	}
	if d.Runtime.Base != infra.BaseUbuntu2404 {
		return reject("Dokploy supports only the Ubuntu 24.04 runtime base")
	}
	if !renderFileName.MatchString(d.Artifact.Name) || !renderFileName.MatchString(d.Artifact.Binary) {
		return reject("artifact archive and binary must be simple file names")
	}
	for _, name := range []string{"Dockerfile", "karakoram-entrypoint", "karakoram.env"} {
		if d.Artifact.Name == name {
			return reject("artifact archive conflicts with a build context file")
		}
	}
	if d.Runtime.Volume != "" && (!renderFileName.MatchString(d.Runtime.Volume) || d.Runtime.Volume == "." || d.Runtime.Volume == "..") {
		return reject("volume must be a simple name")
	}
	keys := make([]string, 0, len(d.Runtime.Env))
	for k, v := range d.Runtime.Env {
		if !renderEnvKey.MatchString(k) || strings.ContainsRune(v, 0) {
			return reject("runtime environment requires valid shell names and values without NUL")
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var env strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&env, "export %s=%s\n", k, renderShellQuote(d.Runtime.Env[k]))
	}
	file, err := os.CreateTemp("", "karakoram-dokploy-*.zip")
	if err != nil {
		return "", fmt.Errorf("create deployment archive: %w", err)
	}
	zipPath = file.Name()
	defer func() {
		file.Close()
		if err != nil {
			os.Remove(zipPath)
			zipPath = ""
		}
	}()
	zw := zip.NewWriter(file)
	entries := []struct {
		name string
		mode os.FileMode
		text string
	}{
		{"Dockerfile", 0644, renderDockerfile(d)},
		{"karakoram-entrypoint", 0755, renderEntrypoint(d.Runtime.Volume)},
		{"karakoram.env", 0644, env.String()},
		{d.Artifact.Name, 0600, ""},
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	for _, entry := range entries {
		h := &zip.FileHeader{Name: entry.name, Method: zip.Deflate, Modified: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)}
		if entry.name == d.Artifact.Name {
			h.Method = zip.Store // The tar.gz is already compressed.
		}
		h.SetMode(entry.mode)
		w, createErr := zw.CreateHeader(h)
		if createErr != nil {
			return zipPath, fmt.Errorf("create deployment entry: %w", createErr)
		}
		if entry.name == d.Artifact.Name {
			err = streamRenderArtifact(w, d.Artifact)
		} else {
			_, err = io.WriteString(w, entry.text)
		}
		if err != nil {
			return zipPath, err
		}
	}
	if err = zw.Close(); err != nil {
		return zipPath, fmt.Errorf("finish deployment archive: %w", err)
	}
	if err = file.Close(); err != nil {
		return zipPath, fmt.Errorf("close deployment archive: %w", err)
	}
	return zipPath, nil
}

func streamRenderArtifact(dst io.Writer, artifact infra.Artifact) error {
	// Tee into the zip while checking the tar stream; neither the compressed
	// archive nor its expanded files are retained in memory.
	r := io.TeeReader(artifact.Reader, dst)
	gz, err := gzip.NewReader(r)
	if err != nil {
		return &infra.RejectedError{Code: "artifact", Message: "artifact must be a readable tar.gz archive"}
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	found := false
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &infra.RejectedError{Code: "artifact", Message: "artifact contains an unreadable tar archive"}
		}
		if (h.Name == artifact.Binary || h.Name == "./"+artifact.Binary) && h.Typeflag == tar.TypeReg {
			found = true
		}
	}
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return &infra.RejectedError{Code: "artifact", Message: "artifact contains an unreadable gzip stream"}
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return fmt.Errorf("stream deployment artifact: %w", err)
	}
	if !found {
		return &infra.RejectedError{Code: "artifact", Message: "artifact must contain its binary at the archive root"}
	}
	return nil
}

func renderShellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

func renderJSON(args []string) string {
	b, _ := json.Marshal(args)
	return string(b)
}

func renderDockerfile(d infra.Deployment) string {
	var b strings.Builder
	fmt.Fprintf(&b, "FROM %s\nSHELL [\"/bin/bash\",\"-o\",\"pipefail\",\"-c\"]\n", baseImage)
	// tar/gzip and coreutils are provided by Ubuntu. Engine prepare commands
	// install their own product dependencies; only shared prerequisites go here.
	fmt.Fprintf(&b, "RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates sudo curl gnupg && rm -rf /var/lib/apt/lists/* && groupadd --gid 10001 karakoram && useradd --uid 10001 --gid 10001 --create-home --shell /bin/bash karakoram && printf 'karakoram ALL=(ALL) NOPASSWD:ALL\\n' > /etc/sudoers.d/karakoram && chmod 0440 /etc/sudoers.d/karakoram && mkdir -p %s %s && chown karakoram:karakoram %s %s\n", layout.AppDir, layout.VolumeRoot, layout.AppDir, layout.VolumeRoot)
	fmt.Fprintf(&b, "USER %s\nWORKDIR %s\n", layout.User, layout.AppDir)
	for _, command := range d.Runtime.Prepare {
		fmt.Fprintf(&b, "RUN %s\n", renderJSON([]string{"/bin/bash", "-o", "pipefail", "-c", command}))
	}
	if d.Runtime.Volume != "" {
		fmt.Fprintf(&b, "RUN mkdir -p %s\n", renderShellQuote(layout.VolumePath(d.Runtime.Volume)))
	}
	fmt.Fprintf(&b, "COPY --chown=karakoram:karakoram %s\n", renderJSON([]string{d.Artifact.Name, "/tmp/karakoram-artifact.tar.gz"}))
	fmt.Fprintf(&b, "RUN tar -xzf /tmp/karakoram-artifact.tar.gz -C %s && chmod +x %s && rm /tmp/karakoram-artifact.tar.gz\n", layout.AppDir, path.Join(layout.AppDir, d.Artifact.Binary))
	fmt.Fprintf(&b, "COPY --chown=karakoram:karakoram --chmod=0644 [\"karakoram.env\",\"%s/karakoram.env\"]\n", layout.AppDir)
	b.WriteString("COPY --chown=karakoram:karakoram --chmod=0755 [\"karakoram-entrypoint\",\"/usr/local/bin/karakoram-entrypoint\"]\n")
	// Swarm withholds service DNS/VIP membership while tasks are starting a
	// Docker healthcheck. Engines need that DNS to bootstrap their own readiness,
	// so mapping deployment readiness here would deadlock startup. Continuous
	// healthcheck-triggered restarts also differ from deployment-time readiness.
	// Deployment.Validate still validates the neutral readiness specification.
	b.WriteString("STOPSIGNAL SIGTERM\n")
	for _, port := range d.Runtime.Ports {
		fmt.Fprintf(&b, "EXPOSE %d/%s\n", port.Port, port.Protocol)
	}
	b.WriteString("ENTRYPOINT [\"/usr/local/bin/karakoram-entrypoint\"]\n")
	fmt.Fprintf(&b, "CMD %s\n", renderJSON(append([]string{path.Join(layout.AppDir, d.Artifact.Binary)}, d.Runtime.Args...)))
	return b.String()
}

func renderEntrypoint(volume string) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -eu\n")
	if volume != "" {
		p := renderShellQuote(layout.VolumePath(volume))
		fmt.Fprintf(&b, "if [ \"$(stat -c '%%u:%%g' %s)\" != '10001:10001' ]; then\n    sudo chown karakoram:karakoram %s\nfi\n", p, p)
	}
	fmt.Fprintf(&b, ". %s/karakoram.env\nexec \"$@\"\n", layout.AppDir)
	return b.String()
}
