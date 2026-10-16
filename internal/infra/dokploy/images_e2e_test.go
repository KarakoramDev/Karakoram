//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// TestEngineImages builds the real release binary and all four engine images
// through the Docker CLI. Set DOKPLOY_IMAGE_BUILD=1 and select the Swarm manager
// using DOCKER_HOST or the active Docker context.
func TestEngineImages(t *testing.T) {
	if os.Getenv("DOKPLOY_IMAGE_BUILD") != "1" {
		t.Skip("set DOKPLOY_IMAGE_BUILD=1 to build engine images")
	}
	dir := t.TempDir()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "karakoram")
	build := exec.Command("go", "build", "-o", binary, "./cmd/karakoram")
	build.Dir = root
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release: %v\n%s", err, out)
	}
	archive := filepath.Join(dir, "karakoram.tar.gz")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	bf, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	st, err := bf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err = tw.WriteHeader(&tar.Header{Name: "karakoram", Mode: 0755, Size: st.Size()}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(tw, bf); err != nil {
		t.Fatal(err)
	}
	bf.Close()
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range engineImageSpecs(t) {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.Open(archive)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			zipPath, err := renderDeployment(infra.Deployment{Artifact: infra.Artifact{Reader: f, Name: engine.ArtifactName, Binary: engine.ArtifactBinary}, Runtime: tc.spec})
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(zipPath)
			contextDir := t.TempDir()
			extractEngineImageContext(t, zipPath, contextDir)
			image := fmt.Sprintf("karakoram-engine-image-test:%s-%x", tc.name, time.Now().UnixNano())
			run := func(args ...string) string {
				t.Helper()
				out, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput()
				if err != nil {
					t.Fatalf("docker command failed: %v\n%s", err, out)
				}
				return strings.TrimSpace(string(out))
			}
			start := time.Now()
			out := run("build", "--progress=plain", "--tag", image, contextDir)
			t.Cleanup(func() {
				out, err := exec.Command("docker", "image", "rm", image).CombinedOutput()
				if err != nil {
					t.Errorf("remove engine test image: %v\n%s", err, out)
				}
			})
			t.Logf("docker build %s:\n%s", tc.name, out)
			elapsed := time.Since(start)
			version := run("run", "--rm", image, "/opt/karakoram/karakoram", "version")
			t.Logf("karakoram version: %s", version)
			probe := run("run", "--rm", image, "/bin/sh", "-c", tc.probe)
			t.Logf("engine probe: %s", probe)
			size := run("image", "inspect", "--format", "{{.Size}}", image)
			t.Logf("ENGINE_IMAGE_RESULT name=%s duration=%s bytes=%s", tc.name, elapsed.Round(time.Millisecond), size)
		})
	}
}

func extractEngineImageContext(t *testing.T, archive, dir string) {
	t.Helper()
	zr, err := zip.OpenReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	for _, entry := range zr.File {
		if filepath.Base(entry.Name) != entry.Name || !entry.Mode().IsRegular() {
			t.Fatal("rendered image context contains an unexpected entry")
		}
		src, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		dst, err := os.OpenFile(filepath.Join(dir, entry.Name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, entry.Mode().Perm())
		if err != nil {
			src.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		readErr := src.Close()
		if copyErr != nil {
			t.Fatal(copyErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
	}
}

type engineImageCase struct {
	name  string
	spec  infra.RuntimeSpec
	probe string
}

func engineImageSpecs(t *testing.T) []engineImageCase {
	t.Helper()
	cases := []engineImageCase{
		{"psmdb", (engine.AgentRuntime{Cluster: "imagetest", Member: "mongo1", Volume: "mongodata", Prepare: psmdb.PrepareCommands("8.0.32-14"), Ports: []infra.Port{{Port: 27017, Protocol: "tcp"}}, Env: map[string]string{"PERCONA_TELEMETRY_DISABLE": "1"}}).Spec(layout), "mongod --version && mongos --version && pbm version"},
		{"rabbitmq", (engine.AgentRuntime{Cluster: "imagetest", Member: "rabbit1", Engine: "rabbitmq", Volume: "rabbitdata", Prepare: rabbitmq.PrepareCommands("4.3.6"), Ports: []infra.Port{{Port: 5672, Protocol: "tcp"}, {Port: 5671, Protocol: "tcp"}, {Port: 15672, Protocol: "tcp"}, {Port: 15671, Protocol: "tcp"}, {Port: 15692, Protocol: "tcp"}, {Port: 4369, Protocol: "tcp"}, {Port: 25672, Protocol: "tcp"}}, ReadinessArgs: []string{"--engine=rabbitmq", "--data-dir=" + layout.VolumePath("rabbitdata")}}).Spec(layout), "test -x /usr/sbin/rabbitmq-server && command -v rabbitmq-server && command -v erl"},
	}
	for _, component := range []string{"elasticsearch", "kibana"} {
		volume := ""
		if component == "elasticsearch" {
			volume = "elasticdata"
		}
		spec, err := elastic.RuntimeSpec(layout, elastic.RuntimeInput{Hostname: component + "1", Cluster: "imagetest", Component: component, Version: "9.5.5", Volume: volume})
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, engineImageCase{component, spec, "test -x /opt/elastic/" + component + "-9.5.5/bin/" + component + " && ls -ld /opt/elastic/" + component + "-9.5.5"})
	}
	return cases
}
