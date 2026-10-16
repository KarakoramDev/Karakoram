// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build e2e

package dokploy

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
)

func liveBackend(t *testing.T) *Backend {
	t.Helper()
	for _, key := range []string{"DOKPLOY_URL", "DOKPLOY_API_KEY", "DOKPLOY_ENVIRONMENT_ID", "DOKPLOY_REGISTRY_ID"} {
		if os.Getenv(key) == "" {
			t.Skip("DOKPLOY_URL, DOKPLOY_API_KEY, DOKPLOY_ENVIRONMENT_ID and DOKPLOY_REGISTRY_ID are required")
		}
	}
	config := map[string]string{}
	for key, env := range map[string]string{"url": "URL", "network": "NETWORK", "registryId": "REGISTRY_ID", "domainSuffix": "DOMAIN_SUFFIX", "publicIPv4": "PUBLIC_IPV4", "certificateType": "CERTIFICATE_TYPE"} {
		if v := os.Getenv("DOKPLOY_" + env); v != "" {
			config[key] = v
		}
	}
	backend, err := NewFactory().New(infra.Target{Kind: "dokploy", ID: os.Getenv("DOKPLOY_ENVIRONMENT_ID"), Token: os.Getenv("DOKPLOY_API_KEY"), Config: config})
	if err != nil {
		t.Fatal(err)
	}
	return backend.(*Backend)
}
func livePrefix(t *testing.T) string {
	t.Helper()
	var random [5]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	return "kkdt" + hex.EncodeToString(random[:])
}
func liveAwait(ctx context.Context, b *Backend, op infra.Op) error {
	for {
		s, err := b.OpStatus(ctx, op)
		if err != nil {
			return err
		}
		if s.Failed {
			return errors.New("live operation failed: " + s.Message)
		}
		if s.Done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
func liveCleanup(t *testing.T, b *Backend, names []string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		for _, name := range names {
			op, err := b.DeleteService(ctx, name)
			if err == nil {
				err = liveAwait(ctx, b, op)
			}
			if err != nil {
				t.Errorf("cleanup %s: %v", name, err)
			}
		}
	})
}
func artifactFromFile(t *testing.T, path string) infra.Artifact {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return infra.Artifact{Reader: f, Name: "karakoram.tar.gz", Binary: "karakoram"}
}

func TestLiveContract(t *testing.T) {
	_ = liveBackend(t)
	archive := liveArtifact(t)
	contracttest.Run(t, func(t *testing.T) contracttest.Harness {
		b := liveBackend(t)
		prefix := livePrefix(t)
		liveCleanup(t, b, []string{prefix + "runtime", prefix + "volume"})
		return contracttest.Harness{Backend: b, Hostname: func(s string) string { return prefix + s }, Artifact: func() infra.Artifact { return artifactFromFile(t, archive) }, PollInterval: time.Second, Timeout: 12 * time.Minute}
	})
}
func TestLivePlacement(t *testing.T) {
	b := liveBackend(t)
	ns, err := b.nodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(eligibleNodes(ns)) < 3 {
		t.Skip("placement test requires three eligible nodes")
	}
	archive := liveArtifact(t)
	prefix := livePrefix(t)
	var specs []infra.ServiceSpec
	var cleanup []string
	for i := range 3 {
		suffix := string(rune('a' + i))
		runtime, volume := prefix+suffix, prefix+suffix+"v"
		specs = append(specs, infra.ServiceSpec{Hostname: volume, Kind: infra.KindVolume, SpreadGroup: prefix}, infra.ServiceSpec{Hostname: runtime, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Volume: volume, MinContainers: 1, MaxContainers: 1, Resources: infra.Resources{MinRAM: 0.125, MaxRAM: 0.25}})
		cleanup = append(cleanup, runtime)
	}
	for i := range 3 {
		cleanup = append(cleanup, prefix+string(rune('a'+i))+"v")
	}
	liveCleanup(t, b, cleanup)
	op, err := b.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	if err = liveAwait(t.Context(), b, op); err != nil {
		t.Fatal(err)
	}
	placed := map[string]bool{}
	for i := range 3 {
		runtime, volume := prefix+string(rune('a'+i)), prefix+string(rune('a'+i))+"v"
		d := infra.Deployment{Artifact: artifactFromFile(t, archive), Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Volume: volume, Env: map[string]string{"KARAKORAM_DATA_DIR": b.Layout().VolumePath(volume)}, Readiness: &infra.Readiness{Args: []string{"readiness"}, FailureTimeout: time.Minute, RetryPeriod: time.Second}, StopBeforeStart: true}}
		op, err = b.Deploy(t.Context(), runtime, d)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		err = liveAwait(ctx, b, op)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		containers, err := b.Containers(t.Context(), runtime)
		if err != nil || len(containers) != 1 {
			t.Fatalf("placement containers: %d, %v", len(containers), err)
		}
		node := containers[0].PlacementGroup
		if node == "" || placed[node] {
			t.Fatalf("placement is not distinct: %q", node)
		}
		placed[node] = true
	}
	t.Logf("three volume runtimes placed on three distinct authoritative nodes")
}

func liveArtifact(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	code := `package main
import("fmt";"net/http";"os";"path/filepath";"time")
func main(){
 for _,arg:=range os.Args[1:]{if arg=="check"||arg=="readiness"{c:=http.Client{Timeout:time.Second};r,e:=c.Get("http://127.0.0.1:8080/");if e!=nil{os.Exit(1)};r.Body.Close();if r.StatusCode!=200{os.Exit(1)};return}}
 dir:=os.Getenv("KARAKORAM_DATA_DIR");if dir==""{dir="."};if e:=os.MkdirAll(dir,0755);e!=nil{panic("data directory unavailable")};marker:=filepath.Join(dir,"contract-marker");if e:=os.WriteFile(marker,[]byte("persistent"),0600);e!=nil{panic("volume not writable")}
 http.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){fmt.Fprintln(w,"ready")});fmt.Println("contract runtime ready");if e:=http.ListenAndServe(":8080",nil);e!=nil{panic("HTTP server failed")}
}`
	if err := os.WriteFile(source, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "karakoram")
	cmd := exec.Command("go", "build", "-trimpath", "-o", binary, source)
	for _, entry := range os.Environ() {
		key := strings.SplitN(entry, "=", 2)[0]
		if strings.Contains(key, "TOKEN") || strings.Contains(key, "KEY") || strings.Contains(key, "SECRET") || strings.Contains(key, "PASSWORD") || strings.HasPrefix(key, "DOKPLOY_") || strings.HasPrefix(key, "KARAKORAM_") || key == "CGO_ENABLED" || key == "GOOS" || key == "GOARCH" {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
	if _, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("building live test artifact failed")
	}
	file, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "artifact.tar.gz")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(out)
	tw := tar.NewWriter(gz)
	if err = tw.WriteHeader(&tar.Header{Name: "karakoram", Mode: 0755, Size: stat.Size()}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(tw, file); err != nil {
		t.Fatal(err)
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err = out.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
