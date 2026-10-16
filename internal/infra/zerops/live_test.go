// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build e2e

package zerops

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// TestLiveSmoke exercises a real prebuilt runtime plus Local Storage. It requires
// project-scoped credentials and deletes only its unique kkzt-prefixed services.
func TestLiveSmoke(t *testing.T) {
	token, project := os.Getenv("ZEROPS_TOKEN"), os.Getenv("ZEROPS_PROJECT_ID")
	if token == "" || project == "" {
		t.Skip("ZEROPS_TOKEN and ZEROPS_PROJECT_ID required")
	}
	b, err := New(Options{BaseURL: os.Getenv("ZEROPS_API_URL"), Token: token, ProjectID: project, WaitTimeout: 8 * time.Minute, PollInterval: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var suffix [5]byte
	if _, err = rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "kkzt" + hex.EncodeToString(suffix[:])
	volume := name + "v"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cleanupCancel()
		for _, hostname := range []string{name, volume} {
			for {
				op, e := b.DeleteService(cleanupCtx, hostname)
				if e == nil {
					e = b.waitOp(cleanupCtx, op)
				}
				if e == nil {
					break
				}
				if cleanupCtx.Err() != nil {
					t.Errorf("cleanup %s did not complete", hostname)
					break
				}
				// A failed deploy may still be rolling back; retry cleanup until bounded deadline.
				if sleep(cleanupCtx, 5*time.Second) != nil {
					t.Errorf("cleanup %s deadline", hostname)
					break
				}
			}
		}
		services, e := b.ListServices(cleanupCtx, "")
		if e != nil {
			t.Error("cleanup service list failed")
			return
		}
		for _, s := range services {
			if s.Hostname == name || s.Hostname == volume {
				t.Errorf("cleanup left service %s", s.Hostname)
			}
		}
		t.Logf("cleanup verified by paginated project service list: %s and %s absent (%d other non-system services)", name, volume, len(services))
	})
	artifact := liveArtifact(t)
	op, err := b.ImportServices(ctx, []infra.ServiceSpec{
		{Hostname: volume, Type: "local-storage:single@1", Resources: infra.Resources{MinDisk: 5, MaxDisk: 5}},
		{Hostname: name, Type: "ubuntu@24.04", MinContainers: 1, MaxContainers: 1, Resources: infra.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 0.25, MaxRAM: 0.25, MinDisk: 1, MaxDisk: 1}, Env: map[string]string{"KKZT_SECRET": "smoke-initial"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.waitOp(ctx, op); err != nil {
		t.Fatal(err)
	}
	t.Log("runtime and volume imported without inline build")
	pipeline := fmt.Sprintf(`zerops:
  - setup: %s
    deploy:
      temporaryShutdown: true
      readinessCheck:
        exec:
          command: /var/www/karakoram check
        failureTimeout: "120s"
        retryPeriod: "10s"
    run:
      base: ubuntu@24.04
      volume:
        hostname: %s
      envVariables:
        DATA_DIR: /mnt/%s
      ports:
        - port: 8080
          protocol: tcp
          scheme: http
      start: ./karakoram
`, name, volume, volume)
	f, err := os.Open(artifact)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	op, err = b.Deploy(ctx, name, infra.Deployment{Artifact: f, ZeropsYAML: pipeline, SetupName: name, Filename: "karakoram-zerops-smoke"})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.waitOp(ctx, op); err != nil {
		t.Fatal(err)
	}
	t.Log("static artifact deployed and absolute readiness passed")
	op, err = b.SetResources(ctx, name, infra.Resources{MinRAM: 0.25, MaxRAM: 0.5, MaxDisk: 2})
	if err != nil {
		if api := (*APIError)(nil); errors.As(err, &api) {
			t.Fatalf("runtime autoscaling rejected: %s; validation details: %s", api.Message, api.Meta)
		}
		t.Fatal(err)
	}
	if err = b.waitOp(ctx, op); err != nil {
		t.Fatal(err)
	}
	op, err = b.SetContainerCount(ctx, name, 1, 1)
	if err != nil {
		if api := (*APIError)(nil); errors.As(err, &api) {
			t.Fatalf("horizontal autoscaling rejected: %s; validation details: %s", api.Message, api.Meta)
		}
		t.Fatal(err)
	}
	if err = b.waitOp(ctx, op); err != nil {
		t.Fatal(err)
	}
	op, err = b.SetResources(ctx, volume, infra.Resources{MinDisk: 5, MaxDisk: 6})
	if err != nil {
		if api := (*APIError)(nil); errors.As(err, &api) {
			t.Fatalf("volume autoscaling rejected: %s; validation details: %s", api.Message, api.Meta)
		}
		t.Fatal(err)
	}
	if err = b.waitOp(ctx, op); err != nil {
		t.Fatal(err)
	}
	t.Log("complete runtime autoscaling and volume growth completed")
	if err = b.SetEnvSecrets(ctx, name, map[string]string{"KKZT_SECRET": "smoke-updated", "KKZT_ADDED": "smoke-added"}); err != nil {
		t.Fatal(err)
	}
	s, err := b.require(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	env, err := b.client.Env(ctx, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	for _, item := range env {
		if item.Key == "KKZT_SECRET" && item.Content == "smoke-updated" && item.Sensitive {
			verified++
		}
		if item.Key == "KKZT_ADDED" && item.Content == "smoke-added" && item.Sensitive {
			verified++
		}
	}
	if verified != 2 {
		t.Fatal("secret create/update not reflected by API")
	}
	op, err = b.Restart(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.waitOp(ctx, op); err != nil {
		t.Fatal(err)
	}
	t.Log("secret create/update and runtime restart completed")
	sObserved, err := b.GetService(ctx, volume)
	if err != nil || sObserved == nil || sObserved.Resources.MaxDisk != 6 {
		t.Fatal("volume capacity configuration did not converge")
	}
	containers, err := b.Containers(ctx, name)
	if err != nil || len(containers) == 0 {
		t.Fatal("runtime has no containers")
	}
}
func liveArtifact(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	code := `// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package main
import("fmt";"net/http";"os";"path/filepath";"time")
func main(){
 if len(os.Args)>1&&os.Args[1]=="check"{c:=http.Client{Timeout:3*time.Second};r,e:=c.Get("http://127.0.0.1:8080/");if e!=nil{os.Exit(1)};r.Body.Close();if r.StatusCode!=200{os.Exit(1)};return}
 path:=filepath.Join(os.Getenv("DATA_DIR"),"kkzt-marker");if e:=os.WriteFile(path,[]byte("persistent"),0600);e!=nil{panic("volume not writable")}
 http.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){if _,e:=os.ReadFile(path);e!=nil{http.Error(w,"volume unavailable",503);return};fmt.Fprintln(w,"ready")})
 if e:=http.ListenAndServe(":8080",nil);e!=nil{panic("HTTP server failed")}
}
`
	if err := os.WriteFile(source, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "karakoram")
	cmd := exec.Command("go", "build", "-trimpath", "-o", binary, source)
	// Never forward project credentials to a child build process.
	for _, entry := range os.Environ() {
		key := strings.SplitN(entry, "=", 2)[0]
		if strings.HasPrefix(key, "ZEROPS_") || key == "CGO_ENABLED" || key == "GOOS" || key == "GOARCH" {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("static smoke build failed: %v %s", err, data)
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
	archive := filepath.Join(dir, "artifact.tar.gz")
	out, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
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
	return archive
}
