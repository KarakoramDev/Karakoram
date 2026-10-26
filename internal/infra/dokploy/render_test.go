// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLayoutPrivateAddressFamily(t *testing.T) {
	if layout.PrivateAddressFamily != infra.AddressFamilyIPv4 {
		t.Fatal("Swarm member DNS requires IPv4 distribution")
	}
}

func renderTestArtifact(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "karakoram", Mode: 0755, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tw, "test"); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func renderTestDeployment(t *testing.T) infra.Deployment {
	return infra.Deployment{
		Artifact: infra.Artifact{Reader: bytes.NewReader(renderTestArtifact(t)), Name: "karakoram.tar.gz", Binary: "karakoram"},
		Runtime:  infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent", "--mode=test"}, Volume: "data-1", Prepare: []string{"printf '%s\\n' 'prepare one'", "sudo mkdir -p /opt/engine && echo ready"}, Env: map[string]string{"Z": "last", "KEY": "first"}, Ports: []infra.Port{{Port: 9443, Protocol: "tcp"}, {Port: 53, Protocol: "udp"}}, Readiness: &infra.Readiness{Args: []string{"agent", "readiness"}, RetryPeriod: 7 * time.Second, FailureTimeout: 90 * time.Second}},
	}
}

func renderTestZip(t *testing.T, d infra.Deployment) (map[string][]byte, string) {
	t.Helper()
	p, _, err := renderDeployment(d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(p) })
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0600 {
		t.Fatalf("archive permissions = %o", st.Mode().Perm())
	}
	zr, err := zip.OpenReader(p)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	files := map[string][]byte{}
	var names []string
	for _, f := range zr.File {
		names = append(names, f.Name)
		if !f.Modified.Equal(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("%s timestamp = %v", f.Name, f.Modified)
		}
		wantMode := os.FileMode(0644)
		if f.Name == "karakoram-entrypoint" {
			wantMode = 0755
		}
		if f.Name == d.Artifact.Name {
			wantMode = 0600
		}
		if f.Mode().Perm() != wantMode {
			t.Errorf("%s permissions = %o", f.Name, f.Mode().Perm())
		}
		r, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		files[f.Name], err = io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("unsorted zip: %v", names)
	}
	return files, p
}

func TestRenderDockerfile(t *testing.T) {
	d := renderTestDeployment(t)
	files, _ := renderTestZip(t, d)
	if len(files) != 4 {
		t.Fatalf("zip entries = %d", len(files))
	}
	df := string(files["Dockerfile"])
	ordered := []string{"FROM " + baseImage, `SHELL ["/bin/bash","-o","pipefail","-c"]`, "apt-get install -y --no-install-recommends ca-certificates sudo curl gnupg", "USER karakoram\nWORKDIR /opt/karakoram\n", "RUN " + renderJSON([]string{"/bin/bash", "-o", "pipefail", "-c", d.Runtime.Prepare[0]}) + "\n", "RUN " + renderJSON([]string{"/bin/bash", "-o", "pipefail", "-c", d.Runtime.Prepare[1]}) + "\n", "RUN mkdir -p '/var/lib/karakoram/volumes/data-1'", `COPY --chown=karakoram:karakoram ["karakoram.tar.gz","/tmp/karakoram-artifact.tar.gz"]`, "RUN tar -xzf", `COPY --chown=karakoram:karakoram --chmod=0644 ["karakoram.env","/opt/karakoram/karakoram.env"]`, "STOPSIGNAL SIGTERM", "EXPOSE 9443/tcp", "EXPOSE 53/udp", "ENTRYPOINT ", "CMD "}
	rest := df
	for _, s := range ordered {
		i := strings.Index(rest, s)
		if i < 0 {
			t.Fatalf("missing/out-of-order %q in:\n%s", s, df)
		}
		rest = rest[i+len(s):]
	}
	if strings.Contains(df, "ENV DEBIAN_FRONTEND") {
		t.Fatal("build-only environment leaked into image")
	}
	for prefix, want := range map[string][]string{"ENTRYPOINT ": {"/usr/local/bin/karakoram-entrypoint"}, "CMD ": {"/opt/karakoram/karakoram", "agent", "--mode=test"}} {
		for _, line := range strings.Split(df, "\n") {
			if strings.HasPrefix(line, prefix) {
				var got []string
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s%v", prefix, got)
				}
			}
		}
	}
	if strings.Contains(df, "HEALTHCHECK") {
		t.Fatal("deployment readiness must not gate Swarm DNS or trigger continuous restarts")
	}
	entry := string(files["karakoram-entrypoint"])
	if !strings.Contains(entry, "sudo chown karakoram:karakoram '/var/lib/karakoram/volumes/data-1'") || strings.Contains(entry, "chown -R") || !strings.HasSuffix(entry, "exec \"$@\"\n") {
		t.Fatalf("entrypoint:\n%s", entry)
	}
}

func TestRenderMultilinePrepare(t *testing.T) {
	for _, script := range []string{
		"set -eu\numask 077\nprintf '%s\\n' 'quoted \"value\"' \\\n'continued'",
		"cat <<'EOF'\nquoted \"value\"\ncontinued\nEOF",
	} {
		d := renderTestDeployment(t)
		d.Runtime.Prepare = []string{script}
		dockerfile := renderDockerfile(d)
		var args []string
		for _, line := range strings.Split(dockerfile, "\n") {
			if strings.HasPrefix(line, "RUN [") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RUN ")), &args); err != nil {
					t.Fatal(err)
				}
			}
		}
		want := []string{"/bin/bash", "-o", "pipefail", "-c", script}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("prepare arguments = %q, want %q", args, want)
		}
		out, err := exec.Command(args[0], args[1:]...).Output()
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != "quoted \"value\"\ncontinued\n" {
			t.Fatalf("prepare output = %q", out)
		}
	}
}

func TestRenderEnvironmentRoundTrip(t *testing.T) {
	values := []string{"", "spaces before and after ", "a'b\"c", "line one\nline two\n", "$HOME ${PATH} `whoami` \\ end", "-----BEGIN CERTIFICATE-----\nabc'123\n-----END CERTIFICATE-----\n"}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			d := renderTestDeployment(t)
			d.Runtime.Env["KEY"] = value
			files, _ := renderTestZip(t, d)
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "karakoram.env"), files["karakoram.env"], 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("sh", "-c", `. ./karakoram.env; printf %s "$KEY"`)
			cmd.Dir = dir
			got, err := cmd.Output()
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != value {
				t.Fatalf("round trip = %q, want %q", got, value)
			}
			if !bytes.HasPrefix(files["karakoram.env"], []byte("export KEY=")) {
				t.Fatal("env keys not sorted")
			}
		})
	}
}

func TestRenderRejectedAndCleanup(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	cases := map[string]func(*infra.Deployment){
		"invalid":           func(d *infra.Deployment) { d.Runtime.Args = nil },
		"unknown base":      func(d *infra.Deployment) { d.Runtime.Base = "other" },
		"bad env":           func(d *infra.Deployment) { d.Runtime.Env["BAD-NAME"] = "value" },
		"nul env":           func(d *infra.Deployment) { d.Runtime.Env["KEY"] = "a\x00b" },
		"archive path":      func(d *infra.Deployment) { d.Artifact.Name = "../outside" },
		"volume path":       func(d *infra.Deployment) { d.Runtime.Volume = "../outside" },
		"context collision": func(d *infra.Deployment) { d.Artifact.Name = "Dockerfile" },
		"bad archive":       func(d *infra.Deployment) { d.Artifact.Reader = strings.NewReader("not gzip") },
		"missing binary":    func(d *infra.Deployment) { d.Artifact.Binary = "missing" },
		"invalid readiness": func(d *infra.Deployment) { d.Runtime.Readiness.RetryPeriod = time.Millisecond },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			d := renderTestDeployment(t)
			mutate(&d)
			p, _, err := renderDeployment(d)
			var rejected *infra.RejectedError
			if p != "" || !errors.Is(err, infra.ErrRejected) || !errors.As(err, &rejected) {
				t.Fatalf("path=%q error=%v", p, err)
			}
			entries, err := os.ReadDir(os.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("temporary files leaked: %v", entries)
			}
		})
	}
}

func TestRenderDeterministicAndOptionalFields(t *testing.T) {
	d := renderTestDeployment(t)
	d.Runtime.Readiness = nil
	d.Runtime.Volume = ""
	d.Runtime.Ports = nil
	files, p1 := renderTestZip(t, d)
	d.Artifact.Reader = bytes.NewReader(renderTestArtifact(t))
	_, p2 := renderTestZip(t, d)
	a, err := os.ReadFile(p1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("identical inputs produced different zip bytes")
	}
	if strings.Contains(string(files["Dockerfile"]), "HEALTHCHECK") || strings.Contains(string(files["Dockerfile"]), "EXPOSE") || strings.Contains(string(files["karakoram-entrypoint"]), "chown") {
		t.Fatal("optional fields rendered when absent")
	}
}

type renderBoundedReader struct {
	r   io.Reader
	max int
}

func (r *renderBoundedReader) Read(p []byte) (int, error) {
	if len(p) > r.max {
		return 0, errors.New("oversized artifact read")
	}
	return r.r.Read(p)
}

func TestRenderStreamsLargeArtifact(t *testing.T) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		gz := gzip.NewWriter(pw)
		tw := tar.NewWriter(gz)
		const size = 64 << 20
		err := tw.WriteHeader(&tar.Header{Name: "karakoram", Mode: 0755, Size: size})
		if err == nil {
			_, err = io.CopyN(tw, renderZeroReader{}, size)
		}
		if closeErr := tw.Close(); err == nil {
			err = closeErr
		}
		if closeErr := gz.Close(); err == nil {
			err = closeErr
		}
		pw.CloseWithError(err)
		done <- err
	}()
	d := renderTestDeployment(t)
	d.Artifact.Reader = &renderBoundedReader{r: pr, max: 32 << 10}
	p, _, err := renderDeployment(d)
	pr.Close()
	if p != "" {
		defer os.Remove(p)
	}
	producerErr := <-done
	if err != nil {
		t.Fatal(err)
	}
	if producerErr != nil {
		t.Fatal(producerErr)
	}
}

type renderZeroReader struct{}

func (renderZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }
