// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package elastic renders and supervises secured Elastic Stack node processes.
// Files are owner-only and replaced atomically. Restart tokens, not file changes,
// authorize process restarts; explicit stop and maintenance holds take precedence.
package elastic

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

// Host is the node toolkit used by the runner. WriteFile writes below
// DataDir()/karakoram; Running reports a live supervised child, not desired state.
type Host interface {
	DataDir() string
	WriteFile(rel string, data []byte, mode os.FileMode) error
	Configure(name string, p Process) error
	Action(ctx context.Context, name, action string) error
	Running(name string) bool
	Stopped(name string) bool
	Maintenance() bool
	Logger() *slog.Logger
}

// Process describes an installed product process, never an arbitrary command.
type Process struct {
	Binary    string
	Args, Env []string
	Dir       string
}

// Runner implements the Elastic Stack node behaviour independently of transport.
// Home and KeystoreCommand are optional test hooks; production uses the installed
// product home and a bounded exec of its elasticsearch-keystore CLI.
type Runner struct {
	Home            func(component, version string) string
	KeystoreCommand func(ctx context.Context, binary string, args, env []string, stdin io.Reader) error
	mu              sync.Mutex
}

// New returns a runner using installed product binaries.
func New() *Runner { return &Runner{} }

// Name returns the stable engine identifier.
func (*Runner) Name() string { return agentspec.Engine }

// Processes returns all allowed supervised process names.
func (*Runner) Processes() []string {
	return []string{agentspec.ComponentElasticsearch, agentspec.ComponentKibana, agentspec.ComponentLogstash, agentspec.ComponentAPM}
}

// RaiseFileLimit raises the inherited descriptor soft limit to the hard limit.
// Failure is harmless to the agent and logged; Elasticsearch's bootstrap check
// still enforces its own minimum of 65535 descriptors.
func RaiseFileLimit() {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		slog.Warn("reading file descriptor limit failed", "error", err)
		return
	}
	lim.Cur = lim.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		slog.Warn("raising file descriptor limit failed", "error", err)
	} else if lim.Cur < 65535 {
		slog.Warn("file descriptor hard limit below Elasticsearch minimum", "limit", lim.Cur)
	}
}

type applied struct {
	Version      string `json:"version"`
	SecureDigest string `json:"secureDigest,omitempty"`
	RestartToken string `json:"restartToken"`
	Applied      bool   `json:"applied"`
}

func (r *Runner) home(s agentspec.Spec) string {
	if r.Home != nil {
		return r.Home(s.Component, s.Version)
	}
	return agentspec.Home(s.Component, s.Version)
}

// Apply renders one member and starts it when permitted. Only a changed applied
// restart token restarts a running process. Holds retain the previous token so
// the pending restart is applied after the hold is released. Missing product
// homes defer cached configuration until the agent fetches the installed version.
func (r *Runner) Apply(ctx context.Context, h Host, spec agentspec.Spec, cluster, member string) (changes []string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	home := r.home(spec)
	if info, e := os.Stat(home); errors.Is(e, os.ErrNotExist) {
		h.Logger().Warn("Elastic product home missing; waiting for current configuration", "component", spec.Component, "version", spec.Version, "home", home)
		// Clear supervisor intent even for a stopped child: its crash retry or
		// maintenance release must not launch an unavailable cached binary.
		return nil, h.Action(ctx, spec.Component, "stop")
	} else if e != nil {
		return nil, e
	} else if !info.IsDir() {
		return nil, fmt.Errorf("elastic product home %s is not a directory", home)
	}
	dataDir := filepath.Join(h.DataDir(), "karakoram")
	files, proc, err := render(spec, dataDir, home, cluster, member)
	if err != nil {
		return nil, err
	}
	proc.Env = append(proc.Env, "HOME="+filepath.Join(dataDir, "home"), "TMPDIR="+filepath.Join(dataDir, "tmp"))
	statePath := filepath.Join(dataDir, "elastic-applied.json")
	var state applied
	if data, e := os.ReadFile(statePath); e == nil {
		if json.Unmarshal(data, &state) != nil {
			return nil, errors.New("invalid applied Elastic state")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	for _, rel := range []string{"", "data", "logs", "tmp", "home"} {
		if err := privateDir(filepath.Join(dataDir, rel)); err != nil {
			return changes, err
		}
	}
	if spec.Component == agentspec.ComponentElasticsearch || spec.Component == agentspec.ComponentLogstash {
		for _, name := range []string{"jvm.options", "log4j2.properties"} {
			dest := filepath.Join(dataDir, "config", name)
			_, e := os.Stat(dest)
			if state.Version != spec.Version || errors.Is(e, os.ErrNotExist) {
				data, e := os.ReadFile(filepath.Join(home, "config", name))
				if e != nil {
					return changes, fmt.Errorf("reading product baseline %s: %w", name, e)
				}
				files[filepath.Join("config", name)] = data
			} else if e != nil {
				return changes, e
			}
		}
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		changed, e := fileChanged(dataDir, name, files[name], 0600)
		if e == nil && changed {
			e = h.WriteFile(name, files[name], 0600)
		}
		if e != nil {
			return changes, fmt.Errorf("writing %s: %w", name, e)
		}
		if changed {
			changes = append(changes, name)
		}
	}
	if spec.Component == agentspec.ComponentElasticsearch {
		digest := secureDigest(spec.Elasticsearch.SecureSettings)
		_, e := os.Stat(filepath.Join(dataDir, "config", "elasticsearch.keystore"))
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return changes, e
		}
		if digest != state.SecureDigest || errors.Is(e, os.ErrNotExist) {
			if e := r.keystore(ctx, h, home, filepath.Join(dataDir, "config"), spec.Elasticsearch.SecureSettings); e != nil {
				return changes, e
			}
			changes = append(changes, "config/elasticsearch.keystore")
			state.SecureDigest = digest
		}
	}
	state.Version = spec.Version
	if err := h.Configure(spec.Component, proc); err != nil {
		return changes, err
	}
	if !h.Maintenance() && !h.Stopped(spec.Component) {
		action := ""
		if !h.Running(spec.Component) {
			action = "start"
		} else if state.Applied && state.RestartToken != spec.RestartToken {
			action = "restart"
		}
		if action != "" {
			if e := h.Action(ctx, spec.Component, action); e != nil {
				return changes, e
			}
		}
		state.RestartToken, state.Applied = spec.RestartToken, true
	}
	data, e := json.Marshal(state)
	if e != nil {
		return changes, e
	}
	old, e := os.ReadFile(statePath)
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return changes, e
	}
	if !bytes.Equal(old, data) {
		if e := h.WriteFile("elastic-applied.json", data, 0600); e != nil {
			return changes, e
		}
		changes = append(changes, "karakoram/elastic-applied.json")
	}
	return changes, nil
}

func privateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return os.Chmod(path, 0700)
}

func fileChanged(dir, name string, data []byte, mode os.FileMode) (bool, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return false, err
	}
	defer root.Close()
	f, err := root.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	old, err := io.ReadAll(f)
	if err != nil {
		return false, err
	}
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	return !bytes.Equal(old, data) || info.Mode().Perm() != mode, nil
}

func secureDigest(settings map[string]string) string {
	// JSON sorts string map keys and distinguishes boundaries between keys/values.
	if settings == nil {
		settings = map[string]string{}
	}
	data, _ := json.Marshal(settings)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (r *Runner) keystore(ctx context.Context, h Host, home, conf string, settings map[string]string) error {
	dir, err := os.MkdirTemp(conf, ".keystore-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	run := r.KeystoreCommand
	if run == nil {
		run = keystoreCommand
	}
	binary := filepath.Join(home, "bin", "elasticsearch-keystore")
	dataDir := filepath.Join(h.DataDir(), "karakoram")
	env := []string{
		"ES_PATH_CONF=" + dir,
		"HOME=" + filepath.Join(dataDir, "home"),
		"ES_TMPDIR=" + filepath.Join(dataDir, "tmp"),
		"TMPDIR=" + filepath.Join(dataDir, "tmp"),
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := run(ctx, binary, []string{"create"}, env, nil); err != nil {
		return errors.New("creating Elasticsearch keystore failed")
	}
	names := make([]string, 0, len(settings))
	for name := range settings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := run(ctx, binary, []string{"add", "-x", "-f", name}, env, strings.NewReader(settings[name]+"\n")); err != nil {
			return errors.New("adding Elasticsearch secure setting failed")
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "elasticsearch.keystore"))
	if err != nil {
		return err
	}
	return h.WriteFile("config/elasticsearch.keystore", data, 0600)
}

func keystoreCommand(ctx context.Context, binary string, args, env []string, stdin io.Reader) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	// The product uses its bundled JDK; never inherit credentials or JVM flags.
	cmd.Env = append([]string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C.UTF-8"}, env...)
	cmd.Stdin = stdin
	// Product CLI output can include secrets; never include it in errors or logs.
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	return cmd.Run()
}

// Readiness probes local application health, including every configured Logstash pipeline.
func (*Runner) Readiness(ctx context.Context, args []string, pipelines ...agentspec.Pipeline) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("readiness requires component and optional port")
	}
	port, ok := map[string]int{"elasticsearch": 9200, "kibana": 5601, "logstash": 9600, "apm-server": 8200}[args[0]]
	if !ok {
		return errors.New("unknown Elastic component")
	}
	if len(args) == 2 {
		p, err := strconv.Atoi(args[1])
		if err != nil || p < 1 || p > 65535 {
			return errors.New("invalid readiness port")
		}
		port = p
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var err error
	var readinessErr error
	for _, host := range []string{"127.0.0.1", "::1"} {
		if args[0] == agentspec.ComponentLogstash {
			client := http.Client{Transport: &http.Transport{DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			err = logstashReadiness(ctx, &client, "http://"+net.JoinHostPort(host, strconv.Itoa(port)), pipelines)
			if err == nil {
				return nil
			}
			if readinessErr == nil {
				readinessErr = err
			}
			continue
		}
		if args[0] == agentspec.ComponentKibana {
			request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, strconv.Itoa(port))+"/api/status", nil)
			if requestErr != nil {
				return requestErr
			}
			client := http.Client{Transport: &http.Transport{DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			response, requestErr := client.Do(request)
			if requestErr != nil {
				err = requestErr
				continue
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("Kibana is not ready: HTTP %d", response.StatusCode)
			}
			return nil
		}
		var conn net.Conn
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
		if err == nil {
			conn.Close()
			return nil
		}
	}
	if readinessErr != nil {
		return readinessErr
	}
	return fmt.Errorf("%s listener unavailable: %w", args[0], err)
}

func logstashReadiness(ctx context.Context, client *http.Client, base string, pipelines []agentspec.Pipeline) error {
	var loaded struct {
		Pipelines map[string]json.RawMessage `json:"pipelines"`
	}
	if err := getJSON(ctx, client, base+"/_node/pipelines", agentspec.Credentials{}, false, &loaded); err != nil {
		return fmt.Errorf("Logstash pipeline inventory unavailable: %w", err)
	}
	var stats struct {
		Pipelines map[string]*struct {
			Events *json.RawMessage `json:"events"`
		} `json:"pipelines"`
	}
	if err := getJSON(ctx, client, base+"/_node/stats/pipelines", agentspec.Credentials{}, false, &stats); err != nil {
		return fmt.Errorf("Logstash pipeline stats unavailable: %w", err)
	}
	if len(pipelines) == 0 {
		pipelines = []agentspec.Pipeline{{ID: "main"}}
	}
	for _, pipeline := range pipelines {
		if info, ok := loaded.Pipelines[pipeline.ID]; !ok || string(info) == "null" {
			return fmt.Errorf("Logstash pipeline %q is not loaded; check its configuration and startup logs", pipeline.ID)
		}
		if running := stats.Pipelines[pipeline.ID]; running == nil || running.Events == nil {
			return fmt.Errorf("Logstash pipeline %q is not running; check its configuration and startup logs", pipeline.ID)
		}
	}
	return nil
}

// Version returns the payload version only when its installed home exists.
func (r *Runner) Version(_ context.Context, spec agentspec.Spec) string {
	if info, err := os.Stat(r.home(spec)); err == nil && info.IsDir() {
		return spec.Version
	}
	return ""
}
