// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package agent supervises member processes and the authenticated node API.
package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

// Options configures node identity, its pinned control-plane connection and local storage.
type Options struct {
	ControlPlaneURL, InternalCAPEM, Token, Cluster, Member, DataDir string
	Listen                                                          string
	Engine                                                          string
	Logger                                                          *slog.Logger
}

// OptionsFromEnv loads the member configuration injected by the infrastructure backend.
func OptionsFromEnv() (Options, error) {
	o := Options{ControlPlaneURL: os.Getenv("KARAKORAM_URL"), InternalCAPEM: os.Getenv("KARAKORAM_INTERNAL_CA"), Token: os.Getenv("KARAKORAM_AGENT_TOKEN"), Cluster: os.Getenv("KARAKORAM_CLUSTER"), Member: os.Getenv("KARAKORAM_MEMBER"), DataDir: os.Getenv("KARAKORAM_DATA_DIR"), Listen: ":9443"}
	o.Engine = os.Getenv("KARAKORAM_ENGINE")
	if o.ControlPlaneURL == "" || o.InternalCAPEM == "" || o.Token == "" || o.Cluster == "" || o.Member == "" || o.DataDir == "" {
		return o, errors.New("all KARAKORAM_URL, INTERNAL_CA, AGENT_TOKEN, CLUSTER, MEMBER and DATA_DIR environment variables are required")
	}
	return o, nil
}

// Readiness dispatches the local probe, defaulting to the built-in PSMDB TCP probe.
func Readiness(ctx context.Context, o Options, args []string, engines ...Engine) error {
	if len(args) > 0 && args[0] == "--startup" {
		dir := o.DataDir
		for _, arg := range args[1:] {
			v, ok := strings.CutPrefix(arg, "--data-dir=")
			if !ok {
				return errors.New("startup readiness accepts only --data-dir=<absolute path>")
			}
			dir = v
		}
		return startupReadiness(ctx, dir)
	}
	engine, err := selectEngine(o.Engine, engines)
	if err != nil {
		return err
	}
	if engine != nil {
		return engine.Readiness(ctx, o, args)
	}
	conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", "127.0.0.1:27017")
	if err != nil {
		return err
	}
	return conn.Close()
}

type nodeAgent struct {
	o               Options
	client          *agentapi.Client
	sup             *supervisor
	mu              sync.RWMutex
	applyMu         controlMutex
	pbmMu           sync.Mutex
	c               agentapi.MemberConfig
	controls        localControls
	files           map[string][]byte
	cert            *tls.Certificate
	started         time.Time
	version         string
	engine          Engine
	controlsMu      sync.RWMutex
	controlsErr     error
	lastError       string
	startupErr      error
	acceptedVersion int64
	volume          *agentapi.VolumeStatus
}

func (a *nodeAgent) config() agentapi.MemberConfig { a.mu.RLock(); defer a.mu.RUnlock(); return a.c }
func (a *nodeAgent) reload(ctx context.Context) ([]string, error) {
	c, err := a.client.Config(ctx)
	if err != nil {
		return nil, err
	}
	return a.apply(ctx, c)
}
func (a *nodeAgent) apply(ctx context.Context, c agentapi.MemberConfig) (changes []string, applyErr error) {
	if err := a.applyMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer a.applyMu.Unlock()
	agentInitialization := true
	superseded := false
	defer func() {
		if superseded {
			return
		}
		a.mu.Lock()
		a.lastError = ""
		if applyErr != nil {
			a.lastError = applyErr.Error()
		}
		if agentInitialization {
			a.startupErr = applyErr
		}
		a.mu.Unlock()
	}()
	if c.Cluster != a.o.Cluster || c.Member != a.o.Member {
		return nil, errors.New("configuration identity does not match this member")
	}
	if engineName(c.Engine) != engineName(a.o.Engine) {
		return nil, errors.New("configuration engine does not match this member")
	}
	if c.ConfigVersion < a.acceptedVersion || c.ConfigVersion < a.config().ConfigVersion {
		superseded = true
		if a.o.Logger != nil {
			a.o.Logger.Warn("superseded configuration refused", "version", c.ConfigVersion, "accepted_version", a.acceptedVersion)
		}
		return nil, fmt.Errorf("stale configuration version %d refused", c.ConfigVersion)
	}
	cert, err := tls.X509KeyPair([]byte(c.AgentTLS.CertPEM), []byte(c.AgentTLS.KeyPEM))
	if err != nil {
		return nil, fmt.Errorf("agent TLS: %w", err)
	}
	// Transport recovery must not depend on successful storage or engine recovery.
	a.mu.Lock()
	if a.cert == nil || !slices.EqualFunc(a.cert.Certificate, cert.Certificate, bytes.Equal) {
		a.cert = &cert
	}
	a.mu.Unlock()
	a.acceptedVersion = c.ConfigVersion
	if err := a.volumeSafety(ctx, c); err != nil {
		return nil, err
	}
	if c.Volume != nil {
		volume := *c.Volume
		volume.Initialized = true
		c.Volume = &volume
	}
	if a.controlsErr != nil {
		controls, err := loadControls(a.o.DataDir)
		if err != nil {
			return nil, err
		}
		a.controls, a.controlsErr = controls, nil
	}
	if err := a.saveControls(a.controls); err != nil {
		return nil, fmt.Errorf("persist safety controls: %w", err)
	}
	a.setStartupError(nil)
	agentInitialization = false
	if a.engine != nil {
		return a.applyEngine(ctx, c)
	}
	c.Maintenance = c.Maintenance || a.controls.Maintenance
	old := a.config()
	files, err := renderedFiles(c, a.o.DataDir)
	if err != nil {
		return nil, err
	}
	changes = changedFiles(a.files, files)
	if err = os.MkdirAll(filepath.Join(a.o.DataDir, "db"), 0700); err != nil {
		a.setStartupError(err)
		return nil, err
	}
	for name, data := range files {
		path := filepath.Join(a.o.DataDir, "karakoram", name)
		current, readErr := os.ReadFile(path)
		stat, statErr := os.Lstat(path)
		mode := os.FileMode(0600)
		if name == "keyfile" {
			mode = 0400
		}
		if readErr == nil && statErr == nil && stat.Mode().IsRegular() && stat.Mode().Perm() == mode && bytes.Equal(current, data) {
			continue
		}
		if err = atomicWrite(filepath.Join(a.o.DataDir, "karakoram", name), data, mode); err != nil {
			a.setStartupError(err)
			return nil, err
		}
	}
	if err = cacheConfig(c, a.o.DataDir); err != nil {
		a.setStartupError(err)
		return nil, err
	}
	a.sup.setMaintenance(c.Maintenance)
	engine := "mongod"
	if c.Role == agentapi.RoleRouter {
		engine = "mongos"
	}
	a.sup.configure(engine, processSpec{binary: engine, args: []string{"--config", filepath.Join(a.o.DataDir, "karakoram", engine+".conf")}})
	if old.Role != "" && old.Role != c.Role {
		previous := "mongod"
		if old.Role == agentapi.RoleRouter {
			previous = "mongos"
		}
		if previous != engine {
			if err = a.sup.action(ctx, previous, "stop"); err != nil {
				return nil, err
			}
		}
	}
	oldPBMURI, _ := backupURI(old, a.o.DataDir)
	nextPBMURI, _ := backupURI(c, a.o.DataDir)
	pbmChanged := oldPBMURI != nextPBMURI || !bytes.Equal(a.files["mongodb.pem"], files["mongodb.pem"]) || !bytes.Equal(a.files["ca.pem"], files["ca.pem"])
	if c.PBMEnabled && c.Role != agentapi.RoleRouter {
		uri, err := backupURI(c, a.o.DataDir)
		if err != nil {
			return nil, err
		}
		// Budget changes configure the next launch only. Never restart a
		// possibly-mutating PBM restore just to apply a new Go soft limit.
		a.sup.configure("pbm-agent", pbmProcessSpec(c, uri))
	}
	desired := func(name string) bool {
		if a.controls.Stopped[name] {
			return false
		}
		if c.DesiredProcesses == nil {
			return name == engine || name == "pbm-agent" && c.PBMEnabled && c.Role != agentapi.RoleRouter
		}
		for _, p := range c.DesiredProcesses {
			if p == name {
				return true
			}
		}
		return false
	}
	states, _ := a.sup.snapshot()
	for _, name := range []string{engine, "pbm-agent"} {
		if _, ok := states[name]; !ok {
			continue
		}
		action := "stop"
		if desired(name) && (name != "pbm-agent" || c.PBMEnabled && c.Role != agentapi.RoleRouter) {
			action = "start"
			if old.Role != "" && (name == engine && len(changes) > 0 || name == "pbm-agent" && pbmChanged) {
				action = "restart"
			}
		}
		// Configuration pulls never start or restart children during physical restore.
		if c.Maintenance && action != "stop" {
			continue
		}
		if err = a.sup.action(ctx, name, action); err != nil {
			return changes, fmt.Errorf("apply %s %s: %w", name, action, err)
		}
	}
	a.mu.Lock()
	a.c = c
	a.mu.Unlock()
	a.files = files
	if pbmChanged && old.Role != "" {
		changes = append(changes, "pbm-agent credentials")
	}
	a.o.Logger.Info("member configuration applied", "version", c.ConfigVersion, "changed", changes)
	return changes, nil
}

// Run pulls configuration, supervises member processes and serves the private HTTPS API.
func Run(ctx context.Context, o Options, engines ...Engine) error {
	engine, err := selectEngine(o.Engine, engines)
	if err != nil {
		return err
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Listen == "" {
		o.Listen = ":9443"
	}
	// Do this before starting runtime goroutines or engine children. Go opens
	// its own files CLOEXEC, but the invoking shell's lock FDs may not be.
	if err := closeInheritedFilesOnExec(); err != nil {
		return fmt.Errorf("isolate inherited process descriptors: %w", err)
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: o.ControlPlaneURL, Token: o.Token, CAPEM: []byte(o.InternalCAPEM)})
	if err != nil {
		return err
	}
	defer client.Close()
	a := &nodeAgent{o: o, client: client, sup: newSupervisor(os.Stdout), started: time.Now(), engine: engine}
	a.sup.ownershipDir = filepath.Join(o.DataDir, "karakoram", "processes")
	a.sup.beforeStart = func() error {
		c := a.config()
		if c.Volume == nil {
			if observed := a.volumeObservation(); observed != nil {
				c.Volume = &observed.Identity
			}
		}
		return a.volumeSafety(context.Background(), c)
	}
	a.controls, a.controlsErr = loadControls(o.DataDir)
	if a.controlsErr != nil {
		a.setStartupError(a.controlsErr)
		o.Logger.Error("local safety controls unavailable", "error", a.controlsErr)
		a.sup.setMaintenance(true)
	}
	shutdownProcesses := sync.OnceFunc(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := a.shutdown(shutdown); err != nil {
			o.Logger.Error("child shutdown incomplete", "error", err)
		}
	})
	defer shutdownProcesses()
	// Engine maintenance shares the agent lifetime and stops before native drain.
	if background, ok := engine.(interface {
		RunBackground(context.Context, *Node)
	}); ok {
		backgroundCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			background.RunBackground(backgroundCtx, &Node{a: a})
		}()
		defer func() {
			cancel()
			<-done
		}()
	}
	startup, err := serveStartup(o.DataDir, a.startupHealth)
	if err != nil {
		return fmt.Errorf("agent startup listener: %w", err)
	}
	defer startup.Close()
	data, err := os.ReadFile(filepath.Join(o.DataDir, "karakoram", "config.json"))
	if err == nil {
		var c agentapi.MemberConfig
		if err = json.Unmarshal(data, &c); err == nil {
			_, err = a.apply(ctx, c)
		} else {
			a.setStartupError(err)
		}
		if err != nil {
			o.Logger.Warn("cached configuration unavailable; awaiting authoritative repair", "error", err)
		}
	} else if !os.IsNotExist(err) {
		o.Logger.Warn("cached configuration unreadable; awaiting authoritative repair", "error", err)
		a.setStartupError(err)
	}
	for {
		a.mu.RLock()
		serving := a.cert != nil
		a.mu.RUnlock()
		if serving {
			break
		}
		if _, err = a.reload(ctx); err == nil {
			break
		}
		a.mu.RLock()
		cached := a.cert != nil
		a.mu.RUnlock()
		if cached {
			o.Logger.Warn("using cached configuration", "error", err)
			break
		}
		o.Logger.Warn("waiting for first configuration", "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
		}
	}
	if a.engine == nil {
		commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		output, _ := exec.CommandContext(commandCtx, "mongod", "--version").Output()
		cancel()
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(line, "db version v") {
				a.version = strings.TrimPrefix(line, "db version v")
				break
			}
		}
	}
	listener, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: a.handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		a.mu.RLock()
		defer a.mu.RUnlock()
		return a.cert, nil
	}}}
	failures := make(chan error, 1)
	go func() { failures <- server.Serve(tls.NewListener(listener, server.TLSConfig)) }()
	go func() {
		if _, err := a.reload(ctx); err != nil {
			o.Logger.Warn("configuration pull failed", "error", err)
		}
	}()
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Hand off the database while its DCS is alive, before slow API
			// clients can consume the platform's termination grace period.
			shutdownProcesses()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return server.Shutdown(shutdown)
		case err := <-failures:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ticker.C:
			if _, err := a.reload(ctx); err != nil {
				o.Logger.Warn("configuration pull failed", "error", err)
			}
		}
	}
}

func (a *nodeAgent) handler() http.Handler {
	mux := http.NewServeMux()
	respond := func(w http.ResponseWriter, v any, err error) {
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(v)
	}
	decode := func(w http.ResponseWriter, r *http.Request, v any) error {
		r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
		return json.NewDecoder(r.Body).Decode(v)
	}
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		states, maintenance := a.sup.snapshot()
		var mongoReady *bool
		var mongoError string
		if process := states["mongos"]; process.State == "paused" {
			ready := false
			mongoReady, mongoError = &ready, process.Error
		} else {
			mongoReady, mongoError = a.mongoRouterHealth(r.Context())
		}
		disk, err := diskUsage(a.o.DataDir)
		a.mu.RLock()
		version := a.version
		stateError := a.lastError
		a.mu.RUnlock()
		if err != nil {
			stateError = fmt.Sprintf("data filesystem unavailable: %v; %s", err, stateError)
		}
		certificateReloadOnly := false
		if capability, ok := a.engine.(interface{ SupportsCertificateReloadOnly() bool }); ok {
			certificateReloadOnly = capability.SupportsCertificateReloadOnly()
		}
		respond(w, agentapi.Status{Processes: states, PBMRestoreFence: a.allowedProcess("pbm-agent"), PBMRestoreIdentity: true, CertificateReloadOnly: certificateReloadOnly, Maintenance: maintenance, Disk: disk, EngineVersion: version, ConfigVersion: a.config().ConfigVersion, Error: stateError, MongoReady: mongoReady, MongoError: mongoError, Volume: a.volumeObservation(), HostFingerprint: hostFingerprint(), UptimeSeconds: int64(time.Since(a.started).Seconds()), Resources: containerResources()}, nil)
	})
	mux.HandleFunc("POST /v1/reload", func(w http.ResponseWriter, r *http.Request) {
		changed, err := a.reload(r.Context())
		respond(w, agentapi.ActionResponse{Success: err == nil, Message: strings.Join(changed, ", ")}, err)
	})
	mux.HandleFunc("POST /v1/processes/{process}/{action}", func(w http.ResponseWriter, r *http.Request) {
		name, action := r.PathValue("process"), r.PathValue("action")
		err := a.processActionRequest(r.Context(), agentapi.ProcessAction{Process: name, Action: action, RequestID: r.Header.Get("Idempotency-Key")})
		respond(w, agentapi.ActionResponse{Success: err == nil}, err)
	})
	mux.HandleFunc("POST /v1/processes", func(w http.ResponseWriter, r *http.Request) {
		var v agentapi.ProcessAction
		err := decode(w, r, &v)
		if err == nil {
			err = a.processActionRequest(r.Context(), v)
		}
		out := agentapi.ActionResponse{Success: err == nil}
		if err == nil {
			out, err = a.processActionResult(v)
		}
		respond(w, out, err)
	})
	mux.HandleFunc("POST /v1/maintenance", func(w http.ResponseWriter, r *http.Request) {
		var v agentapi.Maintenance
		err := decode(w, r, &v)
		if err == nil {
			err = a.maintenanceContext(r.Context(), v.Enabled)
		}
		respond(w, agentapi.ActionResponse{Success: err == nil}, err)
	})
	if a.engine == nil {
		mux.HandleFunc("POST /v1/bootstrap", func(w http.ResponseWriter, r *http.Request) {
			var v agentapi.BootstrapRequest
			err := decode(w, r, &v)
			var out agentapi.BootstrapResponse
			if err == nil {
				out, err = a.bootstrap(r.Context(), v)
			}
			respond(w, out, err)
		})
		mux.HandleFunc("POST /v1/pbm", func(w http.ResponseWriter, r *http.Request) {
			var v agentapi.PBMRequest
			err := decode(w, r, &v)
			var out agentapi.PBMResponse
			if err == nil {
				out, err = a.pbm(r.Context(), v)
			}
			respond(w, out, err)
		})
	}
	mux.HandleFunc("GET /v1/logs", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("lines"))
		if n <= 0 {
			n = 100
		}
		respond(w, agentapi.LogsResponse{Lines: a.sup.logTail(r.URL.Query().Get("process"), min(n, 1000))}, nil)
	})
	mux.HandleFunc("GET /v1/metrics", func(w http.ResponseWriter, r *http.Request) { a.writeMetrics(w, r) })
	if a.engine != nil {
		engineMux := http.NewServeMux()
		a.engine.Routes(engineMux, &Node{a: a})
		mux.Handle("/v1/"+a.engine.Name()+"/", engineMux)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !agentapi.AuthenticateBearer(r, a.o.Token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
