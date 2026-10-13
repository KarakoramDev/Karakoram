// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/apiclient"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// Options supplies streams and a config location for embedding and testing.
type Options struct {
	ConfigPath string
	In         io.Reader
	Out, Err   io.Writer
}

type app struct {
	opts                               Options
	server, token, contextName, format string
	namespace                          string
	namespaceExplicit                  bool
	insecure                           bool
	config                             Config
	selected                           string
	client, root                       *apiclient.Client
	http                               *http.Client
	csrf                               string
	cookies                            []string
}

// NewCommand constructs a fresh command tree. ExecuteContext controls waits and SSE.
func NewCommand(o Options) *cobra.Command {
	if o.In == nil {
		o.In = os.Stdin
	}
	if o.Out == nil {
		o.Out = os.Stdout
	}
	if o.Err == nil {
		o.Err = os.Stderr
	}
	a := &app{opts: o}
	root := &cobra.Command{Use: "karactl", Short: "Operate Karakoram clusters and control-plane resources", SilenceUsage: true, SilenceErrors: true}
	root.SetIn(o.In)
	root.SetOut(o.Out)
	root.SetErr(o.Err)
	f := root.PersistentFlags()
	f.StringVar(&a.server, "server", "", "Server URL (overrides KARAKORAM_SERVER and context)")
	f.StringVar(&a.token, "token", "", "API token (prefer KARAKORAM_TOKEN to avoid shell history)")
	f.StringVar(&a.contextName, "context", "", "Named connection context")
	f.StringVarP(&a.namespace, "namespace", "n", "", "Namespace (overrides context; collections default to all)")
	f.StringVarP(&a.format, "output", "o", "table", "Output: table, json, yaml")
	f.StringArrayVar(&a.cookies, "cookie", nil, "Session/OIDC cookie NAME=VALUE (advanced auth flows; not saved)")
	f.StringVar(&a.csrf, "csrf-token", "", "Session CSRF token (advanced auth flows; not saved)")
	f.BoolVar(&a.insecure, "insecure-skip-tls-verify", false, "Disable server certificate verification (unsafe)")
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error { return a.connect(cmd) }
	a.addCommands(root)
	return root
}

func (a *app) connect(cmd *cobra.Command) error {
	if a.format != "table" && a.format != "json" && a.format != "yaml" {
		return fmt.Errorf("unsupported output format %q", a.format)
	}
	path := a.opts.ConfigPath
	if path == "" {
		var err error
		path, err = ConfigPath()
		if err != nil {
			return err
		}
		a.opts.ConfigPath = path
	}
	var err error
	a.config, err = LoadConfig(path)
	if err != nil {
		return err
	}
	a.selected = a.contextName
	if a.selected == "" {
		a.selected = a.config.CurrentContext
	}
	if a.selected == "" {
		a.selected = "default"
	}
	c, exists := a.config.Contexts[a.selected]
	if a.contextName != "" && !exists && cmd.Name() != "login" {
		return fmt.Errorf("context %q does not exist", a.selected)
	}
	a.namespaceExplicit = cmd.Flags().Changed("namespace")
	if !a.namespaceExplicit {
		a.namespace = c.Namespace
	}
	if a.namespace == "" {
		a.namespace = "default"
	}
	server := c.Server
	token := c.Token
	if e := os.Getenv("KARAKORAM_SERVER"); e != "" {
		server = e
	}
	if e := os.Getenv("KARAKORAM_TOKEN"); e != "" {
		token = e
	}
	if cmd.Flags().Changed("server") {
		server = a.server
	}
	if cmd.Flags().Changed("token") {
		token = a.token
	}
	if server == "" {
		return fmt.Errorf("server is required: use --server, KARAKORAM_SERVER, or login to a context")
	}
	u, err := url.Parse(server)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return fmt.Errorf("server must be an http(s) URL without credentials, query or fragment")
	}
	a.server = strings.TrimSuffix(server, "/")
	a.token = token
	// Accept a saved /api/v1 URL while keeping health and Prometheus rooted at the installation base.
	base := strings.TrimSuffix(a.server, "/api/v1")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if a.insecure {
		fmt.Fprintln(a.opts.Err, "WARNING: TLS certificate verification is disabled")
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	} // explicitly requested opt-in
	jar, _ := cookiejar.New(nil)
	a.http = &http.Client{Transport: transport, Jar: jar, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	editor := func(ctx context.Context, r *http.Request) error {
		if a.token != "" {
			r.Header.Set("Authorization", "Bearer "+a.token)
		}
		if a.csrf != "" {
			r.Header.Set("X-Karakoram-CSRF", a.csrf)
		}
		for _, cookie := range a.cookies {
			name, value, ok := strings.Cut(cookie, "=")
			if !ok || name == "" {
				return fmt.Errorf("cookie must be NAME=VALUE")
			}
			r.AddCookie(&http.Cookie{Name: name, Value: value})
		}
		return nil
	}
	a.client, err = apiclient.NewClient(base+"/api/v1", apiclient.WithHTTPClient(a.http), apiclient.WithRequestEditorFn(editor))
	if err != nil {
		return err
	}
	a.root, err = apiclient.NewClient(base, apiclient.WithHTTPClient(a.http), apiclient.WithRequestEditorFn(editor))
	return err
}

func (a *app) request(ctx context.Context, op string, args []string, q map[string]any, body any) (any, error) {
	q = a.namespaceQuery(op, q)
	var b []byte
	var err error
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	resp, err := dispatch(ctx, a.client, a.root, op, args, q, b)
	if err != nil {
		return nil, err
	}
	return decodeResponse(resp)
}

func (a *app) namespaceQuery(op string, q map[string]any) map[string]any {
	collection := op == "listClusters" || op == "listBackupStorages" || op == "listOperations" || op == "listEvents" || op == "streamEvents" || op == "listOrphanedVolumes" || op == "getPrometheusMetrics"
	if collection && !a.namespaceExplicit {
		return q
	}
	if q == nil {
		q = map[string]any{}
	}
	if _, exists := q["namespace"]; !exists {
		q["namespace"] = a.namespace
	}
	return q
}

func decodeResponse(resp *http.Response) (any, error) {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return map[string]any{"location": resp.Header.Get("Location"), "status": resp.StatusCode}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr apiclient.Error
		if json.Unmarshal(b, &apiErr) == nil && apiErr.Message != "" {
			return nil, fmt.Errorf("HTTP %d: %s: %s", resp.StatusCode, apiErr.Code, apiErr.Message)
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	if len(b) == 0 {
		return map[string]any{"status": "ok"}, nil
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		return string(b), nil
	}
	var v any
	if err = json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (a *app) fileBody(cmd *cobra.Command) (map[string]any, error) {
	path, _ := cmd.Flags().GetString("file")
	if path == "" {
		return nil, fmt.Errorf("--file is required (YAML or JSON; '-' reads stdin)")
	}
	var b []byte
	var err error
	if path == "-" {
		b, err = io.ReadAll(a.opts.In)
	} else {
		b, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	var v map[string]any
	if err = yaml.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("input must be an object")
	}
	return v, nil
}

func pollPause(ctx context.Context) error {
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
