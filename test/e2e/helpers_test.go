//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const operationTimeout = 35 * time.Minute

// orphanHandoff is the worst-case wait for one member volume that Zerops
// refuses to delete ("still mounted"): Karakoram records it as an orphaned
// platform volume only after 30 minutes of such failures plus one capped
// (10-minute) retry interval. Volumes are removed one at a time, so refused
// volumes add up serially.
const orphanHandoff = 45 * time.Minute

// convergenceTimeout bounds waits that may remove up to two member volumes
// (scale 5 -> 3, member replacement).
const convergenceTimeout = operationTimeout + 2*orphanHandoff

// observedMember includes additive infrastructure observations without coupling
// the suite to the control plane's internal provider or store implementation.
type observedMember struct {
	api.Member
	PublicHost string `json:"publicHost"`
	Resources  *struct {
		CPUCores float64 `json:"cpuCores"`
		MemoryGB float64 `json:"memoryGB"`
		DiskGB   float64 `json:"diskGB"`
	} `json:"resources"`
}

type suite struct {
	t                   *testing.T
	ctx                 context.Context
	client              *api.Client
	root                *api.Client
	base, token, prefix string
	namespace           *string
	keep                bool
	evidenceSeq         int
}

func newSuite(t *testing.T, data bool) *suite {
	t.Helper()
	base, token := os.Getenv("KARAKORAM_E2E_URL"), os.Getenv("KARAKORAM_E2E_TOKEN")
	if base == "" || token == "" {
		t.Skip("set KARAKORAM_E2E_URL and KARAKORAM_E2E_TOKEN for E2E against your deployment")
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		t.Fatal("KARAKORAM_E2E_URL must be a public HTTPS origin (or /api/v1 URL), without credentials/query")
	}
	base = strings.TrimSuffix(strings.TrimRight(base, "/"), "/api/v1")
	prefix := os.Getenv("KARAKORAM_E2E_PREFIX")
	if prefix == "" {
		prefix = "e2e"
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9]{0,3}$`).MatchString(prefix) {
		t.Fatal("KARAKORAM_E2E_PREFIX must be 1-4 lowercase alphanumeric characters, starting with a letter")
	}
	if data {
		requireCIDR(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	t.Cleanup(cancel)
	s := &suite{t: t, ctx: ctx, base: base, token: token, prefix: prefix, keep: os.Getenv("KARAKORAM_E2E_KEEP") == "1"}
	s.client = s.newClient(token, base+"/api/v1", nil)
	s.root = s.newClient(token, base, nil)
	return s
}

// inNamespace shares suite credentials and deadlines while scoping resource helpers.
func (s *suite) inNamespace(namespace string) *suite {
	scoped := *s
	scoped.namespace = ptr(namespace)
	return &scoped
}

func requireCIDR(t *testing.T) string {
	t.Helper()
	v := os.Getenv("KARAKORAM_E2E_ALLOW_CIDR")
	if v == "" {
		t.Skip("set KARAKORAM_E2E_ALLOW_CIDR for public IPv6 MongoDB data checks")
	}
	p, err := netip.ParsePrefix(v)
	if err != nil || !p.Addr().Is6() {
		t.Fatal("KARAKORAM_E2E_ALLOW_CIDR must be the test client's IPv6 CIDR")
	}
	return p.Masked().String()
}

// safeReadRetryTransport retries GET/HEAD requests after transport errors or
// HTTP 502/503/504 responses.
// Unsafe methods are never retried, so mutations are not duplicated.
type safeReadRetryTransport struct{ next http.RoundTripper }

func (o safeReadRetryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return o.next.RoundTrip(r)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for {
		response, err := o.next.RoundTrip(r)
		retry := err != nil || response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusGatewayTimeout
		if !retry || time.Now().After(deadline) || r.Context().Err() != nil {
			return response, err
		}
		if response != nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func (s *suite) newClient(token, base string, h *http.Client) *api.Client {
	s.t.Helper()
	if h == nil {
		h = &http.Client{Timeout: 6 * time.Minute, Transport: safeReadRetryTransport{http.DefaultTransport}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	}
	c, err := api.NewClient(base, api.WithHTTPClient(h), api.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		return nil
	}))
	if err != nil {
		s.t.Fatal(err)
	}
	return c
}

func (s *suite) name() string {
	s.t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		s.t.Fatal(err)
	}
	return s.prefix + hex.EncodeToString(b[:])
}

// expect never prints response bodies: connection/token responses contain secrets.
func (s *suite) expect(out any, codes ...int) func(*http.Response, error) {
	s.t.Helper()
	return func(r *http.Response, err error) {
		s.t.Helper()
		if err != nil {
			s.t.Fatalf("API transport: %s", s.redact(err.Error()))
		}
		defer r.Body.Close()
		ok := r.StatusCode >= 200 && r.StatusCode < 300
		if len(codes) > 0 {
			ok = false
			for _, c := range codes {
				if r.StatusCode == c {
					ok = true
				}
			}
		}
		if !ok {
			s.t.Fatalf("%s %s: unexpected HTTP %d", r.Request.Method, r.Request.URL.Path, r.StatusCode)
		}
		if out != nil {
			if err := json.NewDecoder(r.Body).Decode(out); err != nil {
				s.t.Fatalf("decode API response: %v", err)
			}
		} else {
			_, _ = io.Copy(io.Discard, r.Body)
		}
	}
}

func (s *suite) redact(v string) string { return strings.ReplaceAll(v, s.token, "[REDACTED]") }

// snapshot accepts only non-secret resources (status, backups, operations, metrics).
func (s *suite) snapshot(label string, v any) {
	s.t.Helper()
	dir := os.Getenv("KARAKORAM_E2E_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	dir = filepath.Join(dir, s.t.Name())
	if err := os.MkdirAll(dir, 0700); err != nil {
		s.t.Fatal(err)
	}
	s.evidenceSeq++
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		s.t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, fmt.Sprintf("%04d-%s.json", s.evidenceSeq, label)), b, 0600); err != nil {
		s.t.Fatal(err)
	}
}

func (s *suite) poll(label string, timeout time.Duration, check func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out after %s waiting for %s", timeout, label)
		}
		select {
		case <-s.ctx.Done():
			s.t.Fatalf("%s: %v", label, s.ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
}

func (s *suite) provider() api.Provider {
	var p api.Provider
	s.expect(&p)(s.client.GetProvider(s.ctx, "psmdb", nil))
	return p
}
func (s *suite) version(major string) string {
	p := s.provider()
	for _, v := range p.Versions {
		if v.Major == major && v.Default && v.Status == api.ProviderVersionStatusSupported {
			return v.Version
		}
	}
	var chosen string
	for _, v := range p.Versions {
		if v.Major == major && v.Status == api.ProviderVersionStatusSupported {
			chosen = v.Version
		}
	}
	if chosen == "" {
		s.t.Fatalf("provider does not advertise a supported %s version", major)
	}
	return chosen
}

func smallResources() api.Resources {
	return api.Resources{CpuMode: api.ResourcesCpuModeSHARED, MinCpu: 1, MaxCpu: 1, MinRam: 1, MaxRam: 1, MinDisk: 5, MaxDisk: 5}
}
func (s *suite) spec(replicas int) api.ClusterSpec {
	spec := s.privateSpec(replicas)
	spec.Exposure = &api.Exposure{Public: true, IpType: ptr(api.ExposureIpTypeIPV6), Allowlist: ptr([]string{requireCIDR(s.t)})}
	return spec
}
func (s *suite) privateSpec(replicas int) api.ClusterSpec {
	return api.ClusterSpec{Provider: "psmdb", Version: s.version("8.0"), Topology: api.Topology{Type: api.TopologyTypeReplicaSet}, Components: api.Components{Engine: api.Component{Replicas: replicas, Resources: smallResources()}}, Tls: &api.TLS{Mode: api.TLSModeRequireTLS, Ca: api.TLSCaSelfSigned}, Exposure: &api.Exposure{Public: false}, Monitoring: &api.MonitoringSpec{Enabled: true}}
}
func ptr[T any](v T) *T { return &v }

// clusterWriteEnvelope also exercises the supported GET-spec round trip.
type clusterWriteEnvelope struct {
	Name string          `json:"name,omitempty"`
	Spec api.ClusterSpec `json:"spec"`
}

func (s *suite) clusterBody(body clusterWriteEnvelope) io.Reader {
	s.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		s.t.Fatal(err)
	}
	return bytes.NewReader(data)
}

func (s *suite) create(spec api.ClusterSpec) string {
	name := s.name()
	s.cleanupCluster(name)
	var c api.Cluster
	s.expect(&c, 201)(s.client.CreateClusterWithBody(s.ctx, &api.CreateClusterParams{Namespace: s.namespace}, "application/json", s.clusterBody(clusterWriteEnvelope{Name: name, Spec: spec})))
	s.t.Logf("created cluster=%s generation=%d phase=%s", name, c.Generation, c.Status.Phase)
	s.ready(name)
	return name
}
func (s *suite) cluster(name string) api.Cluster {
	var c api.Cluster
	s.expect(&c)(s.client.GetCluster(s.ctx, name, &api.GetClusterParams{Namespace: s.namespace}))
	return c
}
func (s *suite) members(name string) []observedMember {
	var m struct {
		Items []observedMember `json:"items"`
	}
	s.expect(&m)(s.client.ListClusterMembers(s.ctx, name, &api.ListClusterMembersParams{Namespace: s.namespace}))
	return m.Items
}
func (s *suite) ready(name string) api.Cluster {
	return s.phase(name, api.ClusterStatusPhaseReady)
}
func (s *suite) phase(name string, phase api.ClusterStatusPhase) api.Cluster {
	var result api.Cluster
	s.poll(name+" phase="+string(phase), convergenceTimeout, func() bool {
		result = s.cluster(name)
		s.t.Logf("cluster=%s phase=%s generation=%d observed=%d members=%d", name, result.Status.Phase, result.Generation, result.Status.ObservedGeneration, len(result.Status.Members))
		var ops api.OperationList
		s.expect(&ops)(s.client.ListClusterOperations(s.ctx, name, &api.ListClusterOperationsParams{Namespace: s.namespace, Limit: ptr(20)}))
		for _, op := range ops.Items {
			s.t.Logf("operation=%s type=%s phase=%s", op.Id, op.Type, op.Phase)
		}
		if result.Status.Phase == api.ClusterStatusPhaseError {
			s.snapshot(name+"-error", result.Status)
			s.t.Fatalf("cluster %s entered Error", name)
		}
		done := result.Status.Phase == phase && (phase != api.ClusterStatusPhaseReady || result.Status.ObservedGeneration >= result.Generation)
		if done {
			s.snapshot(name+"-status", result.Status)
		}
		return done
	})
	return result
}
func (s *suite) update(name string, edit func(*api.ClusterSpec)) api.Cluster {
	c := s.cluster(name)
	edit(&c.Spec)
	var changed api.Cluster
	s.expect(&changed)(s.client.ReplaceClusterWithBody(s.ctx, name, &api.ReplaceClusterParams{Namespace: s.namespace}, "application/json", s.clusterBody(clusterWriteEnvelope{Spec: c.Spec})))
	return s.ready(name)
}
func (s *suite) operation(r *http.Response, err error) {
	var op api.Operation
	s.expect(&op, 202)(r, err)
	if op.Id == "" {
		s.t.Fatal("operation response omitted id")
	}
	s.poll("operation "+op.Id, convergenceTimeout, func() bool {
		s.expect(&op)(s.client.GetOperation(s.ctx, op.Id, nil))
		s.t.Logf("operation=%s type=%s phase=%s", op.Id, op.Type, op.Phase)
		if op.Phase == api.OperationPhaseFailed {
			s.snapshot("operation-failed", op)
			s.t.Fatalf("operation %s failed", op.Id)
		}
		if op.Phase == api.OperationPhaseSucceeded {
			s.snapshot("operation", op)
			return true
		}
		return false
	})
}
func (s *suite) cleanupCluster(name string) {
	s.t.Cleanup(func() {
		if s.keep {
			s.t.Logf("KEEP cluster=%s", name)
			return
		}
		s.deleteCluster(name, false)
	})
}
func (s *suite) deleteCluster(name string, keepVolumes bool) {
	s.deleteClusterOptions(name, keepVolumes, false)
}

func (s *suite) deleteClusterOptions(name string, keepVolumes, keepBackups bool) {
	// Cleanup gets its own deadline even if the test's context expired; each
	// member volume may need an orphan handoff.
	budget := operationTimeout + orphanHandoff
	if r, err := s.client.GetCluster(context.Background(), name, &api.GetClusterParams{Namespace: s.namespace}); err == nil {
		var c api.Cluster
		if r.StatusCode == 200 && json.NewDecoder(r.Body).Decode(&c) == nil && len(c.Status.Members) > 1 {
			budget = operationTimeout + time.Duration(len(c.Status.Members))*orphanHandoff
		}
		r.Body.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	r, err := s.client.DeleteCluster(ctx, name, &api.DeleteClusterParams{Namespace: s.namespace, KeepVolumes: ptr(keepVolumes), KeepBackups: ptr(keepBackups)})
	if err != nil {
		s.t.Errorf("delete cluster=%s transport failure", name)
		return
	}
	_, _ = io.Copy(io.Discard, r.Body)
	r.Body.Close()
	if r.StatusCode == 404 {
		return
	}
	if r.StatusCode != 202 {
		s.t.Errorf("delete cluster=%s HTTP %d", name, r.StatusCode)
		return
	}
	for {
		r, err = s.client.GetCluster(ctx, name, &api.GetClusterParams{Namespace: s.namespace})
		if err != nil {
			s.t.Errorf("wait deletion cluster=%s transport failure", name)
			return
		}
		if r.StatusCode == 404 {
			r.Body.Close()
			if keepVolumes {
				s.t.Errorf("suspension removed cluster %s", name)
			}
			s.recordOrphans(ctx, name)
			return
		}
		var c api.Cluster
		err = json.NewDecoder(r.Body).Decode(&c)
		r.Body.Close()
		if err != nil {
			s.t.Errorf("decode deletion status: %v", err)
			return
		}
		s.t.Logf("teardown cluster=%s phase=%s", name, c.Status.Phase)
		if keepVolumes && c.Status.Phase == api.ClusterStatusPhaseSuspended {
			s.snapshot(name+"-suspended", c.Status)
			return
		}
		if c.Status.Phase == api.ClusterStatusPhaseError {
			s.t.Errorf("cluster=%s deletion entered Error", name)
			return
		}
		select {
		case <-ctx.Done():
			s.t.Errorf("cluster=%s deletion timed out", name)
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// recordOrphans logs and snapshots this cluster's volumes that remain after
// deletion; orphaned volumes are tracked separately from cluster lifecycle status.
func (s *suite) recordOrphans(ctx context.Context, cluster string) {
	var list api.OrphanedVolumeList
	r, err := s.client.ListOrphanedVolumes(ctx, &api.ListOrphanedVolumesParams{Namespace: s.namespace})
	if err != nil {
		return
	}
	defer r.Body.Close()
	if r.StatusCode != 200 || json.NewDecoder(r.Body).Decode(&list) != nil {
		return
	}
	var mine []api.OrphanedVolume
	for _, v := range list.Items {
		if v.Cluster == cluster {
			s.t.Logf("cluster=%s orphaned platform volume %s (service %s, attempts %d, last process %s)", cluster, v.Hostname, v.ServiceId, v.Attempts, v.LastProcessId)
			mine = append(mine, v)
		}
	}
	if len(mine) > 0 {
		s.snapshot(cluster+"-orphaned-volumes", mine)
	}
}

func (s *suite) connection(name string) api.Connection {
	var c api.Connection
	s.expect(&c)(s.client.GetClusterConnection(s.ctx, name, &api.GetClusterConnectionParams{Namespace: s.namespace}))
	if c.Connection.Uri == "" {
		s.t.Fatal("connection URI missing")
	}
	return c
}
func credential(c api.Connection, user string) (string, string) {
	for _, v := range c.Credentials {
		if v.Username == user && v.Password != nil {
			return v.Database, *v.Password
		}
	}
	return "", ""
}

func (s *suite) dial(ctx context.Context, name, user, password, db string, useTLS bool) (*mongo.Client, error) {
	var c api.Connection
	r, err := s.client.GetClusterConnection(ctx, name, &api.GetClusterConnectionParams{Namespace: s.namespace})
	if err = readJSON(&c, r, err); err != nil {
		return nil, err
	}
	if password == "" {
		db, password = credential(c, user)
		if password == "" {
			return nil, fmt.Errorf("connection omitted %s credentials", user)
		}
	}
	var cluster api.Cluster
	r, err = s.client.GetCluster(ctx, name, &api.GetClusterParams{Namespace: s.namespace})
	if err = readJSON(&cluster, r, err); err != nil {
		return nil, err
	}
	var hosts []string
	if cluster.Spec.Topology.Type == api.TopologyTypeSharded {
		if c.Connection.PublicHosts != nil {
			hosts = *c.Connection.PublicHosts
		}
	} else {
		var members struct {
			Items []observedMember `json:"items"`
		}
		r, err = s.client.ListClusterMembers(ctx, name, &api.ListClusterMembersParams{Namespace: s.namespace})
		if err = readJSON(&members, r, err); err != nil {
			return nil, err
		}
		for _, m := range members.Items {
			if m.Role == api.MemberRolePrimary && m.PublicHost != "" {
				hosts = []string{m.PublicHost}
				break
			}
		}
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("cluster %s has no public endpoint for primary/router", name)
	}
	opts := options.Client().SetHosts(hosts[:1]).SetAuth(options.Credential{Username: user, Password: password, AuthSource: db}).SetRetryWrites(true).SetServerSelectionTimeout(8 * time.Second).SetConnectTimeout(8 * time.Second).SetWriteConcern(writeconcern.Majority())
	if cluster.Spec.Topology.Type == api.TopologyTypeReplicaSet {
		opts.SetDirect(true)
	}
	if useTLS {
		if c.CaCertificate == nil {
			return nil, fmt.Errorf("connection omitted cluster CA")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(*c.CaCertificate)) {
			return nil, fmt.Errorf("invalid cluster CA")
		}
		opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool})
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

// readJSON is error-returning so the continuous writer never calls Fatal from a goroutine.
func readJSON(out any, r *http.Response, err error) error {
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("API HTTP %d", r.StatusCode)
	}
	return json.NewDecoder(r.Body).Decode(out)
}
func (s *suite) mongo(name string) *mongo.Client {
	return s.mongoAs(name, "databaseAdmin", "", "")
}

// publicConnectRetryable excludes authentication and certificate failures even
// when the driver embeds them in a server-selection timeout.
func publicConnectRetryable(err error) bool {
	var server mongo.ServerError
	message := strings.ToLower(err.Error())
	if stringsAuthentication(err) || (errors.As(err, &server) && (server.HasErrorCode(18) || server.HasErrorCode(13))) ||
		strings.Contains(message, "x509:") || strings.Contains(message, "tls:") {
		return false
	}
	var network *net.OpError
	return mongo.IsNetworkError(err) || mongo.IsTimeout(err) || errors.As(err, &network) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// publicRoutingGrace bounds positive public probes while public endpoints
// become reachable after routing changes.
const publicRoutingGrace = 8 * time.Minute

// retryPublicProbe bounds positive transport/authentication probes and
// idempotent proof reads/writes.
// Each attempt gets a fresh deadline, but never outlives the caller's context.
func retryPublicProbe(ctx context.Context, probe func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, publicRoutingGrace)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempt, cancelAttempt := context.WithTimeout(ctx, 20*time.Second)
		err := probe(attempt)
		cancelAttempt()
		if err == nil {
			return nil
		}
		if !publicConnectRetryable(err) || ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("public probe retry window ended (%v): %w", ctx.Err(), err)
		case <-time.After(10 * time.Second):
		}
	}
}

// retryPublicOperation retries an idempotent database operation after public
// transport failures only. Server maxTimeMS expiry and write errors (including
// write-concern failures) are database answers, not transport failures; they
// end the retry immediately with the original message.
func retryPublicOperation(ctx context.Context, op func(context.Context) error) error {
	return retryPublicProbe(ctx, func(attempt context.Context) error {
		err := op(attempt)
		var command mongo.CommandError
		var write mongo.WriteException
		if (errors.As(err, &command) && command.IsMaxTimeMSExpiredError()) || errors.As(err, &write) {
			return fmt.Errorf("server-side failure: %v", err)
		}
		return err
	})
}

// connectPublic allows public routing to settle after restarts and project-wide
// route synchronization. Only positive probes use this bounded retry; rejection
// probes keep calling dial once so downtime cannot pass as an auth rejection.
func (s *suite) connectPublic(name, user, password, db string, useTLS bool) *mongo.Client {
	s.t.Helper()
	start := time.Now()
	var c *mongo.Client
	err := retryPublicProbe(s.ctx, func(attempt context.Context) error {
		var err error
		c, err = s.dial(attempt, name, user, password, db, useTLS)
		return err
	})
	if err != nil {
		s.t.Fatalf("connect public MongoDB cluster=%s user=%s TLS=%t after %s (restart/public-routing grace up to %s): %s", name, user, useTLS, time.Since(start).Round(time.Second), publicRoutingGrace, s.redact(err.Error()))
	}
	if waited := time.Since(start); waited > 30*time.Second {
		s.t.Logf("public MongoDB cluster=%s reachable after %s", name, waited.Round(time.Second))
	}
	return c
}

// mongoAs connects publicly as user (empty password = revealed credential).
func (s *suite) mongoAs(name, user, password, db string) *mongo.Client {
	s.t.Helper()
	c := s.connectPublic(name, user, password, db, true)
	s.t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return c
}

// put upserts a proof document. The upsert is idempotent, so it shares the
// bounded public-routing retry used by positive connects: a route can drop
// again right after the connect probe succeeded.
func (s *suite) put(name, id, value string) {
	s.t.Helper()
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	err := retryPublicOperation(s.ctx, func(attempt context.Context) error {
		_, err := c.Database("e2e").Collection("proof").UpdateOne(attempt, bson.M{"_id": id}, bson.M{"$set": bson.M{"value": value}}, options.UpdateOne().SetUpsert(true))
		return err
	})
	if err != nil {
		s.t.Fatalf("write proof cluster=%s: %s", name, s.redact(err.Error()))
	}
}

// mongoMember connects to one public member, including secondaries, without
// following the replica set's private advertised hostnames.
func (s *suite) mongoMember(ctx context.Context, name string, member observedMember, user string) *mongo.Client {
	s.t.Helper()
	c := s.connection(name)
	db, password := credential(c, user)
	if password == "" || c.CaCertificate == nil || member.PublicHost == "" {
		s.t.Fatalf("member=%s connection credentials/CA/public endpoint missing", member.Name)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(*c.CaCertificate)) {
		s.t.Fatal("invalid cluster CA")
	}
	start := time.Now()
	var client *mongo.Client
	err := retryPublicProbe(ctx, func(attempt context.Context) error {
		var err error
		client, err = mongo.Connect(options.Client().SetHosts([]string{member.PublicHost}).SetDirect(true).
			SetReadPreference(readpref.Nearest()).SetServerSelectionTimeout(10 * time.Second).SetConnectTimeout(10 * time.Second).
			SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}).
			SetAuth(options.Credential{Username: user, Password: password, AuthSource: db}))
		if err != nil {
			return err
		}
		if err = client.Ping(attempt, readpref.Nearest()); err != nil {
			_ = client.Disconnect(context.Background())
			return err
		}
		return nil
	})
	if err != nil {
		s.t.Fatalf("member=%s public connect user=%s failed after %s (routing grace up to %s): %s", member.Name, user, time.Since(start).Round(time.Second), publicRoutingGrace, s.redact(err.Error()))
	}
	return client
}
func (s *suite) proof(name, id, want string) {
	s.t.Helper()
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	var result struct {
		Value string `bson:"value"`
	}
	err := retryPublicOperation(s.ctx, func(attempt context.Context) error {
		return c.Database("e2e").Collection("proof").FindOne(attempt, bson.M{"_id": id}).Decode(&result)
	})
	if err != nil {
		s.t.Fatalf("read proof %s: %v", id, err)
	}
	if result.Value != want {
		s.t.Fatalf("proof=%s got=%q want=%q", id, result.Value, want)
	}
}
func (s *suite) absent(name, id string) {
	s.t.Helper()
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	err := retryPublicOperation(s.ctx, func(attempt context.Context) error {
		return c.Database("e2e").Collection("proof").FindOne(attempt, bson.M{"_id": id}).Err()
	})
	if err != mongo.ErrNoDocuments {
		s.t.Fatalf("expected proof %s absent, got %v", id, err)
	}
}

func (s *suite) mongoTime(name string) time.Time {
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	var hello struct {
		LocalTime time.Time `bson:"localTime"`
	}
	if err := c.Database("admin").RunCommand(s.ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		s.t.Fatal(err)
	}
	if hello.LocalTime.IsZero() {
		s.t.Fatal("MongoDB hello omitted localTime")
	}
	target := hello.LocalTime.UTC().Truncate(time.Second).Add(time.Second)
	// BSON oplog timestamps have one-second resolution; make subsequent mutations later.
	select {
	case <-s.ctx.Done():
		s.t.Fatal(s.ctx.Err())
	case <-time.After(2 * time.Second):
	}
	return target
}
