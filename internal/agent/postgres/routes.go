// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func observedServerVersion(serverVersion, description string) string {
	if version := parseVersion(description, false); version != "" {
		return version
	}
	// Upstream builds (including Percona Distribution 16) may omit a
	// product revision and append branding to server_version.
	if fields := strings.Fields(serverVersion); len(fields) > 0 {
		return fields[0]
	}
	return ""
}

func tlsClient(s pgspec.Spec) (*http.Client, error) {
	if s.ServerCert == nil {
		return nil, errors.New("internal TLS certificate required")
	}
	cert, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
	if err != nil {
		return nil, errors.New("invalid internal TLS certificate")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(s.CAPEM)) {
		return nil, errors.New("invalid internal CA")
	}
	return &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{cert}}}, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}
func internalRequest(ctx context.Context, s pgspec.Spec, host string, port int, method, path string, input, output any, auth bool) error {
	client, err := tlsClient(s)
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()
	var body io.Reader
	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://"+net.JoinHostPort(host, strconv.Itoa(port))+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if auth {
		req.SetBasicAuth(s.PatroniREST.Username, s.PatroniREST.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("internal PostgreSQL service unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if method == http.MethodPost && path == "/switchover" {
			if resp.StatusCode >= 500 {
				return errors.New("switchover_unavailable")
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return errors.New(classifySwitchoverRejection(string(body)))
		}
		return fmt.Errorf("internal PostgreSQL service returned HTTP %d", resp.StatusCode)
	}
	if output != nil {
		if err = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(output); err != nil {
			return errors.New("invalid internal service response")
		}
	}
	return nil
}
func patroniRequest(ctx context.Context, s pgspec.Spec, method, path string, input, output any) error {
	return internalRequest(ctx, s, localHost(s), pgspec.PortPatroni, method, path, input, output, true)
}
func containsConfig(current, desired any) bool {
	switch d := desired.(type) {
	case map[string]any:
		c, ok := current.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range d {
			if !containsConfig(c[k], v) {
				return false
			}
		}
		return true
	default:
		return fmt.Sprint(current) == fmt.Sprint(desired)
	}
}
func patchDynamic(ctx context.Context, s pgspec.Spec, p paths) error {
	var current map[string]any
	if err := patroniRequest(ctx, s, "GET", "/config", nil, &current); err != nil {
		return err
	}
	desired := dynamicConfig(s, p)
	// DCS is authoritative for cluster parameters only. Null removes retired
	// settings and prevents node-local paths from overriding other members.
	if pg, ok := current["postgresql"].(map[string]any); ok {
		if params, ok := pg["parameters"].(map[string]any); ok {
			next := desired["postgresql"].(map[string]any)["parameters"].(map[string]any)
			for name := range params {
				if _, exists := next[name]; !exists {
					next[name] = nil
				}
			}
		}
	}
	if containsConfig(current, desired) {
		return nil
	}
	return patroniRequest(ctx, s, "PATCH", "/config", desired, nil)
}
func (e *Runner) status(ctx context.Context, s pgspec.Spec, p paths) pgspec.StatusResponse {
	out := pgspec.StatusResponse{
		Role: "starting", CredentialReceipts: true,
		HealthCapabilities: &pgspec.HealthCapabilities{
			LocalEtcd: true, ReplicationSlots: true, WALArchive: true,
			ReplicationDistance: true, PatroniPause: true,
		},
	}
	out.Citus = s.Citus
	out.CitusIsCoordinator = s.Citus != nil && s.Citus.Group == 0
	if s.Etcd != nil && len(s.Etcd.Endpoints) > 0 {
		out.HealthCapabilities.LocalEtcd = false
	}
	out.BootstrapGeneration = appliedBootstrapGeneration(p)
	out.DataRecoveryRequired = dataRecoveryRequired(s, p)
	out.DataMissing = dataMissing(s, p)
	if s.Role == pgspec.RoleProxy {
		out.Role = "proxy"
		c, err := proxyConnect(ctx, s.Proxy)
		if err != nil {
			return out
		}
		defer c.Close(ctx)
		d := net.Dialer{Timeout: time.Second}
		frontend, err := d.DialContext(ctx, "tcp", frontendAddress(s.ListenHost))
		if err == nil {
			out.Running = true
			frontend.Close()
		}
		out.ProxyBackends, err = proxyBackendStatus(ctx, c)
		if out.Running && err == nil && !proxyWriterReady(s.Proxy, out.ProxyBackends) && proxyProbeWriter(ctx, s) {
			out.ProxyBackends, err = proxyBackendStatus(ctx, c)
		}
		out.Running = out.Running && err == nil && proxyWriterReady(s.Proxy, out.ProxyBackends)
		if out.Running && len(s.Proxy.Users) > 0 {
			users, err := queryTable(ctx, c, "SELECT username,active FROM runtime_pgsql_users")
			observed := keyRows(users, "username")
			out.Running = err == nil
			for _, user := range s.Proxy.Users {
				out.Running = out.Running && observed[user.Username]["active"] == "1"
			}
		}
		proxyDiagnostics(ctx, out.ProxyBackends, func(ctx context.Context, sql string) ([]tableRow, error) {
			return queryTable(ctx, c, sql)
		})
		return out
	}
	c, err := localConnect(ctx, s, p)
	if err == nil {
		defer c.Close(ctx)
		out.Running = true
		// Read the running server, not the assigned catalog version. Percona's
		// product release includes a third component absent from server_version.
		var serverVersion, description string
		if c.QueryRow(ctx, "SELECT pg_catalog.current_setting('server_version'), pg_catalog.version()").Scan(&serverVersion, &description) == nil {
			out.Version = observedServerVersion(serverVersion, description)
		}
		_ = c.QueryRow(ctx, "SELECT COALESCE(pg_catalog.row_to_json(ROW(d))->'f1'->>'datlocale',pg_catalog.row_to_json(ROW(d))->'f1'->>'daticulocale',d.datcollate) FROM pg_catalog.pg_database d WHERE d.datname='template1'").Scan(&out.Locale)
		if s.Topology == pgspec.TopologySingle {
			_ = c.QueryRow(ctx, "SELECT COALESCE(pg_catalog.bool_or(pending_restart),false) FROM pg_catalog.pg_settings").Scan(&out.PendingRestart)
		}
		_ = c.QueryRow(ctx, "SELECT pg_catalog.jsonb_build_object('max_connections',pg_catalog.current_setting('max_connections')::bigint,'max_prepared_transactions',pg_catalog.current_setting('max_prepared_transactions')::bigint,'max_locks_per_transaction',pg_catalog.current_setting('max_locks_per_transaction')::bigint,'max_wal_senders',pg_catalog.current_setting('max_wal_senders')::bigint,'max_worker_processes',pg_catalog.current_setting('max_worker_processes')::bigint)").Scan(&out.SharedMemoryParameters)
		out.Role = "standalone"
		var recovery bool
		if c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery(), system_identifier::text FROM pg_catalog.pg_control_system()").Scan(&recovery, &out.SystemID) == nil {
			if usesPatroni(s) {
				out.Role = "primary"
				if recovery {
					out.Role = "replica"
				}
			} else if recovery {
				out.Role = "starting"
			}
		}
		// Patroni startup must not wait for a database before the other DCS
		// members have started. Reconcile scoped ACLs once the local primary
		// is observed, before reporting it ready to the control plane.
		if (out.Role == "primary" || out.Role == "standalone") && s.DataAccess != nil {
			if err := e.refreshDataAccess(ctx, c, s, time.Now()); err != nil {
				out.Running = false
			}
		}
		_ = c.QueryRow(ctx, "SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()").Scan(&out.Timeline)
		out.ArchiveStatusKnown = c.QueryRow(ctx, "SELECT COALESCE(pg_catalog.to_char(last_archived_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),''), COALESCE(last_failed_time > last_archived_time, last_failed_time IS NOT NULL) FROM pg_catalog.pg_stat_archiver").Scan(&out.LastArchivedAt, &out.ArchiveFailing) == nil
		if recovery {
			_ = c.QueryRow(ctx, "SELECT COALESCE(pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_receive_lsn(),pg_catalog.pg_last_wal_replay_lsn()),0)::bigint, EXISTS(SELECT FROM pg_catalog.pg_stat_wal_receiver WHERE status='streaming')").Scan(&out.LagBytes, &out.Streaming)
			_ = c.QueryRow(ctx, "SELECT pg_catalog.pg_last_wal_replay_lsn()::text").Scan(&out.WALPosition)
		} else {
			_ = c.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&out.WALPosition)
			if s.Backup != nil {
				out.RecoverableUntil = e.watermark.recoverableUntil(ctx, c, out.SystemID)
			}
			_ = c.QueryRow(ctx, "SELECT COALESCE(pg_catalog.max(pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),replay_lsn)),0)::bigint, COALESCE(pg_catalog.string_agg(DISTINCT sync_state,','),'') FROM pg_catalog.pg_stat_replication").Scan(&out.LagBytes, &out.SyncState)
		}
		sampleWALHealth(ctx, c, &out)
	}
	if usesPatroni(s) {
		var ps struct {
			Role             string `json:"role"`
			Timeline         int    `json:"timeline"`
			Pause            bool   `json:"pause"`
			ReplicationState string `json:"replication_state"`
			PendingRestart   bool   `json:"pending_restart"`
			Patroni          struct {
				Pause bool `json:"pause"`
			} `json:"patroni"`
		}
		if patroniRequest(ctx, s, "GET", "/patroni", nil, &ps) == nil {
			out.Paused = ps.Pause || ps.Patroni.Pause
			// Patroni compares its desired configuration with the running server.
			// Native pg_settings can retain recovery_target* pending flags after
			// Patroni adopts an existing postmaster, with no actual change pending.
			out.PendingRestart = ps.PendingRestart
			if ps.Timeline > 0 {
				out.Timeline = ps.Timeline
			}
			out.Streaming = out.Streaming || ps.ReplicationState == "streaming"
		}
		var cluster patroniCluster
		if patroniRequest(ctx, s, "GET", "/cluster", nil, &cluster) == nil {
			for _, m := range cluster.Members {
				if m.Role == "leader" || m.Role == "primary" {
					out.Leader = m.Name
				}
				if m.Name == s.Member {
					out.FailoverFenced = retirementTagsApplied(m.Tags)
					out.RetirementTagsCleared = !m.Tags.failoverDisabled() && !m.Tags.NoLoadBalance
					if m.Role == "sync_standby" || m.Role == "quorum_standby" {
						out.SyncState = "sync"
					}
				}
			}
		}
		members, err := etcdMembers(ctx, s, pgspec.EtcdMemberRequest{Op: "list"})
		if err == nil {
			for _, m := range members.Members {
				if m.Name == s.Member {
					out.EtcdMemberID = m.ID
				}
			}
		}
		if out.Role == "primary" && patroniRequest(ctx, s, http.MethodGet, "/primary", nil, nil) != nil {
			out.Role = "starting"
		}
		quorumCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var quorum map[string]any
		out.EtcdHealthy = etcdRequest(quorumCtx, s, "/v3/kv/range", map[string]any{"key": base64.StdEncoding.EncodeToString([]byte(dcsPrefix(s) + "leader")), "serializable": false}, &quorum) == nil
		cancel()
	}
	return out
}
func etcdRequest(ctx context.Context, s pgspec.Spec, path string, in, out any) error {
	hosts := []string{net.JoinHostPort(localHost(s), "2379")}
	if s.Etcd != nil && len(s.Etcd.Endpoints) > 0 {
		hosts = s.Etcd.Endpoints
	}
	var err error
	for i, endpoint := range hosts {
		host, port, splitErr := net.SplitHostPort(endpoint)
		if splitErr != nil {
			return splitErr
		}
		number, parseErr := strconv.Atoi(port)
		if parseErr != nil {
			return parseErr
		}
		// Reserve a share of the remaining probe budget for every endpoint.
		// In particular, a blackholed coordinator must not exhaust status's
		// two-second quorum deadline before a healthy voter can answer.
		budget := 5 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			budget = min(budget, time.Until(deadline)/time.Duration(len(hosts)-i))
		}
		attemptCtx, cancel := context.WithTimeout(ctx, budget)
		err = internalRequest(attemptCtx, s, host, number, "POST", path, in, out, false)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

type etcdMemberList struct {
	Members []struct {
		ID       string   `json:"ID"`
		Name     string   `json:"name"`
		PeerURLs []string `json:"peerURLs"`
		Learner  bool     `json:"isLearner"`
	} `json:"members"`
}

func etcdMembers(ctx context.Context, s pgspec.Spec, r pgspec.EtcdMemberRequest) (pgspec.EtcdMemberResponse, error) {
	var result pgspec.EtcdMemberResponse
	path := "/v3/cluster/member/list"
	input := map[string]any{}
	switch r.Op {
	case "list":
	case "add-learner":
		if !identifier.MatchString(r.Name) || len(r.PeerURL) > 512 || !strings.HasPrefix(r.PeerURL, "https://") {
			return result, errors.New("invalid learner name or peer URL")
		}
		host, port, err := net.SplitHostPort(strings.TrimPrefix(r.PeerURL, "https://"))
		if err != nil || port != "2380" || !validHost(host) {
			return result, errors.New("learner URL requires https on port 2380")
		}
		path = "/v3/cluster/member/add"
		input = map[string]any{"peerURLs": []string{r.PeerURL}, "isLearner": true}
	case "promote", "remove":
		id, err := strconv.ParseUint(r.MemberID, 16, 64)
		if err != nil || id == 0 {
			return result, errors.New("invalid etcd member ID")
		}
		path = "/v3/cluster/member/" + r.Op
		input = map[string]any{"ID": strconv.FormatUint(id, 10)}
	default:
		return result, errors.New("unsupported etcd membership action")
	}
	var response etcdMemberList
	if err := etcdRequest(ctx, s, path, input, &response); err != nil {
		return result, err
	}
	if r.Op != "list" {
		if err := etcdRequest(ctx, s, "/v3/cluster/member/list", map[string]any{}, &response); err != nil {
			return result, err
		}
	}
	initial := []string{}
	for _, m := range response.Members {
		id, err := strconv.ParseUint(m.ID, 10, 64)
		if err != nil {
			return result, errors.New("invalid etcd member response")
		}
		name := m.Name
		if name == "" && r.Op == "add-learner" {
			for _, u := range m.PeerURLs {
				if u == r.PeerURL {
					name = r.Name
				}
			}
		}
		result.Members = append(result.Members, pgspec.EtcdMember{ID: strconv.FormatUint(id, 16), Name: name, PeerURL: m.PeerURLs, Learner: m.Learner})
		for _, u := range m.PeerURLs {
			if name != "" {
				initial = append(initial, name+"="+u)
			}
		}
	}
	if r.Op == "add-learner" {
		result.InitialCluster = strings.Join(initial, ",")
	}
	return result, nil
}
func dcsReset(ctx context.Context, n *agent.Node, s pgspec.Spec, formation ...bool) error {
	states, _ := n.Snapshot()
	if states["patroni"].PID != 0 {
		return errors.New("stop Patroni on every member before resetting DCS")
	}
	for _, peer := range s.Peers {
		if internalRequest(ctx, s, peer.Host, pgspec.PortPatroni, "GET", "/patroni", nil, nil, true) == nil {
			return errors.New("a cluster member still runs Patroni")
		}
	}
	return resetDCSNamespace(ctx, s, len(formation) > 0 && formation[0])
}

func resetDCSNamespace(ctx context.Context, s pgspec.Spec, wholeFormation bool) error {
	prefix := []byte(dcsPrefix(s))
	if wholeFormation {
		prefix = []byte("/karakoram/" + s.Scope + "/")
	}
	end := append([]byte(nil), prefix...)
	end[len(end)-1]++
	encode := base64.StdEncoding.EncodeToString
	// Compare-and-delete is atomic: a concurrent leader acquisition must not
	// race a preceding observation of an absent leader key.
	input := map[string]any{"compare": []any{map[string]any{"key": encode(append(append([]byte(nil), prefix...), []byte("leader")...)), "target": "VERSION", "result": "EQUAL", "version": "0"}}, "success": []any{map[string]any{"request_delete_range": map[string]string{"key": encode(prefix), "range_end": encode(end)}}}}
	if wholeFormation {
		// Fence the whole observed namespace revision, not only this group's
		// leader. A newly acquired leader in any group aborts the transaction.
		var observed struct {
			Header struct {
				Revision string `json:"revision"`
			} `json:"header"`
			KVs []struct {
				Key string `json:"key"`
			} `json:"kvs"`
		}
		if err := etcdRequest(ctx, s, "/v3/kv/range", map[string]any{"key": encode(prefix), "range_end": encode(end)}, &observed); err != nil {
			return err
		}
		for _, kv := range observed.KVs {
			key, err := base64.StdEncoding.DecodeString(kv.Key)
			if err != nil || strings.HasSuffix(string(key), "/leader") {
				return errors.New("refusing formation DCS reset while a leader key is held")
			}
		}
		revision, err := strconv.ParseInt(observed.Header.Revision, 10, 64)
		if err != nil || revision < 1 || revision == 1<<63-1 {
			return errors.New("invalid DCS namespace revision")
		}
		input["compare"] = []any{map[string]any{"key": encode(prefix), "range_end": encode(end), "target": "MOD", "result": "LESS", "mod_revision": strconv.FormatInt(revision+1, 10)}}
	}
	var response struct {
		Succeeded bool `json:"succeeded"`
	}
	if err := etcdRequest(ctx, s, "/v3/kv/txn", input, &response); err != nil {
		return err
	}
	if !response.Succeeded {
		return errors.New("refusing DCS reset while a Patroni leader key is held")
	}
	return nil
}
func decodeAction(w http.ResponseWriter, r *http.Request, out any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	var raw json.RawMessage
	if err := d.Decode(&raw); err != nil {
		return errors.New("invalid action payload")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("action requires one JSON object")
	}
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("action requires one JSON object")
	}
	payload := json.NewDecoder(bytes.NewReader(raw))
	payload.DisallowUnknownFields()
	if err := payload.Decode(out); err != nil {
		return errors.New("invalid action payload")
	}
	return nil
}
func actionJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (e *Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	e.sqlBootstrapRoute(mux, n)
	e.backupRoutes(mux, n)
	e.accessRoutes(mux, n)
	e.upgradeRoutes(mux, n)
	e.citusRoutes(mux, n)
	mux.HandleFunc("POST /v1/postgres/status", func(w http.ResponseWriter, r *http.Request) {
		var request struct{}
		if err := decodeAction(w, r, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		var out pgspec.StatusResponse
		err := n.WithLock(ctx, func(locked *agent.Node) error {
			s, err := decodeSpec(locked.Config())
			if err != nil {
				return err
			}
			out = e.status(ctx, s, dataPaths(locked.DataDir()))
			out.RetirementStopped = retirementStopped(locked, s, dataPaths(locked.DataDir()))
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		actionJSON(w, http.StatusOK, out)
	})
	for _, action := range []string{pgspec.ActionSwitchover, pgspec.ActionReinit, pgspec.ActionEtcdMember, pgspec.ActionDCSReset, pgspec.ActionRestart, pgspec.ActionRetireFence, pgspec.ActionRetireStop} {
		mux.HandleFunc("POST /v1/postgres/"+action, func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			var input json.RawMessage
			if err := decodeAction(w, r, &input); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			var result any = pgspec.ActionResponse{OK: true}
			err := n.WithLock(ctx, func(locked *agent.Node) error {
				s, err := decodeSpec(locked.Config())
				if err != nil {
					return err
				}
				if s.Role != pgspec.RoleData || !usesPatroni(s) {
					return errors.New("action requires an HA data member")
				}
				decode := func(v any) error {
					d := json.NewDecoder(bytes.NewReader(input))
					d.DisallowUnknownFields()
					return d.Decode(v)
				}
				switch action {
				case pgspec.ActionRetireFence:
					var req struct{}
					if err = decode(&req); err != nil {
						return errors.New("retirement fence accepts an empty object")
					}
					return e.reloadRetirement(ctx, locked, s, dataPaths(locked.DataDir()))
				case pgspec.ActionRetireStop:
					var req struct{}
					if err = decode(&req); err != nil {
						return errors.New("retirement stop accepts an empty object")
					}
					return e.retireStop(ctx, locked, s, dataPaths(locked.DataDir()))
				case pgspec.ActionSwitchover:
					var req pgspec.SwitchoverRequest
					if err = decode(&req); err != nil {
						return errors.New("invalid switchover payload")
					}
					if req.Candidate != "" {
						known := false
						for _, p := range s.Peers {
							known = known || p.Name == req.Candidate
						}
						if !known {
							return errors.New("unknown switchover candidate")
						}
					}
					status := e.status(ctx, s, dataPaths(locked.DataDir()))
					if status.Running && status.Role == "replica" && status.Streaming && status.Leader != "" && status.Leader != s.Member {
						return nil // A concurrent failover already demoted this member.
					}
					if !status.Running || status.Role != "primary" || status.Leader != s.Member || !status.EtcdHealthy {
						return errors.New("switchover_leader_changed")
					}
					if status.Paused {
						return errors.New("switchover_paused")
					}
					return patroniRequest(ctx, s, "POST", "/switchover", map[string]string{"leader": s.Member, "candidate": req.Candidate}, nil)
				case pgspec.ActionReinit:
					var req struct{}
					if err = decode(&req); err != nil {
						return errors.New("reinit accepts an empty object")
					}
					return patroniRequest(ctx, s, "POST", "/reinitialize", map[string]bool{"force": true}, nil)
				case pgspec.ActionRestart:
					var req pgspec.RestartRequest
					if err = decode(&req); err != nil {
						return errors.New("invalid restart request")
					}
					st := e.status(ctx, s, dataPaths(locked.DataDir()))
					if req.PrimaryFirst {
						if !primaryDecreaseAllowed(s, st) {
							return errors.New("primary-first restart requires a running primary and only shared-memory decreases")
						}
						return patroniRequest(ctx, s, http.MethodPost, "/restart", map[string]string{"role": "primary"}, nil)
					}
					if singletonCitusRestartAllowed(s, st) {
						return patroniRequest(ctx, s, http.MethodPost, "/restart", map[string]string{"role": "primary"}, nil)
					}
					if st.Role != "replica" {
						return errors.New("restart requires a replica; switch over the primary first")
					}
					return patroniRequest(ctx, s, http.MethodPost, "/restart", map[string]string{"role": "replica"}, nil)
				case pgspec.ActionEtcdMember:
					var req pgspec.EtcdMemberRequest
					if err = decode(&req); err != nil {
						return errors.New("invalid etcd member payload")
					}
					response, err := etcdMembers(ctx, s, req)
					result = response
					return err
				case pgspec.ActionDCSReset:
					var req pgspec.DCSResetRequest
					if err = decode(&req); err != nil {
						return errors.New("invalid DCS reset request")
					}
					return dcsReset(ctx, locked, s, req.Formation)
				}
				return errors.New("unsupported postgres action")
			})
			if err != nil {
				if action == pgspec.ActionSwitchover {
					actionJSON(w, http.StatusConflict, agentapi.ErrorResponse{Error: safeSwitchoverRejection(err)})
				} else {
					http.Error(w, err.Error(), http.StatusConflict)
				}
				return
			}
			actionJSON(w, http.StatusOK, result)
		})
	}
}
