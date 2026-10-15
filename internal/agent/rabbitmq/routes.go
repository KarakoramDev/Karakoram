// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

// boundedBuffer keeps untrusted CLI output bounded while continuing to drain it.
type boundedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (2 << 20) - b.Len()
	if len(p) > remaining {
		p = p[:max(0, remaining)]
		b.overflow = true
	}
	_, err := b.Buffer.Write(p)
	return n, err
}

// commandFailure keeps diagnostic bytes private; Error never exposes credentials.
type commandFailure struct {
	tool, action string
	cause        error
	stderr       []byte
}

func (e *commandFailure) Error() string {
	return fmt.Sprintf("%s %s failed: %v", e.tool, e.action, e.cause)
}
func (e *commandFailure) Unwrap() error { return e.cause }
func command(ctx context.Context, env []string, tool string, stdin io.Reader, timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary(tool), args...)
	cmd.Env = env
	if cmd.Env == nil {
		cmd.Env = []string{}
	}
	cmd.Stdin = stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	var out, stderr boundedBuffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return out.Bytes(), ctx.Err()
	}
	if out.overflow || stderr.overflow {
		return nil, errors.New("RabbitMQ command output exceeded limit")
	}
	// Preserve the separate diagnostic channel for structured health checks,
	// without exposing it through route errors (passwords can be echoed there).
	if err != nil {
		return out.Bytes(), &commandFailure{tool: tool, action: args[0], cause: err, stderr: stderr.Bytes()}
	}
	return out.Bytes(), nil
}
func (e *Runner) cli(ctx context.Context, tool string, stdin io.Reader, args ...string) ([]byte, error) {
	e.mu.Lock()
	env := slices.Clone(e.env)
	e.mu.Unlock()
	return command(ctx, env, tool, stdin, 60*time.Second, args...)
}
func jsonArgs(args ...string) []string { return append(args, "--quiet", "--formatter", "json") }
func checkQueueResults(out []byte) error {
	var results []struct {
		VHost  string `json:"vhost"`
		Name   string `json:"name"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(out, &results); err != nil {
		return fmt.Errorf("invalid queue membership JSON: %w", err)
	}
	if bytes.Equal(bytes.TrimSpace(out), []byte("null")) {
		return errors.New("missing queue membership results")
	}
	for _, result := range results {
		if result.Result != "ok" {
			return fmt.Errorf("queue membership change failed for %q in %q: %s", result.Name, result.VHost, result.Result)
		}
	}
	return nil
}
func decode(r *http.Request, v any) error {
	data, err := io.ReadAll(io.LimitReader(r.Body, 16385))
	if err != nil || len(data) > 16384 || len(bytes.TrimSpace(data)) == 0 || bytes.TrimSpace(data)[0] != '{' {
		return errors.New("request must be a bounded JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return errors.New("invalid JSON request")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("request must contain one JSON object")
	}
	return nil
}
func validName(s string) bool {
	return len(s) > 0 && len(s) <= 255 && utf8.ValidString(s) && !strings.HasPrefix(s, "-") && !strings.ContainsAny(s, "\x00\r\n")
}
func (e *Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{"status", "bootstrap", "join", "forget", "await-quorum", "quorum-critical", "drain", "revive", "stop", "stop-app", "start", "grow", "shrink", "rebalance", "enable-feature-flags", "clear-pem-cache", "set-password", "stream-replicas"} {
		mux.HandleFunc("POST /v1/rabbitmq/"+action, func(w http.ResponseWriter, r *http.Request) {
			var request any
			switch action {
			case "join":
				request = &rmqspec.JoinRequest{}
			case "forget", "grow", "shrink":
				request = &rmqspec.NodeRequest{}
			case "await-quorum":
				request = &rmqspec.AwaitQuorumRequest{}
			case "set-password":
				request = &rmqspec.SetPasswordRequest{}
			case "stream-replicas":
				request = &rmqspec.StreamReplicasRequest{}
			default:
				request = &rmqspec.EmptyRequest{}
			}
			if err := decode(r, request); err != nil {
				writeJSON(w, 400, agentapi.ErrorResponse{Error: err.Error()})
				return
			}
			if err := validateRequest(request); err != nil {
				writeJSON(w, 400, agentapi.ErrorResponse{Error: err.Error()})
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
			defer cancel()
			var result any
			err := n.WithLock(ctx, func(locked *agent.Node) error {
				var err error
				result, err = e.execute(ctx, locked, action, request)
				return err
			})
			if err != nil {
				writeJSON(w, 503, agentapi.ErrorResponse{Error: err.Error()})
				return
			}
			writeJSON(w, 200, result)
		})
	}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func validateRequest(v any) error {
	switch r := v.(type) {
	case *rmqspec.JoinRequest:
		if !nodePattern.MatchString(r.Seed) {
			return errors.New("invalid seed node")
		}
	case *rmqspec.NodeRequest:
		if !nodePattern.MatchString(r.Node) {
			return errors.New("invalid node")
		}
	case *rmqspec.AwaitQuorumRequest:
		if r.TimeoutSeconds < 0 || r.TimeoutSeconds > 120 {
			return errors.New("timeout must be between 1 and 120 seconds (0 uses default)")
		}
	case *rmqspec.SetPasswordRequest:
		if r.User != "karakoram" && r.User != "admin" && r.User != "monitor" {
			return errors.New("only system users may be rotated")
		}
	case *rmqspec.StreamReplicasRequest:
		if !validName(r.VHost) || !validName(r.Queue) {
			return errors.New("invalid vhost or queue name")
		}
		if r.Action != "list" && r.Action != "add" && r.Action != "delete" {
			return errors.New("invalid stream replica action")
		}
		if r.Action != "list" && !nodePattern.MatchString(r.Node) {
			return errors.New("invalid node")
		}
		if r.Action == "list" && r.Node != "" {
			return errors.New("list does not accept a node")
		}
	}
	return nil
}
func (e *Runner) status(ctx context.Context, n *agent.Node) (rmqspec.StatusResponse, error) {
	var result rmqspec.StatusResponse
	out, err := e.cli(ctx, "rabbitmqctl", nil, jsonArgs("cluster_status")...)
	if err != nil {
		return result, err
	}
	var raw struct {
		RunningNodes []string          `json:"running_nodes"`
		DiskNodes    []string          `json:"disk_nodes"`
		Alarms       json.RawMessage   `json:"alarms"`
		Partitions   json.RawMessage   `json:"partitions"`
		Maintenance  map[string]string `json:"maintenance_status"`
		Versions     map[string]struct {
			RabbitMQ string `json:"rabbitmq_version"`
		} `json:"versions"`
		Flags []rmqspec.FeatureFlag `json:"feature_flags"`
	}
	if err = json.Unmarshal(out, &raw); err != nil {
		return result, fmt.Errorf("invalid cluster status JSON: %w", err)
	}
	s, err := spec(n.Config())
	if err != nil {
		return result, err
	}
	result = rmqspec.StatusResponse{NodeName: s.NodeName, RunningNodes: raw.RunningNodes, DiskNodes: raw.DiskNodes, Alarms: raw.Alarms, Partitions: raw.Partitions, Maintenance: raw.Maintenance[s.NodeName] == "marked for maintenance", FeatureFlags: raw.Flags, Version: raw.Versions[s.NodeName].RabbitMQ}
	return result, nil
}
func (e *Runner) execute(ctx context.Context, n *agent.Node, action string, request any) (any, error) {
	ok := rmqspec.ActionResponse{OK: true}
	run := func(tool string, args ...string) error {
		_, err := e.cli(ctx, tool, nil, jsonArgs(args...)...)
		return err
	}
	switch action {
	case "status":
		return e.status(ctx, n)
	case "bootstrap":
		return ok, e.bootstrap(ctx, n.Config())
	case "join":
		seed := request.(*rmqspec.JoinRequest).Seed
		status, err := e.status(ctx, n)
		if err != nil {
			return nil, err
		}
		if slices.Contains(status.DiskNodes, seed) {
			return ok, nil
		}
		if err = run("rabbitmqctl", "stop_app"); err != nil {
			return nil, err
		}
		joinErr := run("rabbitmqctl", "join_cluster", seed)
		startErr := run("rabbitmqctl", "start_app")
		return ok, errors.Join(joinErr, startErr)
	case "forget":
		node := request.(*rmqspec.NodeRequest).Node
		status, err := e.status(ctx, n)
		if err != nil {
			return nil, err
		}
		if node == status.NodeName {
			return nil, errors.New("cannot forget local node")
		}
		if !slices.Contains(status.DiskNodes, node) {
			return ok, nil
		}
		return ok, run("rabbitmqctl", "forget_cluster_node", node)
	case "await-quorum":
		seconds := request.(*rmqspec.AwaitQuorumRequest).TimeoutSeconds
		if seconds == 0 {
			seconds = 60
		}
		e.mu.Lock()
		env := slices.Clone(e.env)
		e.mu.Unlock()
		_, err := command(ctx, env, "rabbitmq-upgrade", nil, time.Duration(seconds+5)*time.Second, jsonArgs("await_online_quorum_plus_one", "-t", strconv.Itoa(seconds))...)
		return ok, err
	case "quorum-critical":
		out, err := e.cli(ctx, "rabbitmq-queues", nil, jsonArgs("check_if_node_is_quorum_critical")...)
		// Failed RabbitMQ health checks emit their JSON on stderr. The final
		// line is the encoded result; preceding lines may be Erlang warnings.
		if err != nil {
			var failure *commandFailure
			if errors.As(err, &failure) {
				data := bytes.TrimSpace(failure.stderr)
				out = data[bytes.LastIndexByte(data, '\n')+1:]
			}
		}
		return parseQuorumCritical(out, err)
	case "drain":
		return ok, run("rabbitmq-upgrade", "drain")
	case "revive":
		return ok, run("rabbitmq-upgrade", "revive")
	case "stop":
		states, _ := n.Snapshot()
		if states[process].PID == 0 {
			e.mu.Lock()
			e.stopped = true
			e.mu.Unlock()
			return ok, n.Action(ctx, process, "stop")
		}
		e.mu.Lock()
		appStopped := e.appStoppedPID == states[process].PID
		e.mu.Unlock()
		if !appStopped {
			if err := run("rabbitmq-upgrade", "drain"); err != nil {
				return nil, err
			}
		}
		// Disarm supervision before shutdown, otherwise the exit schedules a restart.
		e.mu.Lock()
		e.stopped = true
		e.mu.Unlock()
		return ok, n.Action(ctx, process, "stop")
	case "stop-app":
		if err := run("rabbitmqctl", "stop_app"); err != nil {
			return nil, err
		}
		states, _ := n.Snapshot()
		e.mu.Lock()
		e.appStoppedPID = states[process].PID
		e.mu.Unlock()
		return ok, nil
	case "start":
		// Application-level starts must honor the same holds as process starts.
		if n.Maintenance() || n.Stopped(process) {
			return ok, nil
		}
		if desired := n.Config().DesiredProcesses; desired != nil && !slices.Contains(desired, process) {
			return ok, nil
		}
		states, _ := n.Snapshot()
		e.mu.Lock()
		appStopped := e.appStoppedPID != 0 && e.appStoppedPID == states[process].PID
		e.mu.Unlock()
		if err := n.Action(ctx, process, "start"); err != nil {
			return nil, err
		}
		if appStopped {
			if err := run("rabbitmqctl", "start_app"); err != nil {
				return nil, err
			}
		}
		e.mu.Lock()
		e.stopped = false
		e.appStoppedPID = 0
		e.mu.Unlock()
		return ok, nil
	case "grow":
		out, err := e.cli(ctx, "rabbitmq-queues", nil, jsonArgs("grow", request.(*rmqspec.NodeRequest).Node, "all")...)
		if err != nil {
			return nil, err
		}
		return ok, checkQueueResults(out)
	case "shrink":
		out, err := e.cli(ctx, "rabbitmq-queues", nil, jsonArgs("shrink", request.(*rmqspec.NodeRequest).Node)...)
		if err != nil {
			return nil, err
		}
		return ok, checkQueueResults(out)
	case "rebalance":
		return ok, run("rabbitmq-queues", "rebalance", "all")
	case "enable-feature-flags":
		return ok, run("rabbitmqctl", "enable_feature_flag", "all")
	case "clear-pem-cache":
		return ok, run("rabbitmqctl", "eval", "ssl:clear_pem_cache().")
	case "set-password":
		user := request.(*rmqspec.SetPasswordRequest).User
		cred, exists := n.Config().SystemUsers[user]
		if !exists {
			return nil, errors.New("system credential unavailable")
		}
		return ok, e.password(ctx, "change_password", user, cred.Password)
	case "stream-replicas":
		r := request.(*rmqspec.StreamReplicasRequest)
		cmd := "stream_status"
		args := []string{"--vhost", r.VHost, r.Queue}
		if r.Action == "add" {
			cmd = "add_replica"
			args = append(args, r.Node)
		}
		if r.Action == "delete" {
			cmd = "delete_replica"
			args = append(args, r.Node)
		}
		out, err := e.cli(ctx, "rabbitmq-streams", nil, jsonArgs(append([]string{cmd}, args...)...)...)
		if err != nil {
			return nil, err
		}
		if !json.Valid(out) {
			return nil, errors.New("invalid stream replica JSON")
		}
		return rmqspec.StreamReplicasResponse{Replicas: out}, nil
	default:
		return nil, errors.New("unsupported action")
	}
}
func (e *Runner) password(ctx context.Context, action, user, password string) error {
	if !validName(user) || len(password) == 0 || len(password) > 4096 || strings.ContainsAny(password, "\x00\r\n") {
		return errors.New("invalid system credential")
	}
	// The distribution wrapper enables stdin only with exactly two arguments.
	// Adding even formatter flags forces -noinput and blocks password reads.
	// Discard its text output; the route returns a structured acknowledgement.
	_, err := e.cli(ctx, "rabbitmqctl", strings.NewReader(password+"\n"), action, user)
	return err
}
func (e *Runner) bootstrap(ctx context.Context, c agentapi.MemberConfig) error {
	out, err := e.cli(ctx, "rabbitmqctl", nil, jsonArgs("list_users")...)
	if err != nil {
		return err
	}
	var users []struct {
		User string `json:"user"`
	}
	if err = json.Unmarshal(out, &users); err != nil {
		return err
	}
	exists := func(name string) bool {
		return slices.ContainsFunc(users, func(u struct {
			User string `json:"user"`
		}) bool {
			return u.User == name
		})
	}
	for _, name := range []string{"karakoram", "admin"} {
		cred, ok := c.SystemUsers[name]
		if !ok {
			return errors.New("system credential unavailable")
		}
		action := "add_user"
		if exists(name) {
			action = "change_password"
		}
		if err = e.password(ctx, action, name, cred.Password); err != nil {
			return err
		}
		if _, err = e.cli(ctx, "rabbitmqctl", nil, jsonArgs("set_user_tags", name, "administrator")...); err != nil {
			return err
		}
		if _, err = e.cli(ctx, "rabbitmqctl", nil, jsonArgs("set_permissions", "--vhost", "/", name, ".*", ".*", ".*")...); err != nil {
			return err
		}
	}
	if exists("guest") {
		_, err = e.cli(ctx, "rabbitmqctl", nil, jsonArgs("delete_user", "guest")...)
	}
	return err
}
