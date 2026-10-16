// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const taskStartupWindow = 15 * time.Minute

func (b *Backend) nowTime() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

type task struct {
	ID           string `json:"containerId"`
	Name         string `json:"name"`
	State        string `json:"state"`
	CurrentState string `json:"currentState"`
	Node         string `json:"node"`
	Error        string `json:"error"`
}
type operation struct {
	Action              string            `json:"action"`
	AppID               string            `json:"appId"`
	AppName             string            `json:"appName"`
	Hostname            string            `json:"hostname"`
	PreviousDeployments []string          `json:"deployments,omitempty"`
	PreviousTasks       []string          `json:"tasks,omitempty"`
	Issued              time.Time         `json:"issued"`
	Node                string            `json:"node,omitempty"`
	Services            map[string]string `json:"services,omitempty"`
}

func encodeOp(o operation) infra.Op {
	if o.Issued.IsZero() {
		o.Issued = time.Now().UTC()
	}
	data, _ := json.Marshal(o)
	return infra.Op{ID: "dokploy:v1:" + base64.RawURLEncoding.EncodeToString(data), Kind: o.Action, ServiceID: o.AppID, Hostname: o.Hostname, Services: o.Services}
}
func decodeOp(op infra.Op) (operation, error) {
	var o operation
	if !strings.HasPrefix(op.ID, "dokploy:v1:") || len(op.ID) > 65536 {
		return o, reject("invalid Dokploy operation handle")
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(op.ID, "dokploy:v1:"))
	if err != nil {
		return o, reject("invalid Dokploy operation handle")
	}
	if json.Unmarshal(data, &o) != nil || o.Issued.IsZero() {
		return o, reject("invalid Dokploy operation handle")
	}
	if o.Action == "import" {
		if len(o.Services) == 0 {
			return o, reject("invalid Dokploy import handle")
		}
		return o, nil
	}
	if o.AppID == "" || o.AppName == "" {
		return o, reject("invalid Dokploy operation handle")
	}
	switch o.Action {
	case "deploy", "start", "stop", "restart", "delete", "wipe":
	default:
		return o, reject("unknown Dokploy operation action")
	}
	return o, nil
}
func (b *Backend) tasks(ctx context.Context, appName string) ([]task, error) {
	var ts []task
	err := b.c.get(ctx, "docker.getServiceContainersByAppName", map[string]string{"appName": appName}, &ts)
	if errors.Is(err, infra.ErrNotFound) {
		return nil, nil
	}
	return ts, err
}
func (b *Backend) deployments(ctx context.Context, id string) ([]deploymentRecord, error) {
	var ds []deploymentRecord
	err := b.c.get(ctx, "deployment.all", map[string]string{"applicationId": id}, &ds)
	return ds, err
}
func (b *Backend) operation(ctx context.Context, a *application, m metadata, action string) (operation, error) {
	o := operation{Action: action, AppID: a.ID, AppName: a.AppName, Hostname: m.Hostname, Issued: b.nowTime().UTC(), Node: m.Node}
	ts, err := b.tasks(ctx, a.AppName)
	if err != nil {
		return o, err
	}
	for _, t := range ts {
		o.PreviousTasks = append(o.PreviousTasks, t.ID)
	}
	if action == "deploy" {
		ds, err := b.deployments(ctx, a.ID)
		if err != nil {
			return o, err
		}
		if len(ds) > 0 {
			o.PreviousDeployments = []string{ds[0].ID}
		}
	}
	return o, nil
}
func (b *Backend) Deploy(ctx context.Context, hostname string, d infra.Deployment) (infra.Op, error) {
	if err := d.Validate(); err != nil {
		return infra.Op{}, reject(err.Error())
	}
	if d.Runtime.Base != infra.BaseUbuntu2404 {
		return infra.Op{}, reject("unsupported runtime base")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if m.Kind != infra.KindRuntime {
		return infra.Op{}, reject("cannot deploy to a volume holder")
	}
	if d.Runtime.Volume != m.Volume {
		return infra.Op{}, reject("deployment volume differs from imported service volume")
	}
	if err = b.ensureNetwork(ctx); err != nil {
		return infra.Op{}, err
	}
	o, err := b.operation(ctx, a, m, "deploy")
	if err != nil {
		return infra.Op{}, err
	}
	zipPath, err := renderDeployment(d)
	if err != nil {
		return infra.Op{}, err
	}
	defer os.Remove(zipPath)
	m.HTTPPorts = nil
	for _, p := range d.Runtime.Ports {
		if p.HTTP {
			m.HTTPPorts = append(m.HTTPPorts, p.Port)
		}
	}
	if _, err = b.reconcileDomains(ctx, a, &m); err != nil {
		return infra.Op{}, err
	}
	settings := resourceSettings(m.Resources)
	settings["sourceType"] = "drop"
	settings["buildType"] = "dockerfile"
	settings["dockerfile"] = "Dockerfile"
	settings["dockerContextPath"] = "."
	settings["dropBuildPath"] = "/"
	settings["networkSwarm"] = b.serviceNetworks(m)
	settings["createEnvFile"] = false
	settings["replicas"] = m.Max
	settings["updateConfigSwarm"] = map[string]any{"Parallelism": 1, "Order": "stop-first", "FailureAction": "continue"}
	settings["rollbackConfigSwarm"] = map[string]any{"Parallelism": 1, "Order": "stop-first", "FailureAction": "continue"}
	settings["stopGracePeriodSwarm"] = int64(120 * time.Second)
	settings["ulimitsSwarm"] = []map[string]any{{"Name": "nofile", "Soft": 65535, "Hard": 65535}}
	settings["restartPolicySwarm"] = map[string]any{"Condition": "any"}
	settings["labelsSwarm"] = map[string]string{"karakoram.managed": "true", "karakoram.hostname": hostname, "karakoram.kind": string(m.Kind), "karakoram.owner": m.Vars["KARAKORAM_OWNER"]}
	if id := b.target.Config["registryId"]; id != "" {
		settings["registryId"] = id
	}
	if m.Node != "" {
		settings["placementSwarm"] = map[string]any{"Constraints": []string{"node.hostname==" + m.Node}}
	}
	if err = b.update(ctx, a, m, settings); err != nil {
		return infra.Op{}, err
	}
	o.Issued = b.nowTime().UTC()
	if err = b.c.upload(ctx, a.ID, zipPath); err != nil {
		return infra.Op{}, err
	}
	m.Deployed = true
	if err = b.update(ctx, a, m, nil); err != nil {
		return infra.Op{}, err
	}
	return encodeOp(o), nil
}
func (b *Backend) action(ctx context.Context, hostname, action string) (infra.Op, error) {
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if m.Kind != infra.KindRuntime {
		return infra.Op{}, reject("volume holders cannot be started or stopped")
	}
	if !m.Deployed {
		return infra.Op{}, reject("service has not been deployed")
	}
	o, err := b.operation(ctx, a, m, action)
	if err != nil {
		return infra.Op{}, err
	}
	proc := action
	body := map[string]any{"applicationId": a.ID}
	if action == "restart" || action == "start" {
		proc = "reload"
		body["appName"] = a.AppName
	}
	if err = b.c.post(ctx, "application."+proc, body, nil); err != nil {
		return infra.Op{}, err
	}
	return encodeOp(o), nil
}
func (b *Backend) Start(ctx context.Context, hostname string) (infra.Op, error) {
	return b.action(ctx, hostname, "start")
}
func (b *Backend) Stop(ctx context.Context, hostname string) (infra.Op, error) {
	return b.action(ctx, hostname, "stop")
}
func (b *Backend) Restart(ctx context.Context, hostname string) (infra.Op, error) {
	return b.action(ctx, hostname, "restart")
}
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func (b *Backend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "" {
		return infra.OpState{Done: true, Status: "done"}, nil
	}
	o, err := decodeOp(op)
	if err != nil {
		return infra.OpState{}, err
	}
	if o.Action == "import" {
		state := infra.OpState{Done: true, Status: "done"}
		for _, id := range o.Services {
			state.ServiceIDs = append(state.ServiceIDs, id)
			err := b.c.get(ctx, "application.one", map[string]string{"applicationId": id}, nil)
			if errors.Is(err, infra.ErrNotFound) {
				state.Expired = true
				state.Status = "expired"
				continue
			}
			if err != nil {
				return infra.OpState{}, err
			}
		}
		return state, nil
	}
	state := infra.OpState{Status: "pending", ServiceIDs: []string{o.AppID}}
	var a application
	err = b.c.get(ctx, "application.one", map[string]string{"applicationId": o.AppID}, &a)
	if errors.Is(err, infra.ErrNotFound) {
		if o.Action == "delete" || o.Action == "wipe" {
			ts, e := b.tasks(ctx, o.AppName)
			if e != nil {
				return state, e
			}
			for _, t := range ts {
				if strings.EqualFold(t.State, "running") || strings.HasPrefix(strings.ToLower(t.CurrentState), "running") {
					return state, nil
				}
			}
			if o.Action == "wipe" {
				return b.finishWipe(ctx, o, state)
			}
			state.Done = true
			state.Status = "done"
			return state, nil
		}
		state.Expired = true
		state.Failed = true
		state.Status = "expired"
		return state, nil
	}
	if err != nil {
		return state, err
	}
	ts, err := b.tasks(ctx, o.AppName)
	if err != nil {
		return state, err
	}
	if o.Action == "wipe" {
		return b.advanceWipe(ctx, o, &a, ts)
	}
	if o.Action == "delete" {
		if runningTasks(ts) {
			return state, nil
		}
		if err = b.c.post(ctx, "application.delete", map[string]any{"applicationId": o.AppID}, nil); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				state.Done = true
				state.Status = "done"
				return state, nil
			}
			return state, err
		}
		return state, nil
	}
	if o.Action == "stop" {
		for _, t := range ts {
			if strings.EqualFold(t.State, "running") || strings.HasPrefix(strings.ToLower(t.CurrentState), "running") {
				return state, nil
			}
		}
		state.Done = true
		state.Status = "done"
		return state, nil
	}
	deploymentDone := o.Action != "deploy"
	finishedAt := ""
	if o.Action == "deploy" {
		ds, err := b.deployments(ctx, o.AppID)
		if err != nil {
			return state, err
		}
		found := len(ds) > 0 && !contains(o.PreviousDeployments, ds[0].ID)
		if found {
			switch ds[0].Status {
			case "error", "cancelled":
				state.Failed = true
				state.Status = "failed"
				state.Message = "Dokploy deployment failed; consult the deployment logs"
				return state, nil
			case "done":
				deploymentDone = true
				finishedAt = ds[0].FinishedAt
			}
		}
	}
	ready := 0
	for _, t := range ts {
		if strings.EqualFold(t.State, "running") && strings.HasPrefix(strings.ToLower(t.CurrentState), "running") && !contains(o.PreviousTasks, t.ID) {
			ready++
		}
	}
	replicas := a.Replicas
	if replicas < 1 {
		replicas = 1
	}
	if deploymentDone && ready >= replicas {
		state.Done = true
		state.Status = "done"
		return state, nil
	}
	// Dokploy serializes builds across applications. Only completed builds
	// start the deployment startup window; queue/build time remains unbounded.
	if deploymentDone {
		started := o.Issued
		if o.Action == "deploy" {
			started, err = time.Parse(time.RFC3339Nano, finishedAt)
			if err != nil {
				return state, silentFailure("completed deployment has no valid finish timestamp")
			}
		}
		if !b.nowTime().Before(started.Add(taskStartupWindow)) {
			state.Failed = true
			state.Status = "failed"
			state.Message = b.startupFailure(a, o, ts, ready, replicas)
		}
	}
	return state, nil
}

func (b *Backend) startupFailure(a application, o operation, tasks []task, ready, replicas int) string {
	message := fmt.Sprintf("Dokploy task startup timed out after 15 minutes: %d/%d new replicas Running", ready, replicas)
	var latest *task
	for i := range tasks {
		if contains(o.PreviousTasks, tasks[i].ID) {
			continue
		}
		if latest == nil {
			latest = &tasks[i]
		}
		if !strings.EqualFold(tasks[i].State, "running") || !strings.HasPrefix(strings.ToLower(tasks[i].CurrentState), "running") {
			latest = &tasks[i]
			break
		}
	}
	if latest == nil {
		return message + "; no replacement task observed"
	}
	values, err := decodeEnvironment(a.Env)
	if err != nil {
		// Do not expose task errors when externally edited environment text
		// prevents reliable credential redaction.
		state := "unknown"
		if fields := strings.Fields(latest.CurrentState); len(fields) > 0 {
			switch fields[0] {
			case "New", "Pending", "Assigned", "Accepted", "Preparing", "Ready", "Starting", "Running", "Complete", "Shutdown", "Failed", "Rejected", "Remove", "Orphaned":
				state = fields[0]
			}
		}
		return message + "; latest task state: " + state + "; error omitted because the environment cannot be safely decoded"
	}
	secrets := make([]string, 0, len(values)+2)
	secrets = append(secrets, b.c.currentToken(), b.target.Token)
	m, _ := parseMetadata(a.Description)
	for key, value := range values {
		if contains(m.SecretKeys, key) || !contains(m.VarKeys, key) {
			secrets = append(secrets, value)
		}
	}
	detail := "latest task state: " + sanitizedMessage(latest.CurrentState, secrets)
	if taskError := sanitizedMessage(latest.Error, secrets); taskError != "" && taskError != "Error:" {
		detail += "; error: " + taskError
	}
	return sanitizedMessage(message+"; "+detail, nil)
}
