// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const fakeAPIKey = "test-dokploy-api-key"

// The fake deliberately uses wire rows, not the backend's response structs.
// Database settings and applied Swarm settings are separate, as in Dokploy.
// It models only the procedures used by this backend; no Docker daemon is run.
// In particular, start scales Swarm to one even when DB replicas differs, while
// stop marks the DB status idle without changing replicas. Tests must not
// accidentally make the API more capable than the observed v0.30.8 behavior.
// holdTasks and one-shot failures expose convergence and partial-write edges.
type fakeAPI struct {
	mu               sync.Mutex
	latency          time.Duration
	requestHook      func(http.ResponseWriter, *http.Request) (bool, func())
	server           *httptest.Server
	serial           int
	apps             map[string]*fakeApplication
	projects         map[string]map[string]any
	environments     map[string]map[string]any
	nodes            []map[string]any
	networks         []map[string]any
	calls            map[string]int
	volumes          map[string]string // volume name -> node hostname (local driver)
	removedVolumes   []string
	failure          map[string]fakeResponse
	failureOnce      map[string]fakeResponse
	failureEffect    map[string]func()         // Called under mu to model concurrent deletion.
	services         map[string]map[string]any // Swarm objects survive application-row loss.
	images           map[string]bool
	uninspectable    map[string]bool
	omitApplications map[string]bool
	omitSearch       map[string]bool
	deploymentQueue  []map[string]any
}

type fakeResponse struct {
	status        int
	code, message string
}
type fakeApplication struct {
	row                    map[string]any
	mounts, ports, domains []map[string]any
	deployments            []*fakeDeployment
	queued                 int
	tasks                  []map[string]any
	appliedPorts           []map[string]any
	appliedNetworks        []map[string]any
	taskPolls              int
	holdTasks              bool
	holdDeployments        bool
	taskCapacity           int // Zero is unlimited; start-first needs a spare slot.
	reloadOrders           []string
	logs                   string
	wiping                 bool
	upload                 []byte
}
type fakeDeployment struct {
	id, status, createdAt string
	finishedAt            string
	omitFinishedAt        bool // Simulates a corrupt/incomplete upstream row.
	polls                 int
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	f := &fakeAPI{apps: map[string]*fakeApplication{}, projects: map[string]map[string]any{"project": {"projectId": "project", "name": "Contract project", "env": ""}}, environments: map[string]map[string]any{"environment": {"environmentId": "environment", "projectId": "project", "name": "production", "env": "", "isDefault": true}}, calls: map[string]int{}, failure: map[string]fakeResponse{}}
	f.volumes = map[string]string{}
	f.failureOnce = map[string]fakeResponse{}
	f.failureEffect = map[string]func(){}
	f.services = map[string]map[string]any{"dokploy": {"ID": "s0000000000000000000000000", "Spec": map[string]any{"Name": "dokploy"}}}
	f.images = map[string]bool{}
	f.uninspectable = map[string]bool{}
	f.omitApplications = map[string]bool{}
	f.omitSearch = map[string]bool{}
	for i, host := range []string{"node-a", "node-b", "node-c"} {
		role := "worker"
		if i == 0 {
			role = "manager"
		}
		f.nodes = append(f.nodes, map[string]any{"ID": fmt.Sprintf("node%d", i), "Spec": map[string]any{"Labels": map[string]any{"karakoram.data": "true"}, "Availability": "active", "Role": role}, "Description": map[string]any{"Hostname": host}, "Status": map[string]any{"State": "ready"}})
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(func() {
		// Discard this server's admission state before Close releases its port.
		// A later fake can reuse both the URL and the generated application IDs.
		deploymentServers.Delete(f.server.URL)
		f.server.Close()
	})
	return f
}
func (f *fakeAPI) next(prefix string) string {
	f.serial++
	return fmt.Sprintf("%s-%06d", prefix, f.serial)
}
func fakeString(m map[string]any, key string) string { s, _ := m[key].(string); return s }
func fakeInt(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return 0
}
func fakeCopy(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
func fakeRowsCopy(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, fakeCopy(row))
	}
	return out
}
func fakeError(w http.ResponseWriter, status int, code, message, procedure string) {
	w.WriteHeader(status)
	body := map[string]any{"message": message, "code": code}
	data := map[string]any{"code": code, "httpStatus": status, "path": procedure, "zodError": nil}
	body["data"] = data
	if strings.HasPrefix(message, "Required input: ") {
		body["message"] = "Input validation failed"
		fields := map[string]any{}
		issues := []map[string]any{}
		for _, key := range strings.Split(strings.TrimPrefix(message, "Required input: "), ",") {
			detail := "Invalid input: expected string, received undefined"
			fields[key] = []string{detail}
			issues = append(issues, map[string]any{"expected": "string", "code": "invalid_type", "path": []string{key}, "message": detail})
		}
		data["zodError"] = map[string]any{"formErrors": []any{}, "fieldErrors": fields}
		body["issues"] = issues
	}
	_ = json.NewEncoder(w).Encode(body)
}
func (f *fakeAPI) appRow(a *fakeApplication, relations bool) map[string]any {
	row := fakeCopy(a.row)
	if relations {
		row["mounts"] = a.mounts
		row["ports"] = a.ports
		row["domains"] = a.domains
		env := fakeCopy(f.environments[fakeString(a.row, "environmentId")])
		env["project"] = f.projects[fakeString(env, "projectId")]
		row["environment"] = env
		row["deployments"] = f.deploymentRows(a)
	}
	return row
}
func (f *fakeAPI) environmentRow(id string) map[string]any {
	e := fakeCopy(f.environments[id])
	apps := []map[string]any{}
	for _, a := range f.apps {
		if f.omitApplications[fakeString(a.row, "applicationId")] {
			continue
		}
		if a.row["environmentId"] == id {
			summary := map[string]any{}
			for _, key := range []string{"name", "applicationId", "createdAt", "applicationStatus", "description", "serverId", "icon", "server"} {
				summary[key] = a.row[key]
			}
			apps = append(apps, summary)
		}
	}
	slices.SortFunc(apps, func(a, b map[string]any) int {
		return strings.Compare(fakeString(a, "applicationId"), fakeString(b, "applicationId"))
	})
	e["applications"] = apps
	e["project"] = f.projects[fakeString(e, "projectId")]
	return e
}
func (f *fakeAPI) deploymentRows(a *fakeApplication) []map[string]any {
	rows := []map[string]any{}
	for _, d := range a.deployments {
		var finished any
		if !d.omitFinishedAt && d.finishedAt == "" && (d.status == "done" || d.status == "error" || d.status == "cancelled") {
			d.finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if d.finishedAt != "" {
			finished = d.finishedAt
		}
		row := map[string]any{"deploymentId": d.id, "status": d.status, "createdAt": d.createdAt, "finishedAt": finished, "applicationId": a.row["applicationId"]}
		if d.omitFinishedAt {
			delete(row, "finishedAt")
		}
		rows = append(rows, row)
	}
	return rows
}
func (f *fakeAPI) shutdown(a *fakeApplication) {
	for _, task := range a.tasks {
		if task["state"] == "running" {
			task["state"] = "shutdown"
			task["currentState"] = "Shutdown 1 second ago"
		}
	}
}
func (f *fakeAPI) apply(a *fakeApplication) { f.applyReplicas(a, fakeInt(a.row, "replicas")) }
func (f *fakeAPI) applyReplicas(a *fakeApplication, replicas int) {
	f.shutdown(a)
	a.taskPolls = 0
	a.appliedPorts = fakeRowsCopy(a.ports)
	// A custom EndpointSpec replaces Dokploy's separately configured ports.
	if endpoint, ok := a.row["endpointSpecSwarm"].(map[string]any); ok {
		a.appliedPorts = nil
		if ports, ok := endpoint["Ports"].([]any); ok {
			for _, raw := range ports {
				p := raw.(map[string]any)
				a.appliedPorts = append(a.appliedPorts, map[string]any{
					"protocol": p["Protocol"], "targetPort": p["TargetPort"],
					"publishedPort": p["PublishedPort"], "publishMode": p["PublishMode"],
				})
			}
		}
	}
	// Supplying networkSwarm replaces, rather than augments, defaults.
	a.appliedNetworks = []map[string]any{{"Target": "dokploy-network"}}
	if networks, ok := a.row["networkSwarm"].([]any); ok {
		a.appliedNetworks = nil
		for _, n := range networks {
			a.appliedNetworks = append(a.appliedNetworks, fakeCopy(n.(map[string]any)))
		}
	}
	host := "node-a"
	if placement, ok := a.row["placementSwarm"].(map[string]any); ok {
		if constraints, ok := placement["Constraints"].([]any); ok {
			for _, constraint := range constraints {
				if s, ok := constraint.(string); ok && strings.HasPrefix(s, "node.hostname==") {
					host = strings.TrimPrefix(s, "node.hostname==")
				}
			}
		}
	}
	for _, m := range a.mounts {
		if name := fakeString(m, "volumeName"); name != "" {
			f.volumes[name] = host
		}
	}
	a.wiping = false
	if policy, ok := a.row["restartPolicySwarm"].(map[string]any); ok {
		a.wiping = policy["Condition"] == "none" && a.row["command"] == "find"
	}
	for i := range replicas {
		a.tasks = append([]map[string]any{{"containerId": f.next("task"), "name": fmt.Sprintf("%s.%d", a.row["appName"], i+1), "state": "running", "currentState": "Preparing 1 second ago", "node": host, "error": ""}}, a.tasks...)
	}
	name := fakeString(a.row, "appName")
	image := name + ":latest"
	if configured := fakeString(a.row, "dockerImage"); a.row["sourceType"] == "docker" && configured != "" {
		image = configured
	}
	f.serial++
	id := fmt.Sprintf("s%011d%013d", f.serial, f.serial)
	if previous := f.services[name]; previous != nil {
		id = fakeString(previous, "ID")
	}
	published := make([]map[string]any, 0, len(a.appliedPorts))
	for _, p := range a.appliedPorts {
		published = append(published, map[string]any{"PublishedPort": p["publishedPort"], "TargetPort": p["targetPort"], "Protocol": p["protocol"], "PublishMode": p["publishMode"]})
	}
	endpointMode := "vip"
	if endpoint, ok := a.row["endpointSpecSwarm"].(map[string]any); ok {
		endpointMode = fakeString(endpoint, "Mode")
	}
	f.services[name] = map[string]any{"ID": id, "Spec": map[string]any{
		"Name": name, "Mode": map[string]any{"Replicated": map[string]any{"Replicas": replicas}},
		"EndpointSpec": map[string]any{"Ports": published, "Mode": endpointMode},
		"TaskTemplate": map[string]any{"ContainerSpec": map[string]any{"Image": image, "Labels": a.row["labelsSwarm"]}},
	}}
	f.images[image] = true
	a.row["applicationStatus"] = "done"
}
func (f *fakeAPI) deploy(a *fakeApplication) {
	for _, d := range a.deployments {
		if d.status == "running" {
			a.queued++
			return
		}
	}
	a.deployments = append([]*fakeDeployment{{id: f.next("deployment"), status: "running", createdAt: time.Now().UTC().Format(time.RFC3339Nano)}}, a.deployments...)
	a.row["applicationStatus"] = "running"
}
func (f *fakeAPI) advanceDeployments(a *fakeApplication) {
	if a.holdDeployments {
		return
	}
	for _, d := range a.deployments {
		if d.status == "running" {
			d.polls++
			if d.polls >= 2 {
				d.status = "done"
				d.finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
				f.apply(a)
				if a.queued > 0 {
					a.queued--
					f.deploy(a)
				}
			}
		}
	}
}
func (f *fakeAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	delay := f.latency
	hook := f.requestHook
	f.mu.Unlock()
	if hook != nil {
		handled, finish := hook(w, r)
		if finish != nil {
			defer finish()
		}
		if handled {
			return
		}
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	respond := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	if r.Header.Get("x-api-key") != fakeAPIKey {
		w.WriteHeader(http.StatusUnauthorized)
		respond(map[string]string{"message": "Unauthorized"})
		return
	}
	proc := strings.TrimPrefix(r.URL.Path, "/api/")
	f.calls[proc]++
	fail := func(response fakeResponse) {
		if effect := f.failureEffect[proc]; effect != nil {
			effect()
		}
		if proc == "application.reload" && response.status == 500 && response.message == "Error reloading application" {
			var body map[string]any
			if json.NewDecoder(r.Body).Decode(&body) == nil {
				if app := f.apps[fakeString(body, "applicationId")]; app != nil {
					app.row["applicationStatus"] = "error"
				}
			}
		}
		fakeError(w, response.status, response.code, response.message, proc)
	}
	if failure, ok := f.failureOnce[proc]; ok {
		delete(f.failureOnce, proc)
		fail(failure)
		return
	}
	if failure, ok := f.failure[proc]; ok {
		fail(failure)
		return
	}
	get := strings.HasSuffix(proc, ".one") || slices.Contains([]string{"application.search", "project.all", "environment.all", "network.all", "cluster.getNodes", "dockerVolume.getVolumes", "docker.getServiceContainersByAppName", "docker.getConfig", "swarm.getNodeApps", "dockerImage.getImages", "deployment.all", "deployment.queueList", "application.readLogs"}, proc)
	method := http.MethodPost
	if get {
		method = http.MethodGet
	}
	if r.Method != method {
		fakeError(w, 400, "BAD_REQUEST", "Wrong method", proc)
		return
	}
	body := map[string]any{}
	if r.Method == http.MethodGet {
		for key, values := range r.URL.Query() {
			body[key] = values[0]
		}
	} else if proc != "application.dropDeployment" {
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			fakeError(w, 400, "BAD_REQUEST", "Invalid JSON input", proc)
			return
		}
	}
	required := func(keys ...string) bool {
		missing := []string{}
		for _, key := range keys {
			if _, ok := body[key]; !ok || body[key] == nil {
				missing = append(missing, key)
			}
		}
		if len(missing) > 0 {
			fakeError(w, 400, "BAD_REQUEST", "Required input: "+strings.Join(missing, ","), proc)
			return false
		}
		return true
	}
	notFound := func() {
		message := "Not found"
		if proc == "application.one" {
			message = "Application not found"
		}
		fakeError(w, 404, "NOT_FOUND", message, proc)
	}
	conflict := func() { fakeError(w, 409, "CONFLICT", "Resource already exists", proc) }
	switch proc {
	case "deployment.queueList":
		rows := []map[string]any{}
		rows = append(rows, f.deploymentQueue...)
		respond(rows)
		return
	case "application.search":
		rows := []map[string]any{}
		for id, a := range f.apps {
			if !f.omitSearch[id] && a.row["environmentId"] == body["environmentId"] && strings.Contains(strings.ToLower(fakeString(a.row, "name")), strings.ToLower(fakeString(body, "name"))) {
				rows = append(rows, f.appRow(a, false))
			}
		}
		slices.SortFunc(rows, func(a, b map[string]any) int {
			return strings.Compare(fakeString(a, "applicationId"), fakeString(b, "applicationId"))
		})
		total := len(rows)
		offset := 0
		if value, err := strconv.Atoi(fakeString(body, "offset")); err == nil {
			offset = min(value, len(rows))
		}
		rows = rows[offset:min(offset+100, len(rows))]
		respond(map[string]any{"items": rows, "total": total})
		return
	case "docker.getConfig":
		id := fakeString(body, "containerId")
		if f.uninspectable[id] {
			respond(nil)
			return
		}
		if f.images[id] || f.images[id+":latest"] {
			respond(map[string]any{"Id": "sha256:" + strings.Repeat("a", 64), "RepoTags": []string{id + ":latest"}, "Config": map[string]any{}, "RootFS": map[string]any{}})
			return
		}
		for name, service := range f.services {
			if name == id || strings.HasPrefix(fakeString(service, "ID"), id) {
				if f.uninspectable[name] {
					respond(nil)
				} else {
					respond(service)
				}
				return
			}
		}
		respond(nil)
		return
	case "swarm.getNodeApps":
		rows := []map[string]any{}
		for name, service := range f.services {
			if strings.HasPrefix(name, "dokploy-") {
				continue
			}
			id := fakeString(service, "ID")
			rows = append(rows, map[string]any{"ID": id[:min(12, len(id))], "Name": name})
		}
		respond(rows)
		return
	case "dockerImage.getImages":
		rows := []map[string]any{}
		for image := range f.images {
			repo, tag, _ := strings.Cut(image, ":")
			rows = append(rows, map[string]any{"Repository": repo, "Tag": tag})
		}
		respond(rows)
		return
	case "project.create":
		if !required("name") {
			return
		}
		id := f.next("project")
		eid := f.next("environment")
		p := fakeCopy(body)
		p["projectId"] = id
		p["env"] = ""
		e := map[string]any{"environmentId": eid, "projectId": id, "name": "production", "env": "", "isDefault": true}
		f.projects[id] = p
		f.environments[eid] = e
		respond(map[string]any{"project": p, "environment": e})
		return
	case "project.one":
		if !required("projectId") {
			return
		}
		id := fakeString(body, "projectId")
		if f.projects[id] == nil {
			notFound()
			return
		}
		p := fakeCopy(f.projects[id])
		envs := []map[string]any{}
		for eid, e := range f.environments {
			if e["projectId"] == id {
				envs = append(envs, f.environmentRow(eid))
			}
		}
		p["environments"] = envs
		respond(p)
		return
	case "project.all":
		rows := []map[string]any{}
		for _, p := range f.projects {
			rows = append(rows, fakeCopy(p))
		}
		respond(rows)
		return
	case "environment.create":
		if !required("name", "projectId") {
			return
		}
		if f.projects[fakeString(body, "projectId")] == nil {
			notFound()
			return
		}
		e := fakeCopy(body)
		id := f.next("environment")
		e["environmentId"] = id
		e["env"] = ""
		f.environments[id] = e
		respond(e)
		return
	case "environment.one":
		if !required("environmentId") {
			return
		}
		id := fakeString(body, "environmentId")
		if f.environments[id] == nil {
			notFound()
			return
		}
		respond(f.environmentRow(id))
		return
	case "cluster.getNodes":
		respond(f.nodes)
		return
	case "registry.one":
		if fakeString(body, "registryId") != "registry" {
			notFound()
			return
		}
		respond(map[string]any{"registryId": "registry", "registryName": "test registry", "registryUrl": "registry.example.test", "imagePrefix": "contract"})
		return
	case "network.all":
		if f.networks == nil {
			respond([]any{})
		} else {
			respond(f.networks)
		}
		return
	case "network.create":
		if !required("name") {
			return
		}
		for _, n := range f.networks {
			if n["name"] == body["name"] {
				fakeError(w, 400, "BAD_REQUEST", "(HTTP code 409) unexpected - network with name "+fakeString(body, "name")+" already exists ", proc)
				return
			}
		}
		n := fakeCopy(body)
		n["networkId"] = f.next("network")
		f.networks = append(f.networks, n)
		respond(n)
		return
	case "application.create":
		if !required("name", "environmentId") {
			return
		}
		if f.environments[fakeString(body, "environmentId")] == nil {
			notFound()
			return
		}
		row := fakeCopy(body)
		id := f.next("application")
		name := fakeString(body, "appName")
		if name == "" {
			name = fakeString(body, "name")
		}
		f.serial++
		row["appName"] = name + "-" + fmt.Sprintf("%06x", f.serial)
		row["applicationId"] = id
		row["replicas"] = 1
		row["applicationStatus"] = "idle"
		row["env"] = nil
		row["networkSwarm"] = nil
		a := &fakeApplication{row: row, mounts: []map[string]any{}, ports: []map[string]any{}, domains: []map[string]any{}, tasks: []map[string]any{}}
		f.apps[id] = a
		respond(f.appRow(a, false))
		return
	case "docker.getServiceContainersByAppName":
		if !required("appName") {
			return
		}
		for _, a := range f.apps {
			if a.row["appName"] == body["appName"] {
				f.advanceDeployments(a)
				if f.services[fakeString(a.row, "appName")] == nil {
					respond([]any{})
					return
				}
				a.taskPolls++
				if a.taskPolls >= 2 && !a.holdTasks {
					for _, task := range a.tasks {
						if task["state"] == "running" {
							if a.wiping {
								task["state"] = "shutdown"
								task["currentState"] = "Complete 1 second ago"
							} else {
								task["currentState"] = "Running 1 second ago"
							}
						}
						if task["state"] == "remove" {
							task["state"] = "shutdown"
							task["currentState"] = "Shutdown 1 second ago"
						}
					}
				}
				respond(a.tasks)
				return
			}
		}
		respond([]any{})
		return
	case "port.delete", "domain.delete":
		key, relation := "portId", "ports"
		if proc == "domain.delete" {
			key, relation = "domainId", "domains"
		}
		if !required(key) {
			return
		}
		for _, a := range f.apps {
			rows := a.ports
			if relation == "domains" {
				rows = a.domains
			}
			for i, row := range rows {
				if row[key] == body[key] {
					rows = slices.Delete(rows, i, i+1)
					if relation == "ports" {
						a.ports = rows
					} else {
						a.domains = rows
					}
					respond(row)
					return
				}
			}
		}
		notFound()
		return
	case "dockerVolume.getVolumes":
		rows := []map[string]any{}
		for name, host := range f.volumes {
			if host == "node-a" {
				rows = append(rows, map[string]any{"Name": name, "Driver": "local", "Labels": map[string]any{}})
			}
		}
		respond(rows)
		return
	case "dockerVolume.removeVolume":
		if !required("volumeName") {
			return
		}
		name := fakeString(body, "volumeName")
		if f.volumes[name] != "node-a" {
			fakeError(w, 500, "INTERNAL_SERVER_ERROR", "Command execution failed: volume not found", proc)
			return
		}
		delete(f.volumes, name)
		f.removedVolumes = append(f.removedVolumes, name)
		return
	}
	if proc == "application.dropDeployment" {
		if r.ParseMultipartForm(16<<20) != nil {
			fakeError(w, 400, "BAD_REQUEST", "Invalid multipart form", proc)
			return
		}
		defer r.MultipartForm.RemoveAll()
		body["applicationId"] = r.FormValue("applicationId")
		file, header, err := r.FormFile("zip")
		if err != nil {
			fakeError(w, 400, "BAD_REQUEST", "zip is required", proc)
			return
		}
		defer file.Close()
		a := f.apps[fakeString(body, "applicationId")]
		if a == nil {
			notFound()
			return
		}
		data, err := io.ReadAll(file)
		if err != nil || !strings.HasSuffix(header.Filename, ".zip") {
			fakeError(w, 400, "BAD_REQUEST", "Invalid zip file", proc)
			return
		}
		if _, err = zip.NewReader(bytes.NewReader(data), int64(len(data))); err != nil {
			fakeError(w, 400, "BAD_REQUEST", "Invalid zip file", proc)
			return
		}
		a.upload = data
		f.deploy(a)
		respond(true)
		return
	}
	id := fakeString(body, "applicationId")
	if proc == "mounts.create" {
		id = fakeString(body, "serviceId")
	}
	a := f.apps[id]
	if a == nil {
		// v0.30.8 queries deployment rows directly; an absent application
		// (including cascaded deletion) has an empty list, not a 404.
		if proc == "deployment.all" {
			respond([]any{})
			return
		}
		notFound()
		return
	}
	switch proc {
	case "application.one":
		respond(f.appRow(a, true))
	case "application.update":
		for key, value := range body {
			a.row[key] = value
		}
		respond(true)
	case "application.saveEnvironment":
		if !required("applicationId", "env", "createEnvFile") {
			return
		}
		for _, key := range []string{"buildArgs", "buildSecrets"} {
			if _, ok := body[key]; !ok {
				fakeError(w, 400, "BAD_REQUEST", "Required input: "+key, proc)
				return
			}
		}
		for _, key := range []string{"env", "buildArgs", "buildSecrets", "createEnvFile"} {
			a.row[key] = body[key]
		}
		respond(true)
	case "application.saveBuildType":
		for _, key := range []string{"applicationId", "buildType", "dockerfile", "dockerContextPath", "dockerBuildStage", "herokuVersion", "railpackVersion"} {
			if _, ok := body[key]; !ok {
				fakeError(w, 400, "BAD_REQUEST", "Required input: "+key, proc)
				return
			}
		}
		for key, value := range body {
			a.row[key] = value
		}
		respond(true)
	case "application.deploy":
		f.deploy(a)
	case "application.reload":
		if !required("appName", "applicationId") {
			return
		}
		if a.row["appName"] != body["appName"] {
			fakeError(w, 400, "BAD_REQUEST", "Application name mismatch", proc)
			return
		}
		update, _ := a.row["updateConfigSwarm"].(map[string]any)
		order := fakeString(update, "Order")
		a.reloadOrders = append(a.reloadOrders, order)
		running := 0
		for _, task := range a.tasks {
			if task["state"] == "running" {
				running++
			}
		}
		if a.taskCapacity > 0 && running >= a.taskCapacity && order != "stop-first" {
			// The native update is accepted but no replacement can start.
			respond(true)
			return
		}
		f.apply(a)
		respond(true)
	case "application.start":
		f.applyReplicas(a, 1)
		respond(f.appRow(a, false))
	case "application.stop":
		a.taskPolls = 0
		a.row["applicationStatus"] = "idle"
		if service := f.services[fakeString(a.row, "appName")]; service != nil {
			service["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": map[string]any{"Replicas": 0}}
		}
		for _, task := range a.tasks {
			if task["state"] == "running" {
				task["state"] = "remove"
			}
		}
		respond(f.appRow(a, false))
	case "application.delete":
		f.shutdown(a)
		delete(f.services, fakeString(a.row, "appName"))
		delete(f.apps, id)
		respond(f.appRow(a, false))
	case "deployment.all":
		f.advanceDeployments(a)
		respond(f.deploymentRows(a))
	case "mounts.create":
		if !required("type", "mountPath", "serviceId") {
			return
		}
		row := fakeCopy(body)
		row["mountId"] = f.next("mount")
		delete(row, "serviceId")
		row["applicationId"] = id
		a.mounts = append(a.mounts, row)
		respond(row)
	case "port.create":
		if !required("publishedPort", "targetPort", "protocol", "publishMode", "applicationId") {
			return
		}
		row := fakeCopy(body)
		row["portId"] = f.next("port")
		a.ports = append(a.ports, row)
		respond(row)
	case "domain.create":
		if !required("host") {
			return
		}
		for _, app := range f.apps {
			for _, domain := range app.domains {
				if domain["host"] == body["host"] {
					conflict()
					return
				}
			}
		}
		row := fakeCopy(body)
		row["domainId"] = f.next("domain")
		a.domains = append(a.domains, row)
		respond(row)
	case "application.readLogs":
		raw := a.logs
		if raw == "" {
			raw = time.Now().UTC().Format(time.RFC3339Nano) + " runtime started\n"
		}
		respond(raw)
	default:
		notFound()
	}
}
