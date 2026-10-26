// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
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

	"gopkg.in/yaml.v3"
)

// fakeAPI keeps REST state rather than replacing the backend being exercised.
type fakeAPI struct {
	mu            sync.Mutex
	url           string
	serial        int
	services      map[string]*ServiceStack
	env           map[string][]EnvItem
	routes        map[string]PublicPortRouting
	versions      map[string]string
	uploaded      map[string]bool
	versionStatus map[string]string
	processes     map[string]Process
}

func newFakeAPI(t *testing.T) *httptest.Server {
	t.Helper()
	server, _ := newFakeAPIWithState(t)
	return server
}

func newFakeAPIWithState(t *testing.T) (*httptest.Server, *fakeAPI) {
	t.Helper()
	f := &fakeAPI{services: map[string]*ServiceStack{}, env: map[string][]EnvItem{}, routes: map[string]PublicPortRouting{}, versions: map[string]string{}, uploaded: map[string]bool{}, versionStatus: map[string]string{}, processes: map[string]Process{}}
	server := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	f.url = server.URL
	t.Cleanup(server.Close)
	return server, f
}
func (f *fakeAPI) next(prefix string) string {
	f.serial++
	return fmt.Sprintf("%s%d", prefix, f.serial)
}
func (f *fakeAPI) process(service string) Process {
	p := Process{ID: f.next("op"), ProjectID: "project", ServiceStackID: service, Status: "FINISHED", Created: time.Now()}
	f.processes[p.ID] = p
	return p
}
func fakePage[T any](w http.ResponseWriter, r *http.Request, list []T) {
	total := len(list)
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	offset = min(max(offset, 0), total)
	list = list[offset:min(offset+limit, total)]
	_ = json.NewEncoder(w).Encode(page[T]{List: list, Total: total, Count: len(list), Limit: limit, Offset: offset})
}
func fakeError(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": "Request rejected"}})
}
func (f *fakeAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	respond := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	decode := func(v any) bool {
		if err := json.NewDecoder(r.Body).Decode(v); err != nil {
			fakeError(w, 400, "invalidRequest")
			return false
		}
		return true
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if r.URL.Path == "/signed-log" {
		if r.Header.Get("Authorization") != "" {
			fakeError(w, 403, "forbidden")
			return
		}
		s := f.services[r.URL.Query().Get("serviceStackId")]
		if s == nil {
			fakeError(w, 404, "serviceStackNotFound")
			return
		}
		respond(map[string]any{"items": []RuntimeLog{{Timestamp: time.Now().UTC(), Hostname: s.Name + "1", Content: "runtime started", FacilityLabel: "app"}}})
		return
	}
	if r.Header.Get("Authorization") != "Bearer token" {
		fakeError(w, 401, "invalidToken")
		return
	}
	if r.URL.Path == "/auth/sudo" {
		respond(map[string]bool{"inSudo": true})
		return
	}
	if len(parts) == 2 && parts[0] == "process" {
		p, ok := f.processes[parts[1]]
		if !ok {
			fakeError(w, 404, "processNotFound")
			return
		}
		respond(p)
		return
	}
	if len(parts) >= 2 && parts[0] == "project" && parts[1] == "project" {
		switch strings.Join(parts[2:], "/") {
		case "":
			respond(Project{ID: "project", Name: "Contract project", PublicIPv4: "192.0.2.1", PublicIPv6: "2001:db8::1", ZeropsSubdomainHost: "example"})
		case "service-stack":
			list := make([]ServiceStack, 0, len(f.services))
			for _, s := range f.services {
				list = append(list, *s)
			}
			slices.SortFunc(list, func(a, b ServiceStack) int { return strings.Compare(a.Name, b.Name) })
			fakePage(w, r, list)
		case "service-stack/import":
			var body map[string]string
			if !decode(&body) {
				return
			}
			var doc struct {
				Services []struct {
					Hostname      string `yaml:"hostname"`
					Type          string `yaml:"type"`
					MinContainers int    `yaml:"minContainers"`
					MaxContainers int    `yaml:"maxContainers"`
					Vertical      struct {
						CPUMode  string   `yaml:"cpuMode"`
						MinCPU   float64  `yaml:"minCpu"`
						MaxCPU   float64  `yaml:"maxCpu"`
						MinRAM   float64  `yaml:"minRam"`
						MaxRAM   float64  `yaml:"maxRam"`
						MinDisk  float64  `yaml:"minDisk"`
						MaxDisk  float64  `yaml:"maxDisk"`
						StartCPU *float64 `yaml:"startCpuCoreCount"`
					} `yaml:"verticalAutoscaling"`
					Env  map[string]string `yaml:"envSecrets"`
					Vars map[string]string `yaml:"envVariables"`
				} `yaml:"services"`
			}
			if err := yaml.Unmarshal([]byte(body["yaml"]), &doc); err != nil {
				fakeError(w, 400, "invalidYaml")
				return
			}
			result := ImportResult{ProjectID: "project"}
			for _, entry := range doc.Services {
				for _, s := range f.services {
					if s.Name == entry.Hostname {
						fakeError(w, 409, "duplicateService")
						return
					}
				}
				id := f.next("service")
				v := entry.Vertical
				config := map[string]json.RawMessage{}
				putJSON(config, "verticalAutoscaling", map[string]any{"cpuMode": v.CPUMode, "startCpuCoreCount": v.StartCPU, "minResource": map[string]float64{"cpuCoreCount": v.MinCPU, "memoryGBytes": v.MinRAM, "diskGBytes": v.MinDisk}, "maxResource": map[string]float64{"cpuCoreCount": v.MaxCPU, "memoryGBytes": v.MaxRAM, "diskGBytes": v.MaxDisk}, "minFreeResource": map[string]any{}})
				putJSON(config, "horizontalAutoscaling", map[string]int{"minContainerCount": entry.MinContainers, "maxContainerCount": entry.MaxContainers})
				status := "READY_TO_DEPLOY"
				if strings.HasPrefix(entry.Type, "local-storage") {
					status = "ACTIVE"
				}
				f.services[id] = &ServiceStack{ID: id, ProjectID: "project", Name: entry.Hostname, Base: entry.Type, Status: status, NestID: "example", CurrentAutoscaling: config}
				for k, value := range entry.Env {
					f.env[id] = append(f.env[id], EnvItem{ID: f.next("env"), Key: k, Content: value, Type: "USER", Sensitive: true})
				}
				for k, value := range entry.Vars {
					f.env[id] = append(f.env[id], EnvItem{ID: f.next("env"), Key: k, Content: value, Type: "USER"})
				}
				result.ServiceStacks = append(result.ServiceStacks, struct {
					ID        string    `json:"id"`
					Name      string    `json:"name"`
					Processes []Process `json:"processes"`
				}{id, entry.Hostname, []Process{f.process(id)}})
			}
			respond(result)
		case "sync-public-port-routing":
			for id, route := range f.routes {
				if route.DeleteOnSync {
					delete(f.routes, id)
				} else {
					route.IsSynced = true
					f.routes[id] = route
				}
			}
			respond(f.process(""))
		case "log":
			respond(map[string]string{"url": f.url + "/signed-log"})
		default:
			fakeError(w, 404, "notFound")
		}
		return
	}
	if len(parts) >= 2 && parts[0] == "service-stack" {
		id := parts[1]
		s := f.services[id]
		if s == nil {
			fakeError(w, 400, "serviceStackNotFound")
			return
		}
		if len(parts) == 2 {
			if r.Method == "DELETE" {
				delete(f.services, id)
				delete(f.env, id)
				for key, route := range f.routes {
					if route.ServiceStackID == id {
						delete(f.routes, key)
					}
				}
				respond(f.process(id))
			} else {
				respond(s)
			}
			return
		}
		switch parts[2] {
		case "env":
			respond(map[string]any{"items": f.env[id]})
		case "user-data":
			var entry EnvItem
			if !decode(&entry) {
				return
			}
			entry.ID = f.next("env")
			entry.Type = "USER"
			f.env[id] = append(f.env[id], entry)
			respond(f.process(id))
		case "autoscaling":
			var body struct {
				Custom map[string]json.RawMessage `json:"customAutoscaling"`
			}
			if !decode(&body) {
				return
			}
			s.CurrentAutoscaling = body.Custom
			s.CustomAutoscaling = body.Custom
			respond(map[string]any{"process": f.process(id)})
		case "start", "restart":
			s.Status = "ACTIVE"
			respond(f.process(id))
		case "stop":
			s.Status = "STOPPED"
			respond(f.process(id))
		case "enable-subdomain-access":
			s.SubdomainAccess = true
			respond(f.process(id))
		case "disable-subdomain-access":
			s.SubdomainAccess = false
			respond(f.process(id))
		case "process":
			list := []Process{}
			for _, process := range f.processes {
				if process.ServiceStackID == id {
					list = append(list, process)
				}
			}
			fakePage(w, r, list)
		case "app-version":
			if r.Method == http.MethodGet {
				list := []AppVersion{}
				for version, service := range f.versions {
					if service == id {
						list = append(list, AppVersion{ID: version, Status: f.versionStatus[version]})
					}
				}
				fakePage(w, r, list)
				return
			}
			var body map[string]string
			if !decode(&body) {
				return
			}
			if body["name"] == "" {
				fakeError(w, 400, "invalidName")
				return
			}
			version := f.next("version")
			f.versions[version] = id
			f.versionStatus[version] = "UPLOADING"
			respond(AppVersion{ID: version})
		case "public-port-routing":
			if r.Method == "POST" {
				var route PublicPortRouting
				if !decode(&route) {
					return
				}
				route.ID = f.next("route")
				route.ServiceStackID = id
				f.routes[route.ID] = route
				respond(route)
			} else {
				list := []PublicPortRouting{}
				for _, route := range f.routes {
					if route.ServiceStackID == id {
						list = append(list, route)
					}
				}
				fakePage(w, r, list)
			}
		case "container":
			list := []RuntimeContainer{}
			if s.Status == "ACTIVE" && !strings.HasPrefix(s.Base, "local-storage") {
				h, _ := decodeObject(s.CurrentAutoscaling["horizontalAutoscaling"])
				for i := range max(1, int(number(h, "minContainerCount"))) {
					list = append(list, RuntimeContainer{ID: fmt.Sprintf("%sc%d", id, i), Hostname: fmt.Sprintf("%s%d", s.Name, i+1), Status: "ACTIVE", IPv4: "192.0.2.2", IPv6: "2001:db8::2"})
				}
			}
			fakePage(w, r, list)
		default:
			fakeError(w, 404, "notFound")
		}
		return
	}
	if len(parts) == 2 && parts[0] == "user-data" {
		var entry EnvItem
		if !decode(&entry) {
			return
		}
		for id, entries := range f.env {
			for i, old := range entries {
				if old.ID == parts[1] {
					entry.ID = old.ID
					entry.Type = "USER"
					f.env[id][i] = entry
					respond(f.process(id))
					return
				}
			}
		}
		fakeError(w, 404, "notFound")
		return
	}
	if len(parts) == 2 && parts[0] == "public-port-routing" {
		route, ok := f.routes[parts[1]]
		if !ok {
			fakeError(w, 404, "notFound")
			return
		}
		route.DeleteOnSync = true
		route.IsSynced = false
		f.routes[route.ID] = route
		respond(map[string]bool{"deleted": false})
		return
	}
	if len(parts) == 2 && parts[0] == "app-version" && r.Method == http.MethodGet {
		if _, ok := f.versions[parts[1]]; !ok {
			fakeError(w, 400, "appVersionNotFound")
			return
		}
		respond(AppVersion{ID: parts[1], Status: f.versionStatus[parts[1]]})
		return
	}
	if len(parts) == 3 && parts[0] == "app-version" {
		id, ok := f.versions[parts[1]]
		if !ok {
			fakeError(w, 400, "appVersionNotFound")
			return
		}
		switch parts[2] {
		case "upload":
			data, err := io.ReadAll(r.Body)
			if err != nil || len(data) == 0 {
				fakeError(w, 400, "invalidArtifact")
				return
			}
			f.uploaded[parts[1]] = true
			f.versionStatus[parts[1]] = "WAITING_TO_DEPLOY"
			respond(map[string]bool{"success": true})
		case "deploy":
			if !f.uploaded[parts[1]] {
				fakeError(w, 409, "artifactMissing")
				return
			}
			var body map[string]string
			if !decode(&body) {
				return
			}
			var doc struct {
				Zerops []struct {
					Setup string `yaml:"setup"`
					Run   struct {
						Ports []struct {
							Port int  `yaml:"port"`
							HTTP bool `yaml:"httpSupport"`
						} `yaml:"ports"`
						Env map[string]string `yaml:"envVariables"`
					} `yaml:"run"`
				} `yaml:"zerops"`
			}
			if err := yaml.Unmarshal([]byte(body["zeropsYaml"]), &doc); err != nil {
				fakeError(w, 400, "invalidYaml")
				return
			}
			s := f.services[id]
			if s == nil {
				fakeError(w, 404, "serviceStackNotFound")
				return
			}
			matched := false
			for _, setup := range doc.Zerops {
				if setup.Setup == body["zeropsYamlSetup"] && setup.Setup == s.Name {
					matched = true
					s.Ports = nil
					for _, p := range setup.Run.Ports {
						scheme := "tcp"
						if p.HTTP {
							scheme = "http"
						}
						s.Ports = append(s.Ports, ServicePort{Port: p.Port, Scheme: scheme, HTTPSupport: p.HTTP})
					}
					for k, value := range setup.Run.Env {
						found := false
						for i, item := range f.env[id] {
							if item.Key == k {
								f.env[id][i].Content = value
								found = true
								break
							}
						}
						if !found {
							f.env[id] = append(f.env[id], EnvItem{ID: f.next("env"), Key: k, Content: value, Type: "USER"})
						}
					}
				}
			}
			if !matched {
				fakeError(w, 400, "setupNotFound")
				return
			}
			s.Status = "ACTIVE"
			f.versionStatus[parts[1]] = "ACTIVE"
			process := f.process(id)
			process.ActionName = "appVersion.deploy"
			process.AppVersion = &AppVersion{ID: parts[1], Status: "ACTIVE"}
			f.processes[process.ID] = process
			respond(process)
		default:
			fakeError(w, 404, "notFound")
		}
		return
	}
	fakeError(w, 404, "notFound")
}
