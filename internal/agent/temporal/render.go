// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"net"
	"net/url"
	"path/filepath"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"gopkg.in/yaml.v3"
)

type configMap = map[string]any

func processName(s tspec.Spec) string {
	if s.Component == tspec.UI {
		return "ui-server"
	}
	return "temporal-server"
}

// render keeps credentials in private files, never in process arguments or environment.
func render(s tspec.Spec, dataDir, address, bind, install string) (agent.ProcessSpec, map[string][]byte, error) {
	dir := filepath.Join(dataDir, "karakoram", "temporal")
	path := func(name string) string { return filepath.Join(dir, name) }
	files := map[string][]byte{
		"temporal/ca.pem":            []byte(s.TLS.CAPEM),
		"temporal/internode.pem":     []byte(s.TLS.InternodeCertPEM),
		"temporal/internode-key.pem": []byte(s.TLS.InternodeKeyPEM),
	}
	p := agent.ProcessSpec{Binary: filepath.Join(install, processName(s)), Dir: dir, Env: []string{"GOMAXPROCS=2"}, Args: []string{"--root", dir, "--config", "."}}
	var config configMap
	if s.Component == tspec.UI {
		publicPath := ""
		if s.UI != nil && s.UI.PublicURL != "" {
			u, err := url.Parse(s.UI.PublicURL)
			if err != nil {
				return p, nil, err
			}
			if u.Path != "" && u.Path != "/" {
				publicPath = u.Path
			}
		}
		auth := configMap{"enabled": false}
		if s.UI != nil && s.UI.OIDC != nil {
			o := s.UI.OIDC
			auth = configMap{
				"enabled": true,
				"providers": []any{configMap{
					"type": "oidc", "label": "OIDC",
					"providerUrl": o.Issuer,
					"clientId":    o.ClientID, "clientSecret": o.ClientSecret,
					"callbackUrl": o.CallbackURL,
					"scopes":      []string{"openid", "profile", "email"},
				}},
			}
		}
		config = configMap{
			"temporalGrpcAddress": s.FrontendAddress,
			"host":                bind, "port": tspec.UIPort,
			"publicPath": publicPath,
			"enableUi":   true, "disableNewsFetch": true,
			"auth": auth,
			"tls": configMap{
				"caFile":   path("ca.pem"),
				"certFile": path("internode.pem"), "keyFile": path("internode-key.pem"),
				"serverName":             s.TLS.FrontendServerName,
				"enableHostVerification": true,
			},
		}
		p.Args = append(p.Args, "start")
	} else {
		d := s.Persistence
		files["temporal/postgres-ca.pem"] = []byte(d.CAPEM)
		files["temporal/frontend.pem"] = []byte(s.TLS.FrontendCertPEM)
		files["temporal/frontend-key.pem"] = []byte(s.TLS.FrontendKeyPEM)
		files["temporal/dynamic.yaml"] = []byte("{}\n")
		store := func(database string, conns int) configMap {
			return configMap{"sql": configMap{
				"pluginName":      "postgres12",
				"databaseName":    database,
				"connectAddr":     net.JoinHostPort(d.Host, strconv.Itoa(d.Port)),
				"connectProtocol": "tcp",
				// lib/pq can force text results even for explicit prepares.
				// pgx's default query mode does not cover that path.
				"connectAttributes": configMap{"disable_prepared_binary_result": "yes"},
				"user":              d.Username, "password": d.Password,
				"maxConns": conns, "maxIdleConns": conns,
				"maxConnLifetime": "1h",
				"tls": configMap{
					"enabled":                true,
					"caFile":                 path("postgres-ca.pem"),
					"serverName":             d.Host,
					"enableHostVerification": true,
				},
			}}
		}
		client := func(name string) configMap {
			return configMap{
				"serverName":  name,
				"rootCaFiles": []string{path("ca.pem")},
			}
		}
		server := func(cert, key string, required bool) configMap {
			return configMap{
				"certFile": path(cert), "keyFile": path(key),
				"clientCaFiles":     []string{path("ca.pem")},
				"requireClientAuth": required,
			}
		}
		tls := configMap{
			"internode": configMap{
				"server": server("internode.pem", "internode-key.pem", true),
				"client": client(s.TLS.InternodeServerName),
			},
			"systemWorker": configMap{
				"certFile": path("internode.pem"), "keyFile": path("internode-key.pem"),
				"client": client(s.TLS.FrontendServerName),
			},
		}
		// Non-frontend roles still need the frontend client trust and server name.
		frontend := configMap{"client": client(s.TLS.FrontendServerName)}
		if hasFrontend(s) {
			frontend["server"] = server("frontend.pem", "frontend-key.pem", s.TLS.RequireClientAuth)
		}
		tls["frontend"] = frontend
		services := configMap{}
		for role, ports := range tspec.Ports {
			rpc := configMap{
				"grpcPort": ports.GRPC, "membershipPort": ports.Membership,
				"bindOnIP": bind,
			}
			if role == tspec.Frontend {
				rpc["httpPort"] = tspec.FrontendHTTPPort
			}
			services[role] = configMap{"rpc": rpc}
		}
		config = configMap{
			"log": configMap{"stdout": true, "level": "warn"},
			"persistence": configMap{
				"defaultStore": "default", "visibilityStore": "visibility",
				"numHistoryShards": s.HistoryShards,
				"datastores": configMap{
					"default":    store(d.Database, d.MaxConns),
					"visibility": store(d.VisibilityDatabase, d.VisibilityMaxConns),
				},
			},
			"global": configMap{
				"membership": configMap{
					"maxJoinDuration":  "30s",
					"broadcastAddress": address,
				},
				"tls": tls,
				"metrics": configMap{"prometheus": configMap{
					"framework": "tally", "timerType": "histogram",
					"listenAddress": "127.0.0.1:9090",
				}},
			},
			"services": services,
			"clusterMetadata": configMap{
				"enableGlobalNamespace":    false,
				"failoverVersionIncrement": 10,
				"masterClusterName":        s.ClusterName, "currentClusterName": s.ClusterName,
				"clusterInformation": configMap{s.ClusterName: configMap{
					"enabled": true, "initialFailoverVersion": 1,
					"rpcName":     "frontend",
					"rpcAddress":  net.JoinHostPort(address, "7233"),
					"httpAddress": net.JoinHostPort(address, "7243"),
				}},
			},
			"dcRedirectionPolicy": configMap{"policy": "noop"},
			"dynamicConfigClient": configMap{
				"filepath": path("dynamic.yaml"), "pollInterval": "10s",
			},
		}
		p.Args = append(p.Args, "--allow-no-auth", "start")
		for _, role := range tspec.Roles(s.Component) {
			p.Args = append(p.Args, "--service="+role)
		}
	}
	data, err := yaml.Marshal(config)
	if err != nil {
		return p, nil, err
	}
	files["temporal/development.yaml"] = data
	return p, files, nil
}

func hasFrontend(s tspec.Spec) bool {
	return s.Component == tspec.Server || s.Component == tspec.Frontend
}
