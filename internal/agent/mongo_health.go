// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func (a *nodeAgent) mongoRouterHealth(ctx context.Context) (*bool, string) {
	c := a.config()
	if c.Role != agentapi.RoleRouter || c.Engine != "" && c.Engine != "psmdb" {
		return nil, ""
	}
	ready := false
	credential, ok := c.SystemUsers["clusterMonitor"]
	if !ok || credential.Password == "" {
		return &ready, "Router monitoring credential unavailable"
	}
	if credential.Username == "" {
		credential.Username = "clusterMonitor"
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	client, err := localMongo(c, &credential)
	if err != nil {
		return &ready, "Router authenticated metadata probe failed; inspect credentials and TLS"
	}
	defer client.Disconnect(context.Background())
	// A local ping or cached routing table does not establish config-server
	// availability. This majority metadata read must reach the config replica set.
	// The driver derives maxTimeMS from the command deadline. Supplying the
	// field in RunCommand as well produces duplicate BSON fields on the wire.
	command := bson.D{{Key: "find", Value: "shards"}, {Key: "filter", Value: bson.D{}}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}, {Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}}}
	commandCtx, commandCancel := context.WithTimeout(ctx, 2*time.Second)
	defer commandCancel()
	if err = client.Database("config").RunCommand(commandCtx, command, options.RunCmd().SetReadPreference(readpref.Primary())).Err(); err != nil {
		diagnostic := routerMongoDiagnostic(err)
		message := "Router cannot read config-server majority metadata; inspect config-server quorum, network, DNS and monitoring credentials; " + diagnostic
		if a.o.Logger != nil {
			a.o.Logger.Warn("router metadata probe failed", "diagnostic", diagnostic)
		}
		return &ready, message
	}
	ready = true
	return &ready, ""
}

// Native messages can contain credentials, URIs or query data. Retain only
// bounded protocol identifiers and static local error categories.
func routerMongoDiagnostic(err error) string {
	var command mongo.CommandError
	if errors.As(err, &command) && command.Code != 0 {
		name := command.Name
		if len(name) == 0 || len(name) > 64 {
			name = "Unknown"
		}
		for _, ch := range name {
			if !(ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9') {
				name = "Unknown"
				break
			}
		}
		return fmt.Sprintf("MongoDB code=%d codeName=%s", command.Code, name)
	}
	var selection topology.ServerSelectionError
	if errors.As(err, &selection) {
		// Selection wraps its deadline, not individual server handshake errors.
		// Inspect those typed causes before calling the whole selection a timeout.
		for _, server := range selection.Desc.Servers {
			if cause := routerMongoHandshakeCause(server.LastError); cause != "" {
				return "MongoDB server selection: " + cause
			}
		}
		for _, server := range selection.Desc.Servers {
			if cause := routerMongoClientCause(server.LastError); cause != "" {
				return "MongoDB server selection: " + cause
			}
		}
		if cause := routerMongoClientCause(err); cause != "" {
			return "MongoDB server selection: " + cause
		}
		return "MongoDB server selection failed"
	}
	if cause := routerMongoHandshakeCause(err); cause != "" {
		return "MongoDB " + cause
	}
	if cause := routerMongoClientCause(err); cause != "" {
		return "MongoDB " + cause
	}
	return "MongoDB client error"
}

func routerMongoHandshakeCause(err error) string {
	var verification *tls.CertificateVerificationError
	var authority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var roots x509.SystemRootsError
	if errors.As(err, &verification) || errors.As(err, &authority) ||
		errors.As(err, &hostname) || errors.As(err, &invalid) || errors.As(err, &roots) {
		return "TLS certificate verification failed"
	}
	var alert tls.AlertError
	var header tls.RecordHeaderError
	if errors.As(err, &alert) || errors.As(err, &header) {
		return "TLS handshake failed"
	}
	var authentication *auth.Error
	if errors.As(err, &authentication) {
		return "authentication failed"
	}
	return ""
}

func routerMongoClientCause(err error) string {
	if errors.Is(err, context.Canceled) {
		return "probe canceled"
	}
	if mongo.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if mongo.IsNetworkError(err) {
		return "network error"
	}
	var connection topology.ConnectionError
	if errors.As(err, &connection) {
		return "network error"
	}
	return ""
}
