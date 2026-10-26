// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func TestMongoRouterHealthFailsClosedWithoutMetadataObservation(t *testing.T) {
	a := &nodeAgent{c: agentapi.MemberConfig{Role: agentapi.RoleReplicaSet}}
	if ready, message := a.mongoRouterHealth(t.Context()); ready != nil || message != "" {
		t.Fatal("non-router subjected to config metadata probe")
	}
	a.c.Role = agentapi.RoleRouter
	if ready, message := a.mongoRouterHealth(t.Context()); ready == nil || *ready || message == "" {
		t.Fatal("router without monitoring credentials accepted")
	}
	a.c.TLS.Mode = "disabled"
	a.c.SystemUsers = map[string]agentapi.Credentials{"clusterMonitor": {Username: "clusterMonitor", Password: "desired-password"}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if ready, message := a.mongoRouterHealth(ctx); ready == nil || *ready || message == "" {
		t.Fatal("unobserved config-server metadata accepted")
	}
}

func TestRouterMongoDiagnosticPreservesCodesWithoutNativeMessages(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"command", mongo.CommandError{Code: 13, Name: "Unauthorized", Message: "mongodb://private:password@host:27017/query"}, "MongoDB code=13 codeName=Unauthorized"},
		{"wrapped", fmt.Errorf("private password: %w", mongo.CommandError{Code: 50, Name: "MaxTimeMSExpired", Message: "private query"}), "MongoDB code=50 codeName=MaxTimeMSExpired"},
		{"unsafe name", mongo.CommandError{Code: 2, Name: "BadValue\npassword=private"}, "MongoDB code=2 codeName=Unknown"},
		{"long name", mongo.CommandError{Code: 2, Name: strings.Repeat("x", 65)}, "MongoDB code=2 codeName=Unknown"},
		{"missing name", mongo.CommandError{Code: 2}, "MongoDB code=2 codeName=Unknown"},
		{"deadline", context.DeadlineExceeded, "MongoDB timeout"},
		{"canceled", context.Canceled, "MongoDB probe canceled"},
		{"driver network wrapper", mongo.CommandError{Labels: []string{"NetworkError"}, Wrapped: topology.ConnectionError{Wrapped: errors.New("private endpoint")}}, "MongoDB network error"},
		{"driver deadline wrapper", mongo.CommandError{Labels: []string{"NetworkError"}, Wrapped: context.DeadlineExceeded}, "MongoDB timeout"},
		{"driver cancellation wrapper", mongo.CommandError{Labels: []string{"NetworkError"}, Wrapped: context.Canceled}, "MongoDB probe canceled"},
		{"driver auth wrapper", mongo.CommandError{Wrapped: &auth.Error{}}, "MongoDB authentication failed"},
		{"TLS verification", mongo.CommandError{Wrapped: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}, "MongoDB TLS certificate verification failed"},
		{"TLS hostname", topology.ConnectionError{Wrapped: x509.HostnameError{Host: "private endpoint"}}, "MongoDB TLS certificate verification failed"},
		{"TLS alert", tls.AlertError(42), "MongoDB TLS handshake failed"},
		{"selection deadline", mongo.CommandError{Wrapped: topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}}, "MongoDB server selection: timeout"},
		{"selection unknown", topology.ServerSelectionError{}, "MongoDB server selection failed"},
		{"selection TLS deadline", mongo.CommandError{Wrapped: topology.ServerSelectionError{
			Wrapped: context.DeadlineExceeded,
			Desc: description.Topology{Servers: []description.Server{
				{LastError: topology.ConnectionError{Wrapped: errors.New("private transport detail")}},
				{LastError: topology.ConnectionError{Wrapped: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}},
			}},
		}}, "MongoDB server selection: TLS certificate verification failed"},
		{"selection auth", topology.ServerSelectionError{
			Wrapped: context.DeadlineExceeded,
			Desc:    description.Topology{Servers: []description.Server{{LastError: topology.ConnectionError{Wrapped: &auth.Error{}}}}},
		}, "MongoDB server selection: authentication failed"},
		{"selection network", topology.ServerSelectionError{
			Wrapped: context.DeadlineExceeded,
			Desc:    description.Topology{Servers: []description.Server{{LastError: topology.ConnectionError{Wrapped: errors.New("private endpoint")}}}},
		}, "MongoDB server selection: network error"},
		{"native code retained over network label", mongo.CommandError{Code: 91, Name: "ShutdownInProgress", Labels: []string{"NetworkError"}, Wrapped: context.DeadlineExceeded}, "MongoDB code=91 codeName=ShutdownInProgress"},
		{"other", errors.New("private password"), "MongoDB client error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := routerMongoDiagnostic(tc.err); got != tc.want {
				t.Fatalf("got %q; want %q", got, tc.want)
			}
		})
	}
}

func TestMongoRouterHealthLogsOnlySanitizedFailure(t *testing.T) {
	var logs bytes.Buffer
	a := &nodeAgent{
		o: Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))},
		c: agentapi.MemberConfig{
			Role:        agentapi.RoleRouter,
			TLS:         agentapi.TLSConfig{Mode: "disabled"},
			SystemUsers: map[string]agentapi.Credentials{"clusterMonitor": {Username: "clusterMonitor", Password: "private-password"}},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ready, message := a.mongoRouterHealth(ctx)
	if ready == nil || *ready || !strings.Contains(message, "MongoDB server selection: probe canceled") || !strings.Contains(logs.String(), "MongoDB server selection: probe canceled") {
		t.Fatalf("failure not retained in status and log: ready=%v message=%q log=%q", ready, message, logs.String())
	}
	if strings.Contains(message+logs.String(), "private-password") {
		t.Fatal("credential leaked")
	}
}
