// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mongoadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mnet"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func TestReadErrorAvailability(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		unavailable bool
	}{
		{"success", nil, false},
		{"deadline", context.DeadlineExceeded, true},
		{"selection", topology.ServerSelectionError{}, true},
		{"network", mongo.CommandError{Labels: []string{"NetworkError"}}, true},
		{"network timeout", mongo.CommandError{Labels: []string{"NetworkTimeoutError"}}, true},
		{"shutdown", mongo.CommandError{Code: 91}, true},
		{"not primary", mongo.CommandError{Code: 10107}, true},
		{"recovering", mongo.CommandError{Code: 11602}, true},
		{"host unreachable", mongo.CommandError{Code: 6}, true},
		{"authentication", mongo.CommandError{Code: 18}, false},
		{"authorization", mongo.CommandError{Code: 13}, false},
		{"auth beats labels", mongo.CommandError{Code: 18, Labels: []string{"NetworkError"}}, false},
		{"handshake auth", &auth.Error{}, false},
		{"selection auth", topology.ServerSelectionError{Wrapped: context.DeadlineExceeded, Desc: description.Topology{Servers: []description.Server{{LastError: topology.ConnectionError{Wrapped: &auth.Error{}}}}}}, false},
		{"selection auth code", topology.ServerSelectionError{Desc: description.Topology{Servers: []description.Server{{LastError: driver.Error{Code: 18}}}}}, false},
		{"bad command", mongo.CommandError{Code: 59}, false},
		{"canceled", context.Canceled, false},
		{"other", errors.New("unexpected failure"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.err
			if err != nil {
				err = fmt.Errorf("read: %w", err)
			}
			got := ReadError(err)
			if errors.Is(got, engine.ErrUnavailable) != tc.unavailable {
				t.Fatalf("ReadError(%v) = %v, unavailable want %v", err, got, tc.unavailable)
			}
			if !tc.unavailable && got != err {
				t.Fatalf("non-transient error changed: %v", got)
			}
			if err != nil && !errors.Is(got, err) {
				t.Fatalf("underlying error lost: %v", got)
			}
		})
	}
}

// Obtain non-empty auth.Error wrappers through the real driver's hello and SASL
// implementations: the wrapper's fields are private and transport is not authn.
func TestReadErrorAuthenticationExchangeFailures(t *testing.T) {
	for _, stage := range []string{"hello", "SASL"} {
		for _, failure := range []string{"network", "timeout", "shutdown", "authentication"} {
			t.Run(stage+"/"+failure, func(t *testing.T) {
				wire := &drivertest.ChannelConn{
					Written:  make(chan []byte, 2),
					ReadResp: make(chan []byte, 1),
					Desc:     description.Server{Kind: description.ServerKindMongos, WireVersion: &description.VersionRange{Min: 0, Max: 25}},
				}
				switch failure {
				case "network":
					wire.WriteErr = io.ErrUnexpectedEOF
				case "timeout":
					wire.WriteErr = context.DeadlineExceeded
				default:
					code := 91
					if failure == "authentication" {
						code = 18
					}
					doc, err := bson.Marshal(bson.M{"ok": 0, "code": code, "errmsg": "test exchange failed"})
					if err != nil {
						t.Fatal(err)
					}
					wire.ReadResp <- drivertest.MakeReply(doc)
				}
				conn := mnet.NewConnection(wire)
				var err error
				if stage == "hello" {
					h := auth.Handshaker(nil, &auth.HandshakeOptions{})
					_, err = h.GetHandshakeInformation(t.Context(), address.Address("router:27017"), conn)
				} else {
					a, createErr := auth.CreateAuthenticator("SCRAM-SHA-256", &auth.Cred{Source: "admin", Username: "admin", Password: "test-password"}, nil)
					if createErr != nil {
						t.Fatal(createErr)
					}
					err = a.Auth(t.Context(), &driver.AuthConfig{Connection: conn})
				}
				var authentication *auth.Error
				if !errors.As(err, &authentication) || authentication.Inner() == nil {
					t.Fatalf("missing real auth wrapper/cause: %T %v", err, err)
				}
				wantUnavailable := failure != "authentication"
				for _, cause := range []error{
					err,
					topology.ServerSelectionError{Wrapped: context.DeadlineExceeded, Desc: description.Topology{Servers: []description.Server{{LastError: topology.ConnectionError{Wrapped: err}}}}},
				} {
					wrapped := fmt.Errorf("read: %w", cause)
					got := ReadError(wrapped)
					if errors.Is(got, engine.ErrUnavailable) != wantUnavailable || !errors.Is(got, wrapped) {
						t.Fatalf("exchange %v: ReadError = %v, unavailable want %v", cause, got, wantUnavailable)
					}
				}
			})
		}
	}
}
