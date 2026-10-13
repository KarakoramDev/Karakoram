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
)

func localMongo(c agentapi.MemberConfig, cred *agentapi.Credentials) (*mongo.Client, error) {
	opts := options.Client().SetHosts([]string{fmt.Sprintf("127.0.0.1:%d", memberPort(c))}).SetDirect(true).SetServerSelectionTimeout(3 * time.Second).SetConnectTimeout(3 * time.Second)
	opts.SetReadPreference(readpref.Nearest())
	if cred != nil {
		user := cred.Username
		if user == "" {
			return nil, errors.New("MongoDB username required")
		}
		db := cred.Database
		if db == "" {
			db = "admin"
		}
		opts.SetAuth(options.Credential{Username: user, Password: cred.Password, AuthSource: db})
	}
	if c.TLS.Mode != "" && c.TLS.Mode != "disabled" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(c.TLS.CAPEM)) {
			return nil, errors.New("invalid MongoDB CA")
		}
		cert, err := tls.X509KeyPair([]byte(c.TLS.CertPEM), []byte(c.TLS.KeyPEM))
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: c.Member, Certificates: []tls.Certificate{cert}})
	}
	return mongo.Connect(opts)
}
func pause(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}
func (a *nodeAgent) bootstrap(ctx context.Context, r agentapi.BootstrapRequest) (agentapi.BootstrapResponse, error) {
	var out agentapi.BootstrapResponse
	c := a.config()
	if c.Role == agentapi.RoleRouter {
		return out, errors.New("cannot bootstrap mongos")
	}
	if r.ReplSetName != c.ReplSetName || len(r.Members) == 0 || r.UserAdmin.Username != "userAdmin" || r.UserAdmin.Password == "" {
		return out, errors.New("invalid bootstrap replica set or userAdmin")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
	}
	client, err := localMongo(c, nil)
	if err != nil {
		return out, err
	}
	defer client.Disconnect(context.Background())
	for {
		if client.Ping(ctx, readpref.Nearest()) == nil {
			break
		}
		if err = pause(ctx); err != nil {
			return out, err
		}
	}
	admin := client.Database("admin")
	var hello bson.M
	err = admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
	if err != nil {
		return out, err
	}
	_, initiated := hello["setName"]
	if !initiated {
		members := bson.A{}
		for _, m := range r.Members {
			v := bson.M{"_id": m.ID, "host": m.Host}
			if m.Priority != nil {
				v["priority"] = *m.Priority
			}
			if m.Votes != nil {
				v["votes"] = *m.Votes
			}
			members = append(members, v)
		}
		config := bson.M{"_id": r.ReplSetName, "members": members}
		if r.ConfigServer || c.Role == agentapi.RoleConfigServer {
			config["configsvr"] = true
		}
		err = admin.RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: config}}).Err()
		var cmdErr mongo.CommandError
		if err != nil && !(errors.As(err, &cmdErr) && cmdErr.Code == 23) {
			return out, err
		}
	}
	out.Initialized = true
	// Authentication/user verification works on a secondary too. This also
	// handles a lost createUser response without waiting for a new election.
	authenticated, authErr := localMongo(c, &r.UserAdmin)
	if authErr != nil {
		return out, authErr
	}
	defer authenticated.Disconnect(context.Background())
	verify := func() error {
		var users struct {
			Users []bson.M `bson:"users"`
		}
		err := authenticated.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: bson.M{"user": "userAdmin", "db": "admin"}}}, options.RunCmd().SetReadPreference(readpref.Nearest())).Decode(&users)
		if err != nil {
			return err
		}
		if len(users.Users) != 1 {
			return errors.New("userAdmin was not found after bootstrap")
		}
		return nil
	}
	if verify() == nil {
		return out, nil
	}
	for {
		hello = bson.M{}
		if err = admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err == nil && hello["isWritablePrimary"] == true {
			break
		}
		// hello can advertise this node as primary before it becomes writable.
		// Wait locally instead of redirecting back to the same agent.
		if primary, ok := hello["primary"].(string); ok && primary != "" && primary != hello["me"] {
			out.Primary = primary
			return out, nil
		}
		if err = pause(ctx); err != nil {
			return out, err
		}
	}
	// createUser is authorized by the localhost exception only while no users exist.
	err = admin.RunCommand(ctx, bson.D{{Key: "createUser", Value: "userAdmin"}, {Key: "pwd", Value: r.UserAdmin.Password}, {Key: "roles", Value: bson.A{bson.M{"role": "userAdminAnyDatabase", "db": "admin"}}}}).Err()
	if err == nil {
		out.UserCreated = true
		return out, nil
	}
	var cmdErr mongo.CommandError
	if !errors.As(err, &cmdErr) || (cmdErr.Code != 13 && cmdErr.Code != 51003) {
		return out, err
	}
	if authErr = verify(); authErr != nil {
		return out, authErr
	}
	return out, nil
}
