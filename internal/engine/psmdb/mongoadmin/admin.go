// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package mongoadmin implements authenticated, quorum-aware MongoDB administration.
package mongoadmin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// DialOptions selects server discovery, authentication and certificate verification.
type DialOptions struct {
	Hosts              []string
	ReplicaSet         string
	Direct             bool
	Username, Password string
	CAPEM              []byte
	TLS                bool
	Timeout            time.Duration
	Dialer             options.ContextDialer
}

// Dial connects and verifies that the authenticated endpoint responds.
func Dial(ctx context.Context, o DialOptions) (*mongo.Client, error) {
	if len(o.Hosts) == 0 {
		return nil, errors.New("MongoDB hosts are empty")
	}
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}
	opt := options.Client().SetHosts(o.Hosts).SetDirect(o.Direct).SetServerSelectionTimeout(o.Timeout).SetConnectTimeout(o.Timeout)
	if o.Dialer != nil {
		opt.SetDialer(o.Dialer)
	}
	if o.ReplicaSet != "" {
		opt.SetReplicaSet(o.ReplicaSet)
	}
	if o.Username != "" {
		opt.SetAuth(options.Credential{AuthSource: "admin", Username: o.Username, Password: o.Password})
	}
	if o.TLS {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(o.CAPEM) {
			return nil, errors.New("invalid MongoDB CA")
		}
		opt.SetTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
	}
	c, err := mongo.Connect(opt)
	if err != nil {
		return nil, err
	}
	if err = c.Ping(ctx, readpref.Nearest()); err != nil {
		_ = c.Disconnect(ctx)
		return nil, err
	}
	return c, nil
}

// Member is an observed replica-set member.
type Member struct {
	ID         int       `bson:"_id"`
	Name       string    `bson:"name"`
	State      string    `bson:"stateStr"`
	Health     float64   `bson:"health"`
	Uptime     int64     `bson:"uptime"`
	OptimeDate time.Time `bson:"optimeDate"`
}

// Status is the server's replica-set status response.
type Status struct {
	Set     string   `bson:"set"`
	Members []Member `bson:"members"`
}

// GetStatus retrieves replication roles and optimes.
func GetStatus(ctx context.Context, c *mongo.Client) (Status, error) {
	var v Status
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}, options.RunCmd().SetReadPreference(readpref.Nearest())).Decode(&v)
	return v, err
}

// GetObservedStatus bounds one replication read without shortening subsequent
// administrative commands.
func GetObservedStatus(ctx context.Context, c *mongo.Client, host string) (Status, error) {
	readCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	status, err := GetStatus(readCtx, c)
	if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(readCtx.Err(), context.DeadlineExceeded)) {
		return status, fmt.Errorf("PSMDB replication status read exceeded %s budget: inspect member %s responsiveness/connectivity: %w", engine.ObservationTimeout, host, err)
	}
	return status, err
}

// GetPrimaryStatus uses the writable side's replication view. A partitioned
// secondary's nearest-server view must not label reachable primary peers dead.
func GetPrimaryStatus(ctx context.Context, c *mongo.Client) (Status, error) {
	var v Status
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}, options.RunCmd().SetReadPreference(readpref.Primary())).Decode(&v)
	return v, err
}

// HasWritablePrimary rejects secondary-only and contradictory replication views.
func (s Status) HasWritablePrimary() bool {
	primaries := 0
	for _, member := range s.Members {
		if member.State == "PRIMARY" && member.Health == 1 {
			primaries++
		}
	}
	return primaries == 1
}

// CheckWritablePrimary selects the primary and verifies its current hello
// response, rather than treating a successful nearest/secondary ping as writable.
func CheckWritablePrimary(ctx context.Context, c *mongo.Client) error {
	var hello struct {
		Writable bool `bson:"isWritablePrimary"`
	}
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}, options.RunCmd().SetReadPreference(readpref.Primary())).Decode(&hello); err != nil {
		return err
	}
	if !hello.Writable {
		return errors.New("no writable primary; restore replica-set quorum and inspect election, storage and network diagnostics; automatic reinitialization is refused")
	}
	return nil
}

// Lag reports seconds behind the newest optime in this replica set.
func (s Status) Lag(m Member) float64 {
	var newest time.Time
	for _, x := range s.Members {
		if x.OptimeDate.After(newest) {
			newest = x.OptimeDate
		}
	}
	if m.OptimeDate.IsZero() {
		return 1e9
	}
	return newest.Sub(m.OptimeDate).Seconds()
}

// ConfigMember is a replica-set configuration entry.
type ConfigMember struct {
	ID       int     `bson:"_id"`
	Host     string  `bson:"host"`
	Priority float64 `bson:"priority"`
	Votes    int     `bson:"votes"`
	Extra    bson.M  `bson:",inline"`
}

// Config preserves additional server-side configuration fields during reconfiguration.
type Config struct {
	ID      string         `bson:"_id"`
	Version int            `bson:"version"`
	Members []ConfigMember `bson:"members"`
	Extra   bson.M         `bson:",inline"`
}

// GetConfig reads the current configuration from the primary.
func GetConfig(ctx context.Context, c *mongo.Client) (Config, error) {
	var v struct {
		Config Config `bson:"config"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}, {Key: "commitmentStatus", Value: true}}).Decode(&v)
	return v.Config, err
}
func reconfig(ctx context.Context, c *mongo.Client, v Config) error {
	v.Version++
	return c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetReconfig", Value: v}}).Err()
}

// AddNonVoting adds one member without changing the voting majority.
func AddNonVoting(ctx context.Context, c *mongo.Client, id int, host string) error {
	v, e := GetConfig(ctx, c)
	if e != nil {
		return e
	}
	for _, m := range v.Members {
		if m.Host == host {
			return nil
		}
		if m.ID == id {
			return fmt.Errorf("member ID %d already exists", id)
		}
	}
	v.Members = append(v.Members, ConfigMember{ID: id, Host: host, Priority: 0, Votes: 0})
	return reconfig(ctx, c, v)
}

// Promote votes a caught-up secondary, changing only one vote per reconfig.
func Promote(ctx context.Context, c *mongo.Client, host string) error {
	v, e := GetConfig(ctx, c)
	if e != nil {
		return e
	}
	s, e := GetObservedStatus(ctx, c, host)
	if e != nil {
		return e
	}
	changed, e := promoteConfig(&v, s, host)
	if e != nil || !changed {
		return e
	}
	return reconfig(ctx, c, v)
}

func promoteConfig(v *Config, s Status, host string) (bool, error) {
	for i, configured := range v.Members {
		if configured.Host != host {
			continue
		}
		for _, observed := range s.Members {
			if observed.Name != host {
				continue
			}
			if observed.Health != 1 || s.Lag(observed) > 10 {
				return false, errors.New("member is not healthy and caught up")
			}
			if configured.Votes == 1 && (observed.State == "PRIMARY" || observed.State == "SECONDARY") {
				return false, nil
			}
			if observed.State != "SECONDARY" {
				return false, errors.New("member is not a caught-up secondary")
			}
			v.Members[i].Votes, v.Members[i].Priority = 1, 1
			return true, nil
		}
		return false, errors.New("member absent from replica-set status")
	}
	return false, errors.New("member absent from replica-set configuration")
}

// Remove removes a non-primary only when the existing majority remains healthy.
func Remove(ctx context.Context, c *mongo.Client, host string) error {
	v, e := GetConfig(ctx, c)
	if e != nil {
		return e
	}
	s, e := GetObservedStatus(ctx, c, host)
	if e != nil {
		return e
	}
	votes, healthy, removedVotes, removedHealthy := 0, 0, 0, 0
	found := false
	for _, m := range v.Members {
		votes += m.Votes
		if m.Host == host {
			found = true
			removedVotes = m.Votes
			for _, x := range s.Members {
				if x.Name == host && x.Health == 1 && x.State == "SECONDARY" {
					removedHealthy = m.Votes
				}
			}
			continue
		}
		for _, x := range s.Members {
			if x.Name == m.Host && x.Health == 1 && (x.State == "PRIMARY" || x.State == "SECONDARY") {
				healthy += m.Votes
			}
		}
	}
	if !found {
		return nil
	}
	for _, m := range s.Members {
		if m.Name == host && m.State == "PRIMARY" {
			return errors.New("refusing removal of primary")
		}
	}
	if healthy+removedHealthy < votes/2+1 || healthy < (votes-removedVotes)/2+1 {
		return errors.New("removal would lose the old or new voting majority")
	}
	out := v.Members[:0]
	for _, m := range v.Members {
		if m.Host != host {
			out = append(out, m)
		}
	}
	v.Members = out
	return reconfig(ctx, c, v)
}

// SafeToRestart refuses a restart that would leave no healthy voting majority.
func SafeToRestart(ctx context.Context, c *mongo.Client, host string) error {
	v, e := GetConfig(ctx, c)
	if e != nil {
		return e
	}
	if len(v.Members) == 1 {
		return nil
	}
	s, e := GetObservedStatus(ctx, c, host)
	if e != nil {
		return e
	}
	votes, healthy := 0, 0
	for _, m := range v.Members {
		votes += m.Votes
		if m.Host == host {
			continue
		}
		for _, x := range s.Members {
			if x.Name == m.Host && x.Health == 1 && (x.State == "PRIMARY" || x.State == "SECONDARY") && s.Lag(x) <= 10 {
				healthy += m.Votes
			}
		}
	}
	if healthy < votes/2+1 {
		return errors.New("restart would lose voting majority")
	}
	return nil
}

// StepDown transfers primary ownership without forcing an unsafe election.
func StepDown(ctx context.Context, c *mongo.Client) error {
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetStepDown", Value: 60}, {Key: "secondaryCatchUpPeriodSecs", Value: 30}}).Err()
	var ce mongo.CommandError
	if errors.As(err, &ce) && (ce.Code == 11600 || ce.Code == 11602) {
		return nil
	}
	return err
}

// FCV reads the current feature compatibility version.
func FCV(ctx context.Context, c *mongo.Client) (string, error) {
	var v struct {
		FCV struct {
			Version string `bson:"version"`
		} `bson:"featureCompatibilityVersion"`
	}
	e := c.Database("admin").RunCommand(ctx, bson.D{{Key: "getParameter", Value: 1}, {Key: "featureCompatibilityVersion", Value: 1}}).Decode(&v)
	return v.FCV.Version, e
}

// SetFCV explicitly raises feature compatibility after binary upgrades.
func SetFCV(ctx context.Context, c *mongo.Client, version string) error {
	cmd := bson.D{{Key: "setFeatureCompatibilityVersion", Value: version}}
	if version != "6.0" {
		cmd = append(cmd, bson.E{Key: "confirm", Value: true})
	}
	return c.Database("admin").RunCommand(ctx, cmd).Err()
}

// AddShard registers a shard idempotently using its replica-set connection string.
func AddShard(ctx context.Context, c *mongo.Client, set, hosts string) error {
	return c.Database("admin").RunCommand(ctx, bson.D{{Key: "addShard", Value: set + "/" + hosts}, {Key: "name", Value: set}}).Err()
}

// SetBalancer changes balancer operation on a mongos.
func SetBalancer(ctx context.Context, c *mongo.Client, enabled bool) error {
	command := "balancerStop"
	if enabled {
		command = "balancerStart"
	}
	return c.Database("admin").RunCommand(ctx, bson.D{{Key: command, Value: 1}}).Err()
}

// Balancer reads enabled/running state from mongos.
func Balancer(ctx context.Context, c *mongo.Client) (string, bool, error) {
	var v struct {
		Mode    string `bson:"mode"`
		Running bool   `bson:"inBalancerRound"`
	}
	e := c.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStatus", Value: 1}}).Decode(&v)
	return v.Mode, v.Running, e
}
