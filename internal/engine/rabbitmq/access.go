// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type VhostLimits struct {
	MaxConnections *int `json:"maxConnections,omitempty"`
	MaxQueues      *int `json:"maxQueues,omitempty"`
}
type Vhost struct {
	Name             string       `json:"name"`
	DefaultQueueType string       `json:"defaultQueueType,omitempty"`
	Description      string       `json:"description,omitempty"`
	Tags             []string     `json:"tags"`
	Limits           *VhostLimits `json:"limits,omitempty"`
}
type User struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}
type UserWrite struct {
	Name         string   `json:"name"`
	Tags         []string `json:"tags"`
	Password     *string  `json:"password,omitempty"`
	PasswordHash *string  `json:"passwordHash,omitempty"`
}
type Permission = mgmt.Permission
type TopicPermission = mgmt.TopicPermission
type Policy struct {
	Vhost      string         `json:"vhost"`
	Name       string         `json:"name"`
	Pattern    string         `json:"pattern"`
	ApplyTo    string         `json:"applyTo"`
	Priority   int            `json:"priority"`
	Definition map[string]any `json:"definition"`
}

// Access exposes RabbitMQ access objects without leaking password hashes.
type Access interface {
	ListVhosts(context.Context, *engine.Context) ([]Vhost, error)
	GetVhost(ctx context.Context, rc *engine.Context, name string) (*Vhost, error)
	CreateVhost(context.Context, *engine.Context, Vhost) error
	UpdateVhost(context.Context, *engine.Context, Vhost) error
	DeleteVhost(ctx context.Context, rc *engine.Context, name string) error
	ListUsers(context.Context, *engine.Context) ([]User, error)
	GetUser(ctx context.Context, rc *engine.Context, name string) (*User, error)
	CreateUser(context.Context, *engine.Context, UserWrite) (string, error)
	UpdateUser(context.Context, *engine.Context, UserWrite) error
	DeleteUser(ctx context.Context, rc *engine.Context, name string) error
	ListPermissions(context.Context, *engine.Context) ([]Permission, error)
	GetPermission(ctx context.Context, rc *engine.Context, vhost, user string) (*Permission, error)
	CreatePermission(context.Context, *engine.Context, Permission) error
	UpdatePermission(context.Context, *engine.Context, Permission) error
	DeletePermission(ctx context.Context, rc *engine.Context, vhost, user string) error
	ListTopicPermissions(context.Context, *engine.Context) ([]TopicPermission, error)
	GetTopicPermission(ctx context.Context, rc *engine.Context, vhost, user string) ([]TopicPermission, error)
	CreateTopicPermission(context.Context, *engine.Context, TopicPermission) error
	UpdateTopicPermission(context.Context, *engine.Context, TopicPermission) error
	DeleteTopicPermission(ctx context.Context, rc *engine.Context, vhost, user string) error
	ListPolicies(context.Context, *engine.Context) ([]Policy, error)
	GetPolicy(ctx context.Context, rc *engine.Context, vhost, name string) (*Policy, error)
	CreatePolicy(context.Context, *engine.Context, Policy) error
	UpdatePolicy(context.Context, *engine.Context, Policy) error
	DeletePolicy(ctx context.Context, rc *engine.Context, vhost, name string) error
	ListOperatorPolicies(context.Context, *engine.Context) ([]Policy, error)
	GetOperatorPolicy(ctx context.Context, rc *engine.Context, vhost, name string) (*Policy, error)
	CreateOperatorPolicy(context.Context, *engine.Context, Policy) error
	UpdateOperatorPolicy(context.Context, *engine.Context, Policy) error
	DeleteOperatorPolicy(ctx context.Context, rc *engine.Context, vhost, name string) error
}

var _ Access = (*Provider)(nil)

func accessName(name string) error {
	if name == "" || len(name) > 255 || !utf8.ValidString(name) || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return engine.Invalid("names must contain 1..255 UTF-8 bytes without control characters")
	}
	return nil
}
func protectedUser(name string) bool {
	return name == "karakoram" || name == "admin" || name == "guest" || name == "monitor"
}
func userValidation(v UserWrite) error {
	if protectedUser(v.Name) {
		return engine.ErrConflict
	}
	if e := accessName(v.Name); e != nil {
		return e
	}
	for _, t := range v.Tags {
		switch t {
		case "management", "policymaker", "monitoring", "administrator", "impersonator":
		default:
			return engine.Invalid("invalid user tag %q", t)
		}
	}
	if v.Password != nil && v.PasswordHash != nil {
		return engine.Invalid("password and passwordHash are mutually exclusive")
	}
	return nil
}
func regexValidation(patterns ...string) error {
	for _, v := range patterns {
		if len(v) == 0 || len(v) > 4096 {
			return engine.Invalid("regular expression must be non-empty and at most 4096 bytes")
		}
	}
	return nil
}
func policyValidation(v Policy) error {
	if e := accessName(v.Vhost); e != nil {
		return e
	}
	if e := accessName(v.Name); e != nil {
		return e
	}
	if e := regexValidation(v.Pattern); e != nil {
		return e
	}
	switch v.ApplyTo {
	case "all", "queues", "exchanges", "classic_queues", "quorum_queues", "streams":
	default:
		return engine.Invalid("invalid policy applyTo")
	}
	if v.Priority < 0 {
		return engine.Invalid("policy priority must be nonnegative")
	}
	if v.Definition == nil {
		return engine.Invalid("policy definition must be a JSON object")
	}
	if _, e := json.Marshal(v.Definition); e != nil {
		return engine.Invalid("policy definition must be JSON")
	}
	return nil
}
func policyManagement(v Policy) mgmt.Policy {
	return mgmt.Policy{Vhost: v.Vhost, Name: v.Name, Pattern: v.Pattern, ApplyTo: v.ApplyTo, Priority: v.Priority, Definition: v.Definition}
}
func policyAccess(v mgmt.Policy) Policy {
	return Policy{Vhost: v.Vhost, Name: v.Name, Pattern: v.Pattern, ApplyTo: v.ApplyTo, Priority: v.Priority, Definition: v.Definition}
}
func vhostAccess(v mgmt.Vhost) Vhost {
	out := Vhost{Name: v.Name, DefaultQueueType: v.DefaultQueueType, Description: v.Description, Tags: v.Tags}
	if out.Tags == nil {
		out.Tags = []string{}
	}
	if len(v.Limits) > 0 {
		out.Limits = &VhostLimits{}
		if n, ok := v.Limits["max-connections"]; ok {
			out.Limits.MaxConnections = &n
		}
		if n, ok := v.Limits["max-queues"]; ok {
			out.Limits.MaxQueues = &n
		}
	}
	return out
}
func vhostValidation(v Vhost) error {
	if e := accessName(v.Name); e != nil {
		return e
	}
	switch v.DefaultQueueType {
	case "", "classic", "quorum", "stream":
	default:
		return engine.Invalid("invalid default queue type")
	}
	for _, tag := range v.Tags {
		if e := accessName(tag); e != nil {
			return e
		}
	}
	if v.Limits != nil {
		for _, n := range []*int{v.Limits.MaxConnections, v.Limits.MaxQueues} {
			if n != nil && *n < -1 {
				return engine.Invalid("vhost limits must be -1 (unlimited) or nonnegative")
			}
		}
	}
	return nil
}
func permissionValidation(v Permission) error {
	if protectedUser(v.User) {
		return engine.ErrConflict
	}
	if e := accessName(v.Vhost); e != nil {
		return e
	}
	if e := accessName(v.User); e != nil {
		return e
	}
	return regexValidation(v.Configure, v.Write, v.Read)
}
func topicValidation(v TopicPermission) error {
	if protectedUser(v.User) {
		return engine.ErrConflict
	}
	if e := accessName(v.Vhost); e != nil {
		return e
	}
	if e := accessName(v.User); e != nil {
		return e
	}
	if e := accessName(v.Exchange); e != nil {
		return e
	}
	return regexValidation(v.Write, v.Read)
}
func (p *Provider) ListVhosts(ctx context.Context, rc *engine.Context) ([]Vhost, error) {
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	items, e := c.ListVhosts(ctx)
	if e != nil {
		return nil, e
	}
	limits, e := c.ListVhostLimits(ctx)
	if e != nil {
		return nil, e
	}
	byVhost := make(map[string]map[string]int, len(limits))
	for _, limit := range limits {
		byVhost[limit.Vhost] = limit.Value
	}
	out := make([]Vhost, 0, len(items))
	for _, v := range items {
		v.Limits = byVhost[v.Name]
		out = append(out, vhostAccess(v))
	}
	return out, nil
}
func (p *Provider) GetVhost(ctx context.Context, rc *engine.Context, name string) (*Vhost, error) {
	if e := accessName(name); e != nil {
		return nil, e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	v, e := c.GetVhost(ctx, name)
	if e != nil {
		return nil, e
	}
	limits, e := c.GetVhostLimits(ctx, name)
	if e != nil {
		return nil, e
	}
	for _, limit := range limits {
		if limit.Vhost == name {
			v.Limits = limit.Value
		}
	}
	out := vhostAccess(*v)
	return &out, nil
}
func (p *Provider) CreateVhost(ctx context.Context, rc *engine.Context, v Vhost) error {
	if e := vhostValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetVhost(ctx, v.Name)
	if e == nil {
		return engine.ErrConflict
	}
	if !errors.Is(e, engine.ErrNotFound) {
		return e
	}
	if e = c.PutVhost(ctx, mgmt.Vhost{Name: v.Name, DefaultQueueType: v.DefaultQueueType, Description: v.Description, Tags: v.Tags}); e != nil {
		return e
	}
	return setVhostLimits(ctx, c, v)
}
func (p *Provider) UpdateVhost(ctx context.Context, rc *engine.Context, v Vhost) error {
	if e := vhostValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetVhost(ctx, v.Name)
	if e != nil {
		return e
	}
	if e = c.PutVhost(ctx, mgmt.Vhost{Name: v.Name, DefaultQueueType: v.DefaultQueueType, Description: v.Description, Tags: v.Tags}); e != nil {
		return e
	}
	return setVhostLimits(ctx, c, v)
}
func (p *Provider) DeleteVhost(ctx context.Context, rc *engine.Context, name string) error {
	if e := accessName(name); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	return c.DeleteVhost(ctx, name)
}
func (p *Provider) ListUsers(ctx context.Context, rc *engine.Context) ([]User, error) {
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	items, e := c.ListUsers(ctx)
	if e != nil {
		return nil, e
	}
	out := make([]User, 0, len(items))
	for _, v := range items {
		out = append(out, User{Name: v.Name, Tags: v.Tags})
	}
	return out, nil
}
func (p *Provider) GetUser(ctx context.Context, rc *engine.Context, name string) (*User, error) {
	if e := accessName(name); e != nil {
		return nil, e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	v, e := c.GetUser(ctx, name)
	if e != nil {
		return nil, e
	}
	out := User{Name: v.Name, Tags: v.Tags}
	return &out, nil
}
func (p *Provider) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	if protectedUser(name) {
		return engine.ErrConflict
	}
	if e := accessName(name); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	return c.DeleteUser(ctx, name)
}
func (p *Provider) ListPermissions(ctx context.Context, rc *engine.Context) ([]Permission, error) {
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	return c.ListPermissions(ctx)
}
func (p *Provider) GetPermission(ctx context.Context, rc *engine.Context, vhost, user string) (*Permission, error) {
	if e := accessName(vhost); e != nil {
		return nil, e
	}
	if e := accessName(user); e != nil {
		return nil, e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	return c.GetPermission(ctx, vhost, user)
}
func (p *Provider) CreatePermission(ctx context.Context, rc *engine.Context, v Permission) error {
	if e := permissionValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetPermission(ctx, v.Vhost, v.User)
	if e == nil {
		return engine.ErrConflict
	}
	if !errors.Is(e, engine.ErrNotFound) {
		return e
	}
	return c.PutPermission(ctx, v)
}
func (p *Provider) UpdatePermission(ctx context.Context, rc *engine.Context, v Permission) error {
	if e := permissionValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetPermission(ctx, v.Vhost, v.User)
	if e != nil {
		return e
	}
	return c.PutPermission(ctx, v)
}
func (p *Provider) DeletePermission(ctx context.Context, rc *engine.Context, vhost, user string) error {
	if protectedUser(user) {
		return engine.ErrConflict
	}
	if e := accessName(vhost); e != nil {
		return e
	}
	if e := accessName(user); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	return c.DeletePermission(ctx, vhost, user)
}
func (p *Provider) ListTopicPermissions(ctx context.Context, rc *engine.Context) ([]TopicPermission, error) {
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	return c.ListTopicPermissions(ctx)
}
func (p *Provider) GetTopicPermission(ctx context.Context, rc *engine.Context, vhost, user string) ([]TopicPermission, error) {
	if e := accessName(vhost); e != nil {
		return nil, e
	}
	if e := accessName(user); e != nil {
		return nil, e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	v, e := c.GetTopicPermission(ctx, vhost, user)
	if e != nil {
		return nil, e
	}
	return v, nil
}
func (p *Provider) CreateTopicPermission(ctx context.Context, rc *engine.Context, v TopicPermission) error {
	if e := topicValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	existing, e := c.GetTopicPermission(ctx, v.Vhost, v.User)
	for _, permission := range existing {
		if permission.Exchange == v.Exchange {
			return engine.ErrConflict
		}
	}
	if e != nil && !errors.Is(e, engine.ErrNotFound) {
		return e
	}
	return c.PutTopicPermission(ctx, v)
}
func (p *Provider) UpdateTopicPermission(ctx context.Context, rc *engine.Context, v TopicPermission) error {
	if e := topicValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	existing, e := c.GetTopicPermission(ctx, v.Vhost, v.User)
	if e != nil {
		return e
	}
	for _, permission := range existing {
		if permission.Exchange == v.Exchange {
			return c.PutTopicPermission(ctx, v)
		}
	}
	return engine.ErrNotFound
}
func (p *Provider) DeleteTopicPermission(ctx context.Context, rc *engine.Context, vhost, user string) error {
	if protectedUser(user) {
		return engine.ErrConflict
	}
	if e := accessName(vhost); e != nil {
		return e
	}
	if e := accessName(user); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	return c.DeleteTopicPermission(ctx, vhost, user)
}
func (p *Provider) ListPolicies(ctx context.Context, rc *engine.Context) ([]Policy, error) {
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	items, e := c.ListPolicies(ctx)
	if e != nil {
		return nil, e
	}
	out := make([]Policy, 0, len(items))
	for _, v := range items {
		out = append(out, policyAccess(v))
	}
	return out, nil
}
func (p *Provider) GetPolicy(ctx context.Context, rc *engine.Context, vhost, name string) (*Policy, error) {
	if e := accessName(vhost); e != nil {
		return nil, e
	}
	if e := accessName(name); e != nil {
		return nil, e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	v, e := c.GetPolicy(ctx, vhost, name)
	if e != nil {
		return nil, e
	}
	out := policyAccess(*v)
	return &out, nil
}
func (p *Provider) CreatePolicy(ctx context.Context, rc *engine.Context, v Policy) error {
	if e := policyValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetPolicy(ctx, v.Vhost, v.Name)
	if e == nil {
		return engine.ErrConflict
	}
	if !errors.Is(e, engine.ErrNotFound) {
		return e
	}
	return c.PutPolicy(ctx, policyManagement(v))
}
func (p *Provider) UpdatePolicy(ctx context.Context, rc *engine.Context, v Policy) error {
	if e := policyValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetPolicy(ctx, v.Vhost, v.Name)
	if e != nil {
		return e
	}
	return c.PutPolicy(ctx, policyManagement(v))
}
func (p *Provider) DeletePolicy(ctx context.Context, rc *engine.Context, vhost, name string) error {
	if e := accessName(vhost); e != nil {
		return e
	}
	if e := accessName(name); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	return c.DeletePolicy(ctx, vhost, name)
}
func (p *Provider) ListOperatorPolicies(ctx context.Context, rc *engine.Context) ([]Policy, error) {
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	items, e := c.ListOperatorPolicies(ctx)
	if e != nil {
		return nil, e
	}
	out := make([]Policy, 0, len(items))
	for _, v := range items {
		out = append(out, policyAccess(v))
	}
	return out, nil
}
func (p *Provider) GetOperatorPolicy(ctx context.Context, rc *engine.Context, vhost, name string) (*Policy, error) {
	if e := accessName(vhost); e != nil {
		return nil, e
	}
	if e := accessName(name); e != nil {
		return nil, e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	v, e := c.GetOperatorPolicy(ctx, vhost, name)
	if e != nil {
		return nil, e
	}
	out := policyAccess(*v)
	return &out, nil
}
func (p *Provider) CreateOperatorPolicy(ctx context.Context, rc *engine.Context, v Policy) error {
	if e := policyValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetOperatorPolicy(ctx, v.Vhost, v.Name)
	if e == nil {
		return engine.ErrConflict
	}
	if !errors.Is(e, engine.ErrNotFound) {
		return e
	}
	return c.PutOperatorPolicy(ctx, policyManagement(v))
}
func (p *Provider) UpdateOperatorPolicy(ctx context.Context, rc *engine.Context, v Policy) error {
	if e := policyValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	_, e = c.GetOperatorPolicy(ctx, v.Vhost, v.Name)
	if e != nil {
		return e
	}
	return c.PutOperatorPolicy(ctx, policyManagement(v))
}
func (p *Provider) DeleteOperatorPolicy(ctx context.Context, rc *engine.Context, vhost, name string) error {
	if e := accessName(vhost); e != nil {
		return e
	}
	if e := accessName(name); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	return c.DeleteOperatorPolicy(ctx, vhost, name)
}
func setVhostLimits(ctx context.Context, c *mgmt.Client, v Vhost) error {
	if v.Limits == nil {
		return nil
	}
	for _, limit := range []struct {
		name  string
		value *int
	}{{"max-connections", v.Limits.MaxConnections}, {"max-queues", v.Limits.MaxQueues}} {
		if limit.value != nil {
			if e := c.SetVhostLimit(ctx, v.Name, limit.name, *limit.value); e != nil {
				return e
			}
		}
	}
	return nil
}
func (p *Provider) CreateUser(ctx context.Context, rc *engine.Context, v UserWrite) (string, error) {
	if e := userValidation(v); e != nil {
		return "", e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return "", e
	}
	_, e = c.GetUser(ctx, v.Name)
	if e == nil {
		return "", engine.ErrConflict
	}
	if !errors.Is(e, engine.ErrNotFound) {
		return "", e
	}
	password := ""
	if v.PasswordHash == nil && (v.Password == nil || *v.Password == "") {
		password, e = secrets.Password()
		if e != nil {
			return "", e
		}
		v.Password = &password
	}
	if e = c.PutUser(ctx, v.Name, mgmt.UserWrite{Tags: v.Tags, Password: v.Password, PasswordHash: v.PasswordHash}); e != nil {
		return "", e
	}
	return password, nil
}
func (p *Provider) UpdateUser(ctx context.Context, rc *engine.Context, v UserWrite) error {
	if e := userValidation(v); e != nil {
		return e
	}
	c, e := p.adminClient(ctx, rc)
	if e != nil {
		return e
	}
	existing, e := c.GetUser(ctx, v.Name)
	if e != nil {
		return e
	}
	if v.Password != nil && *v.Password == "" {
		return engine.Invalid("updated password must not be empty")
	}
	algorithm := ""
	if v.Password == nil && v.PasswordHash == nil {
		// RabbitMQ clears authentication when PUT omits both credential fields.
		// Replay the existing hash privately; it never enters an access response.
		v.PasswordHash = &existing.PasswordHash
		algorithm = existing.HashingAlgorithm
	}
	return c.PutUser(ctx, v.Name, mgmt.UserWrite{Tags: v.Tags, Password: v.Password, PasswordHash: v.PasswordHash, HashingAlgorithm: algorithm})
}
