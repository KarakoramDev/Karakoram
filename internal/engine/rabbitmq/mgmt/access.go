// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mgmt

import (
	"context"
	"strings"
)

func (c *Client) ListVhosts(ctx context.Context) ([]Vhost, error) {
	v := []Vhost{}
	e := c.request(ctx, "GET", []string{"vhosts"}, nil, &v)
	return v, e
}
func (c *Client) GetVhost(ctx context.Context, name string) (*Vhost, error) {
	var v Vhost
	e := c.request(ctx, "GET", []string{"vhosts", name}, nil, &v)
	return &v, e
}
func (c *Client) PutVhost(ctx context.Context, v Vhost) error {
	return c.request(ctx, "PUT", []string{"vhosts", v.Name}, VhostWrite{DefaultQueueType: v.DefaultQueueType, Description: v.Description, Tags: strings.Join(v.Tags, ",")}, nil)
}
func (c *Client) DeleteVhost(ctx context.Context, name string) error {
	return c.request(ctx, "DELETE", []string{"vhosts", name}, nil, nil)
}
func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	v := []User{}
	e := c.request(ctx, "GET", []string{"users"}, nil, &v)
	return v, e
}
func (c *Client) GetUser(ctx context.Context, name string) (*User, error) {
	var v User
	e := c.request(ctx, "GET", []string{"users", name}, nil, &v)
	return &v, e
}
func (c *Client) PutUser(ctx context.Context, name string, v UserWrite) error {
	if v.Tags == nil {
		v.Tags = []string{}
	}
	return c.request(ctx, "PUT", []string{"users", name}, v, nil)
}
func (c *Client) DeleteUser(ctx context.Context, name string) error {
	return c.request(ctx, "DELETE", []string{"users", name}, nil, nil)
}
func (c *Client) ListPermissions(ctx context.Context) ([]Permission, error) {
	v := []Permission{}
	e := c.request(ctx, "GET", []string{"permissions"}, nil, &v)
	return v, e
}
func (c *Client) GetPermission(ctx context.Context, vhost, user string) (*Permission, error) {
	var v Permission
	e := c.request(ctx, "GET", []string{"permissions", vhost, user}, nil, &v)
	return &v, e
}
func (c *Client) PutPermission(ctx context.Context, v Permission) error {
	return c.request(ctx, "PUT", []string{"permissions", v.Vhost, v.User}, v, nil)
}
func (c *Client) DeletePermission(ctx context.Context, vhost, user string) error {
	return c.request(ctx, "DELETE", []string{"permissions", vhost, user}, nil, nil)
}
func (c *Client) ListTopicPermissions(ctx context.Context) ([]TopicPermission, error) {
	v := []TopicPermission{}
	e := c.request(ctx, "GET", []string{"topic-permissions"}, nil, &v)
	return v, e
}
func (c *Client) GetTopicPermission(ctx context.Context, vhost, user string) ([]TopicPermission, error) {
	v := []TopicPermission{}
	e := c.request(ctx, "GET", []string{"topic-permissions", vhost, user}, nil, &v)
	return v, e
}
func (c *Client) PutTopicPermission(ctx context.Context, v TopicPermission) error {
	return c.request(ctx, "PUT", []string{"topic-permissions", v.Vhost, v.User}, v, nil)
}
func (c *Client) DeleteTopicPermission(ctx context.Context, vhost, user string) error {
	return c.request(ctx, "DELETE", []string{"topic-permissions", vhost, user}, nil, nil)
}
func (c *Client) ListPolicies(ctx context.Context) ([]Policy, error) {
	v := []Policy{}
	e := c.request(ctx, "GET", []string{"policies"}, nil, &v)
	return v, e
}
func (c *Client) GetPolicy(ctx context.Context, vhost, name string) (*Policy, error) {
	var v Policy
	e := c.request(ctx, "GET", []string{"policies", vhost, name}, nil, &v)
	return &v, e
}
func (c *Client) PutPolicy(ctx context.Context, v Policy) error {
	return c.request(ctx, "PUT", []string{"policies", v.Vhost, v.Name}, v, nil)
}
func (c *Client) DeletePolicy(ctx context.Context, vhost, name string) error {
	return c.request(ctx, "DELETE", []string{"policies", vhost, name}, nil, nil)
}
func (c *Client) ListOperatorPolicies(ctx context.Context) ([]Policy, error) {
	v := []Policy{}
	e := c.request(ctx, "GET", []string{"operator-policies"}, nil, &v)
	return v, e
}
func (c *Client) GetOperatorPolicy(ctx context.Context, vhost, name string) (*Policy, error) {
	var v Policy
	e := c.request(ctx, "GET", []string{"operator-policies", vhost, name}, nil, &v)
	return &v, e
}
func (c *Client) PutOperatorPolicy(ctx context.Context, v Policy) error {
	return c.request(ctx, "PUT", []string{"operator-policies", v.Vhost, v.Name}, v, nil)
}
func (c *Client) DeleteOperatorPolicy(ctx context.Context, vhost, name string) error {
	return c.request(ctx, "DELETE", []string{"operator-policies", vhost, name}, nil, nil)
}
func (c *Client) SetVhostLimit(ctx context.Context, vhost, name string, value int) error {
	return c.request(ctx, "PUT", []string{"vhost-limits", vhost, name}, map[string]int{"value": value}, nil)
}
func (c *Client) DeleteVhostLimit(ctx context.Context, vhost, name string) error {
	return c.request(ctx, "DELETE", []string{"vhost-limits", vhost, name}, nil, nil)
}

// Queue describes replica placement needed for safe stream membership changes.
type Queue struct {
	Name    string   `json:"name"`
	Vhost   string   `json:"vhost"`
	Type    string   `json:"type"`
	Members []string `json:"members"`
	Leader  string   `json:"leader"`
}

func (c *Client) Queues(ctx context.Context) ([]Queue, error) {
	v := []Queue{}
	e := c.request(ctx, "GET", []string{"queues"}, nil, &v)
	return v, e
}

type VhostLimit struct {
	Vhost string         `json:"vhost"`
	Value map[string]int `json:"value"`
}

func (c *Client) ListVhostLimits(ctx context.Context) ([]VhostLimit, error) {
	v := []VhostLimit{}
	e := c.request(ctx, "GET", []string{"vhost-limits"}, nil, &v)
	return v, e
}

func (c *Client) GetVhostLimits(ctx context.Context, name string) ([]VhostLimit, error) {
	v := []VhostLimit{}
	e := c.request(ctx, "GET", []string{"vhost-limits", name}, nil, &v)
	return v, e
}
