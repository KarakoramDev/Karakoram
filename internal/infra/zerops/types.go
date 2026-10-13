// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"fmt"
	"time"
)

// APIError preserves Zerops error details without including sensitive response data in Error.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Meta       json.RawMessage
}

func (e *APIError) Error() string { return fmt.Sprintf("zerops HTTP %d (%s)", e.StatusCode, e.Code) }

// ErrorCode allows providers to recognize scheduled platform restrictions
// without depending on the Zerops transport implementation.
func (e *APIError) ErrorCode() string { return e.Code }

// Process is a Zerops asynchronous operation, not a service readiness assertion.
type Process struct {
	ID             string          `json:"id"`
	ProjectID      string          `json:"projectId"`
	ServiceStackID string          `json:"serviceStackId"`
	Status         string          `json:"status"`
	ActionName     string          `json:"actionName"`
	Error          json.RawMessage `json:"error"`
}

// ImportResult includes every process for every imported service.
type ImportResult struct {
	ProjectID     string `json:"projectId"`
	ServiceStacks []struct {
		ID        string    `json:"id"`
		Name      string    `json:"name"`
		Processes []Process `json:"processes"`
	} `json:"serviceStacks"`
}

// ServiceStack contains service identity and the effective autoscaling configuration.
// Raw configuration fields preserve platform attributes unknown to Karakoram.
type ServiceStack struct {
	ID                   string `json:"id"`
	ProjectID            string `json:"projectId"`
	Name                 string `json:"name"`
	Status               string `json:"status"`
	Base                 string `json:"base"`
	IsSystem             bool   `json:"isSystem"`
	ServiceStackTypeInfo struct {
		Category string `json:"serviceStackTypeCategory"`
		Version  string `json:"serviceStackTypeVersionName"`
	} `json:"serviceStackTypeInfo"`
	CurrentAutoscaling map[string]json.RawMessage `json:"currentAutoscaling"`
	CustomAutoscaling  map[string]json.RawMessage `json:"customAutoscaling"`
}

func (s ServiceStack) protected() bool {
	return s.IsSystem || s.Name == "core" || s.ServiceStackTypeInfo.Category == "CORE" || s.ServiceStackTypeInfo.Category == "SYSTEM"
}

// Project is the REST project response, including optional public addresses.
type Project struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PublicIPv4 string `json:"publicIpV4"`
	PublicIPv6 string `json:"publicIpV6"`
}

// AppVersion intentionally excludes credential-bearing upload URLs.
type AppVersion struct {
	ID string `json:"id"`
}

// EnvItem is a user or system environment entry. Content may be sensitive.
type EnvItem struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Content   string `json:"content"`
	Type      string `json:"type"`
	Sensitive bool   `json:"sensitive"`
}

// PublicPortRouting represents staged or synchronized public routing.
type PublicPortRouting struct {
	ID                string   `json:"id,omitempty"`
	ServiceStackID    string   `json:"serviceStackId,omitempty"`
	PublicIPType      string   `json:"publicIpType"`
	PublicPort        int      `json:"publicPort"`
	InternalPort      int      `json:"internalPort"`
	InternalProtocol  string   `json:"internalProtocol"`
	FirewallPolicy    string   `json:"firewallPolicy"`
	FirewallIPRanges  []string `json:"firewallIpRanges"`
	FirewallAllowMyIP bool     `json:"firewallAllowMyIp"`
	IsSynced          bool     `json:"isSynced,omitempty"`
	DeleteOnSync      bool     `json:"deleteOnSync,omitempty"`
}

// RuntimeContainer tolerates unnamed containers during creation.
type RuntimeContainer struct {
	ID                      string `json:"id"`
	Hostname                string `json:"hostname"`
	Status                  string `json:"status"`
	IPv4                    string `json:"ipV4"`
	IPv6                    string `json:"ipV6"`
	CurrentHardwareResource *struct {
		CPUCores float64 `json:"cpuCoreCount"`
		MemoryMB float64 `json:"memoryMBytes"`
		DiskGB   float64 `json:"diskGBytes"`
	} `json:"currentHardwareResource"`
}

// RuntimeLog is an application log entry returned by the signed logging endpoint.
type RuntimeLog struct {
	Timestamp     time.Time `json:"timestamp"`
	Hostname      string    `json:"hostname"`
	Content       string    `json:"content"`
	Message       string    `json:"message"`
	FacilityLabel string    `json:"facilityLabel"`
}

type page[T any] struct {
	Count  int `json:"count"`
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	List   []T `json:"list"`
}
