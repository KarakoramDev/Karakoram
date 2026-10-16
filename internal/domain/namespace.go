// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "time"

// Namespace is a project registration. Enrollment state is never exposed by the API.
type Namespace struct {
	Name                   string               `json:"name"`
	Description            string               `json:"description,omitempty"`
	Backend                string               `json:"backend,omitempty"`
	Config                 map[string]string    `json:"config,omitempty"`
	ProjectID              string               `json:"projectId"`
	ProjectName            string               `json:"projectName"`
	Default                bool                 `json:"default"`
	Phase                  string               `json:"phase"`
	Token                  NamespaceTokenStatus `json:"token"`
	Relay                  NamespaceRelayStatus `json:"relay"`
	CreatedAt              time.Time            `json:"createdAt"`
	UpdatedAt              time.Time            `json:"updatedAt"`
	ClientCert             []byte               `json:"clientCert,omitempty"`
	ClientSerial           string               `json:"clientSerial,omitempty"`
	PreviousClientSerial   string               `json:"previousClientSerial,omitempty"`
	PreviousCertNotAfter   time.Time            `json:"previousCertNotAfter,omitempty"`
	PendingOpID            string               `json:"pendingOpId,omitempty"`
	PendingOpKind          string               `json:"pendingOpKind,omitempty"`
	DeployedExecutableHash string               `json:"deployedExecutableHash,omitempty"`
	OwnershipMarker        string               `json:"ownershipMarker,omitempty"`
	PendingExecutableHash  string               `json:"pendingExecutableHash,omitempty"`
	SecretsPending         bool                 `json:"secretsPending,omitempty"`
	RetryAt                time.Time            `json:"retryAt,omitempty"`
	Failures               int                  `json:"failures,omitempty"`
}

type NamespaceTokenStatus struct {
	Source          string    `json:"source"`
	Valid           bool      `json:"valid"`
	LastValidatedAt time.Time `json:"lastValidatedAt,omitempty"`
	LastError       string    `json:"lastError,omitempty"`
	RotatedAt       time.Time `json:"rotatedAt,omitempty"`
}

type NamespaceRelayStatus struct {
	Hostname       string    `json:"hostname,omitempty"`
	ServiceID      string    `json:"serviceId,omitempty"`
	Phase          string    `json:"phase"`
	Connected      bool      `json:"connected"`
	ConnectedSince time.Time `json:"connectedSince,omitempty"`
	LastSeenAt     time.Time `json:"lastSeenAt,omitempty"`
	Version        string    `json:"version,omitempty"`
	DesiredVersion string    `json:"desiredVersion,omitempty"`
	CertNotAfter   time.Time `json:"certNotAfter,omitempty"`
	Reconnects     int64     `json:"reconnects"`
	LastError      string    `json:"lastError,omitempty"`
}
