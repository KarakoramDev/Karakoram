// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "time"

// PostgresBinding is a durable dependency on a particular source incarnation.
// It contains no credentials; State records provisioning progress.
type PostgresBinding struct {
	ID                   string    `json:"id"`
	Incarnation          string    `json:"-"`
	SourceNamespace      string    `json:"sourceNamespace"`
	SourceCluster        string    `json:"sourceCluster"`
	SourceUID            string    `json:"sourceUid"`
	ConsumerKind         string    `json:"consumerKind"`
	ConsumerNamespace    string    `json:"consumerNamespace"`
	ConsumerName         string    `json:"consumerName"`
	Databases            []string  `json:"databases"`
	Extensions           []string  `json:"extensions"`
	RoleName             string    `json:"roleName"`
	MinMajor             int       `json:"minMajor"`
	MaxMajor             int       `json:"maxMajor"`
	State                string    `json:"state"`
	CredentialGeneration int64     `json:"credentialGeneration"`
	CreatedAt            time.Time `json:"createdAt"`
	UpdatedAt            time.Time `json:"updatedAt"`
}

func (b PostgresBinding) SourceRef() ClusterRef {
	return ClusterRef{Namespace: b.SourceNamespace, Name: b.SourceCluster}
}
func (b PostgresBinding) ConsumerRef() ClusterRef {
	return ClusterRef{Namespace: b.ConsumerNamespace, Name: b.ConsumerName}
}
