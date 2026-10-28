// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const metadataPrefix = "karakoram:v1:"

type Backend struct {
	c      *client
	target infra.Target
	mu     *contextMutex
	now    func() time.Time
	state  *targetState
}

var _ infra.Backend = (*Backend)(nil)
var _ infra.SubdomainAccess = (*Backend)(nil)
var _ infra.CredentialRotator = (*Backend)(nil)

type metadata struct {
	Kind         infra.ServiceKind `json:"kind"`
	Hostname     string            `json:"hostname"`
	Environment  string            `json:"environment,omitempty"`
	ImportDigest string            `json:"importDigest,omitempty"`
	Importing    bool              `json:"importing,omitempty"`
	Volume       string            `json:"volume,omitempty"`
	// Holder identity and its own hostname-bound marker are not the runtime's
	// ownership marker. Bind both at import alongside the exact named mount.
	VolumeID               string             `json:"volumeId,omitempty"`
	VolumeOwner            string             `json:"volumeOwner,omitempty"`
	Node                   string             `json:"node,omitempty"`
	SpreadGroup            string             `json:"spreadGroup,omitempty"`
	VarKeys                []string           `json:"varKeys,omitempty"`
	SecretKeys             []string           `json:"secretKeys,omitempty"`
	Vars                   map[string]string  `json:"vars,omitempty"`
	HTTPPorts              []int              `json:"httpPorts,omitempty"`
	SubdomainEnabled       bool               `json:"subdomainEnabled,omitempty"`
	DomainHosts            []string           `json:"domainHosts,omitempty"`
	PublicPorts            []infra.PublicPort `json:"publicPorts,omitempty"`
	PortsPending           bool               `json:"portsPending,omitempty"`
	PortChange             *portChange        `json:"portChange,omitempty"`
	EndpointRepairAttempts int                `json:"endpointRepairAttempts,omitempty"`
	EndpointRepairAfter    time.Time          `json:"endpointRepairAfter,omitempty"`
	Resources              infra.Resources    `json:"resources"`
	Min                    int                `json:"min"`
	Max                    int                `json:"max"`
	Deployed               bool               `json:"deployed,omitempty"`
	AppliedSpecification   string             `json:"appliedSpecification,omitempty"`
	DeploymentDigest       string             `json:"deploymentDigest,omitempty"`
	DeploymentBudget       *deploymentBudget  `json:"deploymentBudget,omitempty"`
	Desired                string             `json:"desired,omitempty"`
	Pending                *operation         `json:"pending,omitempty"`
	MutationUntil          time.Time          `json:"mutationUntil,omitempty"`
	DeleteRemoval          *deleteRemoval     `json:"deleteRemoval,omitempty"`
	DeleteStopConfirmed    bool               `json:"deleteStopConfirmed,omitempty"`
	Image                  string             `json:"image,omitempty"`
	WriterFenced           bool               `json:"writerFenced,omitempty"`
	Wiping                 bool               `json:"wiping,omitempty"`
	WipePreviousTasks      []string           `json:"wipePreviousTasks,omitempty"`
	WipePreviousDeployment string             `json:"wipePreviousDeployment,omitempty"`
}

func (m metadata) description() string { b, _ := json.Marshal(m); return metadataPrefix + string(b) }
func parseMetadata(s string) (metadata, bool) {
	var m metadata
	if !strings.HasPrefix(s, metadataPrefix) {
		return m, false
	}
	err := json.Unmarshal([]byte(strings.TrimPrefix(s, metadataPrefix)), &m)
	return m, err == nil && m.Hostname != "" && (m.Kind == infra.KindRuntime || m.Kind == infra.KindVolume)
}

type application struct {
	ID           string         `json:"applicationId"`
	Name         string         `json:"name"`
	AppName      string         `json:"appName"`
	Environment  string         `json:"environmentId"`
	Description  string         `json:"description"`
	Env          string         `json:"env"`
	Status       string         `json:"applicationStatus"`
	Replicas     int            `json:"replicas"`
	SourceType   string         `json:"sourceType"`
	BuildType    string         `json:"buildType"`
	UpdateConfig map[string]any `json:"updateConfigSwarm"`
	Mounts       []mount        `json:"mounts"`
	Ports        []port         `json:"ports"`
	Domains      []domain       `json:"domains"`
	Networks     []struct {
		Target string `json:"Target"`
	} `json:"networkSwarm"`
}
type mount struct {
	ID         string `json:"mountId"`
	Type       string `json:"type"`
	VolumeName string `json:"volumeName"`
	MountPath  string `json:"mountPath"`
}
type port struct {
	ID            string `json:"portId"`
	PublishedPort int    `json:"publishedPort"`
	TargetPort    int    `json:"targetPort"`
	Protocol      string `json:"protocol"`
	PublishMode   string `json:"publishMode"`
}
type domain struct {
	ID    string `json:"domainId"`
	Host  string `json:"host"`
	Port  int    `json:"port"`
	HTTPS bool   `json:"https"`
}
type environment struct {
	ID           string        `json:"environmentId"`
	Name         string        `json:"name"`
	Applications []application `json:"applications"`
	Project      struct {
		Name string `json:"name"`
	} `json:"project"`
}
type node struct {
	ID   string `json:"ID"`
	Spec struct {
		Labels       map[string]string `json:"Labels"`
		Availability string            `json:"Availability"`
		Role         string            `json:"Role"`
	} `json:"Spec"`
	Description struct {
		Hostname string `json:"Hostname"`
	} `json:"Description"`
	Status struct {
		State string `json:"State"`
	} `json:"Status"`
}
type deploymentRecord struct {
	ID         string `json:"deploymentId"`
	Status     string `json:"status"`
	CreatedAt  string `json:"createdAt"`
	FinishedAt string `json:"finishedAt"`
}
