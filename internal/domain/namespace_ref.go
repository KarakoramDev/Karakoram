// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

const DefaultNamespace = "default"

// ClusterRef identifies a cluster within a namespace.
type ClusterRef struct{ Namespace, Name string }

// NormalizedNamespace supplies the legacy default for omitted identities.
func NormalizedNamespace(namespace string) string {
	if namespace == "" {
		return DefaultNamespace
	}
	return namespace
}

func (c Cluster) Ref() ClusterRef {
	return ClusterRef{Namespace: NormalizedNamespace(c.Namespace), Name: c.Name}
}

// Key preserves the legacy identity and secret AAD for the default namespace.
func (r ClusterRef) Key() string {
	if r.Namespace == "" || r.Namespace == DefaultNamespace {
		return r.Name
	}
	return r.Namespace + ":" + r.Name
}

// ParseClusterKey decodes a canonical cluster storage identity.
func ParseClusterKey(key string) ClusterRef {
	if namespace, name, ok := strings.Cut(key, ":"); ok {
		return ClusterRef{Namespace: namespace, Name: name}
	}
	return ClusterRef{Namespace: DefaultNamespace, Name: key}
}

var namespaceNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}[a-z0-9]$`)

// ValidateNamespaceName validates namespace syntax; default is reserved on creation.
func ValidateNamespaceName(name string) error {
	if !namespaceNamePattern.MatchString(name) {
		return fmt.Errorf("namespace name must contain 2–32 lowercase letters, digits or hyphens, start with a letter and end with a letter or digit")
	}
	return nil
}

func decodeNamespaced(data []byte, value any, namespace *string) error {
	if err := json.Unmarshal(data, value); err != nil {
		return err
	}
	*namespace = NormalizedNamespace(*namespace)
	return nil
}

func (c *Cluster) UnmarshalJSON(data []byte) error {
	type plain Cluster
	return decodeNamespaced(data, (*plain)(c), &c.Namespace)
}

func (v *Operation) UnmarshalJSON(data []byte) error {
	type plain Operation
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *Event) UnmarshalJSON(data []byte) error {
	type plain Event
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *Backup) UnmarshalJSON(data []byte) error {
	type plain Backup
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *Restore) UnmarshalJSON(data []byte) error {
	type plain Restore
	if err := decodeNamespaced(data, (*plain)(v), &v.Namespace); err != nil {
		return err
	}
	if v.SourceNamespace == "" {
		v.SourceNamespace = v.Namespace
	}
	return nil
}

func (v *BackupStorage) UnmarshalJSON(data []byte) error {
	type plain BackupStorage
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *OrphanedVolume) UnmarshalJSON(data []byte) error {
	type plain OrphanedVolume
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *AgentToken) UnmarshalJSON(data []byte) error {
	type plain AgentToken
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *RetainedVolume) UnmarshalJSON(data []byte) error {
	type plain RetainedVolume
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}

func (v *MetricSample) UnmarshalJSON(data []byte) error {
	type plain MetricSample
	return decodeNamespaced(data, (*plain)(v), &v.Namespace)
}
