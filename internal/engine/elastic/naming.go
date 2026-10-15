// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"regexp"
	"strconv"
)

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)
var hostnamePattern = regexp.MustCompile(`^[a-z0-9]{1,25}$`)

// ValidClusterName checks the cluster naming contract shared with Zerops.
func ValidClusterName(name string) bool { return clusterName.MatchString(name) }

// ValidHostname checks Zerops' lowercase alphanumeric service hostname limit.
func ValidHostname(name string) bool { return hostnamePattern.MatchString(name) }

// NodeHostname names a combined or data node using its monotonic member index.
func NodeHostname(cluster string, index int) string { return cluster + "n" + strconv.Itoa(index) }

// MasterHostname names a dedicated master using its monotonic member index.
func MasterHostname(cluster string, index int) string { return cluster + "m" + strconv.Itoa(index) }

// VolumeHostname names the local storage service belonging to a node.
func VolumeHostname(node string) string { return node + "v" }

// KibanaHostname names the shared Kibana service.
func KibanaHostname(cluster string) string { return cluster + "kb" }

// LogstashHostname names the shared Logstash service.
func LogstashHostname(cluster string) string { return cluster + "ls" }

// APMHostname names the shared APM Server service.
func APMHostname(cluster string) string { return cluster + "apm" }
