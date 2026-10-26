// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agentapi

// VolumeIdentity binds a durable data directory to one cluster/member incarnation.
// Initialized is a control-plane receipt: a missing sentinel must not authorize
// fresh initialization after this member has previously held initialized data.
type VolumeIdentity struct {
	ClusterUID  string `json:"clusterUid"`
	Member      string `json:"member"`
	Volume      string `json:"volume"`
	VolumeID    string `json:"volumeId"`
	Initialized bool   `json:"initialized"`
}

// VolumeStatus remains observable even when the data safety gate blocks Apply.
type VolumeStatus struct {
	Identity VolumeIdentity `json:"identity"`
	Verified bool           `json:"verified"`
	Lost     bool           `json:"lost"`
	Message  string         `json:"message,omitempty"`
}
