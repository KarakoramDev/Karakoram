// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
)

// RestartToken hashes a canonical JSON array of ordered restart-relevant parts.
// JSON framing prevents ambiguous concatenations; callers canonicalize any
// structured parts first. The digest contains no raw configuration or secrets.
func RestartToken(parts ...string) string {
	if parts == nil {
		parts = []string{}
	}
	b, _ := json.Marshal(parts) // Strings always marshal successfully.
	return digest(b)
}

func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

// NodeRestartToken computes the ES process restart identity from version, heap,
// the unordered role set and static provider/user settings. Maps are flattened
// then JSON-encoded (sorted keys), so nested/dotted spelling and insertion order
// do not affect the token. User settings override provider settings, after the
// caller has validated them. Seed hosts and initial master nodes are excluded:
// seed files hot-reload and bootstrap nodes must never trigger a restart when
// removed after formation. Certificates, file realm and secure settings are
// intentionally not inputs; their reloads are managed independently.
func NodeRestartToken(version string, heapMB int, roles []string, providerSettings, userSettings map[string]any) (string, error) {
	settings, err := FlattenSettings(providerSettings)
	if err != nil {
		return "", err
	}
	user, err := FlattenSettings(userSettings)
	if err != nil {
		return "", err
	}
	for key, value := range user {
		settings[key] = value
	}
	delete(settings, "discovery.seed_hosts")
	delete(settings, "cluster.initial_master_nodes")
	roleSet := make([]string, len(roles))
	copy(roleSet, roles)
	slices.Sort(roleSet)
	roleSet = slices.Compact(roleSet)
	b, err := json.Marshal(struct {
		Version  string         `json:"version"`
		HeapMB   int            `json:"heapMB"`
		Roles    []string       `json:"roles"`
		Settings map[string]any `json:"settings"`
	}{version, heapMB, roleSet, settings})
	if err != nil {
		return "", errors.New("restart settings must contain JSON-compatible values")
	}
	return digest(b), nil
}

// S3ClientName returns a stable, bounded node client name scoped by namespace and
// storage. JSON array framing avoids collisions between differently split names.
func S3ClientName(namespace, storage string) string {
	b, _ := json.Marshal([]string{namespace, storage})
	return "kk" + digest(b)[:10]
}

// S3ClientSettings builds static elasticsearch.yml client settings. endpoint must
// be the storage's full URL including scheme; the deprecated protocol setting is
// never emitted. These values require a rolling restart when changed.
func S3ClientSettings(name, endpoint, region string) map[string]any {
	if region == "" {
		region = "us-east-1"
	}
	prefix := "s3.client." + name + "."
	return map[string]any{prefix + "endpoint": endpoint, prefix + "region": region, prefix + "path_style_access": true}
}

// S3SecureSettings builds reloadable keystore credentials. Never log this map.
func S3SecureSettings(name, accessKey, secret string) map[string]string {
	prefix := "s3.client." + name + "."
	return map[string]string{prefix + "access_key": accessKey, prefix + "secret_key": secret}
}

// RepositoryName returns the cluster-local repository name for a storage.
func RepositoryName(storage string) string { return "kk-" + storage }

// RepositoryBasePath scopes a snapshot repository to one cluster lifetime.
// Empty segments and repeated slashes are removed without interpreting dot
// segments: S3 keys are not filesystem paths.
func RepositoryBasePath(storagePrefix, namespace, cluster, uid string) string {
	return strings.Join(strings.FieldsFunc(strings.Join([]string{storagePrefix, namespace, cluster, uid}, "/"), func(r rune) bool { return r == '/' }), "/")
}
