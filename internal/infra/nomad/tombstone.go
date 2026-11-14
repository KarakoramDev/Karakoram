// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"maps"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent/tombstone"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const tombstoneKeyPrefix = "karakoram_tombstone_"

var _ infra.ResolutionTombstoneProjector = (*Backend)(nil)

// ProjectResolutionTombstones CAS-merges only the reserved keys in this member's
// existing job variable. Concurrent environment changes cause a conflict, never
// a last-write-wins overwrite. The caller may reconcile again from fresh state.
func (b *Backend) ProjectResolutionTombstones(ctx context.Context, member string, hosts []string) error {
	if !tombstone.ValidHost(member) || len(hosts) > 64 {
		return reject("invalid resolution tombstone projection")
	}
	for _, host := range hosts {
		if !tombstone.ValidHost(host) || host == member {
			return reject("invalid retired hostname or own hostname")
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, member)
	if err != nil {
		return err
	}
	if r.Kind != infra.KindRuntime {
		return reject("resolution tombstones require a runtime")
	}
	v, err := b.readVariable(ctx, "nomad/jobs/"+r.Hostname)
	if err != nil {
		return err
	}
	items := maps.Clone(v.Items)
	if items == nil {
		items = map[string]string{}
	}
	for key := range items {
		if strings.HasPrefix(key, tombstoneKeyPrefix) {
			delete(items, key)
		}
	}
	for _, host := range hosts {
		items[tombstoneKeyPrefix+host] = host
	}
	if maps.Equal(items, v.Items) {
		return nil
	}
	v.Items = items
	return b.writeVariable(ctx, &v)
}

func renderTombstoneHostsTemplate(hostname string) string {
	variable := strconv.Quote("nomad/jobs/" + hostname)
	// Only canonical key/value pairs become rows; arbitrary values can neither
	// inject template code nor create duplicate live-address mappings.
	prefix := "# karakoram resolution tombstones: enabled\n{{ $retired := \"|\" }}{{ if nomadVarExists " + variable + " }}{{ with nomadVar " + variable + " }}{{ range .Tuples }}{{ if and (regexMatch \"^[a-z][a-z0-9-]{0,62}$\" .V) (eq .K (printf \"karakoram_tombstone_%s\" .V)) }}{{ $retired = printf \"%s%s|\" $retired .V }}239.192.0.1 {{ .V }}\nff18::4b4b:1 {{ .V }}\n{{ end }}{{ end }}{{ end }}{{ end }}"
	base := renderHostsTemplate()
	base = strings.Replace(base, "{{ range nomadService $name }}", "{{ if not (contains $name (split \"|\" $retired)) }}{{ range nomadService $name }}", 1)
	base = strings.Replace(base, "{{ end }}{{ end }}{{ if and $hostname", "{{ end }}{{ end }}{{ end }}{{ if and $hostname", 1)
	base = strings.Replace(base, "{{ if and $hostname (not $registered) }}", "{{ if and $hostname (not $registered) (not (contains $hostname (split \"|\" $retired))) }}", 1)
	return prefix + base
}
