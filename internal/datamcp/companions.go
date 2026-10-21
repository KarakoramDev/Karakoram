package datamcp

import (
	"embed"
	"encoding/json"
)

//go:embed manifests/*.json
var companionFiles embed.FS

// CompanionManifests returns independently owned, validated version-pinned manifests.
// Invalid embedded policy is a packaging error and panics rather than failing open.
func CompanionManifests() map[string]Manifest {
	entries, e := companionFiles.ReadDir("manifests")
	if e != nil {
		panic(e)
	}
	out := make(map[string]Manifest, len(entries))
	for _, entry := range entries {
		b, e := companionFiles.ReadFile("manifests/" + entry.Name())
		if e != nil {
			panic(e)
		}
		var m Manifest
		if e = json.Unmarshal(b, &m); e != nil {
			panic(e)
		}
		if e = validateManifest(m); e != nil {
			panic(e)
		}
		if _, ok := out[m.Upstream]; ok {
			panic("duplicate companion manifest")
		}
		out[m.Upstream] = m
	}
	return out
}
