package companion

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/nativees"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/nativermq"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type nativeStatusProvider struct {
	*provider
	upstream string
}

func (p *nativeStatusProvider) DataMcp(*domain.Cluster) (engine.DataMcpBackend, error) {
	return engine.DataMcpBackend{Kind: engine.DataMcpNative, Upstream: p.upstream}, nil
}

func TestNativeStatusReportsReviewedManifestVersion(t *testing.T) {
	for _, manifest := range []struct{ upstream, version string }{
		{nativees.Manifest().Upstream, nativees.Manifest().Version},
		{nativermq.Manifest().Upstream, nativermq.Manifest().Version},
	} {
		t.Run(manifest.upstream, func(t *testing.T) {
			f := setup(t)
			p := &nativeStatusProvider{provider: f.provider, upstream: manifest.upstream}
			registry, err := engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			f.controller.Registry = registry
			var status domain.ClusterStatus
			if err = f.controller.Observe(t.Context(), f.rc, &status); err != nil {
				t.Fatal(err)
			}
			if status.DataMcp == nil || status.DataMcp.Phase != domain.DataMcpPhaseReady || status.DataMcp.Kind != string(engine.DataMcpNative) || status.DataMcp.Upstream != manifest.upstream || status.DataMcp.UpstreamVersion != manifest.version || status.DataMcp.UpstreamVersion == "" {
				t.Fatal(status.DataMcp)
			}
			if len(status.Members) != 0 {
				t.Fatal("native toolset counted as a database member")
			}
		})
	}
}
