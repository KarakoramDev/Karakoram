package companion

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestCompanionAccessAndResourceChanges(t *testing.T) {
	f := setup(t)
	f.converge(t)
	cfg, _ := MemberConfig(t.Context(), f.rc, "demomcp")
	f.rc.Cluster.Spec.DataMcp.Access = domain.DataMcpAccessRead
	f.rc.Cluster.Spec.DataMcp.Resources = &domain.Resources{CPUMode: infra.CPUShared, MinCPU: 1, MaxCPU: 1, MinRAM: 0.5, MaxRAM: 1}
	f.converge(t)
	next, err := MemberConfig(t.Context(), f.rc, "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	if next.ConfigVersion != cfg.ConfigVersion+1 || !slices.Equal(next.DesiredProcesses, []string{companionspec.ProcessRead}) {
		t.Fatal(next)
	}
	if !slices.Contains(f.backend.Calls(), "SetResources") {
		t.Fatal("resource override not applied")
	}
	if _, err = Transport(t.Context(), f.rc, engine.DataAccessReadWrite); err == nil {
		t.Fatal("write transport after downgrade")
	}
	if _, err = Transport(t.Context(), f.rc, engine.DataAccessLevel("invalid")); err == nil {
		t.Fatal("invalid level accepted")
	}
	if _, err = MemberConfig(t.Context(), f.rc, "unknownmcp"); err == nil {
		t.Fatal("unknown member config exposed")
	}
	f.rc.Cluster.Spec.DataMcp.Tools.Deny = []string{"find"}
	f.converge(t)
	newer, err := MemberConfig(t.Context(), f.rc, "demomcp")
	if err != nil || newer.ConfigVersion != next.ConfigVersion+1 {
		t.Fatal("spec change did not bump config", newer, err)
	}
}

func TestFingerprintTracksTransportMaterial(t *testing.T) {
	f := setup(t)
	f.converge(t)
	fingerprint := func() string {
		value, err := Fingerprint(t.Context(), f.rc, engine.DataAccessRead)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	original := fingerprint()
	tok, err := f.rc.Secrets.GetAgentToken(t.Context(), f.rc.Cluster.Ref(), "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	tok.Token = "rotated-member-token"
	if err = f.rc.Secrets.SetAgentToken(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	rotated := fingerprint()
	if rotated == original {
		t.Fatal("member token omitted from fingerprint")
	}
	ca, err := pki.NewCA("new-ca", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.rc.Secrets.SetClusterSecret(t.Context(), f.rc.Cluster.Ref(), "tls/ca.crt", ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	if fingerprint() == rotated {
		t.Fatal("cluster CA omitted from fingerprint")
	}
	cfg, err := MemberConfig(t.Context(), f.rc, "demomcp")
	if err != nil {
		t.Fatal(err)
	}
	before := fingerprint()
	cfg.AgentTLS.CertPEM = "different-certificate"
	raw, _ := json.Marshal(cfg)
	if err = f.rc.Secrets.SetClusterSecret(t.Context(), f.rc.Cluster.Ref(), configKey, raw); err != nil {
		t.Fatal(err)
	}
	if fingerprint() == before {
		t.Fatal("certificate omitted from fingerprint")
	}
}

func TestCompanionRefusesHostnameCollision(t *testing.T) {
	f := setup(t)
	_, err := f.backend.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "demomcp", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.controller.Sync(t.Context(), f.rc); err == nil {
		t.Fatal("adopted project-wide hostname collision")
	}
	if slices.Contains(f.backend.Calls(), "Deploy") {
		t.Fatal("deployed into collision")
	}
}
func TestCompanionRefusesReplacementDeletion(t *testing.T) {
	f := setup(t)
	f.converge(t)
	s, err := load(t.Context(), f.rc)
	if err != nil {
		t.Fatal(err)
	}
	s.ID = "another-service"
	if err = save(t.Context(), f.rc, s); err != nil {
		t.Fatal(err)
	}
	if done, err := f.controller.Cleanup(t.Context(), f.rc); err == nil || done {
		t.Fatal("deleted changed service identity", done, err)
	}
	if slices.Contains(f.backend.Calls(), "DeleteServiceByID") {
		t.Fatal("deletion submitted for replacement")
	}
}
func TestDisableBeforeImportAndInterruptedSecretCleanup(t *testing.T) {
	f := setup(t)
	if _, err := f.controller.Sync(t.Context(), f.rc); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Spec.DataMcp.Enabled = false
	f.converge(t)
	if slices.Contains(f.backend.Calls(), "ImportServices") {
		t.Fatal("disabled frozen plan imported")
	}
	// A crash after deleting the token but before deleting the state remains resumable.
	if err := save(t.Context(), f.rc, &state{Stage: "delete"}); err != nil {
		t.Fatal(err)
	}
	done, err := f.controller.Cleanup(t.Context(), f.rc)
	if err != nil || !done {
		t.Fatal(done, err)
	}
}
func TestAdmissionUnavailableArtifact(t *testing.T) {
	f := setup(t)
	if err := Validate(f.rc.Cluster, engine.DataMcpBackend{Kind: engine.DataMcpCompanion, Upstream: "missing"}); err == nil {
		t.Fatal("unsupported upstream accepted")
	}
	f.rc.Cluster.Spec.DataMcp.Enabled = false
	if err := Validate(f.rc.Cluster, engine.DataMcpBackend{Kind: engine.DataMcpCompanion, Upstream: "missing"}); err != nil {
		t.Fatal("disabled feature required an artifact", err)
	}
}
