// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Release archive produced by ArtifactSource.
const (
	ArtifactName   = "karakoram.tar.gz"
	ArtifactBinary = "karakoram"
)

// Default readiness timings for member agents.
const (
	DefaultReadinessTimeout = 300 * time.Second
	ReadinessRetryPeriod    = 10 * time.Second
)

// DataDirVariable records a runtime's data directory. On a runtime with a
// volume it is also mount evidence: it equals Layout.VolumePath(volume).
const DataDirVariable = "KARAKORAM_DATA_DIR"

// AgentRuntime describes one member service that runs `karakoram agent`.
type AgentRuntime struct {
	Cluster, Member string
	// Engine sets KARAKORAM_ENGINE when non-empty.
	Engine        string
	Volume        string
	AgentURL      string
	InternalCAPEM string
	Ports         []infra.Port
	Prepare       []string
	// Env adds engine-specific non-secret variables.
	Env map[string]string
	// ReadinessArgs overrides startup readiness for stateless replacements whose
	// dependencies were healthy when the engine selected this deployment.
	ReadinessArgs []string
	// ReadinessTimeout defaults to DefaultReadinessTimeout.
	ReadinessTimeout time.Duration
}

// Spec returns the neutral runtime description of a member agent: the
// release binary started as `karakoram agent`, its data directory on the
// mounted volume, a readiness probe and stop-before-start replacement.
func (a AgentRuntime) Spec(l infra.Layout) infra.RuntimeSpec {
	env := map[string]string{
		"KARAKORAM_URL": a.AgentURL, "KARAKORAM_CLUSTER": a.Cluster, "KARAKORAM_MEMBER": a.Member,
		"KARAKORAM_INTERNAL_CA": a.InternalCAPEM, DataDirVariable: l.DataDir(a.Volume),
	}
	if a.Engine != "" {
		env["KARAKORAM_ENGINE"] = a.Engine
	}
	for k, v := range a.Env {
		env[k] = v
	}
	timeout := a.ReadinessTimeout
	if timeout == 0 {
		timeout = DefaultReadinessTimeout
	}
	readinessArgs := a.ReadinessArgs
	if len(readinessArgs) == 0 {
		readinessArgs = []string{"--startup", "--data-dir=" + l.DataDir(a.Volume)}
	}
	return infra.RuntimeSpec{
		Base: infra.BaseUbuntu2404, Prepare: a.Prepare, Env: env, Args: []string{"agent"},
		Ports: a.Ports, Volume: a.Volume, StopBeforeStart: true,
		Readiness: &infra.Readiness{Args: append([]string{"agent", "readiness"}, readinessArgs...), FailureTimeout: timeout, RetryPeriod: ReadinessRetryPeriod},
	}
}

// StatelessReadiness freezes a deployment's probe choice until the engine has
// observed native convergence and clears decision. Fresh, healthy dependencies
// retain overlap-until-native-ready for rolling frontend replacements. Initial
// creation and dependency recovery must not wait on those dependencies while
// holding a platform deployment slot.
func StatelessReadiness(rc *Context, member string, decision **bool, nativeArgs ...string) []string {
	if *decision == nil {
		status := FreshStatus(rc.Cluster.Status, deploymentNow(rc))
		healthy := status.ObservedGeneration > 0
		dependencies := 0
		for _, peer := range status.Members {
			if peer.Name == member {
				continue
			}
			dependencies++
			healthy = healthy && peer.Health && peer.Agent.Healthy
		}
		native := healthy && dependencies > 0
		*decision = &native
	}
	if **decision {
		return nativeArgs
	}
	return nil
}

// Deploy ships the release artifact with spec to hostname.
func Deploy(ctx context.Context, rc *Context, hostname string, spec infra.RuntimeSpec) (infra.Op, error) {
	if DeploymentBlocked(rc, hostname, spec) {
		f := rc.Cluster.Status.DeploymentFailures[hostname]
		return infra.Op{}, &infra.DeploymentExhaustedError{Service: hostname, Version: f.Version, Attempts: f.Attempts, LastError: f.LastError}
	}
	// New desired intent invalidates the old fence, even if submission waits.
	delete(rc.Cluster.Status.DeploymentFailures, hostname)
	artifact, err := rc.Artifact.Open(ctx)
	if err != nil {
		return infra.Op{}, err
	}
	defer artifact.Close()
	op, err := rc.Backend.Deploy(ctx, hostname, infra.Deployment{Artifact: infra.Artifact{Reader: artifact, Name: ArtifactName, Binary: ArtifactBinary}, Runtime: spec, RetryEpoch: rc.Cluster.Spec.RetryEpoch})
	if err != nil {
		err = RecordDeploymentExhausted(ctx, rc, hostname, spec, err)
		if deploymentWaiting(err) {
			at := infra.RetryAt(err)
			if at.IsZero() {
				at = deploymentNow(rc).Add(15 * time.Second)
			}
			message := infra.SafeDetail(err)
			eventErr := recordDeploymentWait(ctx, rc, err, message)
			return op, errors.Join(&PlatformRetryWait{RetryAt: at, Message: message, Cause: err}, eventErr)
		}
	}
	return op, err
}

// OwnershipEnv returns the legacy secret identity variables of a member service.
func OwnershipEnv(cluster, member, token string) map[string]string {
	return map[string]string{"KARAKORAM_CLUSTER": cluster, "KARAKORAM_MEMBER": member, "KARAKORAM_AGENT_TOKEN": token}
}

// MemberService describes a member runtime service to import.
type MemberService struct {
	Cluster, Member, Token string
	// Volume is the member's volume hostname, empty for stateless services.
	Volume     string
	Resources  domain.Resources
	Containers int
}

// RuntimeServiceSpec returns the import spec of a member runtime. Its secret
// data-directory variable is the mount evidence checked by MountsVolume.
func RuntimeServiceSpec(l infra.Layout, m MemberService) infra.ServiceSpec {
	env := OwnershipEnv(m.Cluster, m.Member, m.Token)
	if m.Volume != "" {
		env[DataDirVariable] = l.VolumePath(m.Volume)
	}
	return infra.ServiceSpec{Hostname: m.Member, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Resources: m.Resources, MinContainers: m.Containers, MaxContainers: m.Containers, Env: env, Vars: OwnerVars(m.Token, m.Cluster, m.Member, m.Member), Volume: m.Volume}
}

// VolumeServiceSpec returns the import spec of a member's persistent volume
// sized by the disk bounds of r, spread across the cluster's placement group.
func VolumeServiceSpec(cluster, member, volume, token string, r domain.Resources) infra.ServiceSpec {
	return infra.ServiceSpec{Hostname: volume, Kind: infra.KindVolume, Resources: domain.Resources{MinDisk: r.MinDisk, MaxDisk: r.MaxDisk}, Env: OwnershipEnv(cluster, member, token), Vars: OwnerVars(token, cluster, member, volume), SpreadGroup: cluster}
}

// MountsVolume reports whether an observed runtime records exactly volume's
// mount path as its data directory.
func MountsVolume(b infra.Backend, runtime *infra.Service, volume string) bool {
	return runtime != nil && volume != "" && runtime.Ownership[DataDirVariable] == b.Layout().VolumePath(volume)
}

// RequirePublicTCP refuses public TCP exposure the backend cannot provide or
// whose allowlist it cannot enforce.
func RequirePublicTCP(b infra.Backend, exposure domain.Exposure) error {
	c := b.Capabilities()
	if !c.PublicTCP {
		return fmt.Errorf("%w: backend %s does not provide public TCP access", ErrNotSupported, b.Name())
	}
	if exposureNeedsTCPAllowlist(exposure) && !c.TCPAllowlist {
		return fmt.Errorf("%w: backend %s cannot enforce a public TCP allowlist", ErrNotSupported, b.Name())
	}
	return nil
}

// PublicHTTPS returns the backend's public HTTPS routing, or ErrNotSupported.
func PublicHTTPS(b infra.Backend) (infra.SubdomainAccess, error) {
	if sub, ok := b.(infra.SubdomainAccess); ok && b.Capabilities().PublicHTTPS {
		return sub, nil
	}
	return nil, fmt.Errorf("%w: backend %s does not provide public HTTPS", ErrNotSupported, b.Name())
}

// RequireDiskGrowth refuses growing a volume on a backend that cannot.
func RequireDiskGrowth(b infra.Backend) error {
	if !b.Capabilities().DiskGrowth {
		return fmt.Errorf("%w: backend %s cannot grow volumes", ErrNotSupported, b.Name())
	}
	return nil
}
