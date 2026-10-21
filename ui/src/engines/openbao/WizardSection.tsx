// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useClusters } from '@/api/hooks';
import { ChoiceGroup, SelectField, TextAreaField, TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import type { WizardSectionProps } from '../types';
import { DEFAULT_TRANSIT_MOUNT, IMMUTABLE_EXPLANATION, LOG_LEVELS, MAX_SHARES, failureTolerance, readOptions, storageClusters, writeOptions, type OpenBaoOptions, type SealType, type StorageType } from './spec';

const LOG_LEVEL_HINT: Record<(typeof LOG_LEVELS)[number], string> = {
  trace: 'everything, including request metadata — development only',
  debug: 'verbose operational detail',
  info: 'default',
  warn: 'problems only',
  error: 'failures only',
};

const SECRET_NOTE = 'Stored encrypted in the Karakoram secret store and never shown again; the saved specification keeps only the non-secret settings.';

/**
 * Engine options for creation and, with `locked`, for the detail editor where the
 * initialization-time choices are shown but cannot change.
 */
export default function OpenBaoOptionsSection({ namespace, provider, spec, onChange, locked = false }: WizardSectionProps & { locked?: boolean }) {
  const options = readOptions(spec);
  const clusters = useClusters(namespace);
  const postgres = storageClusters(clusters.data?.items ?? [], namespace ?? 'default');
  const postgresStorageAvailable = provider.capabilities?.includes('postgresql-storage') ?? false;
  const set = (patch: (o: OpenBaoOptions) => OpenBaoOptions) => onChange(writeOptions(spec, patch(options)));
  const setTransit = (patch: Partial<OpenBaoOptions['seal']['transit']>) => set((o) => ({ ...o, seal: { ...o.seal, transit: { ...o.seal.transit, ...patch } } }));
  const setAws = (patch: Partial<OpenBaoOptions['seal']['awskms']>) => set((o) => ({ ...o, seal: { ...o.seal, awskms: { ...o.seal.awskms, ...patch } } }));
  const members = spec.components.engine.replicas;
  const sealChoice = (value: SealType, title: string, description: string) => ({ value, title, description, disabled: locked && options.seal.type !== value });
  const storageChoice = (value: StorageType, title: string, description: string) => ({ value, title, description, disabled: (locked && options.storage.type !== value) || (!locked && value === 'postgresql' && !postgresStorageAvailable) });

  return (
    <section className="stack" aria-label="OpenBao configuration">
      {locked ? <Callout tone="info" title="Fixed at initialization">{IMMUTABLE_EXPLANATION} Only the log level can be changed here.</Callout> : <Callout tone="info" title="Karakoram keeps the unseal material">
        Initialization happens once, on the first member. The resulting shares and the initial root token are stored encrypted by the control plane, which re-unseals members after every restart. Reveal them from the OpenBao tab afterwards and store them offline: a control-plane master-key loss would otherwise leave the cluster sealed for good.
      </Callout>}
      <fieldset className="stack" disabled={locked} style={{ border: 0, margin: 0, padding: 0 }}>
        <legend className="field-label">{options.seal.type === 'shamir' ? 'Unseal shares' : 'Recovery shares'}</legend>
        <div className="grid-2">
          <TextField label="Shares" type="number" min={1} max={MAX_SHARES} step={1} value={options.unseal.shares} onChange={(event) => set((o) => ({ ...o, unseal: { ...o.unseal, shares: Number(event.target.value) } }))} hint={`Key shares generated at initialization (1–${MAX_SHARES}).`} />
          <TextField label="Threshold" type="number" min={1} max={options.unseal.shares} step={1} value={options.unseal.threshold} onChange={(event) => set((o) => ({ ...o, unseal: { ...o.unseal, threshold: Number(event.target.value) } }))} hint="Shares needed to unseal or to generate a new root token." />
        </div>
        <p className="small muted">{options.seal.type === 'shamir' ? `Karakoram holds all ${options.unseal.shares} shares and submits ${options.unseal.threshold} of them to unseal a member.` : `With auto-unseal the shares are recovery shares: OpenBao unseals itself through the seal provider and the shares only authorize root-token generation.`}</p>
      </fieldset>

      <ChoiceGroup name="openbao-seal" legend="Seal" value={options.seal.type} onChange={(type) => set((o) => ({ ...o, seal: { ...o.seal, type } }))} options={[
        sealChoice('shamir', 'Shamir', 'Default. Members are unsealed by the control plane with its stored shares; nothing outside Karakoram is needed.'),
        sealChoice('transit', 'Transit auto-unseal', 'An external OpenBao or Vault transit key wraps the root key. Members unseal without the control plane but depend on that server.'),
        sealChoice('awskms', 'AWS KMS auto-unseal', 'An AWS KMS key wraps the root key through the KMS plugin. Members depend on AWS reachability and the credentials below.'),
      ]} />
      {options.seal.type === 'transit' ? (
        <fieldset className="stack" disabled={locked} style={{ border: 0, margin: 0, padding: 0 }}>
          <legend className="field-label">Transit seal</legend>
          <div className="grid-2">
            <TextField label="Address" value={options.seal.transit.address} onChange={(event) => setTransit({ address: event.target.value })} placeholder="https://vault.example.net:8200" required mono hint="https:// URL of the external OpenBao/Vault." />
            <TextField label="Key name" value={options.seal.transit.keyName} onChange={(event) => setTransit({ keyName: event.target.value })} required mono />
            <TextField label="Mount path" value={options.seal.transit.mountPath} onChange={(event) => setTransit({ mountPath: event.target.value })} placeholder={DEFAULT_TRANSIT_MOUNT} mono hint="Transit secrets engine mount, ending with a slash." />
            <TextField label="TLS server name (optional)" value={options.seal.transit.tlsServerName} onChange={(event) => setTransit({ tlsServerName: event.target.value })} mono hint="SNI/verification name when it differs from the address host." />
          </div>
          <TextAreaField label="CA certificate PEM (optional)" value={options.seal.transit.caPEM} onChange={(event) => setTransit({ caPEM: event.target.value })} mono rows={4} hint="Verifies the transit server; empty uses the system roots." />
          {locked ? null : <TextField label="Token" type="password" autoComplete="new-password" value={options.seal.transit.token} onChange={(event) => setTransit({ token: event.target.value })} required hint={`Token with update access to the transit key. ${SECRET_NOTE}`} />}
        </fieldset>
      ) : null}
      {options.seal.type === 'awskms' ? (
        <fieldset className="stack" disabled={locked} style={{ border: 0, margin: 0, padding: 0 }}>
          <legend className="field-label">AWS KMS seal</legend>
          <div className="grid-2">
            <TextField label="Region" value={options.seal.awskms.region} onChange={(event) => setAws({ region: event.target.value })} placeholder="eu-central-1" required mono />
            <TextField label="KMS key ID" value={options.seal.awskms.kmsKeyId} onChange={(event) => setAws({ kmsKeyId: event.target.value })} required mono hint="Key ID, ARN or alias/… name." />
          </div>
          <TextField label="Endpoint (optional)" value={options.seal.awskms.endpoint} onChange={(event) => setAws({ endpoint: event.target.value })} placeholder="https://kms.eu-central-1.amazonaws.com" mono hint="Overrides the regional KMS endpoint, for example a VPC endpoint." />
          {locked ? null : <div className="grid-2">
            <TextField label="Access key ID" type="password" autoComplete="off" value={options.seal.awskms.accessKeyId} onChange={(event) => setAws({ accessKeyId: event.target.value })} required />
            <TextField label="Secret access key" type="password" autoComplete="new-password" value={options.seal.awskms.secretAccessKey} onChange={(event) => setAws({ secretAccessKey: event.target.value })} required hint={SECRET_NOTE} />
          </div>}
        </fieldset>
      ) : null}

      <ChoiceGroup name="openbao-storage" legend="Storage" value={options.storage.type} onChange={(type) => set((o) => ({ ...o, storage: { ...o.storage, type } }))} options={[
        storageChoice('raft', 'Integrated Raft', `Default. Each member stores data on its own volume; ${members === 1 ? 'a single member has no redundancy' : `${members} members tolerate ${failureTolerance(members)} failure${members === 3 ? '' : 's'}`}. Raft snapshots back the cluster up.`),
        storageChoice('postgresql', 'PostgreSQL', 'A Karakoram PostgreSQL cluster in this namespace stores the data; members become stateless HA peers and backups belong to that database.'),
      ]} />
      {!locked && !postgresStorageAvailable ? <Callout>PostgreSQL storage is unavailable: this control plane has no PostgreSQL binding service. Use integrated Raft storage.</Callout> : null}
      {options.storage.type === 'postgresql' ? (
        <SelectField label="PostgreSQL cluster" value={options.storage.postgresql.cluster} onChange={(event) => set((o) => ({ ...o, storage: { ...o.storage, postgresql: { cluster: event.target.value } } }))} disabled={locked || !postgresStorageAvailable} required hint={clusters.isPending ? 'Loading clusters…' : postgres.length ? 'The secrets store then depends on this database being available.' : 'No PostgreSQL cluster exists in this namespace yet.'}>
          <option value="">Choose a cluster…</option>
          {[...new Set([...postgres.map((c) => c.name), ...(options.storage.postgresql.cluster ? [options.storage.postgresql.cluster] : [])])].map((name) => <option key={name} value={name}>{name}</option>)}
        </SelectField>
      ) : null}

      <SelectField label="Log level" value={options.logLevel} onChange={(event) => set((o) => ({ ...o, logLevel: event.target.value as OpenBaoOptions['logLevel'] }))} hint="Applied to bao server; changes roll members one at a time.">
        {LOG_LEVELS.map((level) => <option key={level} value={level}>{level} — {LOG_LEVEL_HINT[level]}</option>)}
      </SelectField>
      <p className="small muted">TLS is always required on the API listener (port 8200). Set the public allowlist and certificate authority in Access &amp; TLS.</p>
    </section>
  );
}
