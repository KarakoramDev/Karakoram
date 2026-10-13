// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { ChoiceGroup, Switch, TextAreaField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';

type TLS = Schemas['TLS'];
type Exposure = Schemas['Exposure'];

const CIDR = /^(\d{1,3}\.){3}\d{1,3}(\/\d{1,2})?$|^[0-9a-fA-F:]+(\/\d{1,3})?$/;

export function validateExposure(e: Exposure | undefined): string[] {
  if (!e?.public) return [];
  const list = e.allowlist ?? [];
  if (list.length === 0) return ['Public exposure requires at least one CIDR in the IP allowlist.'];
  const bad = list.filter((c) => !CIDR.test(c));
  return bad.length ? [`Invalid CIDR entries: ${bad.join(', ')}`] : [];
}

export function validateTLS(t: TLS | undefined): string[] {
  if (!t || t.mode === 'disabled' || t.ca !== 'userProvided') return [];
  const errs: string[] = [];
  if (!t.caCertificate?.trim()) errs.push('CA certificate PEM is required for user-provided TLS.');
  if (!t.certificate?.trim() || !t.privateKey?.trim()) errs.push('Server certificate and private key PEM are required for user-provided TLS.');
  return errs;
}

export function TLSEditor({ value, onChange, editing }: { value: TLS; onChange: (t: TLS) => void; editing?: boolean }) {
  return (
    <div className="stack" style={{ gap: 14 }}>
      <ChoiceGroup<TLS['mode']>
        name="tls-mode"
        legend="TLS mode"
        value={value.mode}
        onChange={(mode) => onChange({ ...value, mode })}
        options={[
          { value: 'requireTLS', title: 'Require TLS', description: 'Clients and members must use TLS. Recommended.' },
          { value: 'preferTLS', title: 'Prefer TLS', description: 'TLS when offered; plaintext still accepted. Use while migrating clients.' },
          { value: 'disabled', title: 'Disabled', description: 'No encryption on the wire. Private network only.' },
        ]}
      />
      {value.mode !== 'disabled' ? (
        <>
          <ChoiceGroup<TLS['ca']>
            name="tls-ca"
            legend="Certificate authority"
            value={value.ca}
            onChange={(ca) => onChange({ ...value, ca })}
            options={[
              { value: 'selfSigned', title: 'Karakoram-managed CA', description: 'A per-cluster CA is generated and rotated for you.' },
              { value: 'userProvided', title: 'Bring your own', description: 'Supply CA and server certificate PEMs.' },
            ]}
          />
          {value.ca === 'userProvided' ? (
            <div className="stack">
              {editing ? <Callout tone="info">Stored PEM material is never returned. Paste replacements to rotate; leave empty to keep the current material.</Callout> : null}
              <TextAreaField label="CA certificate (PEM)" mono value={value.caCertificate ?? ''} onChange={(e) => onChange({ ...value, caCertificate: e.target.value })} rows={4} />
              <TextAreaField label="CA private key (PEM, optional — needed to issue member certificates)" mono value={value.caPrivateKey ?? ''} onChange={(e) => onChange({ ...value, caPrivateKey: e.target.value })} rows={3} />
              <div className="grid-2">
                <TextAreaField label="Server certificate chain (PEM)" mono value={value.certificate ?? ''} onChange={(e) => onChange({ ...value, certificate: e.target.value })} rows={4} />
                <TextAreaField label="Server private key (PEM)" mono value={value.privateKey ?? ''} onChange={(e) => onChange({ ...value, privateKey: e.target.value })} rows={4} />
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

export function ExposureEditor({ value, onChange }: { value: Exposure; onChange: (e: Exposure) => void }) {
  const errors = validateExposure(value);
  return (
    <div className="stack" style={{ gap: 14 }}>
      <Switch label="Expose publicly on port 27017" checked={value.public} onChange={(e) => onChange({ ...value, public: e.target.checked })} />
      {value.public ? (
        <>
          <Callout tone="warn" title="Public exposure opens the database to the internet">
            Every member (or every router for sharded clusters) receives a public TCP route. Use the IP allowlist as defense in depth, not as a substitute for required TLS and strong credentials. Keep the database private when public access is not needed.
            Zerops public TCP needs IPv6 or a dedicated IPv4 address on the project.
          </Callout>
          <ChoiceGroup<Exposure['ipType']>
            name="exposure-ip"
            legend="Address family"
            value={value.ipType}
            onChange={(ipType) => onChange({ ...value, ipType })}
            options={[
              { value: 'IPV6', title: 'IPv6', description: 'Included with every Zerops project.' },
              { value: 'IPV4', title: 'Dedicated IPv4', description: 'Requires a dedicated IPv4 add-on; shared IPv4 is HTTP-only.' },
            ]}
          />
          <TextAreaField
            label="IP allowlist (one CIDR per line)"
            mono
            rows={4}
            value={(value.allowlist ?? []).join('\n')}
            onChange={(e) =>
              onChange({
                ...value,
                allowlist: e.target.value
                  .split(/\n+/)
                  .map((s) => s.trim())
                  .filter(Boolean),
              })
            }
            hint="Public exposure requires a nonempty CIDR allowlist. Karakoram configures Zerops routing; use the allowlist as defense in depth alongside required TLS and strong credentials."
            error={errors[0]}
          />
        </>
      ) : (
        <p className="small muted">Private: reachable only from services inside this Zerops project.</p>
      )}
    </div>
  );
}
