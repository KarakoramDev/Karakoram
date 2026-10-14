// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { Checkbox, ChoiceGroup, Switch, TextAreaField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { coversAllAddresses, tlsErrors, validateExposure, wireExposure } from '../wizard/access';

type TLS = Schemas['TLS'];
type Exposure = Schemas['Exposure'];


export function TLSEditor({ value, onChange, editing }: { value: TLS; onChange: (t: TLS) => void; editing?: boolean }) {
  const errors = tlsErrors(value, editing);
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
              { value: 'userProvided', title: 'Bring your own', description: 'Supply a CA certificate and signing key; server certificates are optional.' },
            ]}
          />
          {value.ca === 'userProvided' ? (
            <div className="stack">
              {editing ? <Callout tone="info">Stored PEM material is never returned. Existing CA material cannot change. Leave PEM fields empty to keep stored material; supply a matching server certificate/key pair to replace member leaves.</Callout> : null}
              <TextAreaField label="CA certificate (PEM)" required={!editing} mono value={value.caCertificate ?? ''} onChange={(e) => onChange({ ...value, caCertificate: e.target.value })} rows={4} error={errors.caCertificate} />
              <TextAreaField label="CA signing private key (PEM)" required={!editing} mono value={value.caPrivateKey ?? ''} onChange={(e) => onChange({ ...value, caPrivateKey: e.target.value })} rows={3} error={errors.caPrivateKey} hint="Required at creation to issue member and agent certificates. Use a dedicated cluster CA." />
              <div className="grid-2">
                <TextAreaField label="Server certificate chain (PEM, optional)" mono value={value.certificate ?? ''} onChange={(e) => onChange({ ...value, certificate: e.target.value })} rows={4} error={errors.certificate} />
                <TextAreaField label="Server private key (PEM, optional)" mono value={value.privateKey ?? ''} onChange={(e) => onChange({ ...value, privateKey: e.target.value })} rows={4} error={errors.privateKey} />
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

export function ExposureEditor({ value, onChange, allAddressesConfirmed, onAllAddressesConfirmedChange, label = 'Expose database publicly' }: {
  value: Exposure;
  onChange: (e: Exposure) => void;
  allAddressesConfirmed: boolean;
  onAllAddressesConfirmedChange: (confirmed: boolean) => void;
  label?: string;
}) {
  const errors = validateExposure(value, allAddressesConfirmed);
  const change = (exposure: Exposure) => {
    onAllAddressesConfirmedChange(false);
    onChange(wireExposure(exposure, false));
  };
  return (
    <div className="stack" style={{ gap: 14 }}>
      <Switch label={label} checked={value.public} onChange={(e) => change({ ...value, public: e.target.checked })} />
      {value.public ? (
        <>
          <Callout tone="warn" title="Public exposure opens the database to the internet">
            Public database endpoints receive internet-facing routes. Use the IP allowlist as defense in depth, not as a substitute for required TLS and strong credentials. Keep the database private when public access is not needed.
            Zerops public TCP needs IPv6 or a dedicated IPv4 address on the project.
          </Callout>
          <ChoiceGroup<Exposure['ipType']>
            name="exposure-ip"
            legend="Address family"
            value={value.ipType}
            onChange={(ipType) => change({ ...value, ipType })}
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
              change({
                ...value,
                allowlist: e.target.value
                  .split(/\n+/)
                  .map((s) => s.trim())
                  .filter(Boolean),
              })
            }
            hint="Public exposure requires a nonempty CIDR allowlist. Karakoram configures Zerops routing; use the allowlist as defense in depth alongside required TLS and strong credentials."
            error={errors.join(' ')}
          />
          {coversAllAddresses(value) ? (
            <Callout tone="danger" title="All internet addresses are allowed">
              The allowlist covers an entire IP family, whether through a /0 CIDR or combined ranges. Anyone on the internet can reach the public service.
              <Checkbox label="I understand and explicitly allow access from all addresses" checked={allAddressesConfirmed} onChange={(e) => {
                onAllAddressesConfirmedChange(e.target.checked);
                onChange(wireExposure(value, e.target.checked));
              }} />
            </Callout>
          ) : null}
        </>
      ) : (
        <p className="small muted">Private: reachable only from services inside this Zerops project.</p>
      )}
    </div>
  );
}
