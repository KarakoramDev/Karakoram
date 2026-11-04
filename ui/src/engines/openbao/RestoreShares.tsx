// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { SelectField, Switch, TextAreaField, TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import type { RestoreSectionProps } from '../types';
import { SEAL_TYPES, buildRestoreShares, emptyRestoreShares, sealLabel, validateRestore, type RestoreSharesDraft } from './spec';

/**
 * Only the enabled editor owns a plaintext draft. Disabling or clearing the
 * write-only parent value unmounts it, including after a failed submission.
 */
export default function RestoreSharesSection(props: RestoreSectionProps) {
  const { source, inPlace, value, onChange } = props;
  const enabled = value.openbaoUnsealShares !== undefined;
  if (!source?.cluster) return null;
  return (
    <div className="stack" style={{ gap: 12 }}>
      <Switch label="Supply the source cluster's unseal shares from an offline copy" checked={enabled} onChange={(event) => { if (event.target.checked) onChange({ ...value, openbaoUnsealShares: buildRestoreShares(emptyRestoreShares()) }); else { const rest = { ...value }; delete rest.openbaoUnsealShares; onChange(rest); } }} />
      <p className="small muted">
        {inPlace ? 'A snapshot of this cluster is restored with its own stored unseal material.' : `Karakoram copies the stored unseal material of a live source automatically.`} Shares are required only when the snapshot belongs to a cluster that has been deleted; paste them from the copy you stored after revealing them. They are sent once with this request and are not kept by the browser.
      </p>
      {enabled ? <RestoreSharesEditor key={JSON.stringify([source.namespace, source.cluster, inPlace])} {...props} /> : null}
    </div>
  );
}

function RestoreSharesEditor({ value, onChange, showErrors = false }: RestoreSectionProps) {
  const [draft, setDraft] = useState<RestoreSharesDraft>(() => {
    const shares = value.openbaoUnsealShares!;
    return { sealType: shares.sealType ?? 'shamir', threshold: shares.threshold ? String(shares.threshold) : '', shares: shares.shares.join('\n') };
  });
  const update = (next: RestoreSharesDraft) => { setDraft(next); onChange({ ...value, openbaoUnsealShares: buildRestoreShares(next) }); };
  const errors = showErrors ? validateRestore(value) : [];
  return (
    <div className="stack">
      {errors.length ? <Callout tone="danger"><ul style={{ margin: 0, paddingLeft: 18 }}>{errors.map((error) => <li key={error}>{error}</li>)}</ul></Callout> : null}
          <div className="grid-2">
            <SelectField label="Seal of the source cluster" value={draft.sealType} onChange={(event) => update({ ...draft, sealType: event.target.value as RestoreSharesDraft['sealType'] })} hint={draft.sealType === 'shamir' ? 'Shamir shares unseal the restored members directly.' : 'Auto-unseal seals use recovery shares; the seal provider must be reachable from the restored cluster.'}>
              {SEAL_TYPES.map((type) => <option key={type} value={type}>{sealLabel(type)}</option>)}
            </SelectField>
            <TextField label="Threshold" type="number" min={1} step={1} value={draft.threshold} onChange={(event) => update({ ...draft, threshold: event.target.value })} required hint="Shares needed to unseal, as revealed with the material." />
          </div>
          <TextAreaField label={draft.sealType === 'shamir' ? 'Unseal shares (one per line)' : 'Recovery shares (one per line)'} value={draft.shares} onChange={(event) => update({ ...draft, shares: event.target.value })} mono rows={6} required autoComplete="off" spellCheck={false} hint="Paste at least the threshold number of shares exactly as revealed." />
    </div>
  );
}
