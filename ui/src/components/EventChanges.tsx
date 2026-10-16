// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useId, useState } from 'react';
import type { components } from '@/api/schema';
import { Button } from './ui/Button';
import { Badge } from './ui/Primitives';
import { eventChangeValue } from './event-changes';

export function EventChanges({ details }: Pick<components['schemas']['Event'], 'details'>) {
  const [expanded, setExpanded] = useState(false);
  const contentId = useId();
  const changes = details?.changes;
  if (!changes?.length && !details?.truncated) return null;

  return (
    <div>
      <Button size="sm" variant="ghost" aria-expanded={expanded} aria-controls={contentId} onClick={() => setExpanded((value) => !value)}>
        Changed fields ({changes?.length ?? 0}{details?.truncated ? ', truncated' : ''})
      </Button>
      <div id={contentId} hidden={!expanded}>
        {details?.truncated ? <p className="small muted">Change details truncated: some values or fields were omitted. This is not a complete comparison.</p> : null}
        <div className="table-wrap">
          <table className="table small">
            <caption className="sr-only">Changed fields</caption>
            <thead><tr><th scope="col">Path</th><th scope="col">Before</th><th scope="col">After</th></tr></thead>
            <tbody>
              {changes?.map((change, index) => (
                <tr key={`${change.path}:${index}`}>
                  <th scope="row" className="mono" style={{ overflowWrap: 'anywhere' }}>
                    {change.path}
                    {change.sensitive ? <> <Badge plain tone="warn">Sensitive</Badge></> : null}
                    {change.truncated ? <> <Badge plain tone="warn">Truncated</Badge></> : null}
                  </th>
                  <td className="mono" style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{eventChangeValue(change.before, change.truncated)}</td>
                  <td className="mono" style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{eventChangeValue(change.after, change.truncated)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
