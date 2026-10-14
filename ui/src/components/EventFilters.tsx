// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useSearchParams } from 'react-router-dom';
import { Button } from './ui/Button';
import { TextField } from './ui/Field';
import { applyEventFilters, eventFilters } from './event-filters';

export function useEventFilters() {
  const [params] = useSearchParams();
  return eventFilters(params);
}

export function EventFilters({ audit, cluster = false }: { audit: boolean; cluster?: boolean }) {
  const [params, setParams] = useSearchParams();
  const fields = [...(audit ? ['actor', 'resource'] : []), 'type', ...(cluster ? ['cluster'] : [])];
  return <form key={params.toString()} className="row wrap" style={{ alignItems: 'flex-end', gap: 12 }} onSubmit={(event) => {
    event.preventDefault();
    const values = new FormData(event.currentTarget);
    setParams((previous) => applyEventFilters(previous, Object.fromEntries(fields.map((field) => [field, String(values.get(field) ?? '')]))));
  }}>
    {fields.map((field) => <TextField key={field} name={field} label={field[0]!.toUpperCase() + field.slice(1)} defaultValue={params.get(field) ?? ''} placeholder="Exact match" className="grow" />)}
    <Button type="submit">Apply filters</Button>
  </form>;
}
