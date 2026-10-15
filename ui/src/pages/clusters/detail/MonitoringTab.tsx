// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useMemo } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { LineChart, type Series } from '@/components/ui/LineChart';
import { Callout, Card, ErrorBlock, Loading, Segmented, formatBytes, formatNum } from '@/components/ui/Primitives';
import { useClusterContext } from './ClusterDetail';
import { monitoringRange, metricCategories, type MonitoringRange as Range } from '@/components/ui/presentation';

const STEP_FOR: Record<Range, string> = { '15m': '15s', '1h': '30s', '6h': '2m', '24h': '10m' };

interface Panel {
  title: string;
  /** Series names to overlay; each member becomes a line per name. */
  names: string[];
  unit?: string;
  yMax?: number;
  format?: (v: number) => string;
}

const PANELS: Panel[] = [
  { title: 'Operations', names: ['opcounters.total'], unit: 'ops/s' },
  { title: 'Connections', names: ['connections.current'], unit: 'count' },
  { title: 'Replication lag', names: ['repl.lagSeconds'], unit: 's' },
  { title: 'Cache used', names: ['cache.usedBytes', 'cache.maxBytes'], unit: 'bytes', format: (v) => formatBytes(v) },
  { title: 'CPU', names: ['cpu.percent'], unit: '%', yMax: 100 },
  { title: 'Memory used', names: ['ram.usedBytes', 'ram.totalBytes'], unit: 'bytes', format: (v) => formatBytes(v) },
  { title: 'Disk used', names: ['disk.usedBytes', 'disk.totalBytes'], unit: 'bytes', format: (v) => formatBytes(v) },
];

function toSeries(all: Schemas['MetricSeries'][], names: string[], multiName: boolean): Series[] {
  return all
    .filter((s) => names.includes(s.name))
    .map((s) => ({
      name: [s.member, multiName ? s.name.split('.').pop() : '', ...Object.entries(s.labels ?? {}).map(([key, value]) => `${key}=${value}`)].filter(Boolean).join(' '),
      points: s.points.map((p) => [new Date(p.timestamp).getTime(), p.value] as [number, number]),
    }));
}

export function MonitoringTab() {
  const { cluster, namespace, name, base, can } = useClusterContext();
  const [params, setParams] = useSearchParams();
  const range = monitoringRange(params.get('range'));
  const setRange = (next: Range) => setParams((prev) => { const out = new URLSearchParams(prev); out.set('range', next); return out; });
  const metrics = useQuery({
    queryKey: [...keys.clusterSub(namespace, name, 'metrics'), range],
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/metrics', { params: { path: { cluster: name }, query: { namespace, range, step: STEP_FOR[range] } } })),
    refetchInterval: 30_000,
    enabled: can('read', 'monitoring'),
  });

  const known = useMemo(() => PANELS.flatMap((p) => p.names), []);
  const extra = useMemo(() => {
    const names = new Set<string>();
    for (const s of metrics.data?.series ?? []) if (!known.includes(s.name)) names.add(s.name);
    return Array.from(names);
  }, [metrics.data, known]);
  const timeRange = useMemo(() => metrics.data ? [Date.parse(metrics.data.start), Date.parse(metrics.data.end)] as const : undefined, [metrics.data]);

  if (!can('read', 'monitoring')) return <Callout tone="info">You do not have permission to read monitoring data for this cluster.</Callout>;

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="row-between wrap">
        <div className="small muted">
          {cluster.spec.monitoring?.enabled === false ? (
            <>
              Metrics collection is disabled for this cluster. Enable it under <Link to={`${base}/topology`}>Topology &amp; scaling</Link>.
            </>
          ) : (
            <>
              Per-member samples collected by the node agents. Scrape all clusters at <code>/metrics</code> (Prometheus, API-token auth) — see <Link to="/settings/monitoring">Settings → Monitoring</Link>.
            </>
          )}
        </div>
        <Segmented<Range>
          label="Time range"
          value={range}
          onChange={setRange}
          options={[
            { value: '15m', label: '15m' },
            { value: '1h', label: '1h' },
            { value: '6h', label: '6h' },
            { value: '24h', label: '24h' },
          ]}
        />
      </div>
      <p className="xs muted">In-UI queries cover at most 24 hours. Use the Prometheus endpoint for longer retention.</p>
      {metrics.isPending ? (
        <Loading label="Loading metrics…" />
      ) : metrics.error ? (
        <ErrorBlock error={metrics.error} retry={() => metrics.refetch()} />
      ) : (
        <>
          <div className="grid-2">
            {PANELS.map((p) => (
              <Card key={p.title}>
                <LineChart title={p.title} unit={p.unit} yMax={p.yMax} timeRange={timeRange} series={toSeries(metrics.data.series, p.names, p.names.length > 1)} formatValue={p.format ?? formatNum} />
              </Card>
            ))}
            {extra.map((n) => {
              const unit = metrics.data.series.find((s) => s.name === n)?.unit;
              const categories = metricCategories(n);
              return (
                <Card key={n}>
                  <LineChart title={n} unit={categories ? 'state code' : unit} categories={categories} timeRange={timeRange} series={toSeries(metrics.data.series, [n], false)} formatValue={unit === 'bytes' ? (v) => formatBytes(v) : formatNum} />
                </Card>
              );
            })}
          </div>
          <p className="xs faint">
            Window {new Date(metrics.data.start).toLocaleString()} → {new Date(metrics.data.end).toLocaleString()}, step {metrics.data.step}. {metrics.data.series.length} series.
          </p>
        </>
      )}
    </div>
  );
}
