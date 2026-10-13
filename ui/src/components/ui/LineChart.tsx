// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useId, useMemo } from 'react';
import { formatNum } from './Primitives';

export interface Series {
  name: string;
  points: Array<[number, number]>; // [epochMillis, value]
}

const PALETTE = ['#2563eb', '#059669', '#d97706', '#7c3aed', '#db2777', '#0891b2', '#65a30d', '#ea580c'];

export interface LineChartProps {
  series: Series[];
  title: string;
  unit?: string;
  height?: number;
  /** Fix the y-axis upper bound (e.g. 100 for percentages). */
  yMax?: number;
  formatValue?: (v: number) => string;
}

/**
 * Dependency-free responsive SVG line chart. Renders a legend, 4 horizontal
 * gridlines with labels and time ticks. Accessible description lists the last
 * value per series.
 */
export function LineChart({ series, title, unit, height = 160, yMax, formatValue = formatNum }: LineChartProps) {
  const id = useId();
  const width = 640;
  const pad = { l: 62, r: 10, t: 8, b: 20 };
  const iw = width - pad.l - pad.r;
  const ih = height - pad.t - pad.b;

  const model = useMemo(() => {
    const all = series.flatMap((s) => s.points);
    if (all.length === 0) return null;
    let xMin = Infinity;
    let xMax = -Infinity;
    let vMax = 0;
    for (const [x, y] of all) {
      if (x < xMin) xMin = x;
      if (x > xMax) xMax = x;
      if (y > vMax) vMax = y;
    }
    if (xMax === xMin) xMax = xMin + 1;
    const top = yMax ?? niceCeil(vMax);
    const sx = (x: number) => pad.l + ((x - xMin) / (xMax - xMin)) * iw;
    const sy = (y: number) => pad.t + ih - (top <= 0 ? 0 : (Math.min(y, top) / top) * ih);
    const paths = series.map((s) => s.points.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${sx(x).toFixed(1)},${sy(y).toFixed(1)}`).join(' '));
    const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({ y: pad.t + ih - f * ih, v: f * top }));
    const xt = [0, 0.5, 1].map((f) => ({ x: pad.l + f * iw, t: xMin + f * (xMax - xMin) }));
    return { paths, ticks, xt, top };
  }, [series, yMax, ih, iw, pad.l, pad.t]);

  const describe = series
    .map((s) => {
      const last = s.points[s.points.length - 1];
      return `${s.name}: ${last ? formatValue(last[1]) : 'no data'}${unit ? ` ${unit}` : ''}`;
    })
    .join('; ');

  return (
    <figure className="stack" style={{ margin: 0, gap: 6 }}>
      <figcaption className="row-between">
        <span className="small" style={{ fontWeight: 600 }}>
          {title}
          {unit ? <span className="faint"> ({unit})</span> : null}
        </span>
      </figcaption>
      <svg className="chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-labelledby={`${id}-t`} aria-describedby={`${id}-d`}>
        <title id={`${id}-t`}>{title}</title>
        <desc id={`${id}-d`}>{describe || 'No data'}</desc>
        {model ? (
          <>
            {model.ticks.map((t, i) => (
              <g key={i}>
                <line x1={pad.l} x2={width - pad.r} y1={t.y} y2={t.y} stroke="var(--border)" strokeWidth="1" />
                <text x={pad.l - 6} y={t.y + 3} fontSize="10" textAnchor="end" fill="var(--text-3)">
                  {formatValue(t.v)}
                </text>
              </g>
            ))}
            {model.xt.map((t, i) => (
              <text key={i} x={t.x} y={height - 5} fontSize="10" textAnchor={i === 0 ? 'start' : i === 2 ? 'end' : 'middle'} fill="var(--text-3)">
                {new Date(t.t).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}
              </text>
            ))}
            {model.paths.map((d, i) => (
              <path key={i} d={d} fill="none" stroke={PALETTE[i % PALETTE.length]} strokeWidth="1.6" strokeLinejoin="round" strokeLinecap="round" />
            ))}
          </>
        ) : (
          <text x={width / 2} y={height / 2} fontSize="12" textAnchor="middle" fill="var(--text-3)">
            No samples in range
          </text>
        )}
      </svg>
      <div className="chart-legend" aria-hidden>
        {series.map((s, i) => {
          const last = s.points[s.points.length - 1];
          return (
            <span key={s.name} style={{ ['--c' as string]: PALETTE[i % PALETTE.length] }}>
              {s.name} <strong>{last ? formatValue(last[1]) : '—'}</strong>
            </span>
          );
        })}
      </div>
    </figure>
  );
}

function niceCeil(v: number): number {
  if (v <= 0) return 1;
  const exp = Math.pow(10, Math.floor(Math.log10(v)));
  const n = v / exp;
  const nice = n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10;
  return nice * exp;
}
