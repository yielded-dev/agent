import { readFileSync } from "node:fs";

export interface Line {
  readonly target: string;
  readonly version: string;
  readonly turns: number;
  readonly open: number;
  readonly turn: readonly number[];
  readonly bytes: number;
  readonly rss?: number;
}

export interface Metric {
  readonly name: string;
  readonly unit: string;
  readonly of: (group: readonly Line[]) => number;
}

export const median = (values: readonly number[]): number => {
  const sorted = values.toSorted((left, right) => left - right);
  const mid = sorted.length >> 1;
  const upper = sorted[mid];
  const lower = sorted[mid - 1];

  if (upper === undefined) return Number.NaN;
  if (sorted.length % 2 === 1 || lower === undefined) return upper;

  return (lower + upper) / 2;
};

export const lines = readFileSync("results/results.jsonl", "utf8")
  .trim()
  .split("\n")
  .map((line) => JSON.parse(line) as Line);

export const label = (line: Line): string => `${line.target} ${line.version}`;

export const labels = [...new Set(lines.map(label))].sort();

export const sizes = [...new Set(lines.map((line) => line.turns))].sort(
  (left, right) => left - right,
);

export const METRICS: readonly Metric[] = [
  {
    name: "cold",
    unit: "ms",
    of: (group) => median(group.map((line) => line.open + (line.turn[0] ?? 0))),
  },
  { name: "warm", unit: "ms", of: (group) => median(group.flatMap((line) => line.turn.slice(1))) },
  { name: "storage", unit: "MB", of: (group) => median(group.map((line) => line.bytes)) / 1e6 },
  {
    name: "rss",
    unit: "MB",
    of: (group) => median(group.flatMap((line) => (line.rss === undefined ? [] : [line.rss]))),
  },
];

export const value = (metric: Metric, target: string, turns: number): number | undefined => {
  const group = lines.filter((line) => label(line) === target && line.turns === turns);

  return group.length === 0 ? undefined : metric.of(group);
};
