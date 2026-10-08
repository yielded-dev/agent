import { writeFileSync } from "node:fs";

import { labels, METRICS, sizes, value, type Metric } from "./results.ts";

const W = 1200;
const H = 860;
const PANEL = { w: 600, h: 400, top: 60 };
const PAD = { left: 64, right: 32, top: 52, bottom: 48 };

const INK = {
  primary: "#0b0b0b",
  secondary: "#52514e",
  muted: "#8a8984",
  grid: "#e8e7e4",
  surface: "#fcfcfb",
};

const SERIES: Readonly<Record<string, string>> = {
  yielded: "#6b4fd8",
  "yielded-inline": "#9985df",
  pi: "#2a78d6",
  tardie: "#eb6834",
};

const NAMES: Readonly<Record<string, string>> = {
  yielded: "yielded",
  "yielded-inline": "yielded-inline",
  pi: "pi-durable",
  tardie: "tardie",
};

const color = (item: string): string => SERIES[item.split(" ")[0] ?? ""] ?? "#8a8984";

const name = (item: string): string => {
  const [target, temperature] = item.split(" ");

  return `${NAMES[target ?? ""] ?? target} ${temperature ?? ""}`.trim();
};

const fmt = (n: number): string => Math.round(n).toLocaleString("en-US");

const text = (x: number, y: number, body: string, attrs = ""): string =>
  `<text x="${x}" y="${y}" ${attrs}>${body}</text>`;

const niceMax = (max: number): { max: number; step: number } => {
  const step = 10 ** Math.floor(Math.log10(Math.max(max, 1) / 4));

  const unit = [1, 2, 2.5, 5, 10]
    .map((multiplier) => multiplier * step)
    .find((candidate) => candidate * 4 >= max);

  return { max: (unit ?? step) * Math.ceil(max / (unit ?? step)), step: unit ?? step };
};

const frame = (x: number, y: number, title: string, max: number) => {
  const inner = {
    x: x + PAD.left,
    y: y + PAD.top,
    w: PANEL.w - PAD.left - PAD.right,
    h: PANEL.h - PAD.top - PAD.bottom,
  };

  const scale = niceMax(max);
  const sy = (measurement: number) => inner.y + inner.h - (measurement / scale.max) * inner.h;

  const ticks = Array.from(
    { length: Math.round(scale.max / scale.step) + 1 },
    (_, index) => index * scale.step,
  );

  const svg = [
    text(x + PAD.left, y + 24, title, `font-size="17" font-weight="600" fill="${INK.primary}"`),
    ...ticks.map(
      (tick) =>
        `<line x1="${inner.x}" x2="${inner.x + inner.w}" y1="${sy(tick)}" y2="${sy(tick)}" stroke="${tick === 0 ? INK.muted : INK.grid}" stroke-width="1"/>` +
        text(
          inner.x - 10,
          sy(tick) + 4,
          fmt(tick),
          `font-size="12" text-anchor="end" fill="${INK.muted}"`,
        ),
    ),
    text(
      inner.x + inner.w / 2,
      inner.y + inner.h + 40,
      "turns of history",
      `font-size="12" text-anchor="middle" fill="${INK.muted}"`,
    ),
  ];

  return { inner, sy, svg };
};

const bars = (x: number, y: number, metric: Metric): string[] => {
  const values = sizes.map((size) => labels.map((item) => value(metric, item, size)));

  const defined = values
    .flat()
    .flatMap((measurement) => (measurement === undefined ? [] : [measurement]));

  const { inner, sy, svg } = frame(
    x,
    y,
    `${metric.name[0]?.toUpperCase() ?? ""}${metric.name.slice(1)} turn (ms)`,
    Math.max(...defined, 1),
  );

  const group = inner.w / Math.max(sizes.length, 1);

  const width = Math.max(
    12,
    Math.min(36, Math.floor((group - 16) / Math.max(labels.length, 1)) - 2),
  );

  sizes.forEach((size, index) => {
    const center = inner.x + group * (index + 0.5);
    const row = values[index] ?? [];

    svg.push(
      text(
        center,
        inner.y + inner.h + 20,
        size.toLocaleString("en-US"),
        `font-size="13" text-anchor="middle" fill="${INK.secondary}"`,
      ),
    );
    row.forEach((measurement, series) => {
      if (measurement === undefined) return;
      const bx = center - (row.length * (width + 2)) / 2 + series * (width + 2);
      const top = sy(measurement);
      const radius = Math.min(4, Math.max(0, inner.y + inner.h - top));

      svg.push(
        `<path d="M${bx},${inner.y + inner.h} V${top + radius} Q${bx},${top} ${bx + radius},${top} H${bx + width - radius} Q${bx + width},${top} ${bx + width},${top + radius} V${inner.y + inner.h} Z" fill="${color(labels[series] ?? "")}"/>`,
      );
      svg.push(
        text(
          bx + width / 2,
          top - 6,
          fmt(measurement),
          `font-size="12" text-anchor="middle" fill="${INK.secondary}"`,
        ),
      );
    });
  });

  return svg;
};

const trend = (x: number, y: number, metric: Metric): string[] => {
  const series = labels.map((item) =>
    sizes.flatMap((size) => {
      const measurement = value(metric, item, size);

      return measurement === undefined ? [] : [[size, measurement] as const];
    }),
  );

  const defined = series.flat().map((point) => point[1]);

  const { inner, sy, svg } = frame(
    x,
    y,
    `${metric.name[0]?.toUpperCase() ?? ""}${metric.name.slice(1)} turn vs history (ms)`,
    Math.max(...defined, 1),
  );

  const lastSize = sizes.at(-1) ?? 1;
  const right = inner.w - 120;
  const sx = (turns: number) => inner.x + (turns / lastSize) * right;
  const step = niceMax(lastSize).step;

  for (let size = 0; size <= lastSize; size += step) {
    svg.push(
      text(
        sx(size),
        inner.y + inner.h + 20,
        size.toLocaleString("en-US"),
        `font-size="13" text-anchor="middle" fill="${INK.secondary}"`,
      ),
    );
  }
  series.forEach((points, seriesIndex) => {
    const xy = points.map(([turns, measurement]) => [sx(turns), sy(measurement)] as const);
    const last = xy.at(-1);
    const ink = color(labels[seriesIndex] ?? "");

    svg.push(
      `<polyline points="${xy.map((point) => point.join(",")).join(" ")}" fill="none" stroke="${ink}" stroke-width="2" stroke-linejoin="round"/>`,
    );
    for (const [px, py] of xy) {
      svg.push(
        `<circle cx="${px}" cy="${py}" r="5" fill="${ink}" stroke="${INK.surface}" stroke-width="2"/>`,
      );
    }
    if (last !== undefined) {
      svg.push(
        text(
          last[0] + 12,
          last[1] + 4,
          `${name(labels[seriesIndex] ?? "").split(" ")[0]} ${fmt(points.at(-1)?.[1] ?? 0)}`,
          `font-size="13" fill="${INK.secondary}"`,
        ),
      );
    }
  });

  return svg;
};

const cold = METRICS[0];
const warm = METRICS[1];

if (cold === undefined || warm === undefined) throw new Error("missing cold/warm metrics");

const legend = labels.map(
  (item, index) =>
    `<rect x="${PAD.left + index * 200}" y="22" width="14" height="14" rx="3" fill="${color(item)}"/>` +
    text(PAD.left + 22 + index * 200, 34, name(item), `font-size="14" fill="${INK.primary}"`),
);

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="Inter, -apple-system, system-ui, sans-serif">
<rect width="${W}" height="${H}" fill="${INK.surface}"/>
${[...legend, ...bars(0, PANEL.top, cold), ...bars(PANEL.w, PANEL.top, warm), ...trend(0, PANEL.top + PANEL.h, cold), ...trend(PANEL.w, PANEL.top + PANEL.h, warm)].join("\n")}
</svg>
`;

writeFileSync("results/chart.svg", svg);
console.log("results/chart.svg");
