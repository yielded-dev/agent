import { labels, METRICS, sizes, value } from "./results.ts";

const cell = (measurement: number | undefined, unit: string): string =>
  measurement === undefined ? "-" : `${measurement.toFixed(unit === "MB" ? 2 : 0)} ${unit}`;

console.log(
  `| turns | ${METRICS.flatMap((metric) => labels.map((name) => `${name} ${metric.name}`)).join(" | ")} |`,
);
console.log(`|---:|${METRICS.flatMap(() => labels.map(() => "---:")).join("|")}|`);
for (const turns of sizes) {
  console.log(
    `| ${turns.toLocaleString("en-US")} | ${METRICS.flatMap((metric) => labels.map((name) => cell(value(metric, name, turns), metric.unit))).join(" | ")} |`,
  );
}
