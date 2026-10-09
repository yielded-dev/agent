import { readFileSync } from "node:fs";

const input = JSON.parse(readFileSync(process.argv[2], "utf8"));
const quantile = (values, p) => {
  const sorted = values.toSorted((a, b) => a - b);
  const index = (sorted.length - 1) * p;
  return (
    sorted[Math.floor(index)] * (1 - (index % 1)) + sorted[Math.ceil(index)] * (index % 1)
  );
};
const groups = Object.groupBy(input.cohorts, (cohort) => cohort.cell);
const rows = Object.entries(groups).map(([cell, cohorts]) => {
  const objects = cohorts.map((cohort) => cohort.epochs.flatMap((epoch) => epoch.driverMs));
  const values = objects.flat();
  return {
    cell,
    n: values.length,
    ...Object.fromEntries([0.5, 0.9, 0.99, 1].map((p) => [`p${p * 100}`, quantile(values, p)])),
    spikes250: objects.reduce(
      (sum, repeats) => sum + repeats.filter((ms) => ms > quantile(repeats, 0.5) + 250).length,
      0,
    ),
    maxObjectRange: Math.max(...objects.map((repeats) => Math.max(...repeats) - Math.min(...repeats))),
  };
});
console.log(JSON.stringify({ run: input.run, rows }, null, 2));
