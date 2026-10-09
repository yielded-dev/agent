import { type Result, type Sample } from "./model.ts";
import { TARGETS } from "./worker/protocol.ts";

export const quantile = (values: readonly number[], q: number): number => {
  const sorted = [...values].sort((a, b) => a - b);
  const index = (sorted.length - 1) * q;
  const lower = Math.floor(index);
  const a = sorted[lower];

  return a === undefined ? NaN : a + ((sorted[lower + 1] ?? a) - a) * (index - lower);
};

export const median = (values: readonly number[]) => quantile(values, 0.5);

const n = (value: number) =>
  Number.isFinite(value) ? Math.round(value).toLocaleString("en-US") : "—";

const interval = (values: readonly number[]) =>
  values.length
    ? `${n(median(values))} [${n(quantile(values, 0.25))}–${n(quantile(values, 0.75))}]`
    : "n/a";

const group = <A>(rows: readonly A[], key: (row: A) => string) => {
  const result = new Map<string, A[]>();

  for (const row of rows) {
    const name = key(row);

    result.set(name, [...(result.get(name) ?? []), row]);
  }

  return result;
};

type Metric = "driverMs" | "firstTextMs" | "firstModelRequestMs";

export const objectMedians = (rows: readonly Sample[], metric: Metric = "driverMs") =>
  [...group(rows, (row) => row.object).values()].flatMap((items) => {
    const values = items.flatMap((item) =>
      typeof item[metric] === "number" ? [item[metric]] : [],
    );

    return values.length ? [median(values)] : [];
  });

const ratio = (samples: readonly Sample[], metric: Metric): string => {
  const yielded = objectMedians(
    samples.filter((row) => row.target === "yielded"),
    metric,
  );

  const pi = objectMedians(
    samples.filter((row) => row.target === "pi"),
    metric,
  );

  const value = median(yielded) / median(pi);

  return Number.isFinite(value)
    ? `${value.toFixed(2)}× [${(Math.min(...yielded) / Math.max(...pi)).toFixed(2)}–${(Math.max(...yielded) / Math.min(...pi)).toFixed(2)}×]`
    : "n/a";
};

export const table = (result: Result): string => {
  const rows = result.samples
    .filter((row) => row.status === "ok" && row.state !== "warmup")
    .sort(
      (a, b) =>
        a.history - b.history ||
        a.ttftMs - b.ttftMs ||
        a.state.localeCompare(b.state) ||
        a.build.localeCompare(b.build),
    );

  const lines = [
    ...(result.complete ? [] : ["INCOMPLETE RUN — diagnostic results only.", ""]),
    "Driver-observed milliseconds; median [Q1–Q3] of Object medians.",
    "Yielded falls back to settled text if no preview arrives; JSON records firstTextSource.",
    "First request crosses Object/driver clocks (approximate). Cold turns exclude observer attachment; first text is n/a for tardie.",
    "Yielded/pi ranges use min(Y)/max(pi)–max(Y)/min(pi) of Object medians; descriptive, unpaired, not confidence intervals.",
    "",
  ];

  for (const [cell, samples] of group(
    rows,
    (row) =>
      `${row.history} history · ${row.ttftMs} ms TTFT · ${row.textStreaming ? "text-streaming" : "standard"} · ${row.state} · ${row.build}`,
  )) {
    lines.push(
      cell,
      "",
      "| Target | First text ms | Turn ms | First request ≈ms | Turn median range | Turn repeat range, median / max | Objects |",
      "|---|---:|---:|---:|---:|---:|---:|",
    );
    for (const target of TARGETS) {
      const turns = samples.filter((row) => row.target === target);

      if (!turns.length) continue;
      const medians = objectMedians(turns);

      const spreads = [...group(turns, (row) => row.object).values()].map((items) => {
        const values = items.flatMap((row) => (row.driverMs === undefined ? [] : [row.driverMs]));

        return Math.max(...values) - Math.min(...values);
      });

      lines.push(
        `| ${target} | ${interval(objectMedians(turns, "firstTextMs"))} | ${interval(medians)} | ${interval(objectMedians(turns, "firstModelRequestMs"))} | ${n(Math.min(...medians))}–${n(Math.max(...medians))} | ${n(median(spreads))} / ${n(Math.max(...spreads))} | ${medians.length} |`,
      );
    }

    if (
      samples.some((row) => row.target === "yielded") &&
      samples.some((row) => row.target === "pi")
    ) {
      lines.push(
        `| Yielded ÷ pi | ${ratio(samples, "firstTextMs")} | ${ratio(samples, "driverMs")} | ${ratio(samples, "firstModelRequestMs")} | | | |`,
      );
    }
    lines.push("");
  }
  if (rows.some((row) => row.build === "baseline")) {
    lines.push(
      "Same-Object Yielded A/B (candidate ÷ baseline; <1 is faster):",
      "",
      "| Cell | Paired ratio, median | Baseline repeat drift, median / max |",
      "|---|---:|---:|",
    );
    for (const [cell, samples] of group(
      rows.filter((row) => row.target === "yielded"),
      (row) => `${row.history}/${row.ttftMs}/${row.state}`,
    )) {
      const ratios: number[] = [];
      const drifts: number[] = [];

      for (const turns of group(samples, (row) => row.object).values()) {
        const a = turns.filter((row) => row.build === "baseline");
        const b = turns.filter((row) => row.build === "candidate");

        const epochs = [...group(a, (row) => String(row.epoch)).values()].map((epoch) =>
          median(objectMedians(epoch)),
        );

        ratios.push(median(objectMedians(b)) / median(objectMedians(a)));
        drifts.push(Math.max(...epochs) / Math.min(...epochs) - 1);
      }
      lines.push(
        `| ${cell} | ${median(ratios).toFixed(3)}× | ${(median(drifts) * 100).toFixed(1)}% / ${(Math.max(...drifts) * 100).toFixed(1)}% |`,
      );
    }
    lines.push(
      "",
      "Do not claim gains smaller than the repeat/control spread. Histories grow across the balanced build sequence.",
      "",
    );
  }
  if (result.cpu) {
    lines.push(
      "Observed invocation CPU (telemetry may be sampled or delayed):",
      "",
      "| Target / invocation | CPU ms | Observed | Non-ok outcomes |",
      "|---|---:|---:|---:|",
    );
    for (const [kind, calls] of group(
      result.cpu,
      (row) =>
        `${row.target ?? "unattributed"}/${row.sample?.match(/^h\d+$/) ? "seed-" : ""}${row.kind}`,
    ))
      lines.push(
        `| ${kind} | ${interval(calls.flatMap((row) => (row.cpuMs === null ? [] : [row.cpuMs])))} | ${calls.length} | ${calls.filter((row) => row.outcome !== "ok").length} |`,
      );
    lines.push(
      "",
      `Unmatched invocation markers: ${result.unmatchedCpuMarkers ?? "unknown"}. CPU totals are not inferred from missing rows.`,
      "",
    );
  }
  lines.push(
    `Failures: ${result.failures.length}. Target cleanup: ${result.kept ? "kept (--keep)" : result.cleanup?.verified ? "verified" : "NOT VERIFIED"}.`,
  );

  return lines.join("\n") + "\n";
};
