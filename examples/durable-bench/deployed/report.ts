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
  `${n(median(values))} [${n(quantile(values, 0.25))}–${n(quantile(values, 0.75))}]`;

const group = <A>(rows: readonly A[], key: (row: A) => string) => {
  const result = new Map<string, A[]>();

  for (const row of rows) {
    const name = key(row);

    result.set(name, [...(result.get(name) ?? []), row]);
  }

  return result;
};

export const objectMedians = (rows: readonly Sample[]) =>
  [...group(rows, (row) => row.object).values()].map((items) =>
    median(items.flatMap((item) => (item.driverMs === undefined ? [] : [item.driverMs]))),
  );

const eligible = (row: Sample) =>
  row.status === "ok" &&
  row.state !== "warmup" &&
  (row.expectedBuild === undefined ||
    (row.fingerprintVerified === true &&
      row.buildVerified === true &&
      (row.state === "fresh-first-turn"
        ? row.freshVerified === true
        : row.residentVerified === true)));

export const table = (result: Result): string => {
  const rows = result.samples
    .filter(eligible)
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
    "Yielded/pi ranges use min(Y)/max(pi)–max(Y)/min(pi) of Object medians; descriptive, unpaired, not confidence intervals.",
    "",
  ];

  if (rows.some((row) => row.storageGroup !== undefined))
    lines.push(
      "Storage groups: grown receives padding in candidate passes; control stays unpadded. Framework code is constant. Compare paired group changes against the repeat/control spread.",
      "",
    );

  if (result.sequence)
    lines.push(
      `Upload order: ${result.sequence.join(" → ")}. Each pass follows acknowledged old-build resets and distinct uploaded code bytes.`,
      "Fresh-first-turn requires matching Worker/Object builds, changed Object isolate and incarnation, one Object constructor, zero prior stateless fetches, first entry, and no prior alarms. Routing Worker health probes are recorded separately.",
      "Warm turns require the same Object incarnation and isolate throughout that epoch; non-fresh first turns do not disqualify otherwise valid warm turns.",
      "",
      "| Target / history / TTFT / build | Verified fresh | Attempted | Excluded | Failed / skipped |",
      "|---|---:|---:|---:|---:|",
    );
  if (result.sequence) {
    for (const [cell, samples] of group(
      result.samples.filter((row) => row.state === "fresh-first-turn"),
      (row) =>
        `${row.target}/${row.history}/${row.ttftMs}/${row.build}${row.storageGroup === undefined ? "" : `/${row.storageGroup}`}`,
    ))
      lines.push(
        `| ${cell} | ${samples.filter(eligible).length} | ${samples.length} | ${samples.filter((row) => row.status === "excluded").length} | ${samples.filter((row) => row.status === "failed").length} / ${samples.filter((row) => row.status === "skipped").length} |`,
      );
    lines.push("");
    if (!rows.some((row) => row.state === "fresh-first-turn"))
      lines.push("No verified fresh-first-turn samples; no fresh-start ratio can be reported.", "");
  }

  for (const [cell, samples] of group(
    rows,
    (row) =>
      `${row.history} history · ${row.ttftMs} ms TTFT · ${row.state} · ${row.build}${row.storageGroup === undefined ? "" : ` · ${row.storageGroup}`}`,
  )) {
    lines.push(
      cell,
      "",
      "| Target | Turn ms | Object median range | Repeat range, median / max | Objects |",
      "|---|---:|---:|---:|---:|",
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
        `| ${target} | ${interval(medians)} | ${n(Math.min(...medians))}–${n(Math.max(...medians))} | ${n(median(spreads))} / ${n(Math.max(...spreads))} | ${medians.length} |`,
      );
    }

    const yielded = objectMedians(samples.filter((row) => row.target === "yielded"));
    const pi = objectMedians(samples.filter((row) => row.target === "pi"));
    const ratio = median(yielded) / median(pi);

    if (Number.isFinite(ratio)) {
      const low = Math.min(...yielded) / Math.max(...pi);
      const high = Math.max(...yielded) / Math.min(...pi);

      lines.push(
        "",
        `Yielded ÷ pi: **${ratio.toFixed(2)}×**; observed ratio range **${low.toFixed(2)}–${high.toFixed(2)}×** (${yielded.length} Yielded / ${pi.length} pi Objects).`,
      );
    }
    lines.push("");
  }
  if (rows.some((row) => row.build === "baseline")) {
    lines.push(
      "Same-Object A/B and pi control (candidate-position ÷ baseline-position; <1 is faster):",
      "",
      "| Target / cell | Paired ratio, median | Baseline repeat drift, median / max | Complete four-pass pairs |",
      "|---|---:|---:|---:|",
    );
    for (const [cell, samples] of group(
      rows,
      (row) =>
        `${row.target}/${row.history}/${row.ttftMs}/${row.state}${row.storageGroup === undefined ? "" : `/${row.storageGroup}`}`,
    )) {
      const ratios: number[] = [];
      const drifts: number[] = [];

      for (const turns of group(samples, (row) => row.object).values()) {
        const first = turns[0];

        if (!first) continue;

        const planned = result.samples.filter(
          (row) =>
            row.target === first.target && row.object === first.object && row.state === first.state,
        );

        const repeats =
          typeof result.options === "object" &&
          result.options !== null &&
          "repeats" in result.options &&
          typeof result.options.repeats === "number"
            ? result.options.repeats
            : undefined;

        const expected = first.state === "warm" ? repeats : 1;
        const passes = group(planned, (row) => String(row.epoch));

        if (
          expected === undefined ||
          passes.size !== 4 ||
          planned.some((row) => !eligible(row)) ||
          [...passes.values()].some((pass) => pass.length !== expected) ||
          new Set(turns.filter((row) => row.build === "baseline").map((row) => row.epoch)).size !==
            2 ||
          new Set(turns.filter((row) => row.build === "candidate").map((row) => row.epoch)).size !==
            2
        )
          continue;
        const a = turns.filter((row) => row.build === "baseline");
        const b = turns.filter((row) => row.build === "candidate");

        const epochs = [...group(a, (row) => String(row.epoch)).values()].map((epoch) =>
          median(objectMedians(epoch)),
        );

        ratios.push(median(objectMedians(b)) / median(objectMedians(a)));
        drifts.push(Math.max(...epochs) / Math.min(...epochs) - 1);
      }
      lines.push(
        ratios.length === 0
          ? `| ${cell} | — | — | 0 |`
          : `| ${cell} | ${median(ratios).toFixed(3)}× | ${(median(drifts) * 100).toFixed(1)}% / ${(Math.max(...drifts) * 100).toFixed(1)}% | ${ratios.length} |`,
      );
    }
    lines.push(
      "",
      "Do not claim gains smaller than the repeat/control spread. Histories grow across the recorded upload sequence.",
      "Pairs with any excluded, failed, skipped, or missing measured turn are omitted; the original rows remain in JSON.",
      "",
    );
  }
  const excluded = result.samples.filter((row) => row.status === "excluded");

  if (excluded.length > 0) {
    lines.push(
      `Excluded completed turns: ${excluded.length} (including warmup). Reasons may overlap:`,
      "",
      "| Reason | Samples |",
      "|---|---:|",
    );
    for (const [reason, entries] of group(
      excluded.flatMap((row) => row.exclusionReasons ?? []),
      (reason) => reason,
    ))
      lines.push(`| ${reason} | ${entries.length} |`);
    lines.push("");
  }
  if (result.cpu) {
    lines.push(
      "Observed invocation CPU (telemetry may be sampled or delayed):",
      "",
      "| Worker / target / invocation | CPU ms | Observed | Non-ok outcomes |",
      "|---|---:|---:|---:|",
    );
    for (const [kind, calls] of group(
      result.cpu,
      (row) =>
        `${row.worker ?? "combined"}/${row.target ?? "unattributed"}/${row.sample?.match(/^h\d+$/) ? "seed-" : ""}${row.kind}`,
    ))
      lines.push(
        `| ${kind} | ${interval(calls.flatMap((row) => (row.cpuMs === null ? [] : [row.cpuMs])))} | ${calls.length} | ${calls.filter((row) => row.outcome !== "ok").length} |`,
      );
    lines.push(
      "",
      `Unmatched invocation markers: ${result.unmatchedCpuMarkers ?? "unknown"}. CPU totals are not inferred from missing rows.`,
      "All non-ok invocation outcomes remain included, including intentional cold-abort exceptions.",
      "",
    );
  }
  lines.push(
    `Failures: ${result.failures.length}; failed samples: ${result.samples.filter((row) => row.status === "failed").length}; skipped samples: ${result.samples.filter((row) => row.status === "skipped").length}. Target cleanup: ${result.kept ? "kept (--keep)" : result.cleanup?.verified ? "verified" : "NOT VERIFIED"}.`,
  );

  return lines.join("\n") + "\n";
};
