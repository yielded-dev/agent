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
  const result = new Map<string, [A, ...A[]]>();

  for (const row of rows) {
    const name = key(row);

    const items = result.get(name);

    if (items) items.push(row);
    else result.set(name, [row]);
  }

  return result;
};

type Metric =
  | "driverMs"
  | "firstTextMs"
  | "constructorAndProbeMs"
  | "objectToFirstModelMs"
  | "providerFirstTokenMs"
  | "firstTextForwardedMs"
  | "settlementMs"
  | "admissionMs";

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

  return Number.isFinite(value) && Math.min(...pi) > 0
    ? `${value.toFixed(2)}× [${(Math.min(...yielded) / Math.max(...pi)).toFixed(2)}–${(Math.max(...yielded) / Math.min(...pi)).toFixed(2)}×]`
    : "n/a";
};

/** Match the slide: pool the nine warm turns from each complete Object, not their medians. */
export const historySummary = (result: Result) => {
  const { repeats } = Schema.decodeUnknownSync(Schema.Struct({ repeats: Schema.Int }))(
    result.options,
  );

  return [
    ...group(result.histories, (row) => `${row.history}/${row.ttftMs}/${row.target}`).values(),
  ]
    .map((builds) => {
      const first = builds[0];

      const batches = [
        ...group(
          result.samples.filter(
            (row) =>
              row.target === first.target &&
              row.history === first.history &&
              row.ttftMs === first.ttftMs,
          ),
          (row) => row.object,
        ).values(),
      ].filter(
        (rows) =>
          rows.length === repeats + 1 &&
          rows.every((row) => row.status === "ok") &&
          rows.filter((row) => row.state === "cold").length === 1 &&
          rows.filter((row) => row.state === "warm").length === repeats,
      );

      const samples = batches.flat();

      const values = (state: Sample["state"]) =>
        samples.flatMap((row) =>
          row.state === state && row.driverMs !== undefined ? [row.driverMs] : [],
        );

      return {
        history: first.history,
        ttftMs: first.ttftMs,
        target: first.target,
        attempted: builds.length,
        built: builds.filter((row) => row.status === "ok").length,
        objects: batches.length,
        buildMs: median(
          builds.flatMap((row) =>
            row.status === "ok" && row.driverMs !== undefined ? [row.driverMs] : [],
          ),
        ),
        coldMs: median(values("cold")),
        warmMs: median(values("warm")),
        bytes: median(
          batches.flatMap((rows) => {
            const last = rows.toSorted((a, b) => b.repeat - a.repeat)[0];

            return last?.bytes === undefined ? [] : [last.bytes];
          }),
        ),
      };
    })
    .sort(
      (a, b) =>
        a.history - b.history ||
        a.ttftMs - b.ttftMs ||
        TARGETS.indexOf(a.target) - TARGETS.indexOf(b.target),
    );
};

const historyTable = (result: Result): string => {
  const rows = historySummary(result);

  const decimal = (value: number, digits: number) =>
    Number.isFinite(value) ? value.toFixed(digits) : "n/a";

  const lines = [
    ...(result.complete ? [] : ["INCOMPLETE RUN — failed Objects are retained below.", ""]),
    ...(result.profiles.length
      ? [
          "Profiled diagnostic: timing includes profiling overhead; exclude it from comparisons.",
          "",
        ]
      : []),
    "Independent Objects per size; real one-tool, one-tool, zero-tool turns in batches of 50 at zero provider TTFT, with runtime restarts between batches.",
    "Each logical sample has a dedicated Worker. Batch isolate tokens are checked for sharing across build cohorts; shared cohorts are rejected.",
    ...(result.resumeCount
      ? [`Resumed ${result.resumeCount} time(s); resumed build times include controller downtime.`]
      : []),
    "Build: end-to-end driver elapsed. Cold: constructor + clock-probe roundtrip + native open and first turn; isolate startup and priming transport are excluded.",
    "Warm: pooled median of the next warm turns, with no extra warmup. Storage: median after the last measured turn, including Tardie's Actor directory.",
    "Network hops are included. Compare ratios within this deployment; absolute times are not comparable to Miniflare.",
    "",
    `Revision: ${result.revision}. Versions: ${Object.entries(result.versions)
      .map(([name, version]) => `${name} ${version}`)
      .join(", ")}.`,
    "",
  ];

  for (const [cell, targets] of group(
    rows,
    (row) => `${row.history} turns · ${row.ttftMs} ms TTFT`,
  )) {
    lines.push(
      cell,
      "",
      "| Target | Build s | Cold ms | Warm ms | DB MB | Objects built / measured / attempted |",
      "|---|---:|---:|---:|---:|---:|",
    );
    for (const row of targets)
      lines.push(
        `| ${row.target} | ${decimal(row.buildMs / 1000, 2)} | ${n(row.coldMs)} | ${n(row.warmMs)} | ${decimal(row.bytes / 1e6, 3)} | ${row.built} / ${row.objects} / ${row.attempted} |`,
      );
    const pi = targets.find((row) => row.target === "pi");

    if (pi)
      for (const row of targets.filter((row) => row.target !== "pi")) {
        const ratios = (["buildMs", "coldMs", "warmMs", "bytes"] as const).map((metric) =>
          Number.isFinite(row[metric] / pi[metric]) && pi[metric] > 0
            ? `${(row[metric] / pi[metric]).toFixed(2)}×`
            : "n/a",
        );

        lines.push(`| ${row.target} ÷ pi | ${ratios.join(" | ")} | |`);
      }
    lines.push("");
  }

  const driverColos = [
    ...new Set(
      result.histories.flatMap((row) =>
        row.batches.flatMap((batch) => (batch.colo ? [batch.colo] : [])),
      ),
    ),
  ];

  const providerColos = [
    ...new Set(
      result.histories.flatMap((row) => row.batches.flatMap((batch) => batch.result.providerColos)),
    ),
  ];

  lines.push(
    `Driver ingress colos: ${driverColos.join(", ") || "unknown"}. Provider ingress colos: ${providerColos.join(", ") || "unknown"}. Object placement hint: wnam; Object colo is not exposed.`,
    "",
  );
  for (const failure of result.failures) lines.push(`- ${failure}`);
  lines.push(
    "",
    `Failures: ${result.failures.length}. Target cleanup: ${result.workerPool ? "Worker pool retained (--teardown to remove)" : result.kept ? "kept" : result.cleanup?.verified ? "verified" : "NOT VERIFIED"}.`,
  );

  return lines.join("\n") + "\n";
};

export const table = (result: Result): string => {
  if (result.histories.length) return historyTable(result);

  const rows = result.samples
    .filter((row) => row.status === "ok" && (row.state === "cold" || row.state === "warm"))
    .sort(
      (a, b) =>
        a.history - b.history ||
        a.ttftMs - b.ttftMs ||
        a.state.localeCompare(b.state) ||
        a.build.localeCompare(b.build),
    );

  const lines = [
    ...(result.complete ? [] : ["INCOMPLETE RUN — diagnostic results only.", ""]),
    ...(result.samples.some((row) => row.status === "invalidated")
      ? [
          `Invalidated pass records: ${result.samples.filter((row) => row.status === "invalidated").length}; retained in JSON and replaced by a verified pass.`,
        ]
      : []),
    "Driver-observed milliseconds; median [Q1–Q3] of Object medians.",
    "Yielded falls back to settled text if no preview arrives; JSON records firstTextSource.",
    "Object → model uses the Object clock and excludes driver transport. Cold turns exclude observer attachment; first text is n/a for tardie.",
    "Yielded/pi ranges use min(Y)/max(pi)–max(Y)/min(pi) of Object medians; descriptive, unpaired, not confidence intervals.",
    "",
  ];

  for (const [cell, samples] of group(
    rows,
    (row) =>
      `${row.history} history · ${row.ttftMs} ms TTFT · ${row.textStreaming ? "text-streaming" : "standard"} · ${row.state} · ${row.build}`,
  )) {
    const firstTextLabel = samples[0]?.textStreaming ? "First text" : "First text (final answer)";

    lines.push(
      cell,
      "",
      `| Target | ${firstTextLabel} ms | Turn ms | Constructor / probe ms | Object → model ms | Turn median range | Turn repeat range, median / max | Objects |`,
      "|---|---:|---:|---:|---:|---:|---:|---:|",
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
        `| ${target} | ${interval(objectMedians(turns, "firstTextMs"))} | ${interval(medians)} | ${interval(objectMedians(turns, "constructorAndProbeMs"))} | ${interval(objectMedians(turns, "objectToFirstModelMs"))} | ${n(Math.min(...medians))}–${n(Math.max(...medians))} | ${n(median(spreads))} / ${n(Math.max(...spreads))} | ${medians.length} |`,
      );
    }

    if (
      samples.some((row) => row.target === "yielded") &&
      samples.some((row) => row.target === "pi")
    ) {
      lines.push(
        `| Yielded ÷ pi | ${ratio(samples, "firstTextMs")} | ${ratio(samples, "driverMs")} | | ${ratio(samples, "objectToFirstModelMs")} | | | |`,
      );
    }
    lines.push("");
  }
  lines.push(
    "Turn phases: Object-clock milliseconds since submission entry; receipt is driver admission latency. Missing markers are not zero.",
    "",
    "| Cell / target / build | Provider first token | First text forwarded | Settlement | Receipt (driver) |",
    "|---|---:|---:|---:|---:|",
  );
  for (const [cell, samples] of group(
    rows,
    (row) => `${row.history}/${row.ttftMs}/${row.state}/${row.target}/${row.build}`,
  )) {
    lines.push(
      `| ${cell} | ${(["providerFirstTokenMs", "firstTextForwardedMs", "settlementMs", "admissionMs"] as const).map((metric) => interval(objectMedians(samples, metric))).join(" | ")} |`,
    );
  }
  lines.push("");
  if (rows.some((row) => row.build === "baseline")) {
    lines.push(
      "Same-Object paired medians; delta is candidate − baseline. 95% percentile bootstrap CI resamples whole Objects (10,000 draws).",
      "",
      "| Cell / metric | Baseline ms | Candidate ms | Paired delta ms [95% CI] | Paired ratio [95% CI] | First − second pass ms [95% CI] |",
      "|---|---:|---:|---:|---:|---:|",
    );
    for (const [cell, samples] of group(
      rows,
      (row) => `${row.target}/${row.history}/${row.ttftMs}/${row.state}`,
    )) {
      for (const metric of ["driverMs", "firstTextMs"] as const) {
        const pairs = [...group(samples, (row) => row.object).values()].flatMap((turns) => {
          const baseline = objectMedians(
            turns.filter((row) => row.build === "baseline"),
            metric,
          );

          const candidate = objectMedians(
            turns.filter((row) => row.build === "candidate"),
            metric,
          );

          const first = objectMedians(
            turns.filter((row) => row.order !== undefined && row.epoch === 0),
            metric,
          )[0];

          const second = objectMedians(
            turns.filter((row) => row.order !== undefined && row.epoch === 1),
            metric,
          )[0];

          return baseline.length === 1 && candidate.length === 1
            ? [
                {
                  baseline: baseline[0]!,
                  candidate: candidate[0]!,
                  orderDelta:
                    first === undefined || second === undefined ? undefined : first - second,
                },
              ]
            : [];
        });

        if (!pairs.length) continue;
        const deltas = pairs.map((pair) => pair.candidate - pair.baseline);
        const ratios = pairs.map((pair) => pair.candidate / pair.baseline);

        const orderDeltas = pairs.flatMap((pair) =>
          pair.orderDelta === undefined ? [] : [pair.orderDelta],
        );

        const balanced = orderDeltas.length === pairs.length;
        let seed = 0x12345678;

        const draws = Array.from({ length: 10000 }, () => {
          const indices = Array.from({ length: pairs.length }, () => {
            seed ^= seed << 13;
            seed ^= seed >>> 17;
            seed ^= seed << 5;

            return (seed >>> 0) % pairs.length;
          });

          return {
            delta: median(indices.map((index) => deltas[index]!)),
            ratio: median(indices.map((index) => ratios[index]!)),
            order: balanced ? median(indices.map((index) => orderDeltas[index]!)) : NaN,
          };
        });

        const deltaDraws = draws.map((draw) => draw.delta);
        const ratioDraws = draws.map((draw) => draw.ratio);
        const orderDraws = draws.map((draw) => draw.order);

        const orderInterval = balanced
          ? `${n(median(orderDeltas))} [${n(quantile(orderDraws, 0.025))}, ${n(quantile(orderDraws, 0.975))}]`
          : "n/a";

        lines.push(
          `| ${cell}/${metric === "driverMs" ? "completion" : "first text"} | ${n(median(pairs.map((pair) => pair.baseline)))} | ${n(median(pairs.map((pair) => pair.candidate)))} | ${n(median(deltas))} [${n(quantile(deltaDraws, 0.025))}, ${n(quantile(deltaDraws, 0.975))}] | ${median(ratios).toFixed(3)}× [${quantile(ratioDraws, 0.025).toFixed(3)}, ${quantile(ratioDraws, 0.975).toFixed(3)}] | ${orderInterval} |`,
        );
      }
    }

    const driftRows = [
      ...group(
        rows.filter((row) => row.target === "yielded" && row.build === "baseline"),
        (row) => `${row.history}/${row.ttftMs}/${row.state}`,
      ).entries(),
    ].flatMap(([cell, samples]) => {
      const drifts = [...group(samples, (row) => row.object).values()].flatMap((turns) => {
        const epochs = [...group(turns, (row) => String(row.epoch)).values()].map((epoch) =>
          median(objectMedians(epoch)),
        );

        return epochs.length > 1 ? [Math.max(...epochs) / Math.min(...epochs) - 1] : [];
      });

      return drifts.length
        ? [
            `| ${cell} | ${(median(drifts) * 100).toFixed(1)}% / ${(Math.max(...drifts) * 100).toFixed(1)}% |`,
          ]
        : [];
    });

    if (driftRows.length)
      lines.push("", "| Cell | Baseline repeat drift, median / max |", "|---|---:|", ...driftRows);
    lines.push("");
  }
  if (result.phases) {
    lines.push("| Phase | Wall seconds |", "|---|---:|");
    for (const [phase, rows] of group(result.phases, (row) => row.phase))
      lines.push(
        `| ${phase} | ${(rows.reduce((sum, row) => sum + row.ms, 0) / 1000).toFixed(1)} |`,
      );
    lines.push("");
  }
  if (result.cpu) {
    lines.push(
      "Observed invocation CPU (telemetry may be sampled or delayed):",
      "",
      "| Target / event / marker | CPU ms | Observed | Non-ok outcomes |",
      "|---|---:|---:|---:|",
    );
    for (const [kind, calls] of group(
      result.cpu,
      (row) =>
        `${row.target ?? "unattributed"}/${row.invocationType ?? "unknown"}/${row.sample?.match(/^h\d+$/) ? "seed-" : ""}${row.kind}`,
    ))
      lines.push(
        `| ${kind} | ${interval(calls.flatMap((row) => (row.cpuMs === null ? [] : [row.cpuMs])))} | ${calls.length} | ${calls.filter((row) => row.outcome !== "ok").length} |`,
      );
    lines.push(
      "",
      `Unmatched invocation markers: ${result.unmatchedCpuMarkers ?? "unknown"}. Invocations without sample attribution: ${result.cpu.filter((row) => row.sample === undefined).length}/${result.cpu.length}. Owning Objects observed: ${new Set(result.cpu.flatMap((row) => (row.object === undefined ? [] : [row.object + "/" + row.target]))).size}. CPU totals are not inferred from missing rows.`,
      "",
    );
  }
  lines.push(
    `Failures: ${result.failures.length}. Target cleanup: ${result.workerPool ? "Worker pool retained (--teardown to remove)" : result.kept ? "kept" : result.cleanup?.verified ? "verified" : "NOT VERIFIED"}.`,
  );

  return lines.join("\n") + "\n";
};

import { Schema } from "effect";
