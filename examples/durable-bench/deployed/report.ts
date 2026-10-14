import { Schema } from "effect";

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

const bootstrapInterval = (values: readonly number[]) => {
  let seed = 0x12345678;

  const draws = Array.from({ length: 10000 }, () =>
    median(
      values.map(() => {
        seed ^= seed << 13;
        seed ^= seed >>> 17;
        seed ^= seed << 5;

        return values[(seed >>> 0) % values.length]!;
      }),
    ),
  );

  return [quantile(draws, 0.025), quantile(draws, 0.975)] as const;
};

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

/** Keep raw attempts intact; a retired Object contributes neither build to any metric. */
const retainedSamples = (result: Result) => {
  const retired = new Set((result.retirements ?? []).map((row) => `${row.target}/${row.object}`));

  return result.samples.filter((row) => !retired.has(`${row.target}/${row.object}`));
};

/** Fixed before measurement: never select exclusions from observed latency. */
export const retirementFailures = (result: Result): string[] => {
  const { objects } = Schema.decodeUnknownSync(Schema.Struct({ objects: Schema.Int }))(
    result.options,
  );

  const retirements = result.retirements ?? [];
  const failures: string[] = [];
  const cells = group(retirements, (row) => `${row.target}/${row.history}/${row.ttftMs}`);

  for (const [cell, rows] of cells) {
    const retired = new Set(rows.map((row) => row.object)).size;

    if (retired / objects > 0.25)
      failures.push(
        `${cell}: retired ${retired}/${objects} Objects, leaving fewer than 75% of pairs.`,
      );
  }
  for (const [cell, rows] of [...cells, ...group(retirements, (row) => row.target)]) {
    const baseline = rows.filter((row) => row.build === "baseline").length;
    const candidate = rows.filter((row) => row.build === "candidate").length;

    if (
      baseline + candidate >= 3 &&
      Math.max(baseline, candidate) >= 2 * Math.min(baseline, candidate)
    )
      failures.push(
        `${cell}: build-attributed retirements are asymmetric (baseline ${baseline}, candidate ${candidate}).`,
      );
  }

  return failures;
};

const retirementTable = (result: Result): string[] => {
  if (!result.retirements?.length) return [];

  return [
    "Retired Object pairs: both builds excluded from latency and CPU; all original attempts remain in JSON. Inputs with unknown outcomes were not resubmitted.",
    "",
    "| Target / history / TTFT / build / epoch | Retired Objects |",
    "|---|---:|",
    ...[
      ...group(
        result.retirements,
        (row) => `${row.target}/${row.history}/${row.ttftMs}/${row.build}/${row.epoch}`,
      ),
    ].map(([cell, rows]) => `| ${cell} | ${rows.length} |`),
    "",
  ];
};

const polledRecoveryTable = (result: Result): string[] => {
  if (!result.polledRecoveries?.length) return [];

  return [
    "Driver outcomes recovered by request ID; polling is outside the measured intervals.",
    "",
    "| Target / history / TTFT / build / epoch | Recovered outcomes | Polls | Proved-not-received resubmissions |",
    "|---|---:|---:|---:|",
    ...[
      ...group(
        result.polledRecoveries,
        (row) => `${row.target}/${row.history}/${row.ttftMs}/${row.build}/${row.epoch}`,
      ),
    ].map(
      ([cell, rows]) =>
        `| ${cell} | ${rows.length} | ${rows.reduce((sum, row) => sum + row.polls, 0)} | ${rows.reduce((sum, row) => sum + row.resubmissions, 0)} |`,
    ),
    "",
  ];
};

type Metric =
  | "driverMs"
  | "firstTextMs"
  | "observerToCompletionMs"
  | "observerToFirstTextMs"
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
          retainedSamples(result).filter(
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

/** Require the controller-planned invocation roles even if their markers never arrive. */
export const sampleCpu = (result: Result) => {
  const { buildHistory } = Schema.decodeUnknownSync(
    Schema.Struct({ buildHistory: Schema.Boolean }),
  )(result.options);

  const cpu = group(result.cpu ?? [], (row) => `${row.target}/${row.object}/${row.sample}`);

  const rows = retainedSamples(result).filter(
    (row) => row.status === "ok" && (row.state === "cold" || row.state === "warm"),
  );

  return rows.map((sample) => {
    const invocations = cpu.get(`${sample.target}/${sample.object}/${sample.sample}`);

    const expectedKinds = [
      ...(sample.target === "yielded" ? ["submit", "await", "dispatch"] : ["run"]),
      ...(sample.settlementRecordRead ? ["result"] : []),
      ...(!buildHistory && sample.target !== "tardie" ? ["text"] : []),
    ];

    const dispatches = invocations?.flatMap((row) => row.dispatches ?? []) ?? [];
    const started = dispatches.filter((dispatch) => !dispatch.completed);
    const completed = dispatches.filter((dispatch) => dispatch.completed);
    const starts = new Map(started.map((dispatch) => [dispatch.id, dispatch.source]));
    const completions = new Map(completed.map((dispatch) => [dispatch.id, dispatch.source]));

    const complete =
      (sample.target !== "yielded" || sample.settlementRecordRead !== undefined) &&
      starts.size === started.length &&
      completions.size === completed.length &&
      starts.size === completions.size &&
      started.every((dispatch) => completions.get(dispatch.id) === dispatch.source) &&
      invocations !== undefined &&
      sample.cpuInvocationIds !== undefined &&
      sample.cpuInvocationIds.length > 0 &&
      expectedKinds.every((kind) =>
        invocations.some((row) => row.kind === kind || row.kinds?.includes(kind)),
      ) &&
      sample.cpuInvocationIds.every((id) =>
        invocations.some((row) => row.invocationId === id || row.invocationIds?.includes(id)),
      ) &&
      invocations.every(
        (row) =>
          row.cpuMs !== null &&
          Number.isFinite(row.cpuMs) &&
          row.cpuMs >= 0 &&
          (!(row.kinds ?? [row.kind]).includes("dispatch") || !!row.dispatches?.length) &&
          (row.outcome === "ok" ||
            (["canceled", "aborted", "responseStreamDisconnected"].includes(row.outcome) &&
              (row.kinds ?? [row.kind]).every((kind) => kind === "text" || kind === "dispatch") &&
              // Tail can label post-response RPC continuations as canceled alarms.
              // Require matched success markers above; a native alarm cancellation still fails.
              (row.dispatches ?? []).every((dispatch) => dispatch.source !== "alarm"))),
      );

    return {
      sample,
      cpuMs: complete ? invocations.reduce((sum, row) => sum + row.cpuMs!, 0) : undefined,
    };
  });
};

/** Missing CPU invalidates the whole Object pair, never contributes a zero. */
export const pairedCpu = (result: Result) => {
  return [
    ...group(
      sampleCpu(result),
      ({ sample }) => `${sample.target}/${sample.history}/${sample.ttftMs}/${sample.state}`,
    ),
  ].map(([cell, samples]) => {
    let missingSamples = 0;
    const objects = [...group(samples, ({ sample }) => sample.object).values()];

    const pairs = objects.flatMap((turns) => {
      const missing = turns.filter((row) => row.cpuMs === undefined).length;

      missingSamples += missing;
      if (missing > 0) return [];

      const baseline = turns.flatMap((row) =>
        row.sample.build === "baseline" ? [row.cpuMs!] : [],
      );

      const candidate = turns.flatMap((row) =>
        row.sample.build === "candidate" ? [row.cpuMs!] : [],
      );

      return baseline.length && candidate.length
        ? [{ baseline: median(baseline), candidate: median(candidate) }]
        : [];
    });

    const deltas = pairs.map((pair) => pair.candidate - pair.baseline);

    return {
      cell,
      objects: objects.length,
      pairedObjects: pairs.length,
      missingSamples,
      complete: result.complete && pairs.length === objects.length && missingSamples === 0,
      baseline: median(pairs.map((pair) => pair.baseline)),
      candidate: median(pairs.map((pair) => pair.candidate)),
      delta: median(deltas),
      ci: bootstrapInterval(deltas),
    };
  });
};

export const table = (result: Result): string => {
  if (result.histories.length) return historyTable(result);

  const rows = retainedSamples(result)
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
          `Invalidated sample attempts: ${result.samples.filter((row) => row.status === "invalidated").length}; original errors, restores and replacement IDs are retained in JSON.`,
          `Recovery prefix turns: ${result.samples.filter((row) => row.state === "recovery").length}; setup only, excluded from timing and CPU comparisons.`,
        ]
      : []),
    ...polledRecoveryTable(result),
    ...retirementTable(result),
    "Driver-observed milliseconds; median [Q1–Q3] of Object medians.",
    "Yielded falls back to settled text if no preview arrives; JSON records firstTextSource.",
    "Submit → X starts after observer acknowledgement. Observer-open → X also includes attachment and any opening work; both cold boundaries are shown separately. First text is n/a for tardie.",
    "Object → model uses the Object clock and excludes driver transport.",
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
      `${cell} · submit → X (after observer acknowledgement)`,
      "",
      `| Target | Submit → ${firstTextLabel.toLowerCase()} ms | Submit → completion ms | Constructor / probe ms | Object → model ms | Completion median range | Completion repeat range, median / max | Objects |`,
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
    if (samples[0]?.state === "cold") {
      lines.push(
        `${cell} · observer-open → X`,
        "",
        "| Target | Observer-open → first text ms | Observer-open → completion ms | Objects |",
        "|---|---:|---:|---:|",
      );
      for (const target of TARGETS) {
        const turns = samples.filter((row) => row.target === target);

        if (!turns.length) continue;
        lines.push(
          `| ${target} | ${interval(objectMedians(turns, "observerToFirstTextMs"))} | ${interval(objectMedians(turns, "observerToCompletionMs"))} | ${objectMedians(turns, "observerToCompletionMs").length} |`,
        );
      }
      if (
        samples.some((row) => row.target === "yielded") &&
        samples.some((row) => row.target === "pi")
      )
        lines.push(
          `| Yielded ÷ pi | ${ratio(samples, "observerToFirstTextMs")} | ${ratio(samples, "observerToCompletionMs")} | |`,
        );
      lines.push("");
    }
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
      const metrics: readonly Metric[] =
        samples[0]?.state === "cold"
          ? ["driverMs", "firstTextMs", "observerToCompletionMs", "observerToFirstTextMs"]
          : ["driverMs", "firstTextMs"];

      for (const metric of metrics) {
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
        const deltaCi = bootstrapInterval(deltas);
        const ratioCi = bootstrapInterval(ratios);
        const orderCi = balanced ? bootstrapInterval(orderDeltas) : [NaN, NaN];

        const orderInterval = balanced
          ? `${n(median(orderDeltas))} [${n(orderCi[0]!)}, ${n(orderCi[1]!)}]`
          : "n/a";

        lines.push(
          `| ${cell}/${metric === "driverMs" ? "submit → completion" : metric === "firstTextMs" ? "submit → first text" : metric === "observerToCompletionMs" ? "observer-open → completion" : "observer-open → first text"} | ${n(median(pairs.map((pair) => pair.baseline)))} | ${n(median(pairs.map((pair) => pair.candidate)))} | ${n(median(deltas))} [${n(deltaCi[0])}, ${n(deltaCi[1])}] | ${median(ratios).toFixed(3)}× [${ratioCi[0].toFixed(3)}, ${ratioCi[1].toFixed(3)}] | ${orderInterval} |`,
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
      "",
      "Paired sample Object CPU: sum marked native invocation CPU, then whole-Object medians and 10,000 bootstrap draws. Planned submit/await/alarm or run roles, every text observer and any fallback record read are required independently of acknowledged markers. Missing attribution is not zero; incomplete pairs cannot establish a win.",
      "",
      "| Cell | Baseline CPU ms | Candidate CPU ms | Delta ms [95% CI] | Complete Objects |",
      "|---|---:|---:|---:|---:|",
    );
    for (const row of pairedCpu(result)) {
      lines.push(
        `| ${row.cell} | ${n(row.baseline)} | ${n(row.candidate)} | ${n(row.delta)} [${n(row.ci[0]!)}–${n(row.ci[1]!)}] | ${row.pairedObjects}/${row.objects}${row.complete ? "" : " (incomplete)"} |`,
      );
    }
    lines.push(
      "Observed invocation CPU (Tail delivery may be delayed or incomplete):",
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
        `| ${kind} | ${interval(calls.flatMap((row) => (row.cpuMs === null ? [] : [row.cpuMs])))} | ${calls.length} | ${calls.filter((row) => !(row.outcome === "ok" || (row.kind === "text" && ["canceled", "aborted"].includes(row.outcome)))).length} |`,
      );
    lines.push(
      "",
      `CPU totals include each marked native Object invocation once; missing records invalidate complete pairs.`,
      "",
    );
  }
  lines.push(
    `Failures: ${result.failures.length}. Target cleanup: ${result.workerPool ? "Worker pool retained (--teardown to remove)" : result.kept ? "kept" : result.cleanup?.verified ? "verified" : "NOT VERIFIED"}.`,
  );

  return lines.join("\n") + "\n";
};
