import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

// Offline reduction only. Every duration originates in a deployed Worker receipt
// or Cloudflare invocation telemetry. Local execution time is never evidence.
const here = dirname(fileURLToPath(import.meta.url));
const phase = process.argv.slice(2).filter((arg) => arg !== "--")[0] ?? "candidates";
const ReductionError = Schema.TaggedError()("AdmissionReductionError", { message: Schema.String });
const lines = (file) => readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
const median = (xs) => {
  const sorted = xs.filter(Number.isFinite).toSorted((a, b) => a - b);
  if (!sorted.length) return null;
  return (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.ceil((sorted.length - 1) / 2)]) / 2;
};
const stats = (xs) => {
  const known = xs.filter(Number.isFinite);
  return { n: known.length, missing: xs.length - known.length, median: median(known),
    min: known.length ? Math.min(...known) : null, max: known.length ? Math.max(...known) : null,
    range: known.length ? Math.max(...known) - Math.min(...known) : null };
};
const group = (xs, key) => {
  const groups = new Map();
  for (const x of xs) {
    const name = key(x);
    if (!groups.has(name)) groups.set(name, []);
    groups.get(name).push(x);
  }
  return groups;
};

NodeRuntime.runMain(Effect.try({
  try: () => {
    const plan = JSON.parse(readFileSync(join(here, `${phase}-plan.json`), "utf8"));
    const schedule = plan.groups.flatMap((g) => g.schedule);
    const key = (row) => `${row.framework}/${row.object}/${row.sample}`;
    if (new Set(schedule.map(key)).size !== schedule.length) throw new Error("Duplicate planned row");
    const completedFile = join(here, `${phase}-completed.json`);
    const completed = existsSync(completedFile) ? JSON.parse(readFileSync(completedFile, "utf8")) : [];
    if (new Set(completed.map(key)).size !== completed.length) throw new Error("Duplicate accepted completion");
    const all = lines(join(here, "requests.jsonl"));
    const cpuFile = join(here, phase, "turns.jsonl");
    const cpu = existsSync(cpuFile) ? lines(cpuFile) : [];
    const selected = all.filter((r) => r.phase === phase && r.status === 200);
    if (new Set(selected.map(key)).size !== selected.length) throw new Error("Duplicate successful response");
    if (new Set(cpu.map(key)).size !== cpu.length) throw new Error("Duplicate joined turn");
    const rows = selected.map((r) => {
      const b = r.response;
      const item = schedule.find((i) => i.object === r.object && i.framework === r.framework && i.sample === r.sample);
      if (!item) throw new Error("Unplanned response");
      const joined = cpu.find((i) => i.object === r.object && i.framework === r.framework && i.sample === r.sample);
      const accepted = completed.find((i) => key(i) === key(r));
      const qualification = [];
      if (!accepted) qualification.push("controller did not accept a completion");
      if (accepted?.transcriptVerified !== true) qualification.push("controller did not verify transcript");
      if (item.state === "cold" && accepted?.coldVerified !== true) qualification.push("unverified cold reset");
      if (item.framework === "yielded" && accepted?.productionAlarmVerified !== true) qualification.push("unverified production alarm path");
      if (item.candidate !== undefined && b.candidate !== item.candidate) qualification.push("candidate differs from plan");
      if (joined?.eligible !== true) qualification.push("turn is not qualified by analyzer");
      if (joined?.cohortEligible !== true) qualification.push("cohort is not complete and qualified");
      const events = b.metrics.admissionEvents ?? [];
      const at = (name) => events.find((e) => e.event === name);
      const count = (name) => events.filter((e) => e.event === name).length;
      const bodyCounts = at("observer/body-return")?.detail?.counts;
      const countNames = { "transaction:start": "transactions", "transactionSync:start": "transactionSync", "sql": "statements", "get:start": "kvGetCalls", "put:start": "kvPutCalls", "getAlarm:start": "getAlarmCalls", "setAlarm:start": "setAlarmCalls" };
      const sync = at("diagnostic/sync:end") && at("diagnostic/sync:start")
        ? at("diagnostic/sync:end").atMs - at("diagnostic/sync:start").atMs : null;
      return {
        object: item.object, framework: item.framework, history: item.history, ttftMs: item.ttftMs,
        state: item.state, sample: item.sample, candidate: b.candidate ?? "baseline-a",
        admissionMs: b.admissionMs, turnMs: b.turnMs,
        submitCpuMs: joined?.submitCpuMs ?? null,
        submitInvocationWallMs: joined?.submitInvocationWallMs ?? null,
        clientMinusInvocationMs: Number.isFinite(joined?.submitInvocationWallMs)
          ? b.admissionMs - joined.submitInvocationWallMs : null,
        eligible: joined?.eligible ?? null, transcriptValid: joined?.transcriptValid ?? null,
        qualified: qualification.length === 0, qualification,
        counts: Object.fromEntries(Object.entries(countNames).map(([event, metric]) => [event, bodyCounts?.[metric] ?? count(event)])),
        countSource: bodyCounts ? "native endpoint body-return snapshot" : "admission trace events",
        traceClockAdvances: new Set(events.map((e) => e.atMs)).size - (events.length ? 1 : 0),
        syncIoMs: sync,
        bodyReturn: at("observer/body-return") ?? null,
        syncReturn: at("diagnostic/sync:end") ?? null,
        anchors: events.filter((e) => e.event.startsWith("diagnostic/")),
        markers: b.metrics.markers,
        constructorCounts: b.metrics.constructorSql ?? null,
      };
    });
    const qualified = rows.filter((r) => r.qualified);
    const objectGroups = [...group(qualified, (r) => `${r.framework}/${r.object}/${r.state}/${r.candidate}`)].map(([key, xs]) => ({
      key, framework: xs[0].framework, object: xs[0].object, history: xs[0].history,
      ttftMs: xs[0].ttftMs, state: xs[0].state, candidate: xs[0].candidate,
      samples: xs.map((x) => x.sample), admission: stats(xs.map((x) => x.admissionMs)),
      turn: stats(xs.map((x) => x.turnMs)), cpu: stats(xs.map((x) => x.submitCpuMs)),
      clientMinusInvocation: stats(xs.map((x) => x.clientMinusInvocationMs)),
      sync: stats(xs.map((x) => x.syncIoMs)),
      counts: Object.fromEntries(Object.keys(xs[0].counts).map((k) => [k, stats(xs.map((x) => x.counts[k]))])),
    }));
    const comparisons = [];
    for (const [key, xs] of group(qualified.filter((r) => r.framework === "yielded" && ["cold", "warm"].includes(r.state)), (r) => `${r.object}/${r.state}`)) {
      const baseline = xs.filter((x) => x.candidate.startsWith("baseline-"));
      const a = baseline.filter((x) => x.candidate === "baseline-a");
      const b = baseline.filter((x) => x.candidate === "baseline-b");
      if (!a.length || !b.length) continue;
      for (const candidate of [...new Set(xs.map((x) => x.candidate))].filter((c) => !c.startsWith("baseline-"))) {
        const selected = xs.filter((x) => x.candidate === candidate);
        const before = stats(baseline.map((x) => x.admissionMs));
        const after = stats(selected.map((x) => x.admissionMs));
        const gain = before.median - after.median;
        const repeatLabelSpread = Math.abs(median(a.map((x) => x.admissionMs)) - median(b.map((x) => x.admissionMs)));
        comparisons.push({ key, history: xs[0].history, ttftMs: xs[0].ttftMs, state: xs[0].state, candidate,
          before, after, savedMs: gain, baselineLabelMedianSpreadMs: repeatLabelSpread,
          exceedsFullBaselineRepeatRange: gain > before.range,
          turnSavedMs: median(baseline.map((x) => x.turnMs)) - median(selected.map((x) => x.turnMs)),
          baselineCpu: stats(baseline.map((x) => x.submitCpuMs)), candidateCpu: stats(selected.map((x) => x.submitCpuMs)),
        });
      }
    }
    const result = {
      phase, planned: schedule.length, received: rows.length, qualified: qualified.length,
      complete: qualified.length === schedule.length && rows.length === schedule.length,
      excluded: rows.filter((r) => !r.qualified).map((r) => ({ key: key(r), reasons: r.qualification })),
      qualification: "Unique planned response and accepted completion, verified transcript/cold/alarm path, and a complete eligible cohort from analyze.mjs. Missing invocation CPU does not disqualify driver latency.",
      criterion: "A positive latency claim must exceed the full pooled baseline admission repeat range within that Object/state. Label-median spread is also shown; neither is a significance test. Cold is a new Object incarnation, not guaranteed a new isolate.",
      cpuLimit: "Known invocation CPU is summarized with explicit n/missing; partial samples are not claimed complete. CPU is platform attribution, not semantic phase accounting.",
      objectGroups, comparisons,
    };
    writeFileSync(join(here, `${phase}-admission.json`), JSON.stringify(result, null, 2) + "\n");
    writeFileSync(join(here, `${phase}-admission-turns.jsonl`), rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
    console.log(JSON.stringify({ phase, planned: schedule.length, received: rows.length, complete: result.complete,
      comparisons: comparisons.map(({key, candidate, savedMs, before, baselineLabelMedianSpreadMs, exceedsFullBaselineRepeatRange}) => ({key, candidate, savedMs, repeatRangeMs: before.range, baselineLabelMedianSpreadMs, exceedsFullBaselineRepeatRange})) }));
  },
  catch: (cause) => new ReductionError({ message: String(cause) }),
}));
