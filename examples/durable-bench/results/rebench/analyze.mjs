import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";

import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

import { history, next, payload, turn } from "../../src/plan.ts";

// Fixed, offline experiment reducer. Local reduction time is never latency evidence.
// Run: vp exec node examples/durable-bench/results/rebench/analyze.mjs
// Inputs (plain or .gz): requests/attempted.jsonl, <phase>-plan/completed.json,
// resources.json, optional resources-main.json, telemetry-primary/provider.json.
// Keep the existing nine-call transcript, driver endpoints and clock probe shapes.
// Tardie: response.metrics is Thread data; directoryStart/directory are the same
// post-timer Actor identity/entry receipt. nativeEntryObserved distinguishes native
// entry from the unused-Actor diagnostic fallback. Preserve both cold aborts.
const here = dirname(fileURLToPath(import.meta.url));
const expectedBaseline = "aa7855a6";
const targets = { yielded: "production", pi: "inline", tardie: "inline" };
const samples = ["cold", "settling", "warm", "warm", "warm"];
const objectsPerTarget = 3;
const finite = Number.isFinite;
const difference = (a, b) => (finite(a) && finite(b) ? a - b : null);
const sumKnown = (xs) => (xs.every(finite) ? xs.reduce((a, b) => a + b, 0) : null);

const quantile = (xs, p) => {
  const sorted = xs.filter(finite).toSorted((a, b) => a - b);

  if (!sorted.length) return null;
  const i = (sorted.length - 1) * p;

  return sorted[Math.floor(i)] + (sorted[Math.ceil(i)] - sorted[Math.floor(i)]) * (i % 1);
};

const median = (xs) => quantile(xs, 0.5);

const stats = (xs) => ({
  n: xs.filter(finite).length,
  missing: xs.filter((x) => !finite(x)).length,
  median: median(xs),
  q1: quantile(xs, 0.25),
  q3: quantile(xs, 0.75),
  min: quantile(xs, 0),
  max: quantile(xs, 1),
  range: difference(quantile(xs, 1), quantile(xs, 0)),
});

const completeMedian = (xs) => (xs.length && xs.every(finite) ? median(xs) : null);

const group = (xs, key) => {
  const result = new Map();

  for (const x of xs) {
    const k = key(x);

    if (!result.has(k)) result.set(k, []);
    result.get(k).push(x);
  }

  return result;
};

const distinct = (xs) => [...new Set(xs)];
const worker = (e) => e.$workers?.scriptName;
const eventKey = (e) => (worker(e) && e.$metadata?.id ? `${worker(e)}/${e.$metadata.id}` : null);

const requestKey = (e) =>
  worker(e) && e.$metadata?.requestId ? `${worker(e)}/${e.$metadata.requestId}` : null;

const isInvocation = (e) => e.$metadata?.type === "cf-worker-event";
const itemKey = (r) => `${r.framework}/${r.object}/${r.sample}`;

const queryOf = (e) => {
  const url = e.$workers?.event?.request?.url;

  if (!url) return null;
  const parsed = new URL(url);

  return { path: parsed.pathname, ...Object.fromEntries(parsed.searchParams) };
};

const spanNames = {
  submissionClaimIoMs: "SqlSubmissionLedger.claim",
  runStorageClaimIoMs: "RunStorage.claim",
  recoveryIoMs: "DurableAgentRuntime.runRecovery",
  processThreadHeadIoMs: "DurableAgentRuntime.processThreadHead",
  publishSettlementIoMs: "SqlSubmissionLedger.publishSettlement",
  finalizeSettlementIoMs: "SqlSubmissionLedger.finalizeSettlement",
};

const modelInvocationKinds = (invocations) =>
  Object.fromEntries(
    [...group(invocations, (invocation) => invocation?.kind ?? "missing")].map(
      ([kind, entries]) => [kind, entries.length],
    ),
  );

const metricNames = [
  "driverTotalMs",
  "controllerWallMs",
  "admissionMs",
  "firstProviderFromSubmitLowerMs",
  "firstProviderFromSubmitUpperMs",
  "firstProviderFromReceiptLowerMs",
  "firstProviderFromReceiptUpperMs",
  "lastProviderToClientLowerMs",
  "lastProviderToClientUpperMs",
  "providerGapMedianMs",
  "providerGapTotalMs",
  ...Array.from({ length: 8 }, (_, i) => `providerGap${i + 1}Ms`),
  "providerServiceMs",
  "driverMinusScriptedMs",
  ...Object.keys(spanNames),
  "recoveryCount",
  "alarmCount",
  "submitCpuMs",
  "awaitCpuMs",
  "alarmCpuMs",
  "alarmPassCpuMedianMs",
  "alarmPassCpuMaxMs",
  "inlineCpuMs",
  "actorCpuMs",
  "actorNativeCpuMs",
  "actorAlarmCpuMs",
  "actorInvocationWallMs",
  "joinedInvocationCpuMs",
  "submitInvocationWallMs",
  "awaitInvocationWallMs",
  "alarmInvocationWallMs",
  "inlineInvocationWallMs",
  "waiterReadCount",
  "waiterReadIntervalMedianMs",
  "waiterReadIntervalMaxMs",
  "eventAcquisitionsDelta",
  "newRuntimeInitializationsBetweenPasses",
  "logicalMutationStatements",
  "writeBindingBytes",
  "transactions",
  "transactionSync",
  "setAlarmCalls",
  "syncCalls",
  "syncWaitIoMs",
  "requestBytes",
  "responseBytes",
];

const limitations = [
  "Primary driverTotalMs is response.turnMs from the deployed driver; controller clientWallMs includes routing, clock probes and diagnostics and is not the primary latency.",
  "Object is the unit: median within each Object, then median/Q1/Q3/min/max/range across Objects (type-7 quantiles: linear interpolation at (n - 1) × p). Missing any repeated metric leaves that Object's metric null. Complete planned cohorts alone enter groups; partial turns remain in turns.jsonl.",
  "Each target has three independent Objects per history/TTFT condition in the same primary Worker. m0 follows an acknowledged cold abort, m1 settles, and m2/m3/m4 are three warm repeats. Cold proves a new Object incarnation, not a fresh isolate. Pi and Tardie use their native paths (variant inline in raw data); Yielded uses production only. Tardie cold proof requires Thread and Actor abort/entry evidence. No Actor.begin RPC precedes the timed turn: when native lookup/allocate uses the Actor, firstNativeEntry is captured before the unchanged implementation and read afterward. If nativeEntryObserved is false, the Actor was unused by the native turn; its post-timer diagnostic entry proves fresh/reset/no-prior-alarm state, but Actor cold hydration was neither required by that path nor charged to the turn. The directory receipt retains nativeEntryObserved and its observation string.",
  "Date.now and Effect span timestamps are frozen I/O clocks, not CPU clocks. Zero span duration means zero observable I/O-clock advance, not zero ownership or recovery cost. Nested/overlapping spans are not additive component costs.",
  "DO-to-driver timestamp differences are invalid for wall decomposition: the pilot demonstrates a lagging, frozen DO clock. diagnosticClock retains these raw differences, including negatives; they do not prove overlap, alarm dispatch, ownership cost, or notification delay and are excluded from comparison summaries.",
  "Provider/driver brackets intersect before/after echo offset intervals [provider arrival minus driver after, provider arrival minus driver before]. They are conditional on a stable offset across the same-colo provider invocations, not a guarantee of synchronized clocks. Missing, different-colo or inconsistent probes yield null bounds. Raw values are retained and never clipped.",
  "Receipt-to-first-provider is a combined alarm/setup/transport residual, not exact dispatch or processing latency. Last-provider-end to client includes finalization, storage gates and RPC return; it cannot isolate storage notification. Provider-to-provider gaps remain provider I/O-clock observations across invocations.",
  "settlementWrites records a logical SQL statement at dispatch, before SQL completion; it does not confirm replication. Publication span end and settlement.settledAt are separate observations, neither a replication acknowledgement.",
  "CPU comes only from cf-worker-event telemetry. Each generated RPC/alarm ID's START log must join to one Worker-scoped requestId and one matching invocation. Cloudflare can attach an alarm END to an await RPC; end-context disagreements are inventoried without overriding the start join. Missing, ambiguous, non-ok, or incomplete CPU is null. Sampling can omit whole invocations.",
  "CPU partitions are Cloudflare invocation attribution, not semantic phase costs: await CPU can include concurrent alarm work. Generic telemetry timestamp is not treated as invocation start. Only an explicit eventTimestamp can supply a platform start; otherwise start and start-plus-wallTime remain unresolved.",
  "Native model calls can carry inline/alarm or missing AsyncLocalStorage contexts. modelInvocationKinds counts these observations; neither ALS context nor native invocation CPU establishes logical Effect fiber ownership or Run ownership transfer. Yielded production requires every model call to carry an alarm context.",
  "Cloudflare invocation wall time is not caller wall time. Whole-invocation CPU and wall values may extend beyond driver completion; boundary alarms are identified and prevent a turn-attributed CPU total. No sum is a critical-path decomposition.",
  "Metrics are read after driver completion. Observed alarms, SQL counters, spans, waiterReads and event acquisitions can include follow-up work. A missing alarm end in the receipt is not proof of failure.",
  "Waiter-read intervals are optional SQL-read observations on the Object I/O clock. Empty waiterReads means no reads matched the observer, not proof of no waiting or polling. Repeated intervals near 500 ms can describe polling cadence, but are not proof of which notification or fallback caused a wake.",
  "Runtime is cached per Object incarnation: a stable incarnation implies zero new runtime initializations between passes. eventAcquisitions counts event-layer acquisition, not runtime construction; runRecovery spans separately describe observed recovery work.",
  "SSE provider receipts are preferred; a unique matching provider log is an explicit fallback. Fallbacks, missing logs, conflicting logs, telemetry poll metadata, and all non-ok outcomes remain inventoried.",
  "The eight headline cells are history 50/250 × TTFT 0/400 × cold/warm. Yielded/pi is the ratio of the two across-Object driver medians, not a paired same-Object effect: matching base names belong to separate namespaces. Ratios require all three Objects for both targets. Settling remains diagnostic only.",
  "Across-Object spread uses Object medians; repeatSpread measures first-to-last drift and range within each Object. Stable repeats on a slow Object remain in the across-Object distribution. Retired Object groups remain excluded evidence, including failures. No significance or causal improvement claim follows from this small run.",
  "Tardie driver timing surrounds the Thread fetch. Thread metrics and the directory Actor identity/entry receipt are read afterward; directoryStart copies that same post-timer receipt, not a pre-timer RPC. Provider calls remain in metrics.calls. Actor native lookup/allocate CPU requires an exact trace from a uniquely joined Thread fetch or alarm invocation for the turn and matching Actor identity/version. An Actor invocation claimed by multiple turns has null attributed CPU/wall in every claimant; raw observations remain retained. Actor alarms and counters are not scoped to turns: actorAlarmCpuMs, actorCpuMs and actorInvocationWallMs remain null, never inferred zero. Sampling can omit native RPCs, so their observed CPU is not a complete Actor or Tardie total. Thread alarm instrumentation remains separate and intact; all captured Actor events remain in the all-phase inventory.",
];

// Independently recompute fingerprints rather than trusting completed.json or plan hashes.
const reference = (size, count) => {
  const messages = [];

  const append = (input) => {
    messages.push({ role: "user", text: input.text });
    const hashes = [];

    for (;;) {
      hashes.push(
        createHash("sha256")
          .update(JSON.stringify(messages.map((m) => [m.role, m.text, m.calls ?? []])))
          .digest("hex")
          .slice(0, 16),
      );
      const step = next(messages);

      if ("answer" in step) {
        messages.push({ role: "assistant", text: step.answer });

        return hashes;
      }
      messages.push(
        { role: "assistant", text: "", calls: [step.call] },
        { role: "tool", text: payload(step.call) },
      );
    }
  };

  let seedFingerprint;

  for (const input of history(0, size)) seedFingerprint = append(input).at(-1);

  return {
    seedFingerprint,
    turns: Object.fromEntries(
      Array.from({ length: count }, (_, i) => [`m${i}`, append(turn(`m${i}`, 8))]),
    ),
  };
};

const AnalysisError = Schema.TaggedError()("RebenchAnalysisError", { message: Schema.String });

export const analyze = ({ inputDir = here, outputDir = here, phase = "main" } = {}) =>
  Effect.try({
    try: () => {
      const inputs = [];

      const read = (name) => {
        const plain = join(inputDir, name);
        const file = existsSync(plain) ? plain : plain + ".gz";

        if (!existsSync(file)) {
          inputs.push({ name, missing: true });

          return null;
        }
        const bytes = readFileSync(file);
        const contents = (file.endsWith(".gz") ? gunzipSync(bytes) : bytes).toString("utf8");

        inputs.push({
          name,
          file,
          gzip: file.endsWith(".gz"),
          bytes: bytes.length,
          sha256: createHash("sha256").update(bytes).digest("hex"),
        });

        return contents;
      };

      const load = (name, fallback) => {
        const text = read(name);

        return text === null ? fallback : JSON.parse(text);
      };

      const lines = (name) =>
        (read(name) ?? "")
          .split("\n")
          .filter((x) => x.trim())
          .map(JSON.parse);

      const rows = lines("requests.jsonl");
      const attempts = lines("attempted.jsonl");
      const plan = load(`${phase}-plan.json`, null);
      const completed = load(`${phase}-completed.json`, []);
      const resources = load("resources.json", null);
      const mainSnapshot = phase === "main" ? load("resources-main.json", null) : null;
      const mainResources = phase === "main" ? (mainSnapshot ?? resources) : null;
      const mainTarget = mainResources?.targets.find((target) => target.role === "primary");

      const mainVersions = distinct(
        [mainResources, resources].flatMap((resource) =>
          (resource?.targets ?? [])
            .filter((target) => target.role === "primary")
            .flatMap((target) =>
              [
                ...(target.deployments ?? []),
                { version: target.expectedVersion, phase: target.phase, build: target.build },
              ]
                .filter(
                  (deployment) =>
                    deployment.version &&
                    mainTarget?.build?.bundleSha256 &&
                    deployment.build?.bundleSha256 === mainTarget.build.bundleSha256 &&
                    deployment.build.repositoryCommit === mainResources?.baselineRevision &&
                    mainResources?.baselineRevision === plan?.baselineRevision,
                )
                .map((deployment) => `${target.name}/${deployment.version}`),
            ),
        ),
      );

      const retiredGroups = plan?.retiredGroups ?? [];
      const retiredByObject = new Map(retiredGroups.map((retired) => [retired.object, retired]));
      const initialPlan = retiredGroups.length ? load(`${phase}-plan-initial.json`, null) : null;

      const captures = ["primary", "provider"].map((role) => ({
        role,
        ...load(`telemetry-${role}.json`, { events: [], polls: [] }),
      }));

      const joinFailures = [];
      const integrityFailures = [];
      const eventsById = new Map();
      const unidentified = [];
      const conflictingEventIds = new Set();

      for (const event of captures.flatMap((c) => c.events)) {
        const id = eventKey(event);

        if (!id) {
          unidentified.push(event);
          joinFailures.push({ kind: "telemetry-event-identity", event });
          continue;
        }
        if (eventsById.has(id) && JSON.stringify(eventsById.get(id)) !== JSON.stringify(event)) {
          conflictingEventIds.add(id);
          joinFailures.push({
            kind: "conflicting-telemetry-event",
            id,
            versions: [eventsById.get(id), event],
          });
        }
        eventsById.set(id, event);
      }
      const events = [...eventsById.values(), ...unidentified];
      const invocations = events.filter(isInvocation);
      const logs = events.filter((e) => !isInvocation(e));
      const byRequest = group(invocations.filter(requestKey), requestKey);
      const logsByRequest = group(logs.filter(requestKey), requestKey);

      const tagged = logs.filter((e) => ["target-rpc", "target-alarm"].includes(e.source?.rebench));

      const logicalKey = (s) =>
        s.rebench === "target-alarm" ? `alarm/${s.alarmId}` : `${s.kind}/${s.id}`;

      const tagsById = group(tagged, (e) => `${worker(e)}/${logicalKey(e.source)}`);

      const idsByRequest = group(
        tagged.filter((e) => e.source.edge === "start" && requestKey(e)),
        requestKey,
      );

      const providerLogs = group(
        logs.filter((e) => e.source?.rebench === "provider" && e.source.requestId),
        (e) => e.source.requestId,
      );

      const fetches = group(
        invocations.filter((e) => queryOf(e)?.path === "/run"),
        (e) => {
          const q = queryOf(e);

          return `${worker(e)}/${q.target}/${q.object}/${q.sample}`;
        },
      );

      const schedule = plan?.groups.flatMap((g) => g.schedule) ?? [];

      const retiredSchedule = retiredGroups.flatMap(
        (retired) =>
          retired.originalGroup?.schedule ??
          initialPlan?.groups.find((g) => g.object === retired.object)?.schedule ??
          [],
      );

      const allSchedule = [...schedule, ...retiredSchedule];
      const planned = group(allSchedule, itemKey);
      const measuredRows = rows.filter((r) => r.phase === phase);
      const rowsByItem = group(measuredRows, itemKey);
      const completedByItem = group(completed, itemKey);

      const references = Object.fromEntries(
        distinct(schedule.map((r) => r.history)).map((h) => [h, reference(h, plan.measuredTurns)]),
      );

      for (const [h, expected] of Object.entries(references)) {
        if (
          expected.seedFingerprint !== { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" }[h] ||
          JSON.stringify(expected) !== JSON.stringify(plan.references?.[h])
        )
          integrityFailures.push({
            kind: "plan-reference",
            history: Number(h),
            expected,
            recorded: plan.references?.[h],
          });
      }
      const planReferenceValid = integrityFailures.length === 0;
      const planFailures = [];

      for (const [key, items] of planned)
        if (items.length !== 1) planFailures.push({ key, reason: "duplicate planned turn" });
      if (!plan) planFailures.push({ reason: "plan missing", phase });
      for (const cohort of plan?.groups ?? []) {
        const actual = measuredRows
          .filter((r) => r.object === cohort.object)
          .toSorted((a, b) => a.startedAt - b.startedAt)
          .map(itemKey);

        const expected = cohort.schedule.map(itemKey);

        if (actual.some((key, index) => key !== expected[index]))
          planFailures.push({
            object: cohort.object,
            reason: "observed turn order differs from planned randomized interleaving",
            actual,
            expected,
          });
      }
      if (plan && phase === "main") {
        if (
          !plan.baselineRevision?.startsWith(expectedBaseline) ||
          resources?.baselineRevision !== plan.baselineRevision ||
          mainResources?.baselineRevision !== plan.baselineRevision
        )
          planFailures.push({
            reason: "expected matching pinned baseline aa7855a6 in plan and deployment manifests",
            recorded: plan.baselineRevision,
            resourcesRevision: resources?.baselineRevision ?? null,
            mainRevision: mainResources?.baselineRevision ?? null,
          });
        if (plan.objectsPerRole !== objectsPerTarget || plan.measuredTurns !== samples.length)
          planFailures.push({
            reason: "expected three Objects per target and five samples per Object",
          });
        if (plan.groups.length !== 12 || schedule.length !== 180)
          planFailures.push({ reason: "expected 12 groups and 180 planned turns" });
        for (const cohort of plan.groups) {
          if (
            cohort.schedule.length !== 15 ||
            cohort.schedule.some((s) => s.object !== cohort.object)
          )
            planFailures.push({
              object: cohort.object,
              reason: "expected three targets sharing a group base name, five turns each",
            });
        }
        for (const h of [50, 250])
          for (const d of [0, 400]) {
            for (const framework of Object.keys(targets)) {
              const objects = distinct(
                schedule
                  .filter((s) => s.framework === framework && s.history === h && s.ttftMs === d)
                  .map((s) => s.object),
              );

              if (objects.length !== objectsPerTarget)
                planFailures.push({
                  history: h,
                  ttftMs: d,
                  framework,
                  reason: "expected three independent Objects",
                  objects,
                });
            }
          }
        for (const [object, items] of group(schedule, (r) => `${r.framework}/${r.object}`)) {
          if (
            items.length !== samples.length ||
            items.some(
              (r) =>
                r.state !== samples[Number(r.sample.slice(1))] ||
                !Object.hasOwn(targets, r.framework) ||
                r.variant !== targets[r.framework] ||
                r.history !== items[0].history ||
                r.ttftMs !== items[0].ttftMs,
            ) ||
            samples
              .map((_, i) => `m${i}`)
              .some((s) => items.filter((r) => r.sample === s).length !== 1)
          )
            planFailures.push({
              object,
              reason:
                "expected production Yielded or native pi/Tardie: m0 cold, m1 settling, m2/m3/m4 warm",
            });
        }
      }

      const joinedEventIds = new Set();
      const records = [];

      for (const [rowIndex, row] of measuredRows.entries()) {
        const key = `${phase}/${row.worker}/${itemKey(row)}`;
        const q = Object.fromEntries(new URL(row.path, "https://rebench.invalid").searchParams);

        const item =
          (planned.get(itemKey(row)) ?? []).length === 1 ? planned.get(itemKey(row))[0] : null;

        const response = row.response ?? {};
        const m = response.metrics ?? {};
        const actor = row.framework === "tardie" ? (response.directory ?? null) : null;
        const actorStart = row.framework === "tardie" ? (response.directoryStart ?? null) : null;
        const calls = m.calls ?? [];
        const modelInvocations = calls.map((call) => call.invocation ?? null);
        const retired = retiredByObject.get(row.object) ?? null;
        const reasons = [];
        const joins = [];

        const issue = (kind, details = {}) => {
          const failure = { key, rowIndex, kind, ...details };

          joins.push(failure);
          joinFailures.push(failure);
        };

        if (!item) reasons.push("missing or ambiguous planned turn");
        if (phase === "main" && row.worker !== mainTarget?.name)
          reasons.push("target did not run in the primary Worker");
        if (retired)
          reasons.push(
            `retired Object group: ${retired.reason}; replacement ${retired.replacement}`,
          );
        if ((rowsByItem.get(itemKey(row)) ?? []).length !== 1)
          reasons.push("duplicate controller requests for planned turn");
        if (
          row.status !== 200 ||
          response.ok !== true ||
          m.ok !== true ||
          response.outcome !== "completed"
        )
          reasons.push("request did not complete successfully");
        if (
          !finite(response.turnMs) ||
          !finite(response.submitStartedMs) ||
          !finite(response.settlementObservedMs)
        )
          reasons.push("driver timestamps missing");
        if (
          finite(response.turnMs) &&
          response.turnMs !== difference(response.settlementObservedMs, response.submitStartedMs)
        )
          reasons.push("driver total disagrees with endpoints");
        if (
          !m.objectId ||
          !m.incarnation ||
          !m.version ||
          m.version !== response.driverVersion ||
          m.generation !== "measure"
        )
          reasons.push("Object/driver identity missing or inconsistent");
        if (phase === "main" && !mainVersions.includes(`${row.worker}/${m.version}`)) {
          reasons.push("version lacks recorded pinned-main bundle/revision proof");
          integrityFailures.push({
            kind: "main-version",
            key,
            worker: row.worker,
            version: m.version ?? null,
          });
        }
        if (
          item &&
          [response, m].some(
            (x) =>
              x.target !== item.framework ||
              x.object !== item.object ||
              x.sample !== item.sample ||
              x.variant !== item.variant ||
              x.history !== item.history ||
              x.ttftMs !== item.ttftMs ||
              x.chunkDelayMs !== (item.ttftMs === 400 ? 10 : 0),
          )
        )
          reasons.push("response does not match planned workload");
        const variant = item?.variant ?? q.variant;
        const production = row.framework === "yielded" && variant === "production";

        if (
          production &&
          (!finite(response.receiptReceivedMs) ||
            response.admissionMs !==
              difference(response.receiptReceivedMs, response.submitStartedMs))
        )
          reasons.push("admission timestamps missing or inconsistent");
        const expected = references[item?.history];

        if (!expected || m.seedFingerprint !== expected.seedFingerprint)
          reasons.push("seed fingerprint mismatch or reference absent");
        if (!planReferenceValid) reasons.push("plan reference verification failed");

        const providers = calls.map((call, index) => {
          const candidates = providerLogs.get(call.providerRequest) ?? [];
          const log = candidates.length === 1 ? candidates[0].source : null;
          const receipt = call.providerReceipt ?? log;

          if (candidates.length !== 1)
            issue("provider-log", {
              call: index,
              providerRequest: call.providerRequest,
              candidates: candidates.map(eventKey),
            });

          const keys = [
            "arrivalMs",
            "firstByteMs",
            "endMs",
            "fingerprint",
            "rawWireFingerprint",
            "requestBytes",
            "colo",
          ];

          if (log && call.providerReceipt && keys.some((k) => log[k] !== call.providerReceipt[k]))
            reasons.push(`provider stream/log disagreement at ${index}`);

          const valid =
            receipt &&
            receipt.requestId === call.providerRequest &&
            Number(receipt.call) === index &&
            call.call === index &&
            receipt.target === row.framework &&
            receipt.object === row.object &&
            receipt.sample === row.sample &&
            receipt.variant === variant &&
            receipt.history === item?.history &&
            receipt.ttftMs === item?.ttftMs &&
            receipt.chunkDelayMs === (item?.ttftMs === 400 ? 10 : 0) &&
            receipt.syncBeforeFetch === false &&
            (receipt.error === null || receipt.error === undefined) &&
            receipt.fingerprint === expected?.turns[row.sample]?.[index] &&
            [receipt.arrivalMs, receipt.firstByteMs, receipt.endMs].every(finite) &&
            call.status === 200 &&
            !call.error &&
            call.sseDone === true;

          if (!valid)
            integrityFailures.push({
              kind: "provider-transcript",
              key,
              call: index,
              expected: expected?.turns[row.sample]?.[index],
              receipt: receipt ?? null,
            });
          if (!call.providerReceipt)
            issue(receipt ? "provider-log-fallback" : "provider-receipt-missing", {
              call: index,
              providerRequest: call.providerRequest,
            });

          return {
            receipt: receipt ?? null,
            source: call.providerReceipt ? "sse" : log ? "log-fallback" : "missing",
            valid: Boolean(valid),
            logMatches: candidates.map(eventKey),
          };
        });

        const transcriptValid =
          calls.length === 9 &&
          distinct(calls.map((c) => c.providerRequest).filter(Boolean)).length === 9 &&
          providers.every((p) => p.valid);

        if (!transcriptValid)
          reasons.push("nine successful reference-matching provider streams required");

        const productionAlarmVerified = production
          ? calls.length === 9 &&
            calls.every((c) => c.invocation?.kind === "alarm" && c.invocation.id)
          : null;

        if (production && !productionAlarmVerified)
          reasons.push("production provider call outside an identified alarm");

        const coldRows = rows.filter(
          (r) =>
            r.phase === `${phase}-cold` &&
            r.worker === row.worker &&
            r.framework === row.framework &&
            r.object === row.object &&
            r.sample === `cold-${row.sample}` &&
            r.endedAt <= row.startedAt,
        );

        const reset = coldRows.length === 1 ? coldRows[0].response : null;

        const construction = logs.filter(
          (e) =>
            e.source?.rebench === "target-constructed" &&
            worker(e) === row.worker &&
            e.source.objectId === m.objectId &&
            e.source.incarnation === m.incarnation,
        );

        const constructorContradiction = construction.some(
          (e) => e.$workers?.eventType === "alarm" || e.source.version !== m.version,
        );

        const threadColdVerified =
          item?.state === "cold"
            ? Boolean(
                reset?.ok &&
                reset.coldRequested &&
                reset.threadAbort?.expectedAbort &&
                reset.before?.objectId === m.objectId &&
                reset.before?.incarnation !== m.incarnation &&
                reset.before?.version === m.version &&
                m.entry?.firstHarnessRequest === true &&
                m.entry.priorAlarmStarts === 0 &&
                m.entry.activeAlarmIds?.length === 0 &&
                !constructorContradiction,
              )
            : null;

        const actorColdVerified =
          item?.state === "cold" && row.framework === "tardie"
            ? Boolean(
                reset?.directoryAbort?.expectedAbort &&
                reset.directoryBefore?.objectId === actorStart?.objectId &&
                reset.directoryBefore?.incarnation &&
                actorStart?.incarnation &&
                reset.directoryBefore.incarnation !== actorStart.incarnation &&
                reset.directoryBefore.version === m.version &&
                actorStart.version === m.version &&
                actorStart.entry?.firstHarnessRequest === true &&
                actorStart.entry.priorAlarmStarts === 0 &&
                actorStart.entry.activeAlarmIds?.length === 0 &&
                (!actor ||
                  (actor.objectId === actorStart.objectId &&
                    actor.incarnation === actorStart.incarnation &&
                    actor.version === actorStart.version)),
              )
            : null;

        const coldVerified =
          item?.state === "cold"
            ? threadColdVerified && (row.framework !== "tardie" || actorColdVerified)
            : null;

        if (item?.state === "cold" && !coldVerified)
          reasons.push("cold incarnation/entry proof missing or contradicted");

        const contextMatches = (s, identity = m) =>
          s.objectId === identity.objectId &&
          s.incarnation === identity.incarnation &&
          s.version === identity.version;

        const activeMatches = (s) =>
          contextMatches(s) &&
          s.active === true &&
          s.query?.object === row.object &&
          s.query?.target === row.framework &&
          s.query?.sample === row.sample;

        const returnedAlarmEvents = m.alarmEvents ?? [];

        const boundaryIds = distinct([
          ...(m.alarmsActiveAtStart ?? []),
          ...(m.alarmsActiveAtEnd ?? []),
        ]);

        const alarmIds = distinct(
          [
            ...returnedAlarmEvents.map((e) => e.alarmId),
            ...boundaryIds,
            ...calls.filter((c) => c.invocation?.kind === "alarm").map((c) => c.invocation.id),
            ...tagged
              .filter(
                (e) =>
                  worker(e) === row.worker &&
                  e.source.rebench === "target-alarm" &&
                  activeMatches(e.source),
              )
              .map((e) => e.source.alarmId),
          ].filter(Boolean),
        );

        const invocationRows = [];

        const joinTagged = (kind, id, identity = m) => {
          const allEdges = tagsById.get(`${row.worker}/${kind}/${id}`) ?? [];
          const edges = allEdges.filter((e) => e.source.edge === "start");
          const requests = distinct(edges.map(requestKey).filter(Boolean));

          const endDisagreements = allEdges.filter(
            (e) => e.source.edge === "end" && !requests.includes(requestKey(e)),
          );

          if (endDisagreements.length)
            issue("end-log-context-disagreement", {
              invocationKind: kind,
              id,
              startRequests: requests,
              ends: endDisagreements.map((e) => ({
                eventId: eventKey(e),
                request: requestKey(e),
                traceId: e.$metadata?.traceId,
              })),
            });
          const traces = distinct(edges.map((e) => e.$metadata?.traceId).filter(Boolean));
          const candidates = requests.flatMap((request) => byRequest.get(request) ?? []);

          const matching = candidates.filter(
            (e) =>
              e.$workers?.durableObjectId === identity.objectId &&
              e.$workers?.scriptVersion?.id === identity.version &&
              (kind === "alarm"
                ? e.$workers.eventType === "alarm"
                : ["rpc", "jsrpc"].includes(e.$workers.eventType) &&
                  (e.$workers.event?.rpcMethod ===
                    (kind === "submit" ? "submitEncoded" : "awaitSettlementEncoded") ||
                    (e.$workers.event?.rpcCallCount === 1 &&
                      e.$workers.event.rpcMethods?.length === 1 &&
                      e.$workers.event.rpcMethods[0] ===
                        (kind === "submit" ? "submitEncoded" : "awaitSettlementEncoded")))),
          );

          const alias = requests.some(
            (request) =>
              distinct((idsByRequest.get(request) ?? []).map((e) => logicalKey(e.source)))
                .length !== 1,
          );

          const badContext = edges.some(
            (e) =>
              (e.$workers?.durableObjectId && e.$workers.durableObjectId !== identity.objectId) ||
              (e.$workers?.scriptVersion?.id && e.$workers.scriptVersion.id !== identity.version) ||
              (kind === "alarm"
                ? !contextMatches(e.source, identity)
                : e.source.incarnation !== identity.incarnation ||
                  e.source.object !== row.object ||
                  e.source.sample !== row.sample),
          );

          const valid =
            edges.length > 0 &&
            edges.every(requestKey) &&
            requests.length === 1 &&
            traces.length <= 1 &&
            !alias &&
            !badContext &&
            candidates.length === 1 &&
            matching.length === 1 &&
            matching[0].$metadata?.traceId &&
            (!traces.length || matching[0].$metadata.traceId === traces[0]) &&
            ![...edges, ...candidates].some((e) => conflictingEventIds.has(eventKey(e)));

          if (!valid)
            issue(`${kind}-join`, {
              id,
              logIds: edges.map(eventKey),
              requests,
              traces,
              candidateIds: candidates.map(eventKey),
              matchingIds: matching.map(eventKey),
              alias,
              badContext,
            });

          return valid ? matching[0] : null;
        };

        const recordInvocation = (kind, id, event, boundary = false) => {
          if (event) joinedEventIds.add(eventKey(event));
          const ok = event?.$workers?.outcome === "ok";
          const platformStartMs = event?.$workers?.eventTimestamp ?? event?.eventTimestamp;

          if (event && (!ok || !finite(event.$workers.cpuTimeMs)))
            issue("invocation-cpu-unavailable", {
              invocationKind: kind,
              id,
              eventId: eventKey(event),
              outcome: event.$workers.outcome,
            });
          invocationRows.push({
            kind,
            id,
            joined: Boolean(event),
            boundary,
            eventId: event ? eventKey(event) : null,
            requestId: event?.$metadata?.requestId ?? null,
            traceId: event?.$metadata?.traceId ?? null,
            objectId: event?.$workers?.durableObjectId ?? null,
            eventType: event?.$workers?.eventType ?? null,
            rpcMethod: event?.$workers?.event?.rpcMethod ?? null,
            rpcMethods: event?.$workers?.event?.rpcMethods ?? null,
            rpcCallCount: event?.$workers?.event?.rpcCallCount ?? null,
            query: event ? queryOf(event) : null,
            outcome: event?.$workers?.outcome ?? null,
            cpuMs: ok && finite(event.$workers.cpuTimeMs) ? event.$workers.cpuTimeMs : null,
            wallMs: ok && finite(event.$workers.wallTimeMs) ? event.$workers.wallTimeMs : null,
            observedCpuMs: event?.$workers?.cpuTimeMs ?? null,
            observedWallMs: event?.$workers?.wallTimeMs ?? null,
            platformStartMs: finite(platformStartMs) ? platformStartMs : null,
            platformStartPlusWallMs:
              finite(platformStartMs) && finite(event?.$workers?.wallTimeMs)
                ? platformStartMs + event.$workers.wallTimeMs
                : null,
            telemetryTimestampDiagnostic: event?.timestamp ?? null,
          });
        };

        for (const kind of ["submit", "awaitSettlement"]) {
          const ids = distinct((m.rpcEvents ?? []).filter((e) => e.kind === kind).map((e) => e.id));

          if (production && !ids.length) {
            issue(`${kind}-receipt-missing`);
            recordInvocation(kind, null, null);
          }
          for (const id of ids) recordInvocation(kind, id, joinTagged(kind, id));
        }
        for (const id of alarmIds) {
          const edges = returnedAlarmEvents.filter((e) => e.alarmId === id);

          const boundary =
            boundaryIds.includes(id) ||
            edges.length !== 2 ||
            !["start", "end"].every((edge) => edges.some((e) => e.edge === edge)) ||
            edges.some((e) => !activeMatches(e));

          recordInvocation("alarm", id, joinTagged("alarm", id), boundary);
        }
        if (production && !alarmIds.length) {
          issue("alarm-receipt-missing");
          recordInvocation("alarm", null, null);
        }
        const matchingFetches = fetches.get(`${row.worker}/${itemKey(row)}`) ?? [];

        if (!production) {
          const candidates = matchingFetches.filter(
            (e) =>
              e.$workers?.durableObjectId === m.objectId &&
              e.$workers?.scriptVersion?.id === m.version,
          );

          const event =
            candidates.length === 1 &&
            requestKey(candidates[0]) &&
            (byRequest.get(requestKey(candidates[0])) ?? []).length === 1 &&
            !conflictingEventIds.has(eventKey(candidates[0]))
              ? candidates[0]
              : null;

          if (!event) issue("inline-fetch-join", { candidates: candidates.map(eventKey) });
          recordInvocation("inline", calls[0]?.invocation?.id ?? null, event);
        }

        const outerCandidates = matchingFetches.filter(
          (e) =>
            e.$workers?.executionModel === "stateless" &&
            e.$metadata?.rayId === row.cfRay?.split("-")[0],
        );

        const outer = outerCandidates.length === 1 ? outerCandidates[0] : null;

        if (!outer) issue("driver-fetch-join", { candidates: outerCandidates.map(eventKey) });

        if (row.framework === "tardie") {
          const threadTraces = distinct(
            invocationRows
              .filter(
                (r) =>
                  r.joined &&
                  r.eventId &&
                  r.traceId &&
                  ["inline", "alarm"].includes(r.kind),
              )
              .map((r) => r.traceId),
          );

          const actorIdentityValid =
            actor?.objectId &&
            actor.objectId !== m.objectId &&
            actor.version === m.version &&
            actor.incarnation &&
            actorStart &&
            contextMatches(actorStart, actor);

          const nativeActorCandidates =
            actorIdentityValid && threadTraces.length > 0
              ? invocations.filter((e) => {
                  const methods =
                    e.$workers?.event?.rpcMethods ??
                    (e.$workers?.event?.rpcMethod ? [e.$workers.event.rpcMethod] : []);

                  return (
                    worker(e) === row.worker &&
                    threadTraces.includes(e.$metadata?.traceId) &&
                    e.$workers?.durableObjectId === actor.objectId &&
                    e.$workers?.scriptVersion?.id === actor.version &&
                    ["rpc", "jsrpc"].includes(e.$workers?.eventType) &&
                    methods.length > 0 &&
                    methods.every((method) => ["lookup", "allocate"].includes(method))
                  );
                })
              : [];

          if (!nativeActorCandidates.length) {
            issue("actor-native-join-missing", {
              actorObjectId: actor?.objectId ?? null,
              threadTraces,
            });
            recordInvocation("actorNative", null, null);
          }
          for (const event of nativeActorCandidates) {
            const valid =
              eventKey(event) &&
              requestKey(event) &&
              (byRequest.get(requestKey(event)) ?? []).length === 1 &&
              !conflictingEventIds.has(eventKey(event));

            if (!valid) issue("actor-native-join-ambiguous", { candidate: eventKey(event) });
            recordInvocation("actorNative", eventKey(event), valid ? event : null);
          }
        }

        const p = providers.map((x) => x.receipt);

        const probes = [response.clockBefore, response.clockAfter].map((probe) => ({
          lowerMs: difference(probe?.provider?.arrivalMs, probe?.afterMs),
          upperMs: difference(probe?.provider?.arrivalMs, probe?.beforeMs),
          colo: probe?.provider?.colo ?? null,
        }));

        const offsetLowerMs = probes.every((probe) => finite(probe.lowerMs))
          ? Math.max(...probes.map((probe) => probe.lowerMs))
          : null;

        const offsetUpperMs = probes.every((probe) => finite(probe.upperMs))
          ? Math.min(...probes.map((probe) => probe.upperMs))
          : null;

        const boundedProviderClock =
          finite(offsetLowerMs) &&
          finite(offsetUpperMs) &&
          offsetLowerMs <= offsetUpperMs &&
          probes[0].colo &&
          probes.every((probe) => probe.colo === probes[0].colo) &&
          p.length === 9 &&
          p.every((receipt) => receipt?.colo === probes[0].colo);

        const providerClock = {
          status: boundedProviderClock
            ? "conditional-stable-offset"
            : !finite(offsetLowerMs) || !finite(offsetUpperMs) || p.length !== 9
              ? "missing-probes-or-receipts"
              : offsetLowerMs > offsetUpperMs
                ? "inconsistent-probe-intervals"
                : "provider-probe-colo-mismatch",
          probes,
          providerColos: distinct(p.map((receipt) => receipt?.colo ?? null)),
          offsetLowerMs,
          offsetUpperMs,
          assumption:
            "same-colo provider invocations share a stable offset from the driver between the two probes; not proven clock synchronization",
        };

        const bracket = (raw, direction) =>
          boundedProviderClock && finite(raw)
            ? direction === "from-provider"
              ? [raw + offsetLowerMs, raw + offsetUpperMs]
              : [raw - offsetUpperMs, raw - offsetLowerMs]
            : [null, null];

        const firstFromSubmit = bracket(
          difference(p[0]?.arrivalMs, response.submitStartedMs),
          "to-provider",
        );

        const firstFromReceipt = bracket(
          difference(p[0]?.arrivalMs, response.receiptReceivedMs),
          "to-provider",
        );

        const lastToClient = bracket(
          difference(response.settlementObservedMs, p.at(-1)?.endMs),
          "from-provider",
        );

        const gaps = Array.from({ length: 8 }, (_, i) =>
          difference(p[i + 1]?.arrivalMs, p[i]?.endMs),
        );

        const firstAlarmId = production ? calls[0]?.invocation?.id : null;

        const firstEntries = returnedAlarmEvents.filter(
          (e) => e.alarmId === firstAlarmId && e.edge === "start" && activeMatches(e),
        );

        const spans = m.spans ?? [];

        const headSpans = spans.filter(
          (s) =>
            s.name === "DurableAgentRuntime.processThreadHead" &&
            s.invocation?.kind === "alarm" &&
            s.invocation.id === firstAlarmId,
        );

        if (production && firstEntries.length !== 1)
          issue("first-provider-alarm-entry", { firstAlarmId, count: firstEntries.length });
        if (production && headSpans.length !== 1)
          issue("first-provider-processThreadHead", { firstAlarmId, count: headSpans.length });

        const spanMetrics = Object.fromEntries(
          Object.entries(spanNames).map(([metric, name]) => {
            const selected = spans.filter((s) => s.name === name);
            const durations = selected.map((s) => difference(s.endMs, s.startMs));

            return [
              metric,
              {
                count: selected.length,
                zeroIoClockSpans: durations.filter((x) => x === 0).length,
                incomplete: durations.filter((x) => !finite(x)).length,
                duration: stats(durations),
                totalIoMs:
                  row.framework === "yielded" && Array.isArray(m.spans) && selected.length > 0
                    ? sumKnown(durations)
                    : null,
              },
            ];
          }),
        );

        const writes = m.settlementWrites ?? [];
        const publication = spans.filter((s) => s.name === "SqlSubmissionLedger.publishSettlement");

        if (production && writes.length !== 1)
          issue("logical-settlement-write", { count: writes.length });
        if (production && publication.length !== 1)
          issue("publication-end", { count: publication.length });
        const waiterReads = m.waiterReads ?? null;

        const waiterIdentityValid = waiterReads?.every(
          (r) => r.invocation?.kind === "awaitSettlement" && r.invocation.id && finite(r.atMs),
        );

        if (waiterReads && !waiterIdentityValid) issue("waiter-read-identity");

        const waiterIntervals = waiterIdentityValid
          ? [...group(waiterReads, (r) => r.invocation.id).values()].flatMap((reads) =>
              reads.slice(1).map((r, i) => difference(r.atMs, reads[i].atMs)),
            )
          : null;

        const cpu = (kind, field) => {
          const selected = invocationRows.filter((r) => r.kind === kind);

          if (!selected.length) return null;

          return sumKnown(selected.map((r) => r[field]));
        };

        const alarmCpuValues = invocationRows.filter((r) => r.kind === "alarm").map((r) => r.cpuMs);

        const record = {
          key,
          rowIndex,
          phase,
          framework: row.framework,
          variant,
          object: row.object,
          objectId: m.objectId ?? null,
          sample: row.sample,
          state: item?.state ?? null,
          history: item?.history ?? m.history ?? null,
          ttftMs: item?.ttftMs ?? m.ttftMs ?? null,
          worker: row.worker,
          incarnation: m.incarnation ?? null,
          version: m.version ?? null,
          status: row.status,
          startedAt: row.startedAt,
          endedAt: row.endedAt,
          reasons,
          eligible: reasons.length === 0,
          transcriptValid,
          productionAlarmVerified,
          retired,
          modelInvocations,
          modelInvocationKinds: modelInvocationKinds(modelInvocations),
          mixedModelInvocationKinds:
            distinct(modelInvocations.map((invocation) => invocation?.kind).filter(Boolean))
              .length > 1,
          coldVerified,
          threadColdVerified,
          actorColdVerified,
          coldReset:
            coldRows.length === 1
              ? {
                  sample: coldRows[0].sample,
                  receipt: reset,
                  before: reset?.before,
                  at: coldRows[0].endedAt,
                }
              : null,
          constructorContradiction,
          constructorLogIds: construction.map(eventKey),
          completedRows: completedByItem.get(itemKey(row)) ?? [],
          driverTotalMs: response.turnMs ?? null,
          admissionMs: production ? (response.admissionMs ?? null) : null,
          submitStartedMs: response.submitStartedMs ?? null,
          receiptReceivedMs: response.receiptReceivedMs ?? null,
          settlementObservedMs: response.settlementObservedMs ?? null,
          settledAtMs: response.settledAtMs ?? null,
          firstProviderAlarmId: firstAlarmId ?? null,
          firstProviderAlarmInvocation:
            invocationRows.find((r) => r.kind === "alarm" && r.id === firstAlarmId) ?? null,
          firstProviderFromSubmitLowerMs: firstFromSubmit[0],
          firstProviderFromSubmitUpperMs: firstFromSubmit[1],
          firstProviderFromReceiptLowerMs: firstFromReceipt[0],
          firstProviderFromReceiptUpperMs: firstFromReceipt[1],
          lastProviderToClientLowerMs: lastToClient[0],
          lastProviderToClientUpperMs: lastToClient[1],
          providerClock,
          wallDecomposition: {
            alarmDispatch: "unresolved",
            processingEntry: "unresolved",
            storageNotification: "unresolved",
            receiptToFirstProvider:
              "combined alarm/setup/transport residual; not exact processing hop",
            lastProviderToClient:
              "combined finalization/storage-gates/RPC residual; not exact notification delay",
          },
          diagnosticClock: {
            validity:
              "DO cross-clock differences are invalid for wall decomposition; retained without clipping",
            alarmEntryMinusReceiptMs:
              firstEntries.length === 1
                ? difference(firstEntries[0].atMs, response.receiptReceivedMs)
                : null,
            processThreadHeadMinusReceiptMs:
              headSpans.length === 1
                ? difference(headSpans[0].startMs, response.receiptReceivedMs)
                : null,
            logicalWriteToClientMs:
              writes.length === 1
                ? difference(response.settlementObservedMs, writes[0].atMs)
                : null,
            publicationEndToClientMs:
              publication.length === 1
                ? difference(response.settlementObservedMs, publication[0].endMs)
                : null,
            settledAtToClientMs: difference(response.settlementObservedMs, response.settledAtMs),
            rawFirstMockRequestMinusSubmitMs: difference(p[0]?.arrivalMs, response.submitStartedMs),
            rawFirstMockRequestMinusReceiptMs: difference(
              p[0]?.arrivalMs,
              response.receiptReceivedMs,
            ),
            rawLastProviderEndToClientMs: difference(
              response.settlementObservedMs,
              p.at(-1)?.endMs,
            ),
          },
          providerGapMedianMs: completeMedian(gaps),
          providerGapTotalMs: sumKnown(gaps),
          providerGapsMs: gaps,
          ...Object.fromEntries(gaps.map((gap, i) => [`providerGap${i + 1}Ms`, gap])),
          providerServiceMs:
            p.length === 9 ? sumKnown(p.map((r) => difference(r?.endMs, r?.arrivalMs))) : null,
          driverMinusScriptedMs: difference(
            response.turnMs,
            item ? 9 * item.ttftMs + 53 * (item.ttftMs === 400 ? 10 : 0) : null,
          ),
          ...Object.fromEntries(Object.entries(spanMetrics).map(([k, v]) => [k, v.totalIoMs])),
          recoveryCount:
            Array.isArray(m.spans) && row.framework === "yielded"
              ? spanMetrics.recoveryIoMs.count
              : null,
          alarmCount: Array.isArray(m.alarmEvents) ? alarmIds.length : null,
          submitCpuMs: cpu("submit", "cpuMs"),
          awaitCpuMs: cpu("awaitSettlement", "cpuMs"),
          alarmCpuMs: cpu("alarm", "cpuMs"),
          alarmPassCpuMedianMs: completeMedian(alarmCpuValues),
          alarmPassCpuMaxMs:
            alarmCpuValues.length && alarmCpuValues.every(finite)
              ? Math.max(...alarmCpuValues)
              : null,
          inlineCpuMs: cpu("inline", "cpuMs"),
          actorNativeCpuMs: cpu("actorNative", "cpuMs"),
          actorAlarmCpuMs: null,
          actorCpuMs: null,
          actorInvocationWallMs: null,
          actorCpuCoverage:
            row.framework !== "tardie"
              ? "inapplicable"
              : invocationRows.some((r) => r.kind === "actorNative" && r.joined)
                ? "native RPC observations only; sampling may omit RPCs, Actor alarms unscoped and total unknown"
                : "missing",
          joinedInvocationCpuMs:
            row.framework !== "tardie" &&
            invocationRows.length &&
            invocationRows.every((r) => !r.boundary)
              ? sumKnown(invocationRows.map((r) => r.cpuMs))
              : null,
          submitInvocationWallMs: cpu("submit", "wallMs"),
          awaitInvocationWallMs: cpu("awaitSettlement", "wallMs"),
          alarmInvocationWallMs: cpu("alarm", "wallMs"),
          inlineInvocationWallMs: cpu("inline", "wallMs"),
          waiterReadCount: waiterReads?.length ?? null,
          waiterReadIntervalMedianMs: waiterIntervals ? completeMedian(waiterIntervals) : null,
          waiterReadIntervalMaxMs:
            waiterIntervals?.length && waiterIntervals.every(finite)
              ? Math.max(...waiterIntervals)
              : null,
          eventAcquisitionsDelta: difference(m.eventAcquisitions, m.eventAcquisitionsAtStart),
          logicalMutationStatements: m.sql?.mutationStatements ?? null,
          writeBindingBytes: m.sql?.writeBindingBytes ?? null,
          transactions: m.sql?.transactions ?? null,
          transactionSync: m.sql?.transactionSync ?? null,
          setAlarmCalls: m.sql?.setAlarmCalls ?? null,
          syncCalls: m.sql?.syncCalls ?? null,
          syncWaitIoMs: m.sql?.syncWaitMs ?? null,
          requestBytes: p.length === 9 ? sumKnown(p.map((r) => r?.requestBytes)) : null,
          responseBytes: calls.length === 9 ? sumKnown(calls.map((c) => c.responseBytes)) : null,
          controllerWallMs: row.clientWallMs ?? null,
          cfRay: row.cfRay ?? null,
          driverInvocation: outer
            ? {
                eventId: eventKey(outer),
                wallMs: outer.$workers.wallTimeMs,
                cpuMs: outer.$workers.cpuTimeMs,
                outcome: outer.$workers.outcome,
              }
            : null,
          ingressColo: response.ingressColo ?? null,
          providerColos: distinct(p.map((r) => r?.colo).filter(Boolean)),
          clockBefore: response.clockBefore ?? null,
          clockAfter: response.clockAfter ?? null,
          providers,
          calls,
          directoryStart: actorStart,
          directory: actor,
          actorObjectId: actor?.objectId ?? actorStart?.objectId ?? null,
          invocations: invocationRows,
          joins,
          spanMetrics,
          spans,
          rpcEvents: m.rpcEvents ?? null,
          alarmEvents: m.alarmEvents ?? null,
          boundaryAlarmIds: boundaryIds,
          settlementWrites: m.settlementWrites ?? null,
          waiterReads,
          waiterReadIntervalsMs: waiterIntervals,
          sql: m.sql ?? null,
          constructorSql: m.constructorSql ?? null,
          eventAcquisitions: m.eventAcquisitions ?? null,
          eventAcquisitionsAtStart: m.eventAcquisitionsAtStart ?? null,
          targetEntryMs: m.targetEntryMs ?? null,
          metricsObservedMs: m.metricsObservedMs ?? null,
        };

        records.push(record);
      }

      const actorClaims = group(
        records.flatMap((record) =>
          record.invocations
            .filter((r) => r.kind === "actorNative" && r.joined && r.eventId)
            .map((invocation) => ({ record, invocation })),
        ),
        ({ invocation }) => invocation.eventId,
      );

      for (const [eventId, claims] of actorClaims) {
        if (distinct(claims.map(({ record }) => record.rowIndex)).length < 2) continue;
        const claimants = claims.map(({ record }) => ({
          key: record.key,
          rowIndex: record.rowIndex,
        }));

        joinedEventIds.delete(eventId);
        for (const { record, invocation } of claims) {
          const failure = {
            key: record.key,
            rowIndex: record.rowIndex,
            kind: "actor-native-multiple-turns",
            eventId,
            claimants,
          };

          record.joins.push(failure);
          joinFailures.push(failure);
          invocation.joined = false;
          invocation.cpuMs = null;
          invocation.wallMs = null;
          record.actorNativeCpuMs = null;
          record.actorCpuCoverage =
            "ambiguous native RPC turn attribution; Actor alarms unscoped and total unknown";
        }
      }

      const cohorts = [];
      const warmProofFailures = [];

      for (const [key, expected] of group(allSchedule, (r) => `${r.framework}/${r.object}`)) {
        const turns = records
          .filter((r) => `${r.framework}/${r.object}` === key)
          .toSorted((a, b) => Number(a.sample.slice(1)) - Number(b.sample.slice(1)));

        const retired = retiredByObject.get(expected[0].object) ?? null;
        const reasons = retired ? [`retired Object group: ${retired.reason}`] : [];

        if (
          turns.length !== expected.length ||
          expected.some((e) => turns.filter((r) => r.sample === e.sample).length !== 1)
        )
          reasons.push("incomplete/duplicate planned cohort");
        if (turns.some((t) => !t.eligible)) reasons.push("invalid turn in cohort");
        if (
          distinct(turns.map((t) => t.version)).length !== 1 ||
          distinct(turns.map((t) => t.objectId)).length !== 1
        )
          reasons.push("Object/version changed or missing");
        const resident = turns;

        if (distinct(resident.map((t) => t.incarnation)).length !== 1)
          reasons.push("incarnation changed after m0");
        if (turns[0]?.framework === "tardie") {
          const actors = turns.map((t) => t.directoryStart);

          if (
            actors.some((actor) => !actor?.objectId || !actor.incarnation || !actor.version) ||
            distinct(
              actors.map((actor) => `${actor?.objectId}/${actor?.incarnation}/${actor?.version}`),
            ).length !== 1
          )
            reasons.push("Actor incarnation changed or entry proof missing after m0");
        }
        if (expected.some((e) => (completedByItem.get(itemKey(e)) ?? []).length !== 1))
          reasons.push("completed ledger missing or duplicated turn");
        if (
          turns.some((t) =>
            t.completedRows.some(
              (c) =>
                c.variant !== t.variant ||
                c.state !== t.state ||
                c.incarnation !== t.incarnation ||
                c.turnMs !== t.driverTotalMs ||
                c.transcriptVerified !== true ||
                (t.state === "cold" && c.coldVerified !== true),
            ),
          )
        )
          reasons.push("completed ledger contradicts independently checked turn");
        if (planFailures.length) reasons.push("invalid experiment plan");
        for (const [i, t] of turns.entries()) {
          const prior = turns[i - 1];

          t.stableIncarnationSincePreviousTurn =
            prior?.incarnation &&
            t.incarnation &&
            prior.objectId &&
            t.objectId &&
            prior.version &&
            t.version
              ? prior.incarnation === t.incarnation &&
                prior.version === t.version &&
                prior.objectId === t.objectId
              : null;
          t.newRuntimeInitializationsBetweenPasses =
            t.framework === "yielded" && t.stableIncarnationSincePreviousTurn === true ? 0 : null;
          if (t.state !== "cold" && t.stableIncarnationSincePreviousTurn === false)
            warmProofFailures.push({
              key: t.key,
              retired,
              reason: "warm Object incarnation/version changed",
              previousIncarnation: prior.incarnation,
              incarnation: t.incarnation,
              previousVersion: prior.version,
              version: t.version,
            });
          t.cohortEligible = reasons.length === 0;
          t.cohortReasons = reasons;
        }
        cohorts.push({
          key,
          expectedTurns: expected.length,
          receivedTurns: turns.length,
          retired,
          eligible: reasons.length === 0,
          reasons,
        });
      }
      const measured = records.filter((r) => r.eligible && r.cohortEligible);

      const unitsFor = (turns) =>
        [...group(turns, (r) => `${r.worker}/${r.framework}/${r.objectId}`).entries()].map(
          ([key, ts]) => ({
            key,
            object: ts[0].object,
            objectId: ts[0].objectId,
            turns: ts.length,
            samples: ts.map((t) => t.sample),
            modelInvocationKinds: modelInvocationKinds(ts.flatMap((t) => t.modelInvocations)),
            mixedModelContextTurns: ts.filter((t) => t.mixedModelInvocationKinds).length,
            metrics: Object.fromEntries(
              metricNames.map((metric) => [metric, completeMedian(ts.map((t) => t[metric]))]),
            ),
            coverage: Object.fromEntries(
              metricNames.map((metric) => [
                metric,
                { observed: ts.filter((t) => finite(t[metric])).length, expected: ts.length },
              ]),
            ),
          }),
        );

      const groups = [
        ...group(
          measured,
          (r) => `${r.framework}/${r.variant}/h${r.history}/d${r.ttftMs}/${r.state}`,
        ).entries(),
      ].map(([key, ts]) => {
        const units = unitsFor(ts);

        return {
          key,
          framework: ts[0].framework,
          variant: ts[0].variant,
          history: ts[0].history,
          ttftMs: ts[0].ttftMs,
          state: ts[0].state,
          objects: units.length,
          expectedObjects: objectsPerTarget,
          turns: ts.length,
          modelInvocationKinds: modelInvocationKinds(ts.flatMap((t) => t.modelInvocations)),
          mixedModelContextTurns: ts.filter((t) => t.mixedModelInvocationKinds).length,
          units,
          metrics: Object.fromEntries(
            metricNames.map((metric) => [metric, stats(units.map((u) => u.metrics[metric]))]),
          ),
        };
      });

      const headline = [50, 250].flatMap((history) =>
        [0, 400].flatMap((ttftMs) =>
          ["cold", "warm"].map((state) => {
            const cellTargets = Object.fromEntries(
              Object.entries(targets).map(([framework, variant]) => {
                const g = groups.find(
                  (g) =>
                    g.framework === framework &&
                    g.variant === variant &&
                    g.history === history &&
                    g.ttftMs === ttftMs &&
                    g.state === state,
                );

                return [
                  framework,
                  {
                    variant,
                    objects: g?.objects ?? 0,
                    expectedObjects: objectsPerTarget,
                    turns: g?.turns ?? 0,
                    driverTotalMs: g?.metrics.driverTotalMs ?? stats([]),
                  },
                ];
              }),
            );

            const complete = [cellTargets.yielded, cellTargets.pi].every(
              (t) =>
                t.objects === objectsPerTarget &&
                t.driverTotalMs.n === objectsPerTarget &&
                t.driverTotalMs.missing === 0,
            );

            return {
              key: `h${history}/d${ttftMs}/${state}`,
              history,
              ttftMs,
              state,
              targets: cellTargets,
              yieldedOverPi:
                complete && cellTargets.pi.driverTotalMs.median > 0
                  ? cellTargets.yielded.driverTotalMs.median / cellTargets.pi.driverTotalMs.median
                  : null,
              ratioDefinition:
                "Yielded across-Object median driverTotalMs divided by pi across-Object median driverTotalMs; independent namespace Objects",
            };
          }),
        ),
      );

      const repeatSpread = groups.map((g) => {
        const turns = measured.filter(
          (t) => `${t.framework}/${t.variant}/h${t.history}/d${t.ttftMs}/${t.state}` === g.key,
        );

        const units = [...group(turns, (t) => t.objectId).values()]
          .filter((ts) => ts.length >= 2)
          .map((ts) => {
            ts.sort((a, b) => Number(a.sample.slice(1)) - Number(b.sample.slice(1)));

            return {
              object: ts[0].object,
              objectId: ts[0].objectId,
              samples: ts.map((t) => t.sample),
              metrics: Object.fromEntries(
                metricNames.map((metric) => {
                  const values = ts.map((t) => t[metric]);
                  const complete = values.every(finite);
                  const drift = complete ? difference(values.at(-1), values[0]) : null;

                  return [
                    metric,
                    {
                      values,
                      signedDrift: drift,
                      absoluteDrift: finite(drift) ? Math.abs(drift) : null,
                      range: complete ? Math.max(...values) - Math.min(...values) : null,
                    },
                  ];
                }),
              ),
            };
          });

        return {
          key: g.key,
          objects: units.length,
          units,
          metrics: Object.fromEntries(
            metricNames.map((metric) => [
              metric,
              Object.fromEntries(
                ["signedDrift", "absoluteDrift", "range"].map((stat) => [
                  stat,
                  stats(units.map((u) => u.metrics[metric][stat])),
                ]),
              ),
            ]),
          ),
        };
      });

      // Inventory covers every phase and both Workers, including deliberate cold aborts.
      const describeEvent = (e) => ({
        id: eventKey(e),
        worker: worker(e),
        timestamp: e.timestamp,
        requestId: e.$metadata?.requestId,
        traceId: e.$metadata?.traceId,
        type: e.$workers?.eventType,
        executionModel: e.$workers?.executionModel,
        outcome: e.$workers?.outcome,
        objectId: e.$workers?.durableObjectId,
        version: e.$workers?.scriptVersion?.id,
        cpuMs: e.$workers?.cpuTimeMs ?? null,
        wallMs: e.$workers?.wallTimeMs ?? null,
        query: queryOf(e),
        logs: (logsByRequest.get(requestKey(e)) ?? []).map((log) => ({
          id: eventKey(log),
          source: log.source,
        })),
      });

      const outcomes = [
        ...group(
          invocations,
          (e) =>
            `${worker(e)}/${e.$workers?.executionModel}/${e.$workers?.eventType}/${e.$workers?.outcome ?? "missing"}`,
        ).entries(),
      ].map(([key, es]) => ({ key, count: es.length }));

      const failedOutcomes = invocations
        .filter((e) => e.$workers?.outcome !== "ok")
        .map(describeEvent);

      const failedRequests = rows.filter(
        (r) => r.status !== 200 || r.error || r.response?.error || r.response?.ok === false,
      );

      const unreturnedAttempts = attempts.filter(
        (a) =>
          !rows.some(
            (r) =>
              r.worker === a.worker &&
              r.phase === a.phase &&
              r.path === a.path &&
              r.startedAt === a.startedAt,
          ),
      );

      const failureFiles = distinct(
        readdirSync(inputDir)
          .filter((n) => /-failures\.json(?:\.gz)?$/.test(n))
          .map((n) => n.replace(/\.gz$/, "")),
      );

      const failures = {
        outcomes,
        failedOutcomes,
        failedRequests,
        unreturnedAttempts,
        controllerErrors: lines("controller-errors.jsonl"),
        controllerFailures: failureFiles.map((file) => ({ file, entries: load(file, []) })),
        applicationFailures: logs
          .filter(
            (e) =>
              e.source?.rebench?.includes("failure") ||
              (e.source?.rebench === "provider" && e.source.error),
          )
          .map((e) => ({
            id: eventKey(e),
            worker: worker(e),
            requestId: e.$metadata?.requestId,
            source: e.source,
          })),
        joinFailures,
        integrityFailures,
        planFailures,
        retiredGroups,
        warmProofFailures,
        incompleteCohorts: cohorts.filter((c) => !c.eligible && !c.retired),
        expectedButMissingTurns: schedule.filter((s) => !rowsByItem.has(itemKey(s))),
        unexpectedTurns: measuredRows.filter(
          (r) => !planned.has(itemKey(r)) && !retiredByObject.has(r.object),
        ),
      };

      const summary = {
        phase,
        generatedAt: new Date().toISOString(),
        reducerSha256: createHash("sha256")
          .update(readFileSync(fileURLToPath(import.meta.url)))
          .digest("hex"),
        expectedBaseline,
        baselineRevision: plan?.baselineRevision ?? resources?.baselineRevision ?? null,
        mainVersionProof:
          phase === "main"
            ? {
                source: mainSnapshot ? "resources-main.json" : "resources.json",
                baselineRevision: mainResources?.baselineRevision ?? null,
                bundleSha256: mainTarget?.build?.bundleSha256 ?? null,
                acceptedWorkerVersions: mainVersions,
              }
            : null,
        accountName: resources?.accountName ?? null,
        unit: "Object medians, then median/Q1/Q3/min/max/range across Objects",
        quantileMethod: "type 7: linear interpolation at (n - 1) * p",
        limitations,
        inputs,
        plan: plan
          ? {
              objectsPerRole: plan.objectsPerRole,
              measuredTurns: plan.measuredTurns,
              driverPlacement: plan.driverPlacement,
              locationHint: plan.locationHint,
              cold: plan.cold,
              idleHeartbeat: plan.idleHeartbeat ?? null,
            }
          : null,
        references,
        groups,
        headline,
        repeatSpread,
        cohorts,
        retiredGroups,
        warmProofFailures,
        coverage: {
          requestsAllPhases: rows.length,
          selectedRequests: measuredRows.length,
          retiredTurns: records.filter((r) => r.retired).length,
          plannedTurns: schedule.length,
          eligibleTurns: records.filter((r) => r.eligible).length,
          measuredTurns: measured.length,
          completedLedgerRows: completed.length,
          telemetryEvents: events.length,
          invocations: invocations.length,
          nonOkOutcomes: failedOutcomes.length,
          failedRequests: failedRequests.length,
          joinFailures: joinFailures.length,
          providerSseReceipts: records.flatMap((r) => r.providers).filter((p) => p.source === "sse")
            .length,
          providerLogFallbacks: records
            .flatMap((r) => r.providers)
            .filter((p) => p.source === "log-fallback").length,
          transcriptValidTurns: records.filter((r) => r.transcriptValid).length,
          verifiedColdTurns: records.filter((r) => r.coldVerified).length,
        },
        telemetry: captures.map((c) => ({
          role: c.role,
          worker: c.worker ?? null,
          events: c.events.length,
          polls: c.polls,
        })),
        outcomes,
        joinFailures,
        integrityFailures,
        planFailures,
        unjoinedSelectedObjectInvocations: invocations
          .filter(
            (e) =>
              records.some(
                (r) =>
                  worker(e) === r.worker &&
                  [r.objectId, r.actorObjectId]
                    .filter(Boolean)
                    .includes(e.$workers?.durableObjectId),
              ) && !joinedEventIds.has(eventKey(e)),
          )
          .map(describeEvent),
        excluded: records
          .filter((r) => !r.eligible || !r.cohortEligible)
          .map((r) => ({
            key: r.key,
            retired: r.retired,
            reasons: [...r.reasons, ...(r.cohortReasons ?? ["unplanned cohort"])],
          })),
      };

      const fmt = (s) =>
        !s || s.median === null
          ? "—"
          : `${s.median.toFixed(1)} [${s.q1.toFixed(1)}, ${s.q3.toFixed(1)}]; ${s.min.toFixed(1)}…${s.max.toFixed(1)} (n=${s.n})`;

      const fmtBound = (g, metric) => {
        const lo = g.metrics[`${metric}LowerMs`];
        const hi = g.metrics[`${metric}UpperMs`];

        return !lo || !hi || lo.median === null || hi.median === null
          ? "—"
          : `${lo.median.toFixed(1)}…${hi.median.toFixed(1)} (n=${lo.n})`;
      };

      const table = (title, columns, entries) => [
        title,
        "",
        `| ${columns.map((c) => c[0]).join(" | ")} |`,
        `| ${columns.map(() => "---").join(" | ")} |`,
        ...entries.map((e) => `| ${columns.map((c) => c[1](e)).join(" | ")} |`),
        "",
      ];

      const tables = [
        `Phase: ${phase}. Object median [Q1, Q3]; min…max across Objects. All durations ms. — means missing or inapplicable.`,
        "",
        ...table(
          "Headline driver latency: three independent Objects per target",
          [
            ["Cell", (g) => g.key],
            ["Yielded production", (g) => fmt(g.targets.yielded.driverTotalMs)],
            ["pi native", (g) => fmt(g.targets.pi.driverTotalMs)],
            ["Tardie native", (g) => fmt(g.targets.tardie.driverTotalMs)],
            [
              "Yielded/pi",
              (g) => (finite(g.yieldedOverPi) ? g.yieldedOverPi.toFixed(3) + "×" : "—"),
            ],
          ],
          headline,
        ),
        ...table(
          "Driver and provider timing",
          [
            ["Cohort", (g) => g.key],
            ["Objects", (g) => `${g.objects}/${g.expectedObjects}`],
            ["Driver", (g) => fmt(g.metrics.driverTotalMs)],
            ["Laptop including diagnostics", (g) => fmt(g.metrics.controllerWallMs)],
            ["Admission", (g) => fmt(g.metrics.admissionMs)],
            ["Submit → first provider bounds*", (g) => fmtBound(g, "firstProviderFromSubmit")],
            ["Receipt → first provider bounds*", (g) => fmtBound(g, "firstProviderFromReceipt")],
            ["Last provider → client bounds*", (g) => fmtBound(g, "lastProviderToClient")],
            ["Gap median*", (g) => fmt(g.metrics.providerGapMedianMs)],
          ],
          groups,
        ),
        ...table(
          "I/O-clock span and waiter diagnostics (not wall decomposition)",
          [
            ["Cohort", (g) => g.key],
            ["Ledger claim†", (g) => fmt(g.metrics.submissionClaimIoMs)],
            ["Run claim†", (g) => fmt(g.metrics.runStorageClaimIoMs)],
            ["Recovery†", (g) => fmt(g.metrics.recoveryIoMs)],
            ["Waiter reads", (g) => fmt(g.metrics.waiterReadCount)],
            ["Read interval†", (g) => fmt(g.metrics.waiterReadIntervalMedianMs)],
          ],
          groups,
        ),
        ...table(
          "Invocation CPU (Cloudflare attribution, not semantic phase cost)",
          [
            ["Cohort", (g) => g.key],
            ["Submit", (g) => fmt(g.metrics.submitCpuMs)],
            ["Await", (g) => fmt(g.metrics.awaitCpuMs)],
            ["Alarm sum‡", (g) => fmt(g.metrics.alarmCpuMs)],
            ["Alarm pass median‡", (g) => fmt(g.metrics.alarmPassCpuMedianMs)],
            ["Alarm pass max‡", (g) => fmt(g.metrics.alarmPassCpuMaxMs)],
            ["Native Thread", (g) => fmt(g.metrics.inlineCpuMs)],
            ["Tardie Actor native observed", (g) => fmt(g.metrics.actorNativeCpuMs)],
            ["Tardie Actor alarms (unscoped)", (g) => fmt(g.metrics.actorAlarmCpuMs)],
            ["Tardie Actor total (unknown)", (g) => fmt(g.metrics.actorCpuMs)],
            ["Observed alarms", (g) => fmt(g.metrics.alarmCount)],
            [
              "Model contexts inline/alarm/missing",
              (g) =>
                `${g.modelInvocationKinds.inline ?? 0}/${g.modelInvocationKinds.alarm ?? 0}/${g.modelInvocationKinds.missing ?? 0}`,
            ],
            ["Mixed-context turns", (g) => g.mixedModelContextTurns],
          ],
          groups,
        ),
        ...table(
          "Repeated-turn spread within each Object",
          [
            ["Cohort", (g) => g.key],
            ["Objects", (g) => g.objects],
            [
              "Absolute first-to-last driver drift",
              (g) => fmt(g.metrics.driverTotalMs.absoluteDrift),
            ],
            ["Driver range", (g) => fmt(g.metrics.driverTotalMs.range)],
          ],
          repeatSpread.filter((g) => g.objects),
        ),
        ...table(
          "All captured invocation outcomes (all phases)",
          [
            ["Worker / model / type / outcome", (r) => r.key],
            ["Count", (r) => r.count],
          ],
          outcomes,
        ),
        `Coverage: ${measured.length}/${schedule.length} planned turns in complete cohorts; ${failedRequests.length} failed controller requests; ${failedOutcomes.length} non-ok invocation outcomes; ${joinFailures.length} join/observation issues. Full inventory: failed-outcomes.json.`,
        `Retired evidence: ${records.filter((r) => r.retired).length} turns across ${retiredGroups.length} excluded Object groups; ${warmProofFailures.length} warm-incarnation proof failures retained.`,
        "",
        "* Bounds show lower…upper limits for the Object-level median, conditional on a stable provider/driver offset consistent with both echo probes. Their full Q1/Q3/range and per-Object intervals are in summary.json. Gaps use provider clocks. Negative values are retained. These residuals do not isolate alarm dispatch, processing entry or storage notification; invalid DO cross-clock differences remain only in turns.jsonl diagnosticClock.",
        "† I/O-clock duration, never CPU; zero does not establish zero work. Missing waiterReads remains missing.",
        "‡ Whole observed alarm invocations can extend beyond client completion. START-only identity joins retain conflicting END contexts separately. Partial or ambiguous start joins remain missing; per-invocation and boundary details are in turns.jsonl.",
        "",
        ...limitations.map((note) => `- ${note}`),
        "",
      ];

      mkdirSync(outputDir, { recursive: true });

      const save = (name, value) =>
        writeFileSync(join(outputDir, name), JSON.stringify(value, null, 2) + "\n");

      save("summary.json", summary);
      save("failed-outcomes.json", failures);
      writeFileSync(
        join(outputDir, "turns.jsonl"),
        records.map((r) => JSON.stringify(r)).join("\n") + (records.length ? "\n" : ""),
      );
      writeFileSync(join(outputDir, "tables.md"), tables.join("\n"));
      console.log(
        JSON.stringify({
          phase,
          outputDir,
          groups: groups.length,
          coverage: summary.coverage,
          planFailures: planFailures.length,
          integrityFailures: integrityFailures.length,
        }),
      );

      return summary;
    },
    catch: (cause) => new AnalysisError({ message: String(cause) }),
  });

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2).filter((arg) => arg !== "--");
  const options = {};

  for (let i = 0; i < args.length; i += 2) {
    const key = { "--phase": "phase", "--input-dir": "inputDir", "--out-dir": "outputDir" }[
      args[i]
    ];

    if (!key || !args[i + 1])
      throw new Error(
        "Usage: analyze.mjs [--phase main|pilot2] [--input-dir directory] [--out-dir directory]",
      );
    options[key] = args[i + 1];
  }
  NodeRuntime.runMain(analyze(options));
}
