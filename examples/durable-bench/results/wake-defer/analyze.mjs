import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

import { reference } from "./reference.mjs";

// Offline, task-local reduction of the reused cf-latency receipt/alarm joins.
const here = process.env.WAKE_DEFER_EVIDENCE_DIR ?? dirname(fileURLToPath(import.meta.url));
const read = (name) => {
  const path = join(here, name);
  return existsSync(path)
    ? readFileSync(path, "utf8")
    : existsSync(path + ".gz")
      ? gunzipSync(readFileSync(path + ".gz")).toString("utf8")
      : undefined;
};
const load = (name, fallback) => {
  const text = read(name);
  return text === undefined ? fallback : JSON.parse(text);
};
const lines = (name) => (read(name) ?? "").split("\n").filter(Boolean).map(JSON.parse);
const save = (name, value) =>
  writeFileSync(join(here, name), JSON.stringify(value, null, 2) + "\n");
const finite = (value) => typeof value === "number" && Number.isFinite(value);
const sum = (values) => values.reduce((a, b) => a + b, 0);
const sumKnown = (values) => (values.every(finite) ? sum(values) : null);
const difference = (a, b) => (finite(a) && finite(b) ? a - b : null);
const quantile = (values, p) => {
  const sorted = values.filter(finite).toSorted((a, b) => a - b);
  if (!sorted.length) return null;
  const index = (sorted.length - 1) * p;
  return (
    sorted[Math.floor(index)] +
    (sorted[Math.ceil(index)] - sorted[Math.floor(index)]) * (index - Math.floor(index))
  );
};
const median = (values) => quantile(values, 0.5);
const strictMedian = (values) => (values.length && values.every(finite) ? median(values) : null);
const stats = (values) => ({
  n: values.filter(finite).length,
  median: median(values),
  q1: quantile(values, 0.25),
  q3: quantile(values, 0.75),
  min: quantile(values, 0),
  max: quantile(values, 1),
});
const group = (values, key) => {
  const groups = new Map();
  for (const value of values) {
    const k = key(value);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(value);
  }
  return groups;
};
const countBy = (values, key) =>
  Object.fromEntries([...group(values, key)].map(([k, items]) => [k, items.length]));
const queryOf = (event) => {
  const url = event.$workers?.event?.request?.url;
  if (!url) return undefined;
  try {
    const parsed = new URL(url);
    return { path: parsed.pathname, ...Object.fromEntries(parsed.searchParams) };
  } catch {
    return undefined;
  }
};
const worker = (event) => event.$workers?.scriptName;
const eventKey = (event) =>
  worker(event) && event.$metadata?.id ? worker(event) + "/" + event.$metadata.id : undefined;
const invocation = (event) => event.$metadata?.type === "cf-worker-event";
const turnKey = (row) => row.object + "/" + row.sample;
const attemptKey = (row) => [row.worker, row.path, row.startedAt].join("/");
const sameIdentity = (source, receipt) =>
  ["objectId", "incarnation", "version"].every((key) => source[key] === receipt[key]);
const alarmKey = (source) =>
  [source.objectId, source.incarnation, source.version, source.alarmId].join("/");
const rayColo = (value) =>
  typeof value === "string" ? (/-([A-Z]{3})$/.exec(value)?.[1] ?? null) : null;
const placementColo = (value) =>
  typeof value === "string" ? (/^(?:remote|local)-([A-Z]{3})$/.exec(value)?.[1] ?? null) : null;
const metricNames = [
  "driverMs",
  "laptopMs",
  "driverMinusScriptedMs",
  "driverMinusProviderMs",
  "laptopMinusScriptedMs",
  "laptopMinusProviderMs",
  "providerMs",
  "firstModelDispatchIoMs",
  "firstModelArrivalFromDriverMs",
  "firstModelArrivalFromLaptopMs",
  "modelGapTotalMs",
  "modelGapMedianMs",
  "objectModelGapTotalIoMs",
  "objectModelGapMedianIoMs",
  ...Array.from({ length: 8 }, (_, index) => "modelGap" + (index + 1) + "Ms"),
  "driverTailFromProviderMs",
  "fetchCpuMs",
  "fetchWallMs",
  "doIoClockElapsedMs",
  "attributedCpuMs",
  "alarmCpuMs",
  "containedAlarmCpuMs",
  "boundaryAlarmCpuMs",
  "alarmOverlapObservedCount",
  "alarmStartedInsideCount",
  "alarmEndedInsideCount",
  "alarmJoinedCount",
  "scheduleNowCalls",
  "suppressedScheduleNow",
  "nondeferredScheduleNow",
  "armNowExecutions",
  "nativeSetAlarmCalls",
  "maintenancePasses",
  "claimAttempts",
  "failedClaims",
  "instrumentedMaintenanceScans",
  "transactions",
  "writeTransactions",
  "writeTransactionSync",
  "writeBindingBytes",
  "requestBytes",
  "receiptBytes",
];

const AnalysisError = Schema.TaggedError()("WakeDeferAnalysisError", { message: Schema.String });
export const program = Effect.try({
  try: () => {
    if (process.argv.includes("--help")) {
      console.log(
        "vp exec node examples/durable-bench/results/wake-defer/analyze.mjs — offline; writes analysis.json and analysis.md",
      );
      return;
    }
    const resources = load("resources.json", { targets: [] });
    const plan = load("network-plan.json", { cohorts: [] });
    const retiredCohorts = plan.retiredCohorts ?? [];
    const retiredObjects = new Set(retiredCohorts.map((cohort) => cohort.object));
    const plannedMeasuredTurns = sum(
      plan.cohorts.map(
        (cohort) => cohort.schedule.filter((item) => ["warm", "cold"].includes(item.mode)).length,
      ),
    );
    const rows = lines("requests.jsonl");
    const attempts = lines("attempted.jsonl");
    const admissions = lines("admissions.jsonl");
    const network = resources.targets.find((target) => target.role === "network");
    const driver = resources.targets.find((target) => target.role === "driver");
    const captures = resources.targets.map((target) => ({
      role: target.role,
      ...load("telemetry-" + target.role + ".json", { events: [], polls: [] }),
    }));
    const allEvents = captures.flatMap((capture) => capture.events);
    const unidentified = allEvents.filter((event) => !eventKey(event));
    const events = [
      ...new Map(allEvents.filter(eventKey).map((event) => [eventKey(event), event])).values(),
    ];
    // Unidentified events cannot establish CPU joins, but their failures must survive.
    const capturedEvents = [...events, ...unidentified];
    const capturedInvocations = capturedEvents.filter(
      (event) => invocation(event) || (!event.source && event.$workers?.outcome !== undefined),
    );
    const invocations = events.filter(invocation);
    const byRequest = group(
      invocations.filter((event) => event.$metadata?.requestId),
      (event) => worker(event) + "/" + event.$metadata.requestId,
    );
    const byFetch = group(
      invocations.filter((event) => queryOf(event)?.path === "/run"),
      (event) => worker(event) + "/" + queryOf(event).object + "/" + queryOf(event).sample,
    );
    const alarmLogs = events.filter(
      (event) => worker(event) === network?.name && event.source?.wakeDefer === "target-alarm",
    );
    const alarmLogsById = group(alarmLogs, (event) => alarmKey(event.source));
    const alarmsByRequest = group(
      alarmLogs.filter((event) => event.$metadata?.requestId),
      (event) => event.$metadata.requestId,
    );
    const constructors = events.filter(
      (event) =>
        worker(event) === network?.name && event.source?.wakeDefer === "target-constructed",
    );
    const mechanismLogs = events.filter(
      (event) => worker(event) === network?.name && event.source?.wakeDefer === "mechanism",
    );
    const providerLogs = group(
      events.filter((event) => event.source?.wakeDefer === "provider"),
      (event) => event.source.requestId,
    );
    const expected = reference();
    const missing = [];
    const records = [];

    for (const row of rows.filter((row) => ["measure", "warmup", "probe"].includes(row.phase))) {
      const r = row.response ?? {};
      const cohort =
        plan.cohorts.find((cohort) => cohort.object === row.object) ??
        retiredCohorts.find((cohort) => cohort.object === row.object);
      const planned = cohort?.schedule.find((item) => item.sample === row.sample);
      const query = new URL(row.path, "https://wake-defer").searchParams;
      const mode = row.phase === "probe" ? (row.sample === "m0" ? "cold" : "warm") : planned?.mode;
      const reasons = [];
      const calls = r.calls ?? [];
      const provider = calls.map((call) => call.providerReceipt);
      const admission = admissions.filter(
        (entry) => turnKey(entry) === turnKey(row) && entry.phase === row.phase,
      );
      if (row.status !== 200 || !r.ok) reasons.push("HTTP/workload failure");
      if (admission.length !== 1 || !admission[0].passed)
        reasons.push("Controller admission missing, duplicate, or failed");
      if (
        row.phase !== "probe" &&
        (!cohort ||
          !planned ||
          planned.variant !== r.variant ||
          cohort.seed?.objectId !== r.objectId)
      )
        reasons.push("Same-Object schedule/seed identity mismatch");
      if (
        r.seedFingerprint !== expected[r.history]?.seed ||
        r.version !== network?.expectedVersion ||
        r.buildId !== network?.build.bundleSha256 ||
        r.object !== row.object ||
        r.sample !== row.sample ||
        r.variant !== query.get("variant")
      )
        reasons.push("Seed/build/query identity mismatch");
      if (
        r.driver?.buildId !== driver?.build.bundleSha256 ||
        r.driver?.version !== driver?.expectedVersion ||
        !finite(r.driver?.workerLatencyMs) ||
        !finite(row.laptopLatencyMs)
      )
        reasons.push("Deployed driver/laptop receipt missing");
      let transcriptValid =
        calls.length === 9 && new Set(calls.map((call) => call.providerRequest)).size === 9;
      let providerLogMatches = 0;
      for (const [index, call] of calls.entries()) {
        const p = provider[index];
        if (
          !p ||
          p.requestId !== call.providerRequest ||
          p.call !== index ||
          call.call !== index ||
          call.status !== 200 ||
          !call.sseDone ||
          call.error ||
          p.error !== null ||
          p.fingerprint !== expected[r.history]?.turns[row.sample]?.[index] ||
          ["target", "history", "ttftMs", "chunkDelayMs", "object", "sample", "variant"].some(
            (key) => p[key] !== r[key],
          ) ||
          ![p.arrivalMs, p.firstByteMs, p.endMs].every(finite) ||
          p.firstByteMs < p.arrivalMs ||
          p.endMs < p.firstByteMs
        )
          transcriptValid = false;
        const logs = providerLogs.get(call.providerRequest) ?? [];
        if (p && logs.length === 1) {
          if (
            [
              "arrivalMs",
              "firstByteMs",
              "endMs",
              "fingerprint",
              "rawWireFingerprint",
              "requestBytes",
              "colo",
            ].every((key) => p[key] === logs[0].source[key])
          )
            providerLogMatches++;
          else reasons.push("Provider stream/log disagreement at step " + index);
        }
      }
      if (!transcriptValid)
        reasons.push("Nine complete native streams with golden transcript receipts required");
      const fingerprintChecks = Array.from({ length: Math.max(9, calls.length) }, (_, index) => {
        const p = provider[index];
        const wanted = expected[cohort?.history ?? r.history]?.turns[row.sample]?.[index];
        return {
          call: index,
          expected: wanted ?? null,
          observed: p?.fingerprint ?? null,
          received: Boolean(p),
          matched: Boolean(p && typeof wanted === "string" && p.fingerprint === wanted),
        };
      });
      const mainCandidates = (
        r.objectId && r.version ? (byFetch.get(network?.name + "/" + turnKey(row)) ?? []) : []
      ).filter(
        (event) =>
          event.$workers.durableObjectId === r.objectId &&
          event.$workers.scriptVersion?.id === r.version,
      );
      const main = mainCandidates.length === 1 ? mainCandidates[0] : undefined;
      if (!main)
        missing.push({ kind: "do-fetch", key: turnKey(row), cardinality: mainCandidates.length });
      if (main && main.$workers.outcome !== "ok")
        reasons.push("DO invocation outcome: " + main.$workers.outcome);

      const coldCandidates = rows.filter(
        (prior) =>
          ["cold-reset", "probe-cold"].includes(prior.phase) &&
          prior.object === row.object &&
          prior.sample === "cold-" + row.sample &&
          prior.endedAt <= row.startedAt,
      );
      const cold = coldCandidates.length === 1 ? coldCandidates[0] : undefined;
      const construction = constructors.filter((event) => sameIdentity(event.source, r));
      const constructorContradiction = construction.some(
        (event) =>
          (event.$workers?.eventType !== undefined && event.$workers.eventType !== "fetch") ||
          (queryOf(event)?.path !== undefined && queryOf(event).path !== "/run") ||
          (main &&
            event.$metadata?.requestId &&
            event.$metadata.requestId !== main.$metadata.requestId) ||
          (main && event.$metadata?.traceId && event.$metadata.traceId !== main.$metadata.traceId),
      );
      const coldVerified = Boolean(
        cold?.response?.ok &&
        cold.response.coldRequested &&
        cold.response.before?.objectId === r.objectId &&
        cold.response.before.incarnation !== r.incarnation &&
        r.entry?.firstHarnessRequest === true &&
        r.entry.priorAlarmStarts === 0 &&
        r.entry.activeAlarmIds?.length === 0 &&
        !constructorContradiction,
      );
      if (mode === "cold" && !coldVerified)
        reasons.push("Cold incarnation/first-entry proof failed");
      const preceding = records.findLast((prior) => prior.object === row.object);
      if (mode === "warm" && (!preceding || preceding.incarnation !== r.incarnation))
        reasons.push("Warm request did not retain the preceding completed turn's incarnation");

      // reset() can change query labels while an older native alarm is still running.
      // Its stable ID and the returned active-window edges/snapshots establish overlap.
      const returned = Array.isArray(r.alarmEvents) ? r.alarmEvents : [];
      const activeEdges = returned.filter((edge) => edge.active && sameIdentity(edge, r));
      const activeAtStart = new Set(
        Array.isArray(r.alarmsActiveAtStart) ? r.alarmsActiveAtStart : [],
      );
      const activeAtEnd = new Set(Array.isArray(r.alarmsActiveAtEnd) ? r.alarmsActiveAtEnd : []);
      const windowReceiptKnown =
        Array.isArray(r.alarmEvents) &&
        Array.isArray(r.alarmsActiveAtStart) &&
        Array.isArray(r.alarmsActiveAtEnd) &&
        finite(r.runStartedMs) &&
        finite(r.runEndedMs) &&
        r.runEndedMs >= r.runStartedMs &&
        [...activeAtStart, ...activeAtEnd].every((id) => typeof id === "string") &&
        returned.every(
          (edge) =>
            typeof edge.alarmId === "string" &&
            typeof edge.active === "boolean" &&
            ["start", "end"].includes(edge.edge) &&
            sameIdentity(edge, r),
        );
      const alarmIds = new Set([
        ...activeEdges.map((edge) => edge.alarmId),
        ...activeAtStart,
        ...activeAtEnd,
      ]);
      let alarmWindowKnown = windowReceiptKnown;
      let alarmJoinComplete = windowReceiptKnown;
      const joined = [];
      const alarmJoins = [];
      for (const alarmId of alarmIds) {
        const edges = alarmLogsById.get(alarmKey({ ...r, alarmId })) ?? [];
        const requests = [
          ...new Set(edges.map((event) => event.$metadata?.requestId).filter(Boolean)),
        ];
        const traces = [...new Set(edges.map((event) => event.$metadata?.traceId).filter(Boolean))];
        const candidates =
          requests.length === 1
            ? (byRequest.get(network?.name + "/" + requests[0]) ?? []).filter(
                (event) =>
                  event.$workers.eventType === "alarm" &&
                  event.$workers.durableObjectId === r.objectId &&
                  event.$workers.scriptVersion?.id === r.version,
              )
            : [];
        const reverseIds =
          requests.length === 1
            ? new Set(
                (alarmsByRequest.get(requests[0]) ?? []).map((event) => alarmKey(event.source)),
              )
            : new Set();
        const unique =
          requests.length === 1 &&
          traces.length === 1 &&
          candidates.length === 1 &&
          reverseIds.size === 1 &&
          edges.every((event) => event.$metadata?.requestId && event.$metadata?.traceId) &&
          edges.some((event) => event.source.edge === "start") &&
          edges.some((event) => event.source.edge === "end") &&
          candidates[0].$metadata?.traceId === traces[0];
        const inReceipt = activeEdges.filter((edge) => edge.alarmId === alarmId);
        const starts = inReceipt.filter((edge) => edge.edge === "start").length;
        const ends = inReceipt.filter((edge) => edge.edge === "end").length;
        const atStart = activeAtStart.has(alarmId);
        const atEnd = activeAtEnd.has(alarmId);
        const windowConsistent = Number(atStart) + starts === 1 && Number(atEnd) + ends === 1;
        const boundary = atStart || atEnd || !windowConsistent;
        if (!windowConsistent) {
          alarmWindowKnown = false;
          alarmJoinComplete = false;
          missing.push({
            kind: "alarm-window",
            key: turnKey(row),
            alarmId,
            atStart,
            atEnd,
            starts,
            ends,
          });
        }
        alarmJoins.push({
          alarmId,
          boundary,
          activeAtStart: atStart,
          activeAtEnd: atEnd,
          startedInside: starts === 1,
          endedInside: ends === 1,
          windowConsistent,
          complete: unique,
          requestIds: requests,
          traceIds: traces,
          invocationIds: candidates.map(eventKey),
          fullInvocationCpuMs: unique ? (candidates[0].$workers.cpuTimeMs ?? null) : null,
          invocationOutcome: unique ? (candidates[0].$workers.outcome ?? null) : null,
          timing: {
            turnStartIoMs: r.runStartedMs ?? null,
            turnEndIoMs: r.runEndedMs ?? null,
            startLogMs: edges
              .filter((event) => event.source.edge === "start")
              .map((event) => event.timestamp ?? null),
            endLogMs: edges
              .filter((event) => event.source.edge === "end")
              .map((event) => event.timestamp ?? null),
            invocationTimestampMs: unique ? (candidates[0].timestamp ?? null) : null,
            invocationWallMs: unique ? (candidates[0].$workers.wallTimeMs ?? null) : null,
          },
        });
        if (!unique) {
          alarmJoinComplete = false;
          missing.push({
            kind: "alarm-join",
            key: turnKey(row),
            alarmId,
            requests: requests.length,
            traces: traces.length,
            candidates: candidates.length,
            reverseIds: reverseIds.size,
          });
        } else joined.push({ event: candidates[0], boundary });
      }
      if (!windowReceiptKnown) missing.push({ kind: "alarm-window-receipt", key: turnKey(row) });
      if (new Set(joined.map(({ event }) => eventKey(event))).size !== joined.length)
        alarmJoinComplete = false;
      const cpu = (values) =>
        alarmJoinComplete ? sumKnown(values.map(({ event }) => event.$workers.cpuTimeMs)) : null;
      const contained = joined.filter((entry) => !entry.boundary);
      const boundaries = joined.filter((entry) => entry.boundary);

      const tail = rows.filter(
        (item) =>
          ["tail", "probe-tail"].includes(item.phase) &&
          item.object === row.object &&
          item.sample === "tail-" + row.sample &&
          sameIdentity(item.response ?? {}, r),
      );
      const mechanism = [
        ...new Map(
          [
            ...(r.mechanism ?? []),
            ...tail.flatMap((item) => item.response?.mechanism ?? []),
            ...mechanismLogs
              .filter((event) => sameIdentity(event.source, r))
              .map((event) => event.source),
          ]
            .filter(
              (event) => event.query?.sample === row.sample && event.query?.object === row.object,
            )
            .map((event) => [event.incarnation + "/" + event.sequence, event]),
        ).values(),
      ].toSorted((a, b) => a.sequence - b.sequence);
      const active = mechanism.filter((event) => event.active);
      const schedules = active.filter((event) => event.event === "scheduleNow");
      const mechanismKnown =
        Array.isArray(r.mechanism) &&
        !r.mechanismTruncated &&
        schedules.length > 0 &&
        schedules.every((event) => typeof event.deferred === "boolean") &&
        active.some((event) => event.event === "notify") &&
        (network?.build.mode !== "ab" ||
          active.some((event) => event.event === "processing" && event.edge === "start"));
      if (!mechanismKnown) reasons.push("Mechanism instrumentation missing or truncated");
      const mechanismCount = (predicate) =>
        mechanismKnown ? active.filter(predicate).length : null;
      const p = transcriptValid ? provider : [];
      const providerMs = transcriptValid ? sum(p.map((call) => call.endMs - call.arrivalMs)) : null;
      const gaps = p.slice(1).map((call, index) => call.arrivalMs - p[index].endMs);
      const objectGaps = calls.slice(1).map((call, index) =>
        difference(call.fetchStartedMs, calls[index].endMs),
      );
      const objectGapsKnown =
        transcriptValid && objectGaps.length === 8 && objectGaps.every(finite);
      const configuredProviderMs = 9 * r.ttftMs + 53 * r.chunkDelayMs;
      const sql = r.sql ?? {};
      const record = {
        key: turnKey(row),
        phase: row.phase,
        object: row.object,
        retired: retiredObjects.has(row.object),
        sample: row.sample,
        history: cohort?.history ?? r.history,
        historyBeforeTurn: planned ? cohort.history + cohort.schedule.indexOf(planned) : null,
        ttftMs: cohort?.ttftMs ?? r.ttftMs,
        mode,
        repeat: planned?.repeat ?? null,
        variant: r.variant,
        objectId: r.objectId,
        incarnation: r.incarnation,
        version: r.version,
        buildId: r.buildId,
        eligible: reasons.length === 0,
        reasons,
        transcriptValid,
        fingerprintChecks,
        providerStreamReceipts: provider.filter(Boolean).length,
        coldVerified,
        constructorContradiction,
        constructorInvocationIds: construction.map((event) => event.$metadata?.requestId),
        providerLogMatches,
        firstModelRequest: calls[0]?.providerRequest,
        providerRequestIds: calls.map((call) => call.providerRequest),
        driverMs: r.driver?.workerLatencyMs ?? null,
        laptopMs: row.laptopLatencyMs ?? null,
        cfRay: row.cfRay ?? null,
        laptopCfRayColo: rayColo(row.cfRay),
        cfPlacement: row.cfPlacement ?? null,
        driver: r.driver ?? null,
        driverIngressColo: r.driver?.ingressColo ?? null,
        driverResponsePlacement: row.cfPlacement ?? null,
        driverExecutionColo: placementColo(row.cfPlacement),
        driverIncomingPlacement: r.driver?.incomingPlacement ?? null,
        driverIncomingPlacementColo: placementColo(r.driver?.incomingPlacement),
        targetCfRay: r.driver?.targetCfRay ?? null,
        targetCfRayColo: rayColo(r.driver?.targetCfRay),
        providerColos: [...new Set(p.map((call) => call.colo))],
        configuredProviderMs,
        driverMinusScriptedMs: difference(r.driver?.workerLatencyMs, configuredProviderMs),
        driverMinusProviderMs: difference(r.driver?.workerLatencyMs, providerMs),
        laptopMinusScriptedMs: difference(row.laptopLatencyMs, configuredProviderMs),
        laptopMinusProviderMs: difference(row.laptopLatencyMs, providerMs),
        providerMs,
        firstModelDispatchIoMs: difference(calls[0]?.fetchStartedMs, r.runStartedMs),
        firstModelArrivalFromDriverMs: difference(p[0]?.arrivalMs, r.driver?.startedMs),
        firstModelArrivalFromLaptopMs: difference(p[0]?.arrivalMs, row.startedAt),
        modelGapsMs: gaps,
        modelGapTotalMs: transcriptValid ? sum(gaps) : null,
        modelGapMedianMs: median(gaps),
        objectModelGapsIoMs: objectGapsKnown ? objectGaps : null,
        objectModelGapTotalIoMs: objectGapsKnown ? sum(objectGaps) : null,
        objectModelGapMedianIoMs: objectGapsKnown ? median(objectGaps) : null,
        ...Object.fromEntries(
          Array.from({ length: 8 }, (_, index) => [
            "modelGap" + (index + 1) + "Ms",
            gaps[index] ?? null,
          ]),
        ),
        driverTailFromProviderMs: difference(r.driver?.endedMs, p.at(-1)?.endMs),
        requestBytes: transcriptValid ? sumKnown(p.map((call) => call.requestBytes)) : null,
        receiptBytes: r.driver?.receiptBytes ?? null,
        fetchCpuMs: main?.$workers.cpuTimeMs ?? null,
        fetchWallMs: main?.$workers.wallTimeMs ?? null,
        fetchInvocationId: main && eventKey(main),
        doIoClockElapsedMs: r.doWallMs ?? null,
        alarmWindowKnown,
        alarmWindow: {
          runStartIoMs: r.runStartedMs ?? null,
          runEndIoMs: r.runEndedMs ?? null,
          activeAtStart: [...activeAtStart],
          activeAtEnd: [...activeAtEnd],
        },
        alarmOverlapObservedCount: alarmWindowKnown ? alarmIds.size : null,
        alarmStartedInsideCount: alarmWindowKnown
          ? alarmJoins.filter((join) => join.startedInside).length
          : null,
        alarmEndedInsideCount: alarmWindowKnown
          ? alarmJoins.filter((join) => join.endedInside).length
          : null,
        alarmJoinComplete,
        alarmJoinedCount: alarmJoinComplete ? joined.length : null,
        alarmJoins,
        alarmJoinCoverage: {
          windowKnown: alarmWindowKnown,
          observedIds: alarmWindowKnown ? alarmIds.size : null,
          uniquelyJoinedIds: alarmJoins.filter((join) => join.complete).length,
          missingOrAmbiguousIds: alarmJoins.filter((join) => !join.complete).length,
          boundaryIds: alarmJoins.filter((join) => join.boundary).length,
          knownAlarmCpuReceipts: joined.filter(({ event }) => finite(event.$workers.cpuTimeMs))
            .length,
        },
        // Diagnostic full costs are non-additive when an invocation spans multiple turns.
        alarmOverlapFullCpuMs: cpu(joined),
        alarmCpuMs: cpu(joined),
        containedAlarmCpuMs: cpu(contained),
        boundaryAlarmCpuMs: cpu(boundaries),
        attributedCpuMs:
          main?.$workers.outcome === "ok" && alarmJoinComplete
            ? sumKnown([
                main.$workers.cpuTimeMs,
                ...contained.map(({ event }) => event.$workers.cpuTimeMs),
              ])
            : null,
        mechanismKnown,
        scheduleNowCalls: mechanismKnown ? schedules.length : null,
        suppressedScheduleNow: mechanismKnown
          ? schedules.filter((event) => event.deferred === true).length
          : null,
        nondeferredScheduleNow: mechanismKnown
          ? schedules.filter((event) => event.deferred === false).length
          : null,
        armNowExecutions: mechanismCount((event) => event.event === "armNow"),
        nativeSetAlarmCalls: sql.setAlarmCalls ?? null,
        maintenancePasses: mechanismCount((event) => event.event === "maintenanceReport"),
        claimAttempts: mechanismCount((event) => event.event === "claimAttempt"),
        failedClaims: mechanismCount((event) => event.event === "claim" && !event.claimed),
        instrumentedMaintenanceScans: mechanismCount((event) =>
          ["maintenanceScan", "checkpointScan"].includes(event.event),
        ),
        notifyKinds: countBy(
          active.filter((event) => event.event === "notify"),
          (event) => event.kind,
        ),
        notifySites: countBy(
          active.filter((event) => event.event === "notifySite"),
          (event) => event.site + (event.tags ? ":" + event.tags.join("+") : ""),
        ),
        mechanismEventCounts: countBy(active, (event) => event.event),
        afterReturnEventCounts: countBy(
          mechanism.filter((event) => !event.active),
          (event) => event.event,
        ),
        maintenanceReports: mechanism.filter((event) => event.event === "maintenanceReport"),
        recovery: mechanism.filter((event) =>
          ["recovery", "recoveryDecision"].includes(event.event),
        ),
        generationCheckpoints: mechanism.filter((event) => event.event === "generationCheckpoint"),
        transactions: sql.transactions ?? null,
        writeTransactions: sql.writeTransactions ?? null,
        writeTransactionSync: sql.writeTransactionSync ?? null,
        writeBindingBytes: sql.writeBindingBytes ?? null,
        constructorCounts: r.constructorSql ?? null,
        runCounts: r.sql ?? null,
        steps: calls.map((call, index) => ({
          call: index,
          providerRequest: call.providerRequest,
          fingerprint: provider[index]?.fingerprint,
          wireFingerprint: provider[index]?.rawWireFingerprint,
          gapBeforeMs: index ? gaps[index - 1] : null,
          sqlSincePreviousCall: call.sqlSincePreviousCall,
          fetchToHeadersIoMs: difference(call.headersMs, call.fetchStartedMs),
        })),
      };
      records.push(record);
    }

    // An alarm can survive /drain and a meter reset into the next turn. Retain its
    // full cost diagnostically, but never silently treat it as two independent costs.
    const alarmMemberships = group(
      records.flatMap((record) => record.alarmJoins.map((join) => ({ record, join }))),
      ({ record, join }) => alarmKey({ ...record, alarmId: join.alarmId }),
    );
    const sharedAlarms = [];
    for (const members of alarmMemberships.values()) {
      const turnKeys = [...new Set(members.map(({ record }) => record.key))];
      const variants = [...new Set(members.map(({ record }) => record.variant))];
      const sharedAcrossTurns = turnKeys.length > 1;
      for (const { join } of members) {
        join.sharedAcrossTurns = sharedAcrossTurns;
        join.sharedAcrossVariants = sharedAcrossTurns && variants.length > 1;
      }
      if (sharedAcrossTurns)
        sharedAlarms.push({
          alarmId: members[0].join.alarmId,
          objectId: members[0].record.objectId,
          incarnation: members[0].record.incarnation,
          version: members[0].record.version,
          invocationIds: [...new Set(members.flatMap(({ join }) => join.invocationIds))],
          sharedAcrossVariants: variants.length > 1,
          turns: members.map(({ record, join }) => ({
            key: record.key,
            phase: record.phase,
            mode: record.mode,
            variant: record.variant,
            startedInside: join.startedInside,
            endedInside: join.endedInside,
            boundary: join.boundary,
          })),
        });
    }
    for (const record of records) {
      const shared = record.alarmJoins.filter((join) => join.sharedAcrossTurns);
      record.sharedAlarmIds = shared.map((join) => join.alarmId);
      record.alarmJoinCoverage.sharedAcrossTurns = shared.length;
      if (shared.length) record.alarmCpuMs = null;
      if (shared.some((join) => join.boundary)) record.boundaryAlarmCpuMs = null;
      if (shared.some((join) => !join.boundary)) {
        record.containedAlarmCpuMs = null;
        record.attributedCpuMs = null;
      }
      record.alarmCpuPoolable = finite(record.alarmCpuMs);
    }
    const joinedAlarmInvocationIds = new Set(
      records.flatMap((record) =>
        record.alarmJoins.filter((join) => join.complete).flatMap((join) => join.invocationIds),
      ),
    );
    const nativeAlarms = invocations.filter(
      (event) => worker(event) === network?.name && event.$workers?.eventType === "alarm",
    );
    const joinedNativeAlarms = nativeAlarms.filter((event) =>
      joinedAlarmInvocationIds.has(eventKey(event)),
    );
    const knownNativeCpu = joinedNativeAlarms
      .map((event) => event.$workers.cpuTimeMs)
      .filter(finite);
    // Presence of a captured handler log is weaker than a complete turn/CPU join.
    // Missing logs do not establish whether a handler ran or why delivery was canceled.
    const handlerLogRequests = new Set(capturedEvents.filter((event) =>
      worker(event) === network?.name && event.source?.wakeDefer === "target-alarm" &&
      event.$metadata?.requestId).map((event) => event.$metadata.requestId));
    const nativeFact = (event) => ({
      outcome: event.$workers?.outcome ?? "missing",
      joined: joinedAlarmInvocationIds.has(eventKey(event)),
      hasHandlerLog: event.$metadata?.requestId
        ? handlerLogRequests.has(event.$metadata.requestId) : null,
      cpuMs: event.$workers?.cpuTimeMs ?? null,
    });
    const nativeFacts = nativeAlarms.map(nativeFact);
    const nativeTally = (values) => {
      const cpu = values.map((value) => value.cpuMs).filter(finite);
      return {
        invocations: values.length,
        knownCpuInvocations: cpu.length,
        missingCpuInvocations: values.length - cpu.length,
        zeroCpuInvocations: cpu.filter((value) => value === 0).length,
        nonzeroCpuInvocations: cpu.filter((value) => value !== 0).length,
        knownFullCpuMs: cpu.length ? sum(cpu) : null,
        fullCpuMs: values.length ? sumKnown(values.map((value) => value.cpuMs)) : null,
      };
    };
    const nativeCategories = [...group(nativeFacts, (value) =>
      JSON.stringify([value.outcome, value.joined, value.hasHandlerLog]))]
      .toSorted(([a], [b]) => a.localeCompare(b))
      .map(([, values]) => ({
        outcome: values[0].outcome, joined: values[0].joined,
        hasHandlerLog: values[0].hasHandlerLog, ...nativeTally(values),
      }));
    const alarmAccounting = {
      scope:
        "All returned turn windows, including warmup/probe and rejected turns; native capture includes every phase",
      knownTurnWindows: records.filter((record) => record.alarmWindowKnown).length,
      returnedTurnWindows: records.length,
      observedAlarmIds: alarmMemberships.size,
      alarmIdsWithoutUniqueInvocation: [...alarmMemberships.values()].filter(
        (members) => !members.some(({ join }) => join.complete),
      ).length,
      capturedNativeInvocations: nativeAlarms.length,
      uniqueJoinedNativeInvocations: joinedNativeAlarms.length,
      capturedNativeInvocationsWithoutOverlapJoin: nativeAlarms.length - joinedNativeAlarms.length,
      knownCpuInvocations: knownNativeCpu.length,
      knownJoinedFullCpuMs: knownNativeCpu.length ? sum(knownNativeCpu) : null,
      uniqueJoinedFullCpuMs: joinedNativeAlarms.length
        ? sumKnown(joinedNativeAlarms.map((event) => event.$workers.cpuTimeMs))
        : null,
      nativeTotals: nativeTally(nativeFacts),
      nativeByOutcome: Object.fromEntries([...group(nativeFacts, (value) => value.outcome)]
        .toSorted(([a], [b]) => a.localeCompare(b))
        .map(([outcome, values]) => [outcome, nativeTally(values)])),
      nativeCategories,
      nativeWithoutEventId: nativeTally(capturedInvocations.filter((event) =>
        !eventKey(event) && worker(event) === network?.name && event.$workers?.eventType === "alarm")
        .map(nativeFact)),
      nativeLimits:
        "Native totals cover captured network alarm invocation records deduplicated by Worker/event ID, across all phases and outcomes. joined means a complete observed-alarm invocation join to a returned turn; hasHandlerLog means any captured target-alarm log with the native requestId (null when requestId is missing). No log does not establish absence of handler work or cancellation cause. Unidentified records are tallied separately because they cannot be safely deduplicated or joined. CPU sums are unweighted captured values, not totals for every scheduling delivery; joined handler CPU is a subset, not an additional cost to add.",
      sharedAlarms,
    };

    const completeObjects = plan.cohorts
      .filter((cohort) => {
        const object = records.filter(
          (record) =>
            record.object === cohort.object && ["measure", "warmup"].includes(record.phase),
        );
        return (
          object.length === cohort.schedule.length &&
          object.every((record) => record.eligible) &&
          cohort.schedule.every(
            (item) =>
              object.filter(
                (record) => record.sample === item.sample && record.variant === item.variant,
              ).length === 1,
          ) &&
          new Set(object.map((record) => record.objectId)).size === 1 &&
          new Set(object.map((record) => record.version)).size === 1
        );
      })
      .map((cohort) => cohort.object);
    const measured = records.filter(
      (record) =>
        record.phase === "measure" && record.eligible && completeObjects.includes(record.object),
    );
    const coverageFor = (values) => ({
      turns: values.length,
      validTranscripts: values.filter((record) => record.transcriptValid).length,
      providerStreamReceipts: sum(values.map((record) => record.providerStreamReceipts)),
      matchedFingerprints: sum(
        values.map((record) => record.fingerprintChecks.filter((check) => check.matched).length),
      ),
      matchedProviderLogs: sum(values.map((record) => record.providerLogMatches)),
      coldVerified: values.filter((record) => record.mode === "cold" && record.coldVerified).length,
      mainCpuReceipts: values.filter((record) => finite(record.fetchCpuMs)).length,
      alarmCountReceipts: values.filter((record) => finite(record.alarmOverlapObservedCount))
        .length,
      observedAlarmOverlaps: sum(
        values.map((record) => record.alarmOverlapObservedCount).filter(finite),
      ),
      observedAlarmStartsInside: sum(
        values.map((record) => record.alarmStartedInsideCount).filter(finite),
      ),
      sharedAlarmOverlapMemberships: sum(values.map((record) => record.sharedAlarmIds.length)),
      uniqueObservedAlarmIds: new Set(
        values.flatMap((record) =>
          record.alarmJoins.map((join) => alarmKey({ ...record, alarmId: join.alarmId })),
        ),
      ).size,
      uniqueJoinedAlarmInvocations: new Set(
        values.flatMap((record) =>
          record.alarmJoins.filter((join) => join.complete).flatMap((join) => join.invocationIds),
        ),
      ).size,
      joinedAlarmOverlaps: sum(values.map((record) => record.alarmJoinCoverage.uniquelyJoinedIds)),
      missingOrAmbiguousAlarmOverlaps: sum(
        values.map((record) => record.alarmJoinCoverage.missingOrAmbiguousIds),
      ),
      completeAlarmJoins: values.filter((record) => record.alarmJoinComplete).length,
      alarmCpuReceipts: values.filter((record) => finite(record.alarmCpuMs)).length,
      fullOverlapCpuReceipts: values.filter((record) => finite(record.alarmOverlapFullCpuMs))
        .length,
      mechanismReceipts: values.filter((record) => record.mechanismKnown).length,
      driverPlacementReceipts: values.filter(
        (record) =>
          typeof record.driverResponsePlacement === "string" &&
          record.driverResponsePlacement.length > 0,
      ).length,
      driverIncomingPlacementReceipts: values.filter(
        (record) =>
          typeof record.driverIncomingPlacement === "string" &&
          record.driverIncomingPlacement.length > 0,
      ).length,
    });
    const paired = [];
    for (const history of [50, 250])
      for (const ttftMs of [0, 400])
        for (const mode of ["warm", "cold"]) {
          const units = [];
          for (const [object, values] of group(
            measured.filter(
              (record) =>
                record.history === history && record.ttftMs === ttftMs && record.mode === mode,
            ),
            (record) => record.object,
          )) {
            const baseline = values
              .filter((record) => record.variant === "baseline")
              .toSorted((a, b) => a.repeat - b.repeat);
            const candidate = values
              .filter((record) => record.variant === "candidate")
              .toSorted((a, b) => a.repeat - b.repeat);
            if (baseline.length !== 2 || candidate.length !== 2) continue;
            units.push({
              object,
              samples: values.map((record) => ({
                sample: record.sample,
                variant: record.variant,
                repeat: record.repeat,
                historyBeforeTurn: record.historyBeforeTurn,
              })),
              metrics: Object.fromEntries(
                metricNames.map((metric) => {
                  const base = strictMedian(baseline.map((record) => record[metric]));
                  const changed = strictMedian(candidate.map((record) => record[metric]));
                  const baselineRepeatDifference = difference(
                    baseline[1][metric],
                    baseline[0][metric],
                  );
                  const candidateRepeatDifference = difference(
                    candidate[1][metric],
                    candidate[0][metric],
                  );
                  return [
                    metric,
                    {
                      baseline: base,
                      candidate: changed,
                      saved: difference(base, changed),
                      baselineRepeatDifference,
                      baselineRepeatAbsoluteDifference: finite(baselineRepeatDifference)
                        ? Math.abs(baselineRepeatDifference)
                        : null,
                      candidateRepeatDifference,
                      candidateRepeatAbsoluteDifference: finite(candidateRepeatDifference)
                        ? Math.abs(candidateRepeatDifference)
                        : null,
                      pairedRepeatSavings: baseline.map((record, index) =>
                        difference(record[metric], candidate[index][metric]),
                      ),
                    },
                  ];
                }),
              ),
            });
          }
          paired.push({
            history,
            ttftMs,
            mode,
            objects: units.length,
            plannedObjects: plan.cohorts.filter(
              (cohort) => cohort.history === history && cohort.ttftMs === ttftMs,
            ).length,
            coverage: {
              plannedTurns: sum(
                plan.cohorts
                  .filter((cohort) => cohort.history === history && cohort.ttftMs === ttftMs)
                  .map((cohort) => cohort.schedule.filter((item) => item.mode === mode).length),
              ),
              returned: coverageFor(
                records.filter(
                  (record) =>
                    record.phase === "measure" &&
                    record.history === history &&
                    record.ttftMs === ttftMs &&
                    record.mode === mode,
                ),
              ),
              admitted: coverageFor(
                measured.filter(
                  (record) =>
                    record.history === history && record.ttftMs === ttftMs && record.mode === mode,
                ),
              ),
            },
            units,
            metrics: Object.fromEntries(
              metricNames.map((metric) => [
                metric,
                Object.fromEntries(
                  [
                    "baseline",
                    "candidate",
                    "saved",
                    "baselineRepeatDifference",
                    "baselineRepeatAbsoluteDifference",
                    "candidateRepeatDifference",
                    "candidateRepeatAbsoluteDifference",
                  ].map((field) => [
                    field,
                    stats(units.map((unit) => unit.metrics[metric][field])),
                  ]),
                ),
              ]),
            ),
          });
        }
    const nonOk = capturedInvocations
      .filter((event) => event.$workers?.outcome !== "ok")
      .map((event) => {
        const query = queryOf(event);
        return {
          eventId: eventKey(event) ?? null,
          worker: worker(event),
          type: event.$workers?.eventType,
          outcome: event.$workers?.outcome ?? "missing",
          query,
          cpuMs: event.$workers?.cpuTimeMs,
          explicitColdRequest:
            query?.path === "/cold" &&
            rows.some(
              (row) =>
                row.object === query.object &&
                row.sample === query.sample &&
                row.response?.coldRequested === true,
            ),
          exceptions: event.$workers?.exceptions,
        };
      });
    const invocationSet = new Set(capturedInvocations);
    const otherOutcomeRecords = capturedEvents
      .filter(
        (event) =>
          !invocationSet.has(event) &&
          event.$workers?.outcome !== undefined &&
          event.$workers.outcome !== "ok",
      )
      .map((event) => ({
        eventId: eventKey(event) ?? null,
        worker: worker(event),
        outcome: event.$workers.outcome,
        source: event.source,
      }));
    const errors = {
      failedRequests: rows
        .filter(
          (row) =>
            row.status !== 200 || row.response?.ok === false || row.error || row.response?.error,
        )
        .map((row) => ({
          key: turnKey(row),
          phase: row.phase,
          path: row.path,
          status: row.status,
          error: row.error ?? row.response?.error,
          cause: row.cause,
          cfRay: row.cfRay,
          cfPlacement: row.cfPlacement,
          driver: row.response?.driver,
        })),
      unansweredAttempts: attempts
        .filter((attempt) => !rows.some((row) => attemptKey(row) === attemptKey(attempt)))
        .map((attempt) => ({ ...attempt, retired: retiredObjects.has(attempt.object) })),
      controller: lines("controller-errors.jsonl"),
      rejectedAdmissions: admissions.filter((entry) => !entry.passed),
      nonOkInvocations: nonOk,
      exceededCpu: nonOk.filter((event) => event.outcome === "exceededCpu").length,
      exceededMemory: nonOk.filter((event) => event.outcome === "exceededMemory").length,
      otherOutcomeRecords,
      otherExceededCpuRecords: otherOutcomeRecords.filter(
        (event) => event.outcome === "exceededCpu",
      ).length,
      otherExceededMemoryRecords: otherOutcomeRecords.filter(
        (event) => event.outcome === "exceededMemory",
      ).length,
      invocationExceptions: capturedInvocations
        .filter((event) => event.$workers?.exceptions?.length)
        .map((event) => ({
          eventId: eventKey(event) ?? null,
          worker: worker(event),
          outcome: event.$workers.outcome,
          exceptions: event.$workers.exceptions,
        })),
      providerFailures: rows.flatMap((row) =>
        (row.response?.calls ?? [])
          .filter(
            (call) =>
              call.error ||
              call.status !== 200 ||
              !call.sseDone ||
              call.providerReceipt?.error != null,
          )
          .map((call) => ({
            key: turnKey(row),
            phase: row.phase,
            call: call.call,
            providerRequest: call.providerRequest,
            status: call.status,
            sseDone: call.sseDone,
            error: call.error,
            providerError: call.providerReceipt?.error,
          })),
      ),
      errorLogs: capturedEvents
        .filter(
          (event) =>
            !invocation(event) &&
            (event.source?.error ||
              event.$metadata?.error ||
              ["error", "fatal"].includes(String(event.$metadata?.level).toLowerCase()) ||
              ["error", "fatal"].includes(String(event.source?.level).toLowerCase())),
        )
        .map((event) => ({
          eventId: eventKey(event) ?? null,
          source: event.source,
          error: event.$metadata?.error,
        })),
      rejectedTurns: records
        .filter((record) => !record.eligible)
        .map(({ key, reasons }) => ({ key, reasons })),
    };
    const measuredAttempts = attempts.filter((row) => row.phase === "measure");
    const returnedMeasured = records.filter((record) => record.phase === "measure");
    const finalSeeds = rows
      .filter(
        (row) =>
          row.phase?.endsWith("-seed") &&
          row.response?.to ===
            Number(new URL(row.path, "https://wake-defer").searchParams.get("history")),
      )
      .map((row) => {
        const history = Number(new URL(row.path, "https://wake-defer").searchParams.get("history"));
        return {
          object: row.object,
          history,
          status: row.status,
          expected: expected[history]?.seed ?? null,
          observed: row.response?.fingerprint ?? null,
          matched: Boolean(
            row.status === 200 &&
            row.response?.ok &&
            expected[history]?.seed &&
            row.response.fingerprint === expected[history].seed,
          ),
        };
      });
    const fingerprintCoverage = {
      expectedSeeds: Object.fromEntries(
        Object.entries(expected).map(([history, entry]) => [history, entry.seed]),
      ),
      finalSeeds,
      verifiedSeedObjects: new Set(
        finalSeeds.filter((seed) => seed.matched).map((seed) => seed.object),
      ).size,
      expectedStepsPerTurn: 9,
      expectedStepsOnAttemptedMeasuredTurns: measuredAttempts.length * 9,
      returnedMeasured: coverageFor(returnedMeasured),
      admittedMeasured: coverageFor(measured),
      failures: records.flatMap((record) =>
        record.fingerprintChecks
          .filter((check) => !check.matched)
          .map((check) => ({ key: record.key, phase: record.phase, ...check })),
      ),
    };
    const matrixComplete =
      plan.cohorts.length > 0 &&
      completeObjects.length === plan.cohorts.length &&
      measured.length === plannedMeasuredTurns &&
      paired.every((cell) => cell.objects === cell.plannedObjects);
    const result = {
      status: !rows.length
        ? "not run"
        : matrixComplete
          ? "active primary matrix receipts complete"
          : "partial; no complete-matrix claim",
      generatedAt: new Date().toISOString(),
      positiveSavingsMean: "baseline minus candidate",
      units:
        "Independent Object medians of two repeats per arm and temperature; model calls are not independent samples",
      clockLimits:
        "Driver/laptop durations use their own monotonic clocks. Model arrival, provider-based inter-model gaps, and provider tail compare Worker wall clocks and may include skew. DO Date.now advances at I/O and is not a CPU stopwatch.",
      objectModelGapMethod:
        "objectModelGapsIoMs[i - 1] = calls[i].fetchStartedMs - calls[i - 1].endMs, i=1..8, using the same Object's Date.now I/O clock. Each gap starts at stream wrapper finalization (EOF/disposal), not exact DONE parsing, and ends at the next fetch dispatch. The array, objectModelGapMedianIoMs, and objectModelGapTotalIoMs require a valid transcript and all eight finite gaps; otherwise they are null. These durations do not measure CPU.",
      alarmLimits:
        "Returned active-window edges and boundary snapshots establish overlap and starts inside the turn; mutable query labels do not. alarmCpuMs is complete observed-handler CPU for the turn, not all native scheduling deliveries. It requires both logged edges and one alarm ID to one invocation/trace in both directions. All alarm CPU is whole native invocation cost, never prorated. alarmOverlapFullCpuMs is a non-additive diagnostic; shared invocations are flagged across all returned turns and nulled in affected paired CPU metrics. alarmAccounting.nativeTotals/nativeCategories also retain unjoined native CPU by outcome and handler-log presence. attributedCpuMs includes fetch plus contained alarms only. A 250 ms drain does not prove alarm completion.",
      telemetryLimits:
        "Native counts and CPU sums describe captured records without sampling weights. sampleInterval > 1 reflects ingestion/platform sampling independently of statistics.abr_level; abr_level=1 cannot certify complete capture. See the [Cloudflare telemetry API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/).",
      baselineScope:
        "A/B baseline bypasses only withProcessing in the candidate build. Both arms use acquireUseRelease in maintenance and the same observer code; observation cost varies with work and has not been subtracted. Driver timing includes the measurement receipt. The original baseline and local-counts provenance are separate evidence.",
      coverage: {
        plannedObjects: plan.cohorts.length,
        retiredObjects: retiredObjects.size,
        completeObjects: completeObjects.length,
        measuredTurns: measured.length,
        attemptedTurns: measuredAttempts.length,
        returnedTurns: returnedMeasured.length,
        expectedMeasuredTurns: plannedMeasuredTurns,
        coldVerified: measured.filter((record) => record.mode === "cold" && record.coldVerified)
          .length,
        mainCpuReceipts: measured.filter((record) => finite(record.fetchCpuMs)).length,
        completeAlarmJoins: measured.filter((record) => record.alarmJoinComplete).length,
        telemetryEvents: events.length,
        unidentifiedTelemetryEvents: unidentified.length,
        returnedMeasured: coverageFor(returnedMeasured),
        admittedMeasured: coverageFor(measured),
        capture: captures.map(({ role, events, polls }) => ({
          role,
          events: events.length,
          polls,
        })),
      },
      incompleteObjects: plan.cohorts
        .filter((cohort) => !completeObjects.includes(cohort.object))
        .map((cohort) => cohort.object),
      retiredCohorts: retiredCohorts.map(({ object, history, ttftMs, reason }) => ({
        object,
        history,
        ttftMs,
        reason,
      })),
      outcomes: countBy(capturedInvocations, (event) =>
        [worker(event), event.$workers?.eventType, event.$workers?.outcome ?? "missing"].join("/"),
      ),
      fingerprintCoverage,
      alarmAccounting,
      paired,
      records,
      missing,
      errors,
      cleanup: load("cleanup.json", { complete: false }),
      secretScan: load("secret-scan.json", { passed: false }),
    };
    save("analysis.json", result);
    const format = (value) => (finite(value) ? value.toFixed(1) : "unknown");
    const table = paired.map((cell) => {
      const metric = cell.metrics.driverMs;
      return (
        "| " +
        [
          cell.history,
          cell.ttftMs,
          cell.mode,
          cell.objects,
          format(metric.baseline.median),
          format(metric.candidate.median),
          format(metric.saved.median),
          format(metric.saved.min) + "–" + format(metric.saved.max),
          format(metric.baselineRepeatAbsoluteDifference.median),
          format(metric.candidateRepeatAbsoluteDifference.median),
        ].join(" | ") +
        " |"
      );
    });
    const nativeTable = nativeCategories.map((category) => "| " + [
      category.outcome,
      category.joined ? "joined" : "unjoined",
      category.hasHandlerLog === null ? "unknown request ID" : category.hasHandlerLog ? "seen" : "not seen",
      category.invocations,
      category.knownCpuInvocations + "/" + category.missingCpuInvocations,
      category.nonzeroCpuInvocations,
      format(category.knownFullCpuMs),
    ].join(" | ") + " |");
    const report = [
      "# Wake deferral measurement",
      "",
      result.status +
        ". " +
        measured.length +
        "/" +
        plannedMeasuredTurns +
        " measured turns admitted across " +
        completeObjects.length +
        "/" +
        plan.cohorts.length +
        " complete active Objects.",
      "",
      "Worker latency in milliseconds. Each cell summarizes the independent Object pairs admitted from the active plan; positive savings mean baseline minus candidate. Retired Objects and unmatched attempts remain in the evidence and error accounting.",
      "",
      "| Seed history | TTFT | State | Objects | Baseline | Candidate | Paired saving | Object saving range | Baseline repeat spread | Candidate repeat spread |",
      "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
      ...table,
      "",
      "The last two columns are median absolute differences between each arm's two planned repeats. These descriptive spreads and the per-Object effect range do not establish whether an effect exceeds noise. Missing CPU joins remain unknown. Full distributions, individual model gaps, mechanism reports, errors, and join evidence are retained in [analysis.json.gz](analysis.json.gz); offline reduction also writes the plain analysis.json.",
      "",
      result.baselineScope,
      "",
      result.clockLimits,
      "",
      result.objectModelGapMethod,
      "",
      result.alarmLimits,
      "",
      "Native alarm records across all captured phases and outcomes; CPU is the known, unweighted sum in each category. Handler-log absence is not a claim about whether or why execution was canceled.",
      "",
      "| Outcome | Turn join | Handler log | Native records | CPU known/missing | Nonzero CPU records | Known CPU ms |",
      "| --- | --- | --- | --- | --- | --- | --- |",
      ...nativeTable,
      "",
      result.telemetryLimits,
      "",
      "Captured error/outcome records: " +
        errors.failedRequests.length +
        " requests; " +
        errors.unansweredAttempts.length +
        " unanswered attempts; " +
        errors.controller.length +
        " controller errors; " +
        errors.nonOkInvocations.length +
        " invocation records with non-ok or missing outcomes (including explicit cold aborts); " +
        errors.exceededCpu +
        " exceededCpu; " +
        errors.exceededMemory +
        " exceededMemory. Review outcome groups and query sampling before interpreting counts as workload failures; no sampling weights are applied. See capture coverage before interpreting zero counts.",
      "",
      "Cleanup verified: " +
        (result.cleanup.complete === true) +
        ". Credential scan passed: " +
        (result.secretScan.passed === true) +
        ".",
      "",
    ].join("\n");
    writeFileSync(join(here, "analysis.md"), report);
    console.log(
      JSON.stringify({
        status: result.status,
        completeObjects: completeObjects.length,
        measuredTurns: measured.length,
        nonOkInvocations: nonOk.length,
        exceededCpu: errors.exceededCpu,
        exceededMemory: errors.exceededMemory,
      }),
    );
  },
  catch: (cause) => new AnalysisError({ message: String(cause) }),
});
if (import.meta.url === "file://" + process.argv[1]) NodeRuntime.runMain(program);
