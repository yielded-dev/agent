import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { Effect, Schema } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { history, next, payload, turn } from "../../src/plan.ts";
import { analyzeProbe } from "./analyze-probe.mjs";
import { analyzeInstrumentation } from "./analyze-instrumentation.mjs";
import { analyzeMinification } from "./analyze-minification.mjs";

// Offline reduction only. No local execution times are benchmark evidence.
const here = dirname(fileURLToPath(import.meta.url));
const read = (name) => {
  const path = join(here, name);
  return (existsSync(path) ? readFileSync(path) : gunzipSync(readFileSync(path + ".gz"))).toString("utf8");
};
const load = (name, fallback) => existsSync(join(here, name)) || existsSync(join(here, name + ".gz"))
  ? JSON.parse(read(name)) : fallback;
const save = (name, value) => writeFileSync(join(here, name), JSON.stringify(value, null, 2) + "\n");
const lines = (name) => read(name).trim().split("\n").filter(Boolean).map(JSON.parse);
const finite = (x) => typeof x === "number" && Number.isFinite(x);
const sum = (xs) => xs.reduce((a, b) => a + b, 0);
const sumKnown = (xs) => xs.every(finite) ? sum(xs) : null;
const difference = (a, b) => finite(a) && finite(b) ? a - b : null;
const quantile = (xs, p) => {
  const a = xs.filter(finite).toSorted((a, b) => a - b);
  if (!a.length) return null;
  const index = (a.length - 1) * p;
  const lo = Math.floor(index);
  return a[lo] + (a[Math.ceil(index)] - a[lo]) * (index - lo);
};
const median = (xs) => quantile(xs, .5);
const stats = (xs) => ({ n: xs.filter(finite).length, median: median(xs), q1: quantile(xs, .25), q3: quantile(xs, .75), min: quantile(xs, 0), max: quantile(xs, 1) });
const group = (xs, key) => {
  const groups = new Map();
  for (const x of xs) {
    const k = key(x);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(x);
  }
  return groups;
};
const receipt = (row) => row.response?.thread ?? row.response;
const fingerprint = (messages) => createHash("sha256").update(JSON.stringify(messages.map((m) => [m.role, m.text, m.calls ?? []]))).digest("hex").slice(0, 16);
const reference = () => {
  const answer = {};
  for (const h of [50, 250]) {
    const messages = [];
    let seed;
    const append = (input) => {
      messages.push({ role: "user", text: input.text });
      const hashes = [];
      for (;;) {
        hashes.push(fingerprint(messages));
        const step = next(messages);
        if ("answer" in step) { messages.push({ role: "assistant", text: step.answer }); break; }
        messages.push({ role: "assistant", text: "", calls: [step.call] }, { role: "tool", text: payload(step.call) });
      }
      return hashes;
    };
    for (const input of history(0, h)) seed = append(input).at(-1);
    const expected = { 50: "b017b487524e44a4", 250: "dcea9f30b0917245" }[h];
    if (seed !== expected) throw new Error(`Reference fixture drift: h${h} ${seed}`);
    answer[h] = { seed, turns: {} };
    for (let m = 0; m < 16; m++) answer[h].turns[`m${m}`] = append(turn(`m${m}`, 8));
  }
  return answer;
};
const queryOf = (event) => {
  const url = event.$workers?.event?.request?.url;
  if (!url) return undefined;
  const parsed = new URL(url);
  return { path: parsed.pathname, ...Object.fromEntries(parsed.searchParams) };
};
const invocation = (event) => event.$metadata?.type === "cf-worker-event";
const worker = (event) => event.$workers?.scriptName;
const eventKey = (event) => worker(event) && event.$metadata?.id ? `${worker(event)}/${event.$metadata.id}` : undefined;
const requestKey = (row) => `${row.worker}/${row.framework}/${row.object}/${row.sample}`;
const metricNames = ["clientMs", "clientMinusScriptedMs", "clientMinusProviderMs", "providerMs", "configuredProviderMs", "firstModelArrivalMs", "stepGapMs", "stepGapTotalMs", "tailFromProviderMs", "doWallMs", "outerWallMs", "clientMinusDoMs", "mainWallMinusFetchCpuMs", "fetchCpuMs", "attributedDoCpuMs", "observedDoCpuMs", "alarmCpuMs", "alarmOverlapCount", "boundaryCpuMs", "sqlBindingBytes", "kvJsonBytes", "transactions", "transactionSync", "writeTransactions", "writeTransactionSync", "overlappingTransactionCallbacks", "transactionRollbacks", "transactionWindowCrossings", "nativeSyncCalls", "nativeSyncWaitMs", "explicitProbeSyncMs", "logicalMutationStatements", "setAlarmCalls", "deleteAlarmCalls", "requestBytes", "responseBytes", "clientReceiptBytes"];
const objectSummaries = (turns) => Object.fromEntries(metricNames.map((metric) => [metric, stats([...group(turns, (t) => t.object).values()].map((object) => median(object.map((t) => t[metric]))))]));

const AnalysisError = Schema.TaggedError()("CfLatencyAnalysisError", { message: Schema.String });
const run = Effect.try({
  try: () => {
    const rows = lines("requests.jsonl");
    const resources = load("resources.json");
    const events = resources.targets.flatMap((target) => load(`telemetry-${target.role}.json`, { events: [] }).events);
    if (events.some((e) => !eventKey(e))) throw new Error("Telemetry event without unique identity");
    const unique = [...new Map(events.map((e) => [eventKey(e), e])).values()];
    const invocations = unique.filter(invocation);
    const logsByRequest = group(unique.filter((e) => !invocation(e) && e.$metadata?.requestId), (e) => `${worker(e)}/${e.$metadata.requestId}`);
    const byTrace = group(invocations.filter((e) => e.$metadata?.traceId), (e) => `${worker(e)}/${e.$metadata.traceId}`);
    const byRequest = group(invocations.filter((e) => e.$metadata?.requestId), (e) => `${worker(e)}/${e.$metadata.requestId}`);
    const alarmLogs = unique.filter((e) => e.source?.cfLatency === "target-alarm");
    const constructors = unique.filter((e) => e.source?.cfLatency === "target-constructed");
    const coldLogs = unique.filter((e) => e.source?.cfLatency === "cold-requested");
    const byFetch = group(invocations.filter((e) => queryOf(e)?.path === "/run"), (e) => {
      const q = queryOf(e);
      return `${worker(e)}/${q.target}/${q.object}/${q.sample}`;
    });
    const providerGroups = group(unique.filter((e) => e.source?.cfLatency === "provider" && e.source.requestId), (e) => e.source.requestId);
    const provider = new Map([...providerGroups].filter(([, events]) => events.length === 1).map(([id, events]) => [id, events[0]]));
    const expected = reference();
    const records = [];
    const missing = [];
    const fingerprintFailures = [];
    const phases = ["network", "network-variant", "network-variant-settle", "minification"];
    for (const row of rows.filter((row) => (phases.includes(row.phase) || /^network-pilot\d*$/.test(row.phase)) && row.status === 200 && receipt(row)?.ok)) {
      const r = receipt(row);
      const reasons = [];
      const isCold = row.sample === "m0" || row.phase === "minification";
      const query = new URL(row.path, "https://cf-latency").searchParams;
      const minificationRound = row.phase === "minification" ? Number(query.get("minificationRound")) : null;
      const minified = row.phase === "minification" ? query.get("minified") === "true" : null;
      if (row.phase === "minification" && (!query.has("minificationRound") || !Number.isInteger(minificationRound) || !["true", "false"].includes(query.get("minified"))))
        reasons.push("invalid minification controller round/build flag");
      const actorIdentityStable = row.framework !== "tardie" || ["objectId", "incarnation", "version"].every((key) => row.response.directoryStart?.[key] && row.response.directoryStart[key] === row.response.directory?.[key]);
      if (!actorIdentityStable) reasons.push("Tardie Actor identity changed between begin/end receipts");
      if (row.framework === "tardie" && row.response.directory?.version !== r.version) reasons.push("Tardie Actor/Thread versions differ");
      const p = r.calls.map((call) => call.providerReceipt ?? provider.get(call.providerRequest)?.source);
      const completeProvider = p.every((receipt, i) => receipt && receipt.requestId === r.calls[i].providerRequest) && p.length === 9 && new Set(r.calls.map((call) => call.providerRequest)).size === 9;
      if (!completeProvider) missing.push({ kind: "provider", key: requestKey(row), missing: r.calls.filter((call) => !call.providerReceipt && !provider.has(call.providerRequest)).map((call) => call.providerRequest) });
      const providerLogMatches = r.calls.filter((call) => {
        const log = provider.get(call.providerRequest)?.source;
        if (!call.providerReceipt || !log) return false;
        const keys = ["arrivalMs", "firstByteMs", "endMs", "fingerprint", "rawWireFingerprint", "requestBytes", "colo"];
        if (keys.some((key) => call.providerReceipt[key] !== log[key])) reasons.push(`provider stream/log disagreement: ${call.call}`);
        return keys.every((key) => call.providerReceipt[key] === log[key]);
      }).length;
      const fetches = byFetch.get(requestKey(row)) ?? [];
      const mainCandidates = fetches.filter((e) => e.$workers?.durableObjectId === r.objectId && e.$workers.scriptVersion?.id === r.version);
      const outerCandidates = fetches.filter((e) => e.$workers?.executionModel === "stateless" && e.$metadata?.rayId === row.cfRay?.split("-")[0]);
      const main = mainCandidates.length === 1 ? mainCandidates[0] : undefined;
      const outer = outerCandidates.length === 1 ? outerCandidates[0] : undefined;
      if (!main) missing.push({ kind: "do-fetch", key: requestKey(row) });
      if (!outer) missing.push({ kind: "outer-fetch", key: requestKey(row) });
      // Native alarm traces have their own root. Explicit start/end tags bind
      // them to the active turn without interpreting the Object's virtual clock.
      const ids = [r.objectId, row.response.directory?.objectId].filter(Boolean);
      const trace = outer?.$metadata?.traceId ?? main?.$metadata?.traceId;
      const identities = [r, row.response.directory].filter(Boolean);
      const matches = (source) => source.active && source.query?.object === row.object && source.query?.target === row.framework && source.query?.sample === row.sample && identities.some((id) => source.objectId === id.objectId && source.incarnation === id.incarnation && source.version === id.version);
      const returnedAlarmEvents = [...(r.alarmEvents ?? []), ...(row.response.directory?.alarmEvents ?? [])];
      const returnedBoundaryIds = new Set([...(r.alarmsActiveAtStart ?? []), ...(r.alarmsActiveAtEnd ?? []), ...(row.response.directory?.alarmsActiveAtStart ?? []), ...(row.response.directory?.alarmsActiveAtEnd ?? [])]);
      const returnedAlarmIds = new Set([...returnedAlarmEvents.filter(matches).map((event) => event.alarmId), ...returnedBoundaryIds]);
      const tagged = alarmLogs.filter((log) => worker(log) === row.worker && (returnedAlarmIds.size ? returnedAlarmIds.has(log.source.alarmId) : matches(log.source)));
      const alarmRequests = [...new Set(tagged.filter((log) => log.$metadata?.requestId).map((log) => `${worker(log)}/${log.$metadata.requestId}`))];
      const alarms = [];
      let alarmJoinComplete = tagged.every((log) => log.$metadata?.requestId);
      // Cloudflare can tag the two edges of one logical alarm under different
      // invocation contexts. Never turn that ambiguity into extra CPU events.
      const candidateAlarmIds = new Set([...returnedAlarmIds, ...tagged.map((log) => log.source.alarmId)]);
      const idsByInvocation = new Map();
      for (const alarmId of candidateAlarmIds) {
        const edges = tagged.filter((log) => log.source.alarmId === alarmId);
        const requests = new Set(edges.map((log) => log.$metadata?.requestId).filter(Boolean));
        const traces = new Set(edges.map((log) => log.$metadata?.traceId).filter(Boolean));
        if (requests.size !== 1 || traces.size > 1) {
          alarmJoinComplete = false;
          missing.push({ kind: "alarm-identity-ambiguity", key: requestKey(row), alarmId, requestIds: [...requests], traceIds: [...traces] });
        }
        for (const request of requests) {
          if (!idsByInvocation.has(request)) idsByInvocation.set(request, new Set());
          idsByInvocation.get(request).add(alarmId);
        }
      }
      for (const [request, alarmIds] of idsByInvocation) if (alarmIds.size !== 1) {
        alarmJoinComplete = false;
        missing.push({ kind: "alarm-invocation-ambiguity", key: requestKey(row), request, alarmIds: [...alarmIds] });
      }
      for (const alarmId of returnedAlarmIds) {
        if (!tagged.some((log) => log.source.alarmId === alarmId && log.$metadata?.requestId)) {
          alarmJoinComplete = false;
          missing.push({ kind: "alarm-log", key: requestKey(row), alarmId });
        }
      }
      for (const key of alarmRequests) {
        const joined = (byRequest.get(key) ?? []).filter((e) => e.$workers.eventType === "alarm" && ids.includes(e.$workers.durableObjectId) && e.$workers.scriptVersion?.id === r.version && e.$metadata?.traceId);
        if (joined.length !== 1) { alarmJoinComplete = false; missing.push({ kind: "alarm", key: requestKey(row), request: key, cardinality: joined.length }); }
        else {
          const edges = tagged.filter((log) => `${worker(log)}/${log.$metadata?.requestId}` === key);
          if (edges.some((log) => (log.$metadata?.traceId && log.$metadata.traceId !== joined[0].$metadata.traceId) || log.source.objectId !== joined[0].$workers.durableObjectId || log.source.version !== joined[0].$workers.scriptVersion?.id)) {
            alarmJoinComplete = false;
            missing.push({ kind: "alarm-context-ambiguity", key: requestKey(row), request: key });
          }
          alarms.push(joined[0]);
        }
      }
      const boundary = alarms.filter((event) => {
        const logs = (logsByRequest.get(`${row.worker}/${event.$metadata.requestId}`) ?? []).filter((log) => log.source?.cfLatency === "target-alarm");
        const ids = new Set(logs.map((log) => log.source.alarmId).filter(Boolean));
        const tags = r.alarmEvents ? returnedAlarmEvents.filter((event) => ids.has(event.alarmId)) : logs.map((log) => log.source);
        return [...ids].some((id) => returnedBoundaryIds.has(id)) || tags.length !== 2 || new Set(tags.map((event) => event.edge)).size !== 2 || !tags.some((event) => event.edge === "start") || !tags.some((event) => event.edge === "end") || tags.some((event) => !matches(event));
      });
      const boundaryTraces = new Set(boundary.map((event) => event.$metadata.traceId));
      const traces = new Set([trace, ...alarms.map((event) => event.$metadata.traceId)].filter(Boolean));
      const known = [...new Map([...traces].flatMap((id) => byTrace.get(`${row.worker}/${id}`) ?? []).filter((e) => ids.includes(e.$workers.durableObjectId)).map((e) => [eventKey(e), e])).values()];
      const boundaryInvocations = known.filter((event) => boundaryTraces.has(event.$metadata.traceId));
      const fullyAttributed = known.filter((event) => !boundaryTraces.has(event.$metadata.traceId));
      const cpuJoined = Boolean(main && trace && alarmJoinComplete && main.$workers.outcome === "ok" && main.$metadata?.traceId === trace);
      let transcriptValid = completeProvider;
      p.forEach((call, i) => {
        const queryValid = call && call.target === row.framework && call.object === row.object && call.sample === row.sample && call.history === r.history && call.ttftMs === r.ttftMs && call.chunkDelayMs === r.chunkDelayMs && Number(call.call) === i && call.syncBeforeFetch === r.syncBeforeFetch && call.variant === r.variant;
        if (!queryValid || !finite(call?.arrivalMs) || !finite(call?.endMs) || !finite(call?.firstByteMs) || call.firstByteMs < call.arrivalMs || call.endMs < call.firstByteMs) transcriptValid = false;
        if (call && (call.fingerprint !== expected[r.history]?.turns[row.sample]?.[i] || call.error != null || !queryValid)) {
          transcriptValid = false;
          fingerprintFailures.push({ key: requestKey(row), call: i, expected: expected[r.history]?.turns[row.sample]?.[i], observed: call.fingerprint, queryValid, error: call.error });
        }
      });
      if (!transcriptValid) reasons.push("nine complete, uniquely joined native provider requests with golden transcript hashes required");
      if (r.calls.length !== 9 || r.calls.some((call, i) => call.call !== i || call.status !== 200 || !call.sseDone || call.error !== undefined)) reasons.push("native stream completion receipt invalid");
      if (r.seedFingerprint !== expected[r.history]?.seed) reasons.push("seed fingerprint mismatch");
      const gaps = completeProvider ? p.slice(1).map((call, i) => call.arrivalMs - p[i].endMs) : [];
      const providerMs = completeProvider ? sum(p.map((call) => call.endMs - call.arrivalMs)) : null;
      const sql = r.sql;
      const actor = row.response.directory?.sql;
      const addRun = (key) => row.framework === "tardie" ? (finite(sql?.[key]) && finite(actor?.[key]) ? sql[key] + actor[key] : null) : (finite(sql?.[key]) ? sql[key] : null);
      const hasInitialization = isCold || r.constructorSql !== undefined || row.response.directoryStart?.constructorSql !== undefined;
      const addConstructor = (key) => !hasInitialization ? 0 : row.framework === "tardie"
        ? (finite(r.constructorSql?.[key]) && finite(row.response.directoryStart?.constructorSql?.[key]) ? r.constructorSql[key] + row.response.directoryStart.constructorSql[key] : null)
        : (finite(r.constructorSql?.[key]) ? r.constructorSql[key] : null);
      const add = (key) => finite(addRun(key)) && finite(addConstructor(key)) ? addRun(key) + addConstructor(key) : null;
      const coldCandidates = rows.filter((prior) => prior.phase === (row.phase === "minification" ? "minification-reset" : "cold-reset") &&
        (row.phase !== "minification" || (prior.sample === (query.get("minificationReset") ?? `minify-reset-${row.sample}`) &&
          prior.endedAt <= row.startedAt && prior.response?.before?.version === r.version)) &&
        prior.worker === row.worker && prior.framework === row.framework && prior.object === row.object);
      const cold = coldCandidates.length === 1 ? coldCandidates[0] : undefined;
      const freshIncarnation = isCold && cold?.response?.before?.objectId === r.objectId && cold.response.before.incarnation !== r.incarnation &&
        (row.phase !== "minification" || cold.response.before.version === r.version) &&
        (row.framework !== "tardie" || (cold.response.directoryBefore?.incarnation !== row.response.directory?.incarnation && cold.response.directoryBefore?.objectId === row.response.directory?.objectId));
      const construction = constructors.filter((event) => worker(event) === row.worker && event.source.objectId === r.objectId && event.source.incarnation === r.incarnation && event.source.version === r.version);
      const requestConstructed = construction.length === 1 && construction[0].$metadata?.requestId === main?.$metadata?.requestId && main !== undefined;
      const actorConstruction = row.framework !== "tardie" ? [] : constructors.filter((event) => worker(event) === row.worker && event.source.objectId === row.response.directory?.objectId && event.source.incarnation === row.response.directory?.incarnation && event.source.version === r.version);
      const actorRequestConstructed = row.framework !== "tardie" || (actorConstruction.length === 1 && actorConstruction[0].$metadata?.traceId === trace && actorConstruction[0].$workers?.event?.rpcMethod === "begin");
      const beforeIdentities = [cold?.response?.before, cold?.response?.directoryBefore].filter(Boolean);
      const requestedAbortLogged = freshIncarnation && beforeIdentities.every((id) => coldLogs.some((event) => worker(event) === row.worker && event.source.objectId === id.objectId && event.source.incarnation === id.incarnation && event.source.sample === cold.sample));
      const entryFresh = (entry) => entry?.firstHarnessRequest === true && entry.priorAlarmStarts === 0 && entry.activeAlarmIds?.length === 0;
      const freshRequestReceipt = entryFresh(r.entry) && (row.framework !== "tardie" || entryFresh(row.response.directoryStart?.entry));
      const expectedAborts = cold?.response?.ok === true && cold.response.coldRequested === true;
      // Response evidence survives platform log sampling. There is no external
      // traffic to these authenticated Objects between the acknowledged abort
      // and m0; lifetime counters reject any intervening alarm warmup.
      const constructorContradiction = [...construction, ...actorConstruction].some((event) => event.$workers?.eventType === "alarm" || (event.$metadata?.traceId && trace && event.$metadata.traceId !== trace)) || construction.some((event) =>
        (event.$metadata?.requestId && main?.$metadata?.requestId && event.$metadata.requestId !== main.$metadata.requestId) ||
        (event.$workers?.eventType !== undefined && event.$workers.eventType !== "fetch") ||
        (queryOf(event)?.path !== undefined && queryOf(event).path !== "/run")) || actorConstruction.some((event) =>
        (event.$workers?.event?.rpcMethod !== undefined && event.$workers.event.rpcMethod !== "begin") ||
        (event.$workers?.eventType !== undefined && !["rpc", "jsrpc"].includes(event.$workers.eventType)));
      // Final-run receipts are mandatory. A sampled constructor event must
      // never override an explicit negative lifetime/first-entry receipt.
      const coldVerified = Boolean(freshIncarnation && expectedAborts && actorIdentityStable && !constructorContradiction && freshRequestReceipt);
      const enabledSync = r.syncBeforeFetch ? sumKnown(r.calls.map((call) => difference(call.syncEndedMs, call.syncStartedMs))) : 0;
      const record = {
        key: requestKey(row), phase: row.phase, role: row.target, framework: row.framework,
        object: row.object, objectId: r.objectId, history: r.history, ttftMs: r.ttftMs,
        sample: row.sample, mode: isCold ? "cold" : row.sample === "m1" ? "settling" : "warm",
        minificationRound, minified,
        variant: r.syncBeforeFetch ? "sync" : r.variant,
        incarnation: r.incarnation, constructedMs: r.constructedMs, version: r.version,
        actorIdentityStable, actorIncarnation: row.response.directory?.incarnation ?? null, actorVersion: row.response.directory?.version ?? null,
        coldVerified, freshIncarnation, freshRequestReceipt, expectedAborts, constructorContradiction, requestConstructed, actorRequestConstructed, requestedAbortLogged,
        constructorInvocationIds: [...construction, ...actorConstruction].map((event) => event.$metadata?.requestId),
        transcriptValid, completeProvider, providerStreamReceipts: r.calls.filter((call) => call.providerReceipt).length, providerLogMatches, recordedAlarmCount: r.alarmEvents ? returnedAlarmIds.size : null, eligible: reasons.length === 0, reasons, cpuJoined,
        clientMs: row.clientWallMs, doWallMs: main?.$workers.wallTimeMs ?? null,
        outerWallMs: outer?.$workers.wallTimeMs ?? null,
        clientMinusDoMs: difference(row.clientWallMs, main?.$workers.wallTimeMs),
        mainWallMinusFetchCpuMs: difference(main?.$workers.wallTimeMs, main?.$workers.cpuTimeMs),
        doIoClockElapsedMs: r.doWallMs, fetchCpuMs: main?.$workers.cpuTimeMs ?? null,
        attributedDoCpuMs: cpuJoined ? sumKnown(fullyAttributed.map((e) => e.$workers.cpuTimeMs)) : null,
        observedDoCpuMs: cpuJoined ? sumKnown(known.map((e) => e.$workers.cpuTimeMs)) : null,
        alarmCpuMs: alarmJoinComplete ? sumKnown(alarms.map((e) => e.$workers.cpuTimeMs)) : null, alarmOverlapCount: alarmJoinComplete ? alarms.length : null,
        boundaryCpuMs: alarmJoinComplete ? sumKnown(boundaryInvocations.map((e) => e.$workers.cpuTimeMs)) : null,
        invocationIds: known.map(eventKey), boundaryInvocationIds: boundaryInvocations.map(eventKey),
        alarmInvocationIds: alarms.map(eventKey), providerRequestIds: r.calls.map((call) => call.providerRequest),
        providerMs, configuredProviderMs: 9 * r.ttftMs + 53 * r.chunkDelayMs,
        clientMinusScriptedMs: row.clientWallMs - (9 * r.ttftMs + 53 * r.chunkDelayMs),
        clientMinusProviderMs: providerMs === null ? null : row.clientWallMs - providerMs,
        clientReceiptBytes: Buffer.byteLength(JSON.stringify(row.response)),
        firstModelArrivalMs: completeProvider ? p[0].arrivalMs - row.startedAt : null,
        stepGapMs: median(gaps), stepGapTotalMs: completeProvider ? sum(gaps) : null,
        tailFromProviderMs: completeProvider ? row.endedAt - p.at(-1).endMs : null,
        providerColos: [...new Set(p.filter(Boolean).map((call) => call.colo))],
        ingressColo: outer?.$workers.event?.request?.cf?.colo ?? row.cfRay?.split("-").at(-1),
        sqlBindingBytes: add("writeBindingBytes"), kvJsonBytes: add("kvJsonBytes"),
        runCounts: Object.fromEntries(Object.keys(sql ?? {}).map((key) => [key, addRun(key)])),
        constructorCounts: Object.fromEntries(Object.keys(sql ?? {}).map((key) => [key, addConstructor(key)])),
        transactions: add("transactions"), transactionSync: add("transactionSync"),
        writeTransactions: add("writeTransactions"), writeTransactionSync: add("writeTransactionSync"), overlappingTransactionCallbacks: add("overlappingTransactionCallbacks"), transactionRollbacks: add("transactionRollbacks"), transactionWindowCrossings: add("transactionWindowCrossings"),
        nativeSyncCalls: add("syncCalls"), nativeSyncWaitMs: add("syncWaitMs"),
        explicitProbeSyncMs: enabledSync,
        logicalMutationStatements: add("mutationStatements"),
        setAlarmCalls: add("setAlarmCalls"), deleteAlarmCalls: add("deleteAlarmCalls"),
        requestBytes: completeProvider ? sum(p.map((call) => call.requestBytes)) : null,
        responseBytes: sum(r.calls.map((call) => call.responseBytes)),
        steps: r.calls.map((call, i) => ({
          call: i, gapBeforeMs: i && completeProvider ? gaps[i - 1] : null,
          providerMs: p[i] ? p[i].endMs - p[i].arrivalMs : null,
          fingerprint: p[i]?.fingerprint, wireFingerprint: p[i]?.rawWireFingerprint,
          fetchToHeadersMs: call.headersMs - call.fetchStartedMs,
          sql: call.sqlSincePreviousCall,
          sqlAfterPreviousStream: i === 0 || !r.calls[i - 1].sqlAtEnd ? null : Object.fromEntries(Object.keys(call.sqlAtStart).map((key) => [key, difference(call.sqlAtStart[key], r.calls[i - 1].sqlAtEnd[key])])),
          syncProbeMs: call.syncStartedMs === undefined ? null : call.syncEndedMs - call.syncStartedMs,
        })),
      };
      records.push(record);
    }
    const completeMainCohorts = new Set([...group(records.filter((r) => r.phase === "network"), (r) => `${r.role}/${r.framework}/${r.object}`)].filter(([, rs]) => rs.length === 6 && ["m0", "m1", "m2", "m3", "m4", "m5"].every((sample) => rs.filter((r) => r.sample === sample).length === 1) && rs.every((r) => r.eligible) && new Set(rs.map((r) => r.incarnation)).size === 1 && new Set(rs.map((r) => r.version)).size === 1 && new Set(rs.map((r) => r.actorIncarnation)).size === 1 && new Set(rs.map((r) => r.actorVersion)).size === 1).map(([key]) => key));
    const admitted = records.filter((r) => r.eligible && completeMainCohorts.has(`${r.role}/${r.framework}/${r.object}`));
    const measured = admitted.filter((r) => r.phase === "network" && (r.mode === "warm" || (r.mode === "cold" && r.coldVerified)));
    const groups = [...group(measured, (r) => `${r.role}/${r.framework}/h${r.history}/d${r.ttftMs}/${r.mode}`).entries()].map(([key, values]) => ({
      key, role: values[0].role, framework: values[0].framework, history: values[0].history, ttftMs: values[0].ttftMs, mode: values[0].mode,
      objects: new Set(values.map((r) => r.object)).size, turns: values.length,
      verifiedTranscripts: values.filter((r) => r.transcriptValid).length,
      telemetryCoverage: { mainFetches: values.filter((r) => finite(r.fetchCpuMs)).length, joinedTurnCpu: values.filter((r) => r.cpuJoined).length, returnedProviderReceipts: sum(values.map((r) => r.providerStreamReceipts)), matchedProviderLogs: sum(values.map((r) => r.providerLogMatches)) },
      metrics: objectSummaries(values),
      ingressColos: [...new Set(values.map((r) => r.ingressColo))],
    }));
    const completeVariantCohorts = new Set([...group(admitted.filter((r) => r.phase === "network-variant"), (r) => `${r.role}/${r.framework}/${r.object}`)].filter(([, rs]) => rs.length === 6 && ["m8", "m9", "m10", "m11", "m12", "m13"].every((sample) => rs.filter((r) => r.sample === sample).length === 1) && ["baseline", "defer-wakes", "sync"].every((variant) => rs.filter((r) => r.variant === variant).length === 2) && rs.every((r) => admitted.some((main) => main.phase === "network" && main.role === r.role && main.framework === r.framework && main.object === r.object && main.incarnation === r.incarnation && main.version === r.version))).map(([key]) => key));
    const admittedVariants = admitted.filter((r) => r.phase === "network-variant" && completeVariantCohorts.has(`${r.role}/${r.framework}/${r.object}`));
    const variants = [...group(admittedVariants, (r) => `h${r.history}/${r.variant}`).entries()].map(([key, values]) => ({ key, metrics: objectSummaries(values) }));
    const pairedVariants = [];
    for (const h of [50, 250]) for (const variant of ["defer-wakes", "sync"]) {
      const units = [];
      const all = admittedVariants.filter((r) => r.history === h);
      for (const [object, values] of group(all, (r) => r.object)) {
        const base = values.filter((r) => r.variant === "baseline");
        const changed = values.filter((r) => r.variant === variant);
        if (!base.length || !changed.length) continue;
        units.push({ object, ...Object.fromEntries(metricNames.map((metric) => [metric, difference(median(base.map((r) => r[metric])), median(changed.map((r) => r[metric])))])) });
      }
      pairedVariants.push({ history: h, variant, positiveMeans: "baseline minus variant; positive is saved", units, metrics: Object.fromEntries(metricNames.map((key) => [key, stats(units.map((r) => r[key]))])) });
    }
    const controls = [];
    for (const h of [50, 250]) for (const d of [0, 400]) for (const mode of ["cold", "warm"]) {
      const units = [];
      const all = measured.filter((r) => r.framework === "yielded" && r.history === h && r.ttftMs === d && r.mode === mode);
      for (const [object, values] of group(all, (r) => r.object)) {
        const primary = values.filter((r) => r.role === "primary");
        const control = values.filter((r) => r.role === "control");
        if (!primary.length || !control.length) continue;
        units.push({ object, ...Object.fromEntries(metricNames.map((metric) => [metric, difference(median(primary.map((r) => r[metric])), median(control.map((r) => r[metric])))])) });
      }
      controls.push({ history: h, ttftMs: d, mode, units, metrics: Object.fromEntries(metricNames.map((key) => [key, stats(units.map((r) => r[key]))])) });
    }
    const coldPremiums = [];
    for (const framework of ["yielded", "pi", "tardie"]) for (const h of [50, 250]) for (const d of [0, 400]) {
      const all = measured.filter((r) => r.role === "primary" && r.framework === framework && r.history === h && r.ttftMs === d);
      const units = [];
      for (const [object, values] of group(all, (r) => r.object)) {
        const cold = values.filter((r) => r.mode === "cold");
        const warm = values.filter((r) => r.mode === "warm");
        if (cold.length !== 1 || warm.length !== 4) continue;
        units.push({ object, ...Object.fromEntries(metricNames.map((key) => [key, difference(cold[0][key], median(warm.map((r) => r[key])))])) });
      }
      coldPremiums.push({ framework, history: h, ttftMs: d, positiveMeans: "cold minus median of four warm turns in the same Object", units, metrics: Object.fromEntries(metricNames.map((key) => [key, stats(units.map((r) => r[key]))])) });
    }
    const clocks = rows.filter((r) => r.phase === "clock-calibration" && r.status === 200).map((r) => ({
      sample: r.sample, roundTripMs: r.clientWallMs, colo: r.response.colo,
      offsetLowerMs: r.response.arrivalMs - r.endedAt, offsetUpperMs: r.response.arrivalMs - r.startedAt,
    }));
    const outcomes = [...group(invocations, (e) => `${worker(e)}/${e.$workers.executionModel}/${e.$workers.eventType}/${e.$workers.outcome}`).entries()].map(([key, events]) => ({ key, count: events.length }));
    const failedOutcomes = invocations.filter((e) => e.$workers.outcome !== "ok").map((e) => ({
      id: eventKey(e), worker: worker(e), timestamp: e.timestamp, type: e.$workers.eventType,
      outcome: e.$workers.outcome, objectId: e.$workers.durableObjectId, query: queryOf(e),
      requestId: e.$metadata.requestId, traceId: e.$metadata.traceId,
      logs: (logsByRequest.get(`${worker(e)}/${e.$metadata?.requestId}`) ?? []).map((log) => log.source),
    }));
    const failedRequests = rows.filter((r) => r.status !== 200).map(({ response, ...row }) => ({ ...row, error: row.error ?? response?.error ?? response }));
    const summary = {
      accountName: resources.accountName, generatedAt: new Date().toISOString(),
      unit: "median per Object, then median [Q1–Q3] across Objects; cold is one observation per Object",
      reference: expected, groups, variants, pairedVariants, controls, coldPremiums, clocks, outcomes,
      missing, fingerprintFailures,
      excluded: records.filter((r) => !r.eligible || (r.mode === "cold" && !r.coldVerified) || (r.phase === "network" && !completeMainCohorts.has(`${r.role}/${r.framework}/${r.object}`)) || (r.phase === "network-variant" && !completeVariantCohorts.has(`${r.role}/${r.framework}/${r.object}`))).map((r) => ({ key: r.key, phase: r.phase, reasons: [...r.reasons, ...(r.phase === "network" && !completeMainCohorts.has(`${r.role}/${r.framework}/${r.object}`) ? ["incomplete or changed-incarnation/version six-turn cohort"] : []), ...(r.phase === "network-variant" && !completeVariantCohorts.has(`${r.role}/${r.framework}/${r.object}`) ? ["incomplete or changed-incarnation/version variant cohort"] : [])], coldVerified: r.coldVerified, requestConstructed: r.requestConstructed, actorRequestConstructed: r.actorRequestConstructed })),
      coverage: { requests: rows.length, telemetry: unique.length, measuredTurns: measured.length,
        uniqueProviderLogReceipts: provider.size,
        mainReturnedProviderReceipts: sum(records.filter((r) => r.phase === "network" && r.eligible).map((r) => r.providerStreamReceipts)),
        validColdResets: measured.filter((r) => r.mode === "cold" && r.coldVerified).length },
      probe: analyzeProbe(rows, unique),
      instrumentation: analyzeInstrumentation(rows, expected, load("instrumentation-completed.json", [])),
      minification: analyzeMinification(records, load("minification.json", null), metricNames, load("minification-metadata.json", null)),
    };
    save("summary.json", summary);
    save("failed-outcomes.json", { outcomes, failedOutcomes, failedRequests });
    writeFileSync(join(here, "turns.jsonl"), records.map((record) => JSON.stringify(record)).join("\n") + "\n");
    const fmt = (value) => value?.median === null || !value ? "—" : `${value.median.toFixed(0)} [${value.q1.toFixed(0)}–${value.q3.toFixed(0)}]`;
    const table = ["| Target | History | Provider TTFT | State | Objects | Client ms | Client − scripted ms | DO wall ms† | Observed DO CPU ms† | Boundary CPU ms† | Step gap ms* | First request ms* |", "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|"];
    for (const g of groups) table.push(`| ${g.role === "control" ? "control " : ""}${g.framework} | ${g.history} | ${g.ttftMs} | ${g.mode} | ${g.objects} | ${fmt(g.metrics.clientMs)} | ${fmt(g.metrics.clientMinusScriptedMs)} | ${fmt(g.metrics.doWallMs)} | ${fmt(g.metrics.attributedDoCpuMs)} | ${fmt(g.metrics.boundaryCpuMs)} | ${fmt(g.metrics.stepGapMs)} | ${fmt(g.metrics.firstModelArrivalMs)} |`);
    table.push("", "† Available invocation telemetry only; CPU excludes boundary traces shown separately and can omit sampled descendants. Neither column is an exact critical-path allocation. See the accompanying summary’s coverage, missing and per-metric n.", "", "* Timestamp differences across I/O clocks, not CPU-independent synchronized wall clocks. Client totals use the controller's monotonic clock. Scripted provider time is 0 or 4,130 ms per turn.");
    writeFileSync(join(here, "network-table.md"), table.join("\n") + "\n");
    console.log(JSON.stringify({ groups: groups.length, coverage: summary.coverage, missing: missing.length, fingerprintFailures: fingerprintFailures.length, failures: failedRequests.length }));
  },
  catch: (cause) => new AnalysisError({ message: String(cause) }),
});
NodeRuntime.runMain(run);
