import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Offline reduction of deployed telemetry; no local workload or timers.
const here = dirname(fileURLToPath(import.meta.url));
const hosted = join(here, "hosted");
const readText = (path) => existsSync(path) ? readFileSync(path, "utf8") : gunzipSync(readFileSync(path + ".gz")).toString();
const load = (path) => JSON.parse(readText(path));
const save = (name, value) => writeFileSync(join(here, name), JSON.stringify(value, null, 2) + "\n");
const resources = load(join(hosted, "resources.json"));
const plan = load(join(hosted, "plan.json"));
const seeds = load(join(hosted, "seeds.json"));
const fingerprints = load(join(hosted, "fingerprints.json"));
const requests = readText(join(hosted, "requests.jsonl")).trim().split("\n").map(JSON.parse);
const events = Object.fromEntries(resources.targets.map((target) => [target.role, load(join(hosted, `telemetry-${target.role}.json`)).events]));
const quantile = (values, fraction) => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b), p = (sorted.length - 1) * fraction, i = Math.floor(p);
  return sorted[i] + (sorted[Math.min(i + 1, sorted.length - 1)] - sorted[i]) * (p - i);
};
const stats = (values) => ({ n: values.length, median: quantile(values, 0.5), q1: quantile(values, 0.25), q3: quantile(values, 0.75), values });
const samples = [];
for (const item of plan.schedule) {
  const target = resources.targets.find((row) => row.role === item.target);
  const records = requests.filter((row) => row.phase === "measured" && row.target === item.target && row.input?.sampleId === item.input.sampleId);
  const request = records[0];
  const complete = fingerprints.find((row) => row.target === item.target && row.round === item.round);
  const seeded = seeds.find((row) => row.target === item.target && row.round === item.round);
  const rows = events[item.target];
  const logs = rows.filter((row) => row.metadataType === "cf-worker" && row.evalCostSample === item.input.sampleId);
  const ingress = rows.filter((row) => row.metadataType === "cf-worker-event" && row.executionModel === "stateless" && row.sample === item.input.sampleId);
  const trace = logs[0]?.traceId ?? ingress[0]?.traceId;
  const invocations = rows.filter((row) => row.metadataType === "cf-worker-event" && row.executionModel === "durableObject" && row.traceId === trace && (row.rpcMethods ?? []).includes("measured"));
  const invocation = invocations.length === 1 ? invocations[0] : undefined;
  const reasons = [];
  if (records.length !== 1 || request?.status !== 200) reasons.push(request?.error ?? `Response cardinality/status ${records.length}/${request?.status}`);
  if (logs.length > 1 || ingress.length > 1 || logs.length + ingress.length === 0 || invocations.length !== 1) reasons.push(`Telemetry cardinality log/ingress/DO ${logs.length}/${ingress.length}/${invocations.length}`);
  if (!trace || (logs.length && ingress.length && logs[0].traceId !== ingress[0].traceId)) reasons.push("Missing or conflicting sample trace");
  if (invocation && (!invocation.id || invocation.rpcCallCount !== 1 || invocation.outcome !== "ok" || invocation.truncated || !Number.isFinite(invocation.cpuTimeMs) || invocation.cpuTimeMs < 0)) reasons.push("Invalid invocation CPU/outcome/cardinality");
  if (request?.status === 200) {
    const receipt = request.response;
    if (receipt.sampleId !== item.input.sampleId || receipt.turn.id !== item.input.turn.id || receipt.turn.text !== item.input.turn.text) reasons.push("Receipt input mismatch");
    if (receipt.generation !== "measure" || receipt.build !== target.build.buildId || receipt.fixture !== target.build.fixtureSha256 || receipt.version !== target.identity.version || receipt.objectId !== seeded?.receipt.objectId) reasons.push("Receipt deployment/seed identity mismatch");
    if (complete && (receipt.instance !== complete.opened.instance || receipt.incarnation !== complete.opened.incarnation)) reasons.push("Runtime changed within measured cohort");
    if (invocation && (invocation.scriptName !== target.name || invocation.scriptVersion?.id !== receipt.version || invocation.durableObjectId !== receipt.objectId)) reasons.push("DO invocation differs from receipt");
    if ([...logs, ...ingress].some((anchor) => anchor.scriptName !== target.name || anchor.scriptVersion?.id !== receipt.version || (anchor.executionModel === "durableObject" && anchor.durableObjectId !== receipt.objectId))) reasons.push("Sample anchor differs from receipt");
  }
  if (seeded?.receipt.fingerprint !== "b017b487524e44a4") reasons.push("Historical fingerprint mismatch");
  if (complete && complete.receipt.fingerprint !== "b73859cee894aca6") reasons.push("Measured fingerprint mismatch");
  samples.push({ ...item, validCpu: reasons.length === 0, completeCohort: Boolean(complete), reasons,
    invocationId: invocation?.id, trace, outcome: invocation?.outcome ?? "unobserved", cpuMs: invocation?.cpuTimeMs ?? null, telemetryDoWallMs: invocation?.wallTimeMs ?? null,
    clientWallMs: request?.clientWallMs ?? null, doWallMs: request?.response?.doWallMs ?? null, receipt: request?.response ?? null,
    responseStatus: request?.status ?? null, anchors: { log: logs.length, ingress: ingress.length }, attempted: records.length > 0 });
}
const claims = new Map();
for (const row of samples) if (row.invocationId) claims.set(row.invocationId, (claims.get(row.invocationId) ?? 0) + 1);
for (const row of samples) if (claims.get(row.invocationId) > 1) { row.validCpu = false; row.reasons.push("Invocation claimed by multiple samples"); }
const valid = samples.filter((row) => row.validCpu && row.completeCohort);
const groups = [];
for (const target of resources.targets) for (let turn = 0; turn < 10; turn++) {
  const rows = valid.filter((row) => row.target === target.role && row.turn === turn);
  groups.push({ target: target.role, turn, precedingHistory: 50 + turn, rounds: rows.map((row) => row.round), cpuMs: stats(rows.map((row) => row.cpuMs)),
    clientWallMs: stats(rows.map((row) => row.clientWallMs)), doWallMs: stats(rows.map((row) => row.doWallMs)), telemetryDoWallMs: stats(rows.map((row) => row.telemetryDoWallMs)) });
}
const comparisons = Array.from({ length: 10 }, (_, turn) => {
  const pairs = valid.filter((row) => row.target === "pin" && row.turn === turn).flatMap((row) => {
    const other = valid.find((candidate) => candidate.target === "pin-control" && candidate.round === row.round && candidate.turn === turn);
    return other && row.cpuMs > 0 ? [{ round: row.round, ratio: other.cpuMs / row.cpuMs }] : [];
  });
  return { turn, rounds: pairs.map((row) => row.round), controlOverPin: stats(pairs.map((row) => row.ratio)) };
});
const warmCohorts = fingerprints.map((cohort) => {
  const rows = valid.filter((row) => row.target === cohort.target && row.round === cohort.round && row.turn > 0);
  return { target: cohort.target, round: cohort.round, cpuMs: stats(rows.map((row) => row.cpuMs)), clientWallMs: stats(rows.map((row) => row.clientWallMs)), doWallMs: stats(rows.map((row) => row.doWallMs)) };
});
const result = { planned: plan.schedule.length, attempted: samples.filter((row) => row.attempted).length, successfulReceipts: samples.filter((row) => row.responseStatus === 200).length,
  joinedCpu: samples.filter((row) => row.validCpu).length, includedCpu: valid.length, completeCohorts: fingerprints.length,
  problems: samples.filter((row) => !row.validCpu || !row.completeCohort).map((row) => ({ target: row.target, round: row.round, turn: row.turn, completeCohort: row.completeCohort, attempted: row.attempted, reasons: row.reasons, outcome: row.outcome })),
  definitions: { firstTurn: "m0: exactly 50 prior historical turns, fresh runtime and completed recovery in a separate RPC; measured CPU includes only the turn RPC", laterWarm: "m1-m9 have 51-59 prior turns and reuse the opened runtime; no resetting or reusing unknown calls", inclusion: "CPU requires a unique anchor/trace/DO invocation with exact identities. Comparisons use only cohorts with both historical and final measured fingerprints. Partial cohort remains in samples.json; it is never replayed.", quartiles: "Linear interpolation of observed ordered values; not confidence intervals" },
  groups, comparisons, warmCohorts,
  warmSummary: resources.targets.map((target) => ({ target: target.role, medianOfObjectWarmMediansMs: stats(warmCohorts.filter((row) => row.target === target.role && row.cpuMs.n >= 5).map((row) => row.cpuMs.median)) })),
  outcomes: Object.fromEntries(resources.targets.map((target) => [target.role, events[target.role].filter((row) => row.metadataType === "cf-worker-event").reduce((out, row) => { out[row.outcome] = (out[row.outcome] ?? 0) + 1; return out; }, {})])),
};
save("samples.json", samples); save("summary.json", result);
console.log(JSON.stringify({ planned: result.planned, attempted: result.attempted, successfulReceipts: result.successfulReceipts, joinedCpu: result.joinedCpu, includedCpu: result.includedCpu, completeCohorts: result.completeCohorts, firstTurn: groups.filter((row) => row.turn === 0), warmSummary: result.warmSummary }, null, 2));
if (result.problems.length) process.exitCode = 1;
