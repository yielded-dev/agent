import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Deterministic reduction only. This script never executes a timed workload.
const here = dirname(fileURLToPath(import.meta.url));
const hosted = join(here, "hosted");
const readText = (name) => existsSync(name) ? readFileSync(name, "utf8") : gunzipSync(readFileSync(name + ".gz")).toString();
const load = (name) => JSON.parse(readText(name));
const save = (name, value) => writeFileSync(name, JSON.stringify(value, null, 2) + "\n");
const requests = readText(join(hosted, "requests.jsonl")).trim().split("\n").map(JSON.parse);
const plan = load(join(hosted, "plan.json"));
const resources = load(join(hosted, "resources.json"));
const roles = resources.targets.map((target) => target.role);
const events = Object.fromEntries(roles.map((role) => [role, load(join(hosted, `telemetry-${role}.json`)).events]));
const counts = Object.fromEntries(["pin", "base", "head"].map((name) => [name, load(join(here, "counts", `${name}.json`))]));
const count = (role, shape, mode, n) => counts[role.replace("-control", "")].find((row) => row.case === shape && row.mode === mode && row.iterations === n);
const quantile = (values, fraction) => {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const position = (sorted.length - 1) * fraction;
  const i = Math.floor(position);
  return sorted[i] + (sorted[Math.min(i + 1, sorted.length - 1)] - sorted[i]) * (position - i);
};
const stats = (values) => ({ n: values.length, median: quantile(values, 0.5), q1: quantile(values, 0.25), q3: quantile(values, 0.75), min: values.length ? Math.min(...values) : null, max: values.length ? Math.max(...values) : null, values });
const samples = [];
const problems = [];
const warnings = [];
for (const scheduled of plan.schedule) {
  const matches = requests.filter((row) => row.phase === "measured" && row.target === scheduled.target && row.input?.id === scheduled.input.id);
  if (matches.length !== 1) { problems.push({ scheduled, problem: `Expected one response record, found ${matches.length}` }); continue; }
  const request = matches[0];
  const logs = events[scheduled.target].filter((row) => row.metadataType === "cf-worker" && row.evalCostSample === scheduled.input.id);
  const ingress = events[scheduled.target].filter((row) => row.metadataType === "cf-worker-event" && row.executionModel === "stateless" && row.sample === scheduled.input.id);
  const trace = logs[0]?.traceId ?? ingress[0]?.traceId;
  const invocation = events[scheduled.target].filter((row) => row.metadataType === "cf-worker-event" && row.executionModel === "durableObject" && row.traceId === trace && (row.rpcMethods ?? [row.rpcMethod]).includes("run"));
  const event = invocation.length === 1 ? invocation[0] : undefined;
  const target = resources.targets.find((row) => row.role === scheduled.target);
  let valid = request.status === 200;
  const invalidReasons = [];
  if (!valid) invalidReasons.push(request.error ?? `HTTP ${request.status}`);
  if (logs.length > 1 || ingress.length > 1 || logs.length + ingress.length === 0 || invocation.length !== 1) { valid = false; invalidReasons.push(`Telemetry cardinality logs/ingress/DO ${logs.length}/${ingress.length}/${invocation.length}`); }
  if (!trace || (logs.length && ingress.length && logs[0].traceId !== ingress[0].traceId)) { valid = false; invalidReasons.push("Available sample anchors have missing or inconsistent trace identities"); }
  if (event && !event.id) { valid = false; invalidReasons.push("Missing DO invocation ID"); }
  if (event && (event.outcome !== "ok" || event.truncated || !Number.isFinite(event.cpuTimeMs) || event.cpuTimeMs < 0 || event.rpcCallCount !== 1)) { valid = false; invalidReasons.push(`Invocation ${event.outcome}, truncated=${event.truncated}, rpcCallCount=${event.rpcCallCount}`); }
  if (event && request.response && (event.scriptVersion?.id !== request.response.version || event.durableObjectId !== request.response.objectId)) { valid = false; invalidReasons.push("Invocation identity differs from receipt"); }
  if (request.status === 200) {
    if (["id", "case", "mode", "iterations"].some((key) => request.response[key] !== scheduled.input[key])) { valid = false; invalidReasons.push("Echoed workload differs from plan"); }
    if (request.worker !== target.name || request.response.build !== target.build.buildId || request.response.fixture !== target.build.fixtureSha256 || request.response.version !== target.identity.version || request.response.objectId !== target.identity.objectId || event?.scriptName !== target.name) { valid = false; invalidReasons.push("Receipt or telemetry differs from recorded deployment"); }
    if ([...logs, ...ingress].some((anchor) => anchor.scriptName !== target.name || anchor.scriptVersion?.id !== request.response.version || (anchor.executionModel === "durableObject" && anchor.durableObjectId !== request.response.objectId))) { valid = false; invalidReasons.push("Sample anchor differs from the receipt identity"); }
    const n = scheduled.input.iterations;
    const remainder = n % 256;
    const expected = count(scheduled.target, scheduled.input.case, scheduled.input.mode, n)?.checksum ?? (scheduled.input.case === "empty" ? ((Math.floor(n / 256) * 32640 + remainder * (remainder - 1) / 2) | 0) : undefined);
    if (expected === undefined || request.response.checksum !== expected) { valid = false; invalidReasons.push("Checksum differs from deterministic capture"); }
  }
  const sample = { ...scheduled, valid, invalidReasons, invocationId: event?.id, traceId: trace, cpuMs: event?.cpuTimeMs ?? null, workerWallMs: event?.wallTimeMs ?? null,
    ingressCpuMs: ingress[0]?.cpuTimeMs ?? null, ingressWallMs: ingress[0]?.wallTimeMs ?? null, clientWallMs: request.clientWallMs,
    doWallMs: request.response?.doWallMs ?? null, version: request.response?.version, objectId: request.response?.objectId,
    incarnation: request.response?.incarnation, instance: request.response?.instance, checksum: request.response?.checksum, outcome: event?.outcome ?? "unobserved", startedAt: request.startedAt, endedAt: request.endedAt, requestStatus: request.status,
    anchors: { log: logs.length, ingress: ingress.length }, receiptWallValid: request.status === 200 && Number.isFinite(request.clientWallMs) && Number.isFinite(request.response?.doWallMs) };
  samples.push(sample);
  if (valid && (logs.length === 0 || ingress.length === 0)) warnings.push({ target: scheduled.target, id: scheduled.input.id, missingAnchor: logs.length === 0 ? "structured log" : "ingress invocation", retained: "unique other anchor, trace, DO invocation and receipt identities agree" });
  if (!valid) problems.push({ scheduled, invalidReasons, cpuMs: sample.cpuMs, outcome: sample.outcome });
}
const invocationClaims = new Map();
for (const sample of samples) if (sample.invocationId) invocationClaims.set(sample.invocationId, (invocationClaims.get(sample.invocationId) ?? 0) + 1);
for (const sample of samples) if (invocationClaims.get(sample.invocationId) > 1) {
  sample.valid = false; sample.invalidReasons.push("DO invocation ID has multiple claimants");
  problems.push({ scheduled: sample.input, target: sample.target, problem: "DO invocation ID has multiple claimants" });
}
save(join(here, "samples.json"), samples);
const validSamples = samples.filter((sample) => sample.valid);
const baseline = (sample) => {
  const row = validSamples.find((row) => row.target === sample.target && row.round === sample.round && row.input.case === "empty" && row.input.mode === "plain" && row.input.iterations === sample.input.iterations);
  return row?.cpuMs;
};
const pairedRows = [];
for (const sample of validSamples.filter((row) => row.input.mode === "effect")) {
  const empty = baseline(sample);
  if (!Number.isFinite(empty)) { problems.push({ scheduled: sample.input, target: sample.target, problem: "No finite exact-N empty-loop baseline" }); continue; }
  const plain = validSamples.find((row) => row.target === sample.target && row.round === sample.round && row.input.case === sample.input.case && row.input.mode === "plain" && row.input.iterations === sample.input.iterations);
  pairedRows.push({ ...sample, netCpuMs: sample.cpuMs - empty, extraOverPlainMs: plain ? sample.cpuMs - plain.cpuMs : null });
}
const groups = [];
for (const role of roles) for (const shape of [...new Set(samples.map((sample) => sample.input.case))]) for (const n of [...new Set(samples.filter((sample) => sample.input.case === shape).map((sample) => sample.input.iterations))]) {
  const effect = validSamples.filter((sample) => sample.target === role && sample.input.case === shape && sample.input.mode === "effect" && sample.input.iterations === n);
  const plain = validSamples.filter((sample) => sample.target === role && sample.input.case === shape && sample.input.mode === "plain" && sample.input.iterations === n);
  if (effect.length === 0) continue;
  const total = count(role, shape, "effect", n);
  const zero = count(role, shape, "effect", 0);
  const marginalE = total.evaluations - zero.evaluations;
  const marginalA = total.allocations - zero.allocations;
  const marginalI = total.inlineSuccesses - zero.inlineSuccesses;
  const net = effect.flatMap((sample) => baseline(sample) === undefined ? [] : [(sample.cpuMs - baseline(sample)) * 1e6]);
  const extra = effect.flatMap((sample) => {
    const paired = plain.find((row) => row.round === sample.round);
    return paired ? [(sample.cpuMs - paired.cpuMs) * 1e6 / n] : [];
  });
  groups.push({ role, case: shape, iterations: n, evaluations: total.evaluations, allocations: total.allocations,
    marginalEvaluations: marginalE, marginalAllocations: marginalA, marginalInlineSuccesses: marginalI,
    baselinePairsComplete: net.length === effect.length && effect.length === plan.rounds,
    evaluationsPerIteration: marginalE / n, allocationsPerIteration: marginalA / n,
    effectCpuMs: stats(effect.map((sample) => sample.cpuMs)), plainCpuMs: stats(plain.map((sample) => sample.cpuMs)),
    baselineSubtractedCpuMs: stats(net.map((ns) => ns / 1e6)), nsPerIteration: stats(net.map((ns) => ns / n)),
    nsPerEvaluation: marginalE > 0 ? stats(net.map((ns) => ns / marginalE)) : null,
    nsPerAllocation: marginalA > 0 ? stats(net.map((ns) => ns / marginalA)) : null,
    extraOverPlainNsPerIteration: stats(extra),
    clientWallMs: stats(effect.map((sample) => sample.clientWallMs)), workerWallMs: stats(effect.map((sample) => sample.workerWallMs)), doWallMs: stats(effect.map((sample) => sample.doWallMs)) });
}
const pairedRatios = (a, b, shape, n, rounds) => validSamples.filter((row) => row.target === a && row.input.case === shape && row.input.mode === "effect" && row.input.iterations === n && (!rounds || rounds.includes(row.round))).flatMap((row) => {
  const other = validSamples.find((candidate) => candidate.target === b && candidate.input.case === shape && candidate.input.mode === row.input.mode && candidate.input.iterations === n && candidate.round === row.round);
  return other?.cpuMs > 0 ? [row.cpuMs / other.cpuMs] : [];
});
const comparisons = groups.filter((group) => group.role === "base").map((group) => {
  const rounds = Array.from({ length: plan.rounds }, (_, round) => round).filter((round) => ["base", "head", "base-control"].every((role) => validSamples.some((row) => row.target === role && row.round === round && row.input.case === group.case && row.input.mode === "effect" && row.input.iterations === group.iterations && row.cpuMs > 0)));
  const candidate = stats(pairedRatios("head", "base", group.case, group.iterations, rounds));
  const control = stats(pairedRatios("base-control", "base", group.case, group.iterations, rounds));
  const pinControl = stats(pairedRatios("pin-control", "pin", group.case, group.iterations));
  const controlEnvelope = control.n ? Math.max(Math.abs(1 - control.q1), Math.abs(1 - control.q3), control.q3 - control.q1) : null;
  return { case: group.case, iterations: group.iterations, matchedRounds: rounds, headOverBase: candidate, headOverControl: stats(pairedRatios("head", "base-control", group.case, group.iterations, rounds)), controlOverBase: control, pinControlOverPin: pinControl,
    controlEnvelope, exceedsControlEnvelope: rounds.length === plan.rounds && Number.isFinite(controlEnvelope) && candidate.median < 1 && 1 - candidate.median > controlEnvelope };
});

// Paired 1x -> 4x slopes remove fixed runner/setup work. The original ratios
// remain visible, including the delayed exact-N empty baselines in the main run.
const slopeGroups = [];
for (const role of roles) for (const shape of [...new Set(groups.map((row) => row.case))]) {
  const sizes = [...new Set(groups.filter((row) => row.role === role && row.case === shape).map((row) => row.iterations))].sort((a, b) => a - b);
  if (sizes.length !== 2) continue;
  const [lowN, highN] = sizes;
  const lowCount = count(role, shape, "effect", lowN), highCount = count(role, shape, "effect", highN);
  const e = highCount.evaluations - lowCount.evaluations, a = highCount.allocations - lowCount.allocations;
  const roundValues = [];
  for (let round = 0; round < plan.rounds; round++) {
    const low = pairedRows.find((row) => row.target === role && row.input.case === shape && row.input.iterations === lowN && row.round === round);
    const high = pairedRows.find((row) => row.target === role && row.input.case === shape && row.input.iterations === highN && row.round === round);
    if (!low || !high) continue;
    const ns = (high.netCpuMs - low.netCpuMs) * 1e6;
    roundValues.push({ round, nsPerIteration: ns / (highN - lowN), nsPerEvaluation: e > 0 ? ns / e : null, nsPerAllocation: a > 0 ? ns / a : null,
      extraOverPlainNsPerIteration: Number.isFinite(low.extraOverPlainMs) && Number.isFinite(high.extraOverPlainMs) ? (high.extraOverPlainMs - low.extraOverPlainMs) * 1e6 / (highN - lowN) : null });
  }
  slopeGroups.push({ role, case: shape, lowN, highN, roundValues, baselinePairsComplete: roundValues.length === plan.rounds,
    evaluationsPerIteration: e / (highN - lowN), allocationsPerIteration: a / (highN - lowN),
    nsPerIteration: stats(roundValues.map((row) => row.nsPerIteration)), nsPerEvaluation: e > 0 ? stats(roundValues.map((row) => row.nsPerEvaluation)) : null,
    nsPerAllocation: a > 0 ? stats(roundValues.map((row) => row.nsPerAllocation)) : null,
    extraOverPlainNsPerIteration: stats(roundValues.map((row) => row.extraOverPlainNsPerIteration).filter(Number.isFinite)) });
}

// A diagnostic model, not an assertion of a universal cost. All fits operate on
// per-iteration medians so large-N cases cannot manufacture correlation.
const dot = (a, b) => a.reduce((sum, value, i) => sum + value * b[i], 0);
const fit = (rows, metric) => {
  if (rows.length < 2 || rows.some((row) => !Number.isFinite(row.nsPerIteration.median) || !Number.isFinite(row.evaluationsPerIteration) || !Number.isFinite(row.allocationsPerIteration))) return null;
  const y = rows.map((row) => row.nsPerIteration.median);
  const e = rows.map((row) => row.evaluationsPerIteration);
  const a = rows.map((row) => row.allocationsPerIteration);
  const ee = dot(e, e), aa = dot(a, a), ea = dot(e, a), ey = dot(e, y), ay = dot(a, y);
  const candidates = [];
  if (metric !== "allocations" && ee > 0) candidates.push({ eWeight: Math.max(0, ey / ee), aWeight: 0 });
  if (metric !== "evaluations" && aa > 0) candidates.push({ eWeight: 0, aWeight: Math.max(0, ay / aa) });
  const det = ee * aa - ea * ea;
  if (metric === "joint" && det > 1e-8 * ee * aa) {
    const eWeight = (ey * aa - ay * ea) / det, aWeight = (ay * ee - ey * ea) / det;
    if (eWeight >= 0 && aWeight >= 0) candidates.push({ eWeight, aWeight });
  }
  if (candidates.length === 0) return null;
  const error = (weights) => y.reduce((sum, value, i) => sum + (value - e[i] * weights.eWeight - a[i] * weights.aWeight) ** 2, 0);
  const { eWeight, aWeight } = candidates.sort((a, b) => error(a) - error(b))[0];
  const predictions = e.map((value, i) => value * eWeight + a[i] * aWeight);
  const residual = y.map((value, i) => value - predictions[i]);
  const average = y.reduce((sum, value) => sum + value, 0) / y.length;
  const variance = y.reduce((sum, value) => sum + (value - average) ** 2, 0);
  return { eWeight, aWeight, rSquared: variance > 0 ? 1 - dot(residual, residual) / variance : null, rmseNsPerIteration: Math.sqrt(dot(residual, residual) / y.length), predictorCosine: ee * aa > 0 ? ea / Math.sqrt(ee * aa) : null, predictions };
};
const fits = [];
for (const basis of ["largest-N", "two-N-slope"]) for (const role of roles) for (const population of ["all-except-empty", "composition-only"]) {
  // Select planned largest N before checking availability; never substitute low N.
  // Every role uses the same retained shape population, with at least 5 complete
  // tuples per role/shape. Partial populations remain explicit in the JSON.
  const planned = (basis === "largest-N" ? groups : slopeGroups).filter((row) => row.case !== "empty" && (population !== "composition-only" || !["sql", "schema-decode", "schema-encode", "allocate"].includes(row.case)))
    .filter((row) => basis !== "largest-N" || !plan.schedule.some((other) => other.target === row.role && other.input.case === row.case && other.input.mode === "effect" && other.input.iterations > row.iterations));
  const rows = planned.filter((row) => row.role === role && roles.every((otherRole) => planned.some((other) => other.role === otherRole && other.case === row.case && other.nsPerIteration.n >= 5 && Number.isFinite(other.nsPerIteration.median))));
  // The separate race supplement has insufficient count-vector diversity for a
  // fitted primitive price. Its per-shape costs remain available for validation.
  if (rows.length < 8) continue;
  for (const metric of ["evaluations", "allocations", "joint"]) {
    const model = fit(rows, metric);
    if (!model) continue;
    const heldModels = rows.map((row, i) => ({ omitted: row.case, ...fit(rows.filter((_row, j) => i !== j), metric) }));
    const loo = rows.map((row, i) => row.evaluationsPerIteration * heldModels[i].eWeight + row.allocationsPerIteration * heldModels[i].aWeight);
    const average = rows.reduce((sum, row) => sum + row.nsPerIteration.median, 0) / rows.length;
    const sst = rows.reduce((sum, row) => sum + (row.nsPerIteration.median - average) ** 2, 0);
    const sse = rows.reduce((sum, row, i) => sum + (row.nsPerIteration.median - loo[i]) ** 2, 0);
    const cohortCoefficients = basis === "two-N-slope" ? Array.from({ length: plan.rounds }, (_, round) => {
      const cohort = rows.map((row) => ({ ...row, nsPerIteration: stats([row.roundValues.find((value) => value.round === round)?.nsPerIteration].filter(Number.isFinite)) }));
      return { round, ...fit(cohort, metric) };
    }).filter((row) => Number.isFinite(row.eWeight) && Number.isFinite(row.aWeight)) : [];
    fits.push({ basis, role, population, metric, ...model, leaveOneShapeOutRSquared: sst > 0 && loo.every(Number.isFinite) ? 1 - sse / sst : null,
      heldModels, cohortCoefficients, cases: rows.map((row) => row.case) });
  }
}
const identityContinuity = roles.map((role) => ({ role, instances: [...new Set(validSamples.filter((row) => row.target === role).map((row) => row.instance))], incarnations: [...new Set(validSamples.filter((row) => row.target === role).map((row) => row.incarnation))], versions: [...new Set(validSamples.filter((row) => row.target === role).map((row) => row.version))] }));
const receiptWalls = roles.map((role) => {
  const rows = samples.filter((row) => row.target === role && row.receiptWallValid);
  const describe = (selected) => ({ n: selected.length, clientWallMs: stats(selected.map((row) => row.clientWallMs)), doWallMs: stats(selected.map((row) => row.doWallMs)) });
  return { role, all: describe(rows), cpuObserved: describe(rows.filter((row) => row.valid)), cpuMissing: describe(rows.filter((row) => !row.valid)) };
});
const result = { scheduled: plan.schedule.length, recorded: samples.length, valid: validSamples.length, problems, warnings, identityContinuity, receiptWalls,
  outcomes: Object.fromEntries(roles.map((role) => [role, events[role].filter((row) => row.cpuTimeMs !== undefined).reduce((out, row) => { out[row.outcome] = (out[row.outcome] ?? 0) + 1; return out; }, {})])),
  definitions: { join: "At least one unique sample anchor (DO structured log or ingress URL), matching traces when both exist, one DO/run invocation with rpcCallCount=1, globally unique invocation ID, exact receipt/deployment/object/version/workload/checksum. Missing ingress loses only ingress metrics. Missing DO CPU is never imputed.", nsPerEvaluation: "(Effect invocation CPU minus exact-N empty-loop CPU) / (evaluations(N) minus evaluations(0)); fixed runner CPU remains, so also see two-N slopes; all CPU from deployed telemetry", nsPerAllocation: "Same CPU numerator divided by marginal selected primitive constructor count; not allocator-only time or actual heap bytes", extraOverPlain: "Matched Effect invocation CPU minus comparator CPU divided by iterations; retains signed differences. Schema comparators are synchronous codecs with the same Effect-backed parser.", slopes: "Within-round difference of baseline-subtracted 4x and 1x CPU divided by difference in N or deterministic counts; removes fixed runner work. Main-run auxiliary empty baselines were collected after all shape rounds, so temporal baseline drift is not controlled.", model: "Diagnostic nonnegative least squares through origin, fitted to per-iteration medians. Same shape set across roles, at least 5 complete tuples per role/shape, at least 8 shapes. All-except-empty includes native SQL; composition-only excludes SQL, Schema and allocation-only ring. Coefficients are not causal primitive prices; E and A are strongly collinear. Held-out and cohort coefficients quantify model sensitivity, not confidence intervals.", controlEnvelope: "max(abs(control Q1-1), abs(control Q3-1), control IQR width); includes systematic identical-code shift; positive primary flag requires all seven matched rounds; exceeding it is descriptive, not a significance test" },
  groups, slopeGroups, comparisons, fits };
save(join(here, "summary.json"), result);
const number = (n) => n === null || n === undefined ? "—" : n.toFixed(1);
const interval = (s) => s?.n ? `${number(s.median)} [${number(s.q1)}–${number(s.q3)}]` : "—";
const largest = groups.filter((row) => row.role === "pin" && row.case !== "empty").filter((row, _i, rows) => !rows.some((other) => other.case === row.case && other.iterations > row.iterations));
const table = ["| Shape | Iterations | E/iteration | A/iteration | Pin CPU ms [Q1–Q3] | Plain CPU ms | ns/eval | ns/primitive allocation | Extra ns/iteration over plain |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|", ...largest.map((row) => `| ${row.case} | ${row.iterations} | ${number(row.evaluationsPerIteration)} | ${number(row.allocationsPerIteration)} | ${interval(row.effectCpuMs)} | ${interval(row.plainCpuMs)} | ${interval(row.nsPerEvaluation)} | ${interval(row.nsPerAllocation)} | ${interval(row.extraOverPlainNsPerIteration)} |`)];
writeFileSync(join(here, "table.md"), table.join("\n") + "\n");
console.log(JSON.stringify({ scheduled: result.scheduled, recorded: result.recorded, valid: result.valid, problems: problems.length, identityContinuity, outcomes: result.outcomes }, null, 2));
if (samples.length !== plan.schedule.length || problems.length > 0) process.exitCode = 1;
