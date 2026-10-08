import fs from "node:fs";
import { gunzipSync } from "node:zlib";

const folder = process.argv[2];
if (!folder) throw new Error("Pass the retained evidence directory");
const input = `${folder}/analysis.json`;
const analysis = JSON.parse(fs.existsSync(input)
  ? fs.readFileSync(input, "utf8")
  : gunzipSync(fs.readFileSync(input + ".gz")).toString("utf8"));
const finite = (x) => typeof x === "number" && Number.isFinite(x);
const quantile = (xs, p) => {
  const s = xs.filter(finite).toSorted((a, b) => a - b);
  if (!s.length) return null;
  const i = (s.length - 1) * p;
  return s[Math.floor(i)] + (s[Math.ceil(i)] - s[Math.floor(i)]) * (i - Math.floor(i));
};
const stats = (xs) => ({
  n: xs.filter(finite).length,
  median: quantile(xs, 0.5),
  min: quantile(xs, 0),
  q1: quantile(xs, 0.25),
  q3: quantile(xs, 0.75),
  max: quantile(xs, 1),
});
const names = [
  "driverMs", "laptopMs", "firstModelDispatchIoMs", "firstModelArrivalFromDriverMs",
  "modelGapMedianMs", "modelGapTotalMs", "objectModelGapMedianIoMs", "objectModelGapTotalIoMs",
  "alarmCpuMs", "containedAlarmCpuMs", "boundaryAlarmCpuMs", "fetchCpuMs",
  "scheduleNowCalls", "nondeferredScheduleNow", "alarmOverlapObservedCount",
  "alarmStartedInsideCount", "nativeSetAlarmCalls", "maintenancePasses", "failedClaims",
];
const countBy = (xs) => xs.reduce((a, x) => (a[x ?? "unknown"] = (a[x ?? "unknown"] ?? 0) + 1, a), {});
const cells = analysis.paired.map((cell) => {
  const metrics = Object.fromEntries(names.map((name) => {
    const paired = cell.units.map((unit) => unit.metrics[name])
      .filter((x) => x && finite(x.baseline) && finite(x.candidate));
    const result = {
      n: paired.length,
      baseline: stats(paired.map((x) => x.baseline)),
      candidate: stats(paired.map((x) => x.candidate)),
      delta: stats(paired.map((x) => x.baseline - x.candidate)),
      baselineRepeat: stats(paired.map((x) => x.baselineRepeatAbsoluteDifference)),
      candidateRepeat: stats(paired.map((x) => x.candidateRepeatAbsoluteDifference)),
    };
    const original = cell.metrics[name]?.saved;
    if (original && (original.n !== result.n || original.median !== result.delta.median))
      throw new Error(`Paired summary disagrees for ${name}`);
    const noise = Math.max(result.baselineRepeat.max ?? 0, result.candidateRepeat.max ?? 0);
    result.largestRepeatDifference = noise;
    result.positiveBeyondObservedRepeatRange = result.n > 0 && result.delta.min > 0 && result.delta.median > noise;
    return [name, result];
  }));
  const pairedObjects = new Set(cell.units.map((unit) => unit.object));
  const records = analysis.records.filter((r) => r.phase === "measure" && r.eligible && pairedObjects.has(r.object) &&
    r.history === cell.history && r.ttftMs === cell.ttftMs && r.mode === cell.mode);
  return {
    history: cell.history, ttftMs: cell.ttftMs, mode: cell.mode, objects: cell.objects,
    metrics,
    laptopColos: countBy(records.map((r) => r.laptopCfRayColo)),
    driverPlacements: countBy(records.map((r) => r.driverResponsePlacement)),
    coverage: cell.coverage,
  };
});
const summary = {
  status: analysis.status,
  sourceGeneratedAt: analysis.generatedAt,
  method: "Two repeats per arm reduce to one median per Object and temperature. Each reported metric uses only Objects with both arm medians known. Positive delta is baseline minus candidate. Repeat columns are absolute differences between the two repeats; the conservative positive-effect flag additionally requires every paired effect positive and the median larger than the largest observed within-arm repeat difference. This is descriptive, not a confidence interval.",
  coverage: analysis.coverage.returnedMeasured,
  cells,
};
fs.writeFileSync(`${folder}/measurement-summary.json`, JSON.stringify(summary, null, 2) + "\n");
const f = (x) => finite(x) ? x.toFixed(1) : "unknown";
const pair = (m) => `${f(m.baseline.median)} → ${f(m.candidate.median)}`;
const spread = (s) => `${f(s.median)} [${f(s.min)}, ${f(s.max)}]`;
const row = (xs) => `| ${xs.join(" | ")} |`;
const label = (c) => [c.history, c.ttftMs, c.mode];
const section = (title, headers, rows) => [title, "", row(headers), row(headers.map(() => "---")), ...rows, ""];
const text = [
  `${analysis.status}. All times below are milliseconds.`, "",
  summary.method, "",
  ...section("### Driver latency and repeats", ["Seed", "TTFT", "State", "Objects", "Driver baseline → candidate", "Paired Δ median [min, max]", "Baseline repeat spread median [min, max]", "Candidate repeat spread median [min, max]"],
    cells.map((c) => row([...label(c), c.metrics.driverMs.n, pair(c.metrics.driverMs), spread(c.metrics.driverMs.delta), spread(c.metrics.driverMs.baselineRepeat), spread(c.metrics.driverMs.candidateRepeat)]))),
  ...section("### Model timing and alarm CPU", ["Seed", "TTFT", "State", "First dispatch from turn start", "Driver → provider arrival*", "Provider gap median*", "Object I/O-clock gap median", "Alarm CPU (paired Objects)"],
    cells.map((c) => row([...label(c), pair(c.metrics.firstModelDispatchIoMs), pair(c.metrics.firstModelArrivalFromDriverMs), pair(c.metrics.modelGapMedianMs), pair(c.metrics.objectModelGapMedianIoMs), `${pair(c.metrics.alarmCpuMs)} (n=${c.metrics.alarmCpuMs.n})`]))),
  "*Provider-arrival comparisons can include cross-clock skew. Object timers advance at I/O; zero does not exclude intervening CPU work. Alarm CPU is whole invocation cost for uniquely joined entered handlers, including labeled boundary work; it is not all native scheduling CPU.", "",
  ...section("### Counts per turn", ["Seed", "TTFT", "State", "All scheduleNow", "Nondeferred scheduleNow", "Overlapping entered handlers", "Native setAlarm calls"],
    cells.map((c) => row([...label(c), pair(c.metrics.scheduleNowCalls), pair(c.metrics.nondeferredScheduleNow), pair(c.metrics.alarmOverlapObservedCount), pair(c.metrics.nativeSetAlarmCalls)]))),
  ...section("### Secondary laptop latency", ["Seed", "TTFT", "State", "Laptop baseline → candidate", "Response CF-Ray colos (turns)"],
    cells.map((c) => row([...label(c), pair(c.metrics.laptopMs), Object.entries(c.laptopColos).map(([k, v]) => `${k}: ${v}`).join(", ")]))),
  "Every response's complete CF-Ray and placement receipt is retained in requests.jsonl.gz. The driver is the primary client clock; laptop elapsed time is secondary.", "",
];
fs.writeFileSync(`${folder}/measurement-tables.md`, text.join("\n"));
console.log(JSON.stringify({status: analysis.status, cells: cells.length, completeObjects: analysis.coverage.completeObjects}));
