import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";

import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

// Offline, fixed attribution experiment; never combines these turns with main.
// vp node examples/durable-bench/results/prod-path/analyze-attribution.mjs
const here = dirname(fileURLToPath(import.meta.url));
const finite = Number.isFinite;
const delta = (a, b) => (finite(a) && finite(b) ? a - b : null);
const unique = (xs) => [...new Set(xs)];
const key = (r) => [r.framework, r.object, r.sample].join("/");
const sameInvocation = (a, b) => !!a?.id && a.id === b?.id && a.kind === b.kind;
const group = (xs, by) => Map.groupBy(xs, by);

const quantile = (xs, p) => {
  const sorted = xs.filter(finite).toSorted((a, b) => a - b);

  if (!sorted.length) return null;
  const i = (sorted.length - 1) * p;

  return sorted[Math.floor(i)] + (sorted[Math.ceil(i)] - sorted[Math.floor(i)]) * (i % 1);
};

const stats = (xs) => ({
  n: xs.filter(finite).length,
  missing: xs.filter((x) => !finite(x)).length,
  median: quantile(xs, 0.5),
  q1: quantile(xs, 0.25),
  q3: quantile(xs, 0.75),
  min: quantile(xs, 0),
  max: quantile(xs, 1),
  range: delta(quantile(xs, 1), quantile(xs, 0)),
});

const completeMedian = (xs) => (xs.length && xs.every(finite) ? quantile(xs, 0.5) : null);

const repeat = (xs) => ({
  ...stats(xs),
  signedDriftMs: xs.length > 1 && xs.every(finite) ? delta(xs.at(-1), xs[0]) : null,
  absoluteDriftMs: xs.length > 1 && xs.every(finite) ? Math.abs(xs.at(-1) - xs[0]) : null,
  repeatRangeMs: xs.length > 1 && xs.every(finite) ? Math.max(...xs) - Math.min(...xs) : null,
});

const Invocation = Schema.Struct({ kind: Schema.String, id: Schema.NonEmptyString });

const Marker = Schema.Struct({
  name: Schema.String,
  localMs: Schema.Number,
  invocation: Schema.optionalKey(Invocation),
  provider: Schema.optionalKey(
    Schema.Struct({
      arrivalMs: Schema.Number,
      colo: Schema.NullOr(Schema.String),
      sample: Schema.NullOr(Schema.String),
    }),
  ),
  error: Schema.optionalKey(Schema.String),
});

const decodeMarker = Schema.decodeUnknownSync(Marker);

const AnalysisError = Schema.TaggedError()("ProdPathAttributionAnalysisError", {
  message: Schema.String,
});

const names = [
  "alarm-entry",
  "alarm-entry-control",
  "process-start",
  "run-session-start",
  "ownership-start",
  "ownership-end",
  "run-session-ready",
  "settlement-transaction-return",
  "await-return",
];

const phaseMetrics = [
  "receiptToAlarmLowerMs",
  "receiptToAlarmUpperMs",
  "receiptToProcessLowerMs",
  "receiptToProcessUpperMs",
  "alarmToProcessMs",
  "ownershipClaimMs",
  "postClaimSetupMs",
  "runSessionTotalMs",
  "settlementReturnToClientLowerMs",
  "settlementReturnToClientUpperMs",
  "settlementReturnToAwaitMs",
  "awaitReturnToClientLowerMs",
  "awaitReturnToClientUpperMs",
  "adjacentControlMs",
  "adjacentControlAbsoluteMs",
];

const limitations = [
  "Supplemental production-only experiment, separate from the main 7fd3 latency matrix. Deployed receipts are the timing evidence; reducer execution time is not evidence.",
  "Beacons are not awaited on the critical path, retain output gates and state.waitUntil ownership, and drain during /metrics after the driver completion clock. They can perturb execution and can arrive after client completion. Arrival differences include transport, scheduling, output-gate delay and path skew; they are not exact commit, CPU or notification timings.",
  "Driver/provider intervals intersect both echo probe offset ranges [provider arrival - driver after, provider arrival - driver before]. Bounds require matching nonempty probe and marker colos and assume a stable offset across those invocations; this is not proven clock synchronization. Provider-to-provider estimates require matching nonempty colos and still assume compatible clocks.",
  "Receipt-to-alarm and receipt-to-process beacon arrivals give conditional upper bounds on alarm entry and processThreadHead start relative to receipt, since output-gate/transport delay follows marker dispatch. Return-to-client beacon residuals can understate the actual return-to-client interval. Differences between two arrivals include differential delivery delay. None is an exact entry timestamp or complete wall decomposition.",
  "DO localMs and span clocks may freeze or lag wall time and are diagnostic only. Selection uses the first model call's markerCountAtStart and original marker array dispatch order, including unfinished starts when checking ambiguity. Missing cutoff or multiple pre-model claims/processes leave those phases unresolved; post-model empty claims remain in the inventory. Zero arrival differences do not prove zero ownership cost.",
  "A normal probed path launches seven beacons before the first model fetch. Nonawaited fetches can still compete for Cloudflare's six waiting-for-headers connection slots and share output gates. On/off differences describe observed perturbation across these samples; control jitter does not bound common transport/output-gate delay or worst-case phase bias.",
  "The seed read in /metrics precedes the final drainMarkers call, with no further explicit await before the snapshot. drainMarkers waits for currently known marker requests until their count stabilizes; later alarms can still add markers after that check. Missing receipts and incomplete late claims remain snapshot coverage gaps; completing the drain does not prove quiescence or zero remaining work.",
  "ownershipClaimMs brackets the DoSubmissionLedger.claim method, including its transaction and surrounding work. postClaimSetupMs brackets ownership-end to RunStorage.claim end, including materialization, writer setup and other intervening work. settlement-transaction-return marks native transaction promise resolution after SQL SET settled, not independently confirmed replication.",
  "All raw markers, duplicates, missing receipts, errors, reordering and signed negative estimates are retained. Negative values and arrival reordering are observations, not errors or exclusions. A negative receipt-to-alarm residual can be consistent with alarm/setup progressing during receipt travel. Missing or ambiguous measurements are null, never zero.",
  "Transcript and cold qualification reuse attribution-completed.json controller receipts. The supplement also checks planned identities, deployment build/version, all-alarm model contexts, warm incarnation continuity and probe alternation; it does not repeat the main reducer's independent transcript reconstruction.",
  "Framework identity uses the matching attribution bundle's passed source-contract build proof against the plan baseline, including its archive match and 184 compiled framework file checks. repositoryCommit retains the actual build checkout. The reducer consumes this archived proof without recomputing its source or bundle hashes.",
  "Statistics use complete cold/settling/warm sets per Object, with Object medians followed by median/Q1/Q3/range across Objects. A phase metric requires all expected on-turn values in that Object/state. Missing metrics retain their denominators.",
  "Probe-on minus probe-off is paired within the same Object/state; adjacent pairs and repeat drift/range remain visible. Alternation is deterministic, history grows with sample index, and cold turns use distinct incarnations. Differences do not establish a causal probe cost or timing precision. Adjacent control measures differential jitter, including dispatch separation; it provides no absolute transit bound or clock correction.",
];

function reduceMarkers(row, issues) {
  const response = row.response ?? {};
  const m = response.metrics ?? {};
  const raw = m.markers;
  const issue = (kind, details = {}) => issues.push({ kind, ...details });
  const observations = [];
  const observe = (kind, details) => observations.push({ kind, ...details });

  const entries = (Array.isArray(raw) ? raw : []).map((marker, index) => {
    let decoded;

    try {
      decoded = decodeMarker(marker);
    } catch (cause) {
      issue("invalid-marker", { index, error: String(cause) });

      return { index, usable: false };
    }
    const expectedSample = [row.object, row.sample, decoded.name, index + 1].join("/");

    const usable =
      !decoded.error &&
      finite(decoded.provider?.arrivalMs) &&
      decoded.provider?.sample === expectedSample &&
      !!decoded.invocation?.id;

    if (!names.includes(decoded.name)) issue("unknown-marker", { index, name: decoded.name });
    if (decoded.error) issue("marker-error", { index, error: decoded.error });
    if (!decoded.provider) issue("missing-marker-receipt", { index });
    else if (decoded.provider.sample !== expectedSample)
      issue("marker-receipt-identity", { index, expectedSample });
    if (!decoded.invocation?.id) issue("missing-marker-invocation", { index });

    return { ...decoded, index, usable };
  });

  const repeatedMarkers = [
    ...group(entries, (e) => [e.invocation?.kind, e.invocation?.id, e.name].join("/")),
  ]
    .filter(([, es]) => es.length > 1)
    .map(([name, es]) => ({ name, indices: es.map((e) => e.index) }));

  for (const [sample, es] of group(
    entries.filter((e) => e.provider?.sample),
    (e) => e.provider.sample,
  )) {
    if (es.length > 1) {
      issue("duplicate-marker-receipt", { sample, indices: es.map((e) => e.index) });
      for (const e of es) e.usable = false;
    }
  }
  if (!m.phaseProbes)
    return {
      markers: raw ?? null,
      observations,
      repeatedMarkers,
      selected: null,
      claimCandidates: [],
      adjacentControls: [],
      metrics: Object.fromEntries(phaseMetrics.map((name) => [name, null])),
      providerClock: null,
    };
  if (!Array.isArray(raw)) issue("missing-markers");
  // Repeated claims are legal. Unequal start/end inventories are still evidence.
  for (const [start, end] of [
    ["alarm-entry", "alarm-entry-control"],
    ["run-session-start", "run-session-ready"],
    ["ownership-start", "ownership-end"],
  ]) {
    const relevant = entries.filter((e) => e.name === start || e.name === end);

    for (const [invocation, es] of group(relevant, (e) =>
      [e.invocation?.kind, e.invocation?.id].join("/"),
    )) {
      const starts = es.filter((e) => e.name === start),
        ends = es.filter((e) => e.name === end);

      if (starts.length !== ends.length)
        issue("unbalanced-marker-pair", {
          invocation,
          start,
          end,
          starts: starts.map((e) => e.index),
          ends: ends.map((e) => e.index),
        });
    }
  }

  const pick = (label, es) => {
    if (es.length !== 1) {
      issue(es.length ? "ambiguous-marker" : "missing-marker", {
        label,
        indices: es.map((e) => e.index),
      });

      return null;
    }

    return es[0];
  };

  const metricDiagnostics = {};

  const between = (label, from, to) => {
    const rawMs = delta(to?.provider?.arrivalMs, from?.provider?.arrivalMs);

    const status =
      !from?.usable || !to?.usable
        ? "missing-or-invalid-marker"
        : !from.provider.colo || !to.provider.colo
          ? "missing-marker-colo"
          : from.provider.colo !== to.provider.colo
            ? "marker-colo-mismatch"
            : "compatible-marker-colos";

    const compatible = status === "compatible-marker-colos";

    metricDiagnostics[label] = {
      rawMs,
      from: from?.index ?? null,
      to: to?.index ?? null,
      colos: [from?.provider?.colo ?? null, to?.provider?.colo ?? null],
      status,
    };
    if (!compatible) issue("unresolved-marker-delta", { label, ...metricDiagnostics[label] });
    if (finite(rawMs) && rawMs < 0) observe("negative-marker-delta", { label, rawMs, status });

    return compatible ? rawMs : null;
  };

  for (let i = 1; i < entries.length; i++) {
    const a = entries[i - 1],
      b = entries[i];

    if (
      a.usable &&
      b.usable &&
      a.provider.colo &&
      a.provider.colo === b.provider.colo &&
      b.provider.arrivalMs < a.provider.arrivalMs
    )
      observe("arrival-order-reversal", {
        from: a.index,
        to: b.index,
        deltaMs: b.provider.arrivalMs - a.provider.arrivalMs,
      });
  }
  const firstCall = m.calls?.[0];
  const firstAlarm = firstCall?.invocation?.kind === "alarm" ? firstCall.invocation : null;
  const inFirstAlarm = entries.filter((e) => sameInvocation(e.invocation, firstAlarm));

  const alarm = pick(
    "first-model alarm-entry",
    inFirstAlarm.filter((e) => e.name === "alarm-entry"),
  );

  // This is a dispatch-order boundary, not a timestamp. The observer records the
  // current marker array length when it records each model call, before dispatch.
  // Arrival order and frozen DO clocks cannot recover that boundary afterward.
  const cutoff = firstCall?.markerCountAtStart;

  const cutoffKnown =
    firstCall?.call === 0 && Number.isInteger(cutoff) && cutoff >= 0 && cutoff <= entries.length;

  if (!cutoffKnown)
    issue("missing-or-invalid-first-model-marker-cutoff", { value: cutoff ?? null });
  if (cutoffKnown && alarm && alarm.index >= cutoff) {
    alarm.usable = false;
    issue("alarm-entry-after-first-model-cutoff", { index: alarm.index, cutoff });
  }
  const processMarkers = inFirstAlarm.filter((e) => e.name === "process-start");

  const processStart = pick(
    "pre-first-model process-start",
    cutoffKnown ? processMarkers.filter((e) => e.index < cutoff) : [],
  );

  const starts = entries.filter((e) => e.name === "run-session-start");

  const claimCandidates = starts.map((start) => {
    const nextStart = starts.find(
      (e) => e.index > start.index && sameInvocation(e.invocation, start.invocation),
    );

    const ready = entries.filter(
      (e) =>
        e.name === "run-session-ready" &&
        sameInvocation(e.invocation, start.invocation) &&
        e.index > start.index &&
        e.index < (nextStart?.index ?? Infinity),
    );

    const end = ready.length === 1 ? ready[0] : null;

    if (!end)
      issue("unpaired-run-session", { start: start.index, ready: ready.map((e) => e.index) });

    return {
      start: start.index,
      ready: end?.index ?? null,
      invocation: start.invocation,
      beforeFirstModel:
        cutoffKnown &&
        sameInvocation(start.invocation, firstAlarm) &&
        start.index < cutoff &&
        !!end &&
        end.index < cutoff,
    };
  });

  // Include unfinished pre-model starts in the ambiguity check. Do not select an
  // older complete claim just because a newer claim is missing its ready marker.
  const candidates = cutoffKnown
    ? claimCandidates.filter((c) => sameInvocation(c.invocation, firstAlarm) && c.start < cutoff)
    : [];

  const claim =
    candidates.length === 1 &&
    candidates[0].beforeFirstModel &&
    processStart &&
    candidates[0].start > processStart.index
      ? candidates[0]
      : null;

  if (!claim)
    issue("ambiguous-pre-model-claim", {
      cutoff: cutoffKnown ? cutoff : null,
      candidateStarts: candidates.map((c) => c.start),
    });
  const runStart = claim ? entries[claim.start] : null;
  const runReady = claim ? entries[claim.ready] : null;

  const withinClaim = claim
    ? entries.filter(
        (e) =>
          sameInvocation(e.invocation, firstAlarm) &&
          e.index > claim.start &&
          e.index < claim.ready,
      )
    : [];

  const ownershipStart = pick(
    "selected ownership-start",
    withinClaim.filter((e) => e.name === "ownership-start"),
  );

  const ownershipEnd = pick(
    "selected ownership-end",
    withinClaim.filter((e) => e.name === "ownership-end"),
  );

  if (ownershipStart && ownershipEnd && ownershipEnd.index < ownershipStart.index) {
    issue("ownership-dispatch-order", { start: ownershipStart.index, end: ownershipEnd.index });
    ownershipStart.usable = false;
    ownershipEnd.usable = false;
  }

  const settlement = pick(
    "settlement-transaction-return",
    entries.filter((e) => e.name === "settlement-transaction-return"),
  );

  const awaitReturn = pick(
    "await-return",
    entries.filter((e) => e.name === "await-return"),
  );

  if (
    settlement &&
    !(m.settlementWrites ?? []).some((w) => sameInvocation(w.invocation, settlement.invocation))
  ) {
    settlement.usable = false;
    issue("settlement-marker-without-logical-write", { index: settlement.index });
  }
  if (
    awaitReturn &&
    !(m.rpcEvents ?? []).some(
      (e) =>
        e.kind === "awaitSettlement" &&
        e.edge === "end" &&
        sameInvocation({ kind: e.kind, id: e.id }, awaitReturn.invocation),
    )
  ) {
    awaitReturn.usable = false;
    issue("await-marker-without-rpc-end", { index: awaitReturn.index });
  }

  const adjacentControls = entries
    .filter((e) => e.name === "alarm-entry")
    .map((entry) => {
      const controls = entries.filter(
        (e) => e.name === "alarm-entry-control" && sameInvocation(e.invocation, entry.invocation),
      );

      const control = pick("alarm-entry-control/" + entry.invocation?.id, controls);

      if (control && control.index !== entry.index + 1)
        issue("nonadjacent-control", { entry: entry.index, control: control.index });
      const rawDeltaMs = between("control/" + entry.index, entry, control);
      const deltaMs = control?.index === entry.index + 1 ? rawDeltaMs : null;

      return {
        entry: entry.index,
        control: control?.index ?? null,
        invocation: entry.invocation,
        deltaMs,
        absoluteMs: finite(deltaMs) ? Math.abs(deltaMs) : null,
      };
    });

  const selectedControl = adjacentControls.find((c) => c.entry === alarm?.index);

  const probes = [response.clockBefore, response.clockAfter].map((p) => ({
    lowerMs: delta(p?.provider?.arrivalMs, p?.afterMs),
    upperMs: delta(p?.provider?.arrivalMs, p?.beforeMs),
    colo: p?.provider?.colo ?? null,
  }));

  const lowerMs = probes.every((p) => finite(p.lowerMs))
    ? Math.max(...probes.map((p) => p.lowerMs))
    : null;

  const upperMs = probes.every((p) => finite(p.upperMs))
    ? Math.min(...probes.map((p) => p.upperMs))
    : null;

  const status =
    !finite(lowerMs) || !finite(upperMs)
      ? "missing-probes"
      : lowerMs > upperMs
        ? "inconsistent-probe-intervals"
        : !probes[0].colo || probes[0].colo !== probes[1].colo
          ? "probe-colo-mismatch"
          : "conditional-stable-offset";

  const bracket = (label, marker, driverMs, fromProvider) => {
    const rawMs = fromProvider
      ? delta(driverMs, marker?.provider?.arrivalMs)
      : delta(marker?.provider?.arrivalMs, driverMs);

    const boundStatus = !marker?.usable
      ? "missing-or-invalid-marker"
      : !finite(rawMs)
        ? "missing-driver-time"
        : status !== "conditional-stable-offset"
          ? status
          : !marker.provider.colo
            ? "missing-marker-colo"
            : marker.provider.colo !== probes[0].colo
              ? "marker-probe-colo-mismatch"
              : status;

    const compatible = boundStatus === "conditional-stable-offset";

    metricDiagnostics[label] = {
      rawMs,
      marker: marker?.index ?? null,
      colo: marker?.provider?.colo ?? null,
      probeColo: probes[0].colo,
      status: boundStatus,
    };
    if (!compatible)
      issue("unresolved-driver-marker-bound", { label, ...metricDiagnostics[label] });

    const values = !compatible
      ? [null, null]
      : fromProvider
        ? [rawMs + lowerMs, rawMs + upperMs]
        : [rawMs - upperMs, rawMs - lowerMs];

    if (values.some((v) => finite(v) && v < 0))
      observe("negative-driver-marker-bound", { label, lowerMs: values[0], upperMs: values[1] });

    return values;
  };

  const receiptToAlarm = bracket("receiptToAlarm", alarm, response.receiptReceivedMs, false);

  const receiptToProcess = bracket(
    "receiptToProcess",
    processStart,
    response.receiptReceivedMs,
    false,
  );

  const settlementToClient = bracket(
    "settlementReturnToClient",
    settlement,
    response.settlementObservedMs,
    true,
  );

  const awaitToClient = bracket(
    "awaitReturnToClient",
    awaitReturn,
    response.settlementObservedMs,
    true,
  );

  const metrics = {
    receiptToAlarmLowerMs: receiptToAlarm[0],
    receiptToAlarmUpperMs: receiptToAlarm[1],
    receiptToProcessLowerMs: receiptToProcess[0],
    receiptToProcessUpperMs: receiptToProcess[1],
    alarmToProcessMs: between("alarmToProcess", alarm, processStart),
    ownershipClaimMs: between("ownershipClaim", ownershipStart, ownershipEnd),
    postClaimSetupMs: between("postClaimSetup", ownershipEnd, runReady),
    runSessionTotalMs: between("runSessionTotal", runStart, runReady),
    settlementReturnToClientLowerMs: settlementToClient[0],
    settlementReturnToClientUpperMs: settlementToClient[1],
    settlementReturnToAwaitMs: between("settlementReturnToAwait", settlement, awaitReturn),
    awaitReturnToClientLowerMs: awaitToClient[0],
    awaitReturnToClientUpperMs: awaitToClient[1],
    adjacentControlMs: selectedControl?.deltaMs ?? null,
    adjacentControlAbsoluteMs: selectedControl?.absoluteMs ?? null,
  };

  return {
    markers: raw ?? null,
    observations,
    repeatedMarkers,
    firstModelMarkerCutoff: cutoffKnown ? cutoff : null,
    claimCandidates,
    adjacentControls,
    metrics,
    metricDiagnostics,
    providerClock: { status, probes, offsetLowerMs: lowerMs, offsetUpperMs: upperMs },
    selected: Object.fromEntries(
      Object.entries({
        alarm,
        processStart,
        runStart,
        ownershipStart,
        ownershipEnd,
        runReady,
        settlement,
        awaitReturn,
      }).map(([name, e]) => [name, e?.index ?? null]),
    ),
  };
}

export const analyzeAttribution = ({
  inputDir = here,
  outputDir = join(here, "attribution"),
} = {}) =>
  Effect.try({
    try: () => {
      const inputs = [];

      const read = (name) => {
        const plain = join(inputDir, name),
          file = existsSync(plain) ? plain : plain + ".gz";

        const missing = !existsSync(file);

        inputs.push({ name, file: missing ? null : file });
        if (missing) return null;
        const bytes = readFileSync(file);

        return (file.endsWith(".gz") ? gunzipSync(bytes) : bytes).toString("utf8");
      };

      const load = (name, fallback) => {
        const text = read(name);

        return text === null ? fallback : JSON.parse(text);
      };

      const lines = (name) =>
        (read(name) ?? "")
          .split("\n")
          .filter((line) => line.trim())
          .map(JSON.parse);

      const requests = lines("requests.jsonl");
      const rows = requests.filter((r) => r.phase === "attribution");
      const plan = load("attribution-plan.json", null);
      const completed = load("attribution-completed.json", []);
      const attempts = lines("attempted.jsonl").filter((r) => r.phase === "attribution");
      const resources = [load("resources-attribution.json", null), load("resources.json", null)];
      const build = load("build-identities/attribution.json", null);
      const sourceContract = load("source-contract.json", null);

      const sourceProofs = (sourceContract?.builds ?? []).filter(
        (proof) => build?.bundleSha256 && proof.bundleSha256 === build.bundleSha256,
      );

      const sourceProof = sourceProofs.length === 1 ? sourceProofs[0] : null;

      const frameworkSourceVerified = Boolean(
        plan?.baselineRevision &&
        sourceContract?.baselineRevision === plan.baselineRevision &&
        sourceProof?.baselineRevision === plan.baselineRevision &&
        sourceProof?.passed === true &&
        sourceProof.bundleArchiveMatches === true &&
        sourceProof.compiledFrameworkFiles === 184 &&
        Array.isArray(sourceProof.checked) &&
        sourceProof.checked.length === sourceProof.compiledFrameworkFiles &&
        sourceProof.checked.every(
          (file) =>
            file.path?.startsWith("packages/") &&
            file.archiveHashMatches === true &&
            file.baselineBlobMatches === true,
        ) &&
        unique(sourceProof.checked.map((file) => file.path)).length ===
          sourceProof.compiledFrameworkFiles,
      );

      const versions = resources
        .flatMap((r) => r?.targets ?? [])
        .filter((t) => t.role === "primary")
        .flatMap((t) =>
          [...(t.deployments ?? []), { version: t.expectedVersion, phase: t.phase, build: t.build }]
            .filter(
              (d) =>
                d.phase === "measure" &&
                d.build?.bundleSha256 === build?.bundleSha256 &&
                frameworkSourceVerified,
            )
            .map((d) => t.name + "/" + d.version),
        );

      const schedule = plan?.groups.flatMap((g) => g.schedule) ?? [];

      const scheduled = group(schedule, key),
        returned = group(rows, key),
        receipts = group(completed, key);

      // Reuse the controller's transcript/cold proof; this reducer owns marker interpretation.
      const turns = rows.map((row) => {
        const { metrics: m = {}, ...driver } = row.response ?? {};

        const issues = [],
          reasons = [];

        const expected = scheduled.get(key(row)) ?? [];
        const item = expected.length === 1 ? expected[0] : null;
        const sampleIndex = /^m[0-7]$/.test(row.sample) ? Number(row.sample.slice(1)) : null;
        const done = receipts.get(key(row)) ?? [];
        const receipt = done.length === 1 ? done[0] : null;

        const probesExpected =
          item && finite(sampleIndex) ? (item.index + sampleIndex) % 2 === 1 : null;

        if (
          !item ||
          item.framework !== "yielded" ||
          item.variant !== "production" ||
          !finite(sampleIndex)
        )
          reasons.push("not a unique planned attribution turn");
        if (returned.get(key(row)).length !== 1) reasons.push("duplicate turn receipt");
        if (row.status !== 200 || !driver.ok || !m.ok || driver.outcome !== "completed")
          reasons.push("failed request");
        if (
          !["object", "sample", "variant", "history", "ttftMs"].every(
            (k) => m[k] === item?.[k] && driver[k] === item?.[k],
          )
        )
          reasons.push("query identity mismatch");
        if (!versions.includes(row.worker + "/" + m.version) || driver.driverVersion !== m.version)
          reasons.push("missing attribution build/version proof");
        if (typeof m.phaseProbes !== "boolean" || m.phaseProbes !== probesExpected)
          reasons.push("probe alternation mismatch");
        if (m.phaseProbes === false && (!Array.isArray(m.markers) || m.markers.length))
          reasons.push("invalid off-turn marker inventory");
        if (
          !receipt?.transcriptVerified ||
          !receipt.productionAlarmVerified ||
          receipt.incarnation !== m.incarnation ||
          receipt.turnMs !== driver.turnMs ||
          receipt.coldVerified !== (item?.state === "cold")
        )
          reasons.push("missing or mismatched controller completion proof");
        const calls = m.calls ?? [];

        if (
          calls.length !== 9 ||
          calls.some((c) => c.invocation?.kind !== "alarm" || !c.invocation?.id)
        )
          reasons.push("model call outside identified alarm or wrong call count");
        if (finite(sampleIndex) && sampleIndex >= 2) {
          const prior = returned.get(key({ ...row, sample: "m" + (sampleIndex - 1) })) ?? [];
          const before = prior.length === 1 ? prior[0].response?.metrics : null;

          if (
            !before ||
            !["objectId", "incarnation", "version"].every((k) => m[k] && m[k] === before[k])
          )
            reasons.push("warm incarnation/version changed or missing");
        }
        if (!finite(driver.turnMs) || !finite(driver.admissionMs))
          reasons.push("missing driver timing");
        const evidence = reduceMarkers(row, issues);

        return {
          ...item,
          key: key(row),
          rowIndex: requests.indexOf(row),
          status: row.status,
          object: row.object,
          sample: row.sample,
          sampleIndex,
          objectId: m.objectId ?? null,
          incarnation: m.incarnation ?? null,
          version: m.version ?? null,
          worker: row.worker,
          phaseProbes: m.phaseProbes ?? null,
          probesExpected,
          eligible: !reasons.length,
          reasons,
          issues,
          driver,
          completionReceipts: done,
          firstModel: calls[0] ?? null,
          spans: m.spans ?? [],
          rpcEvents: m.rpcEvents ?? [],
          settlementWrites: m.settlementWrites ?? [],
          ...evidence,
        };
      });

      const pairedMetrics = [
        "onMs",
        "offMs",
        "onMinusOffMs",
        "onRepeatRangeMs",
        "offRepeatRangeMs",
        "onAbsoluteDriftMs",
        "offAbsoluteDriftMs",
      ];

      const objects = [...group(schedule, (s) => s.object + "/" + s.state)].map(([, expected]) => {
        const { object, history, ttftMs, state } = expected[0];

        const ts = turns
          .filter((t) => t.object === object && t.state === state)
          .toSorted((a, b) => a.sampleIndex - b.sampleIndex);

        const complete =
          ts.length === expected.length &&
          ts.every((t) => t.eligible) &&
          expected.every((e) => ts.filter((t) => t.sample === e.sample).length === 1);

        const on = ts.filter((t) => t.phaseProbes === true),
          off = ts.filter((t) => t.phaseProbes === false);

        const onRepeats = repeat(on.map((t) => t.driver.turnMs)),
          offRepeats = repeat(off.map((t) => t.driver.turnMs));

        const onMs = complete ? completeMedian(on.map((t) => t.driver.turnMs)) : null;
        const offMs = complete ? completeMedian(off.map((t) => t.driver.turnMs)) : null;
        const adjacentPairs = [];

        for (let i = 0; i < expected.length; i += 2) {
          const samples = expected.slice(i, i + 2).map((e) => e.sample),
            pair = ts.filter((t) => samples.includes(t.sample));

          const a = pair.find((t) => t.phaseProbes === true),
            b = pair.find((t) => t.phaseProbes === false);

          adjacentPairs.push({
            samples,
            onFirst: pair.length === 2 ? pair[0].phaseProbes : null,
            onMinusOffMs:
              pair.length === 2 && a?.eligible && b?.eligible
                ? delta(a.driver.turnMs, b.driver.turnMs)
                : null,
          });
        }

        const metrics = Object.fromEntries(
          phaseMetrics.map((name) => [
            name,
            complete && on.length === expected.length / 2
              ? completeMedian(on.map((t) => t.metrics[name]))
              : null,
          ]),
        );

        Object.assign(metrics, {
          onMs,
          offMs,
          onMinusOffMs: delta(onMs, offMs),
          onRepeatRangeMs: onRepeats.repeatRangeMs,
          offRepeatRangeMs: offRepeats.repeatRangeMs,
          onAbsoluteDriftMs: onRepeats.absoluteDriftMs,
          offAbsoluteDriftMs: offRepeats.absoluteDriftMs,
        });

        return {
          object,
          history,
          ttftMs,
          state,
          complete,
          expectedTurns: expected.length,
          observedTurns: ts.length,
          onTurns: on.length,
          offTurns: off.length,
          reasons: ts.flatMap((t) => t.reasons),
          metrics,
          onRepeats,
          offRepeats,
          adjacentPairs,
          phaseCoverage: Object.fromEntries(
            phaseMetrics.map((name) => [
              name,
              {
                expected: expected.length / 2,
                observed: on.filter((t) => finite(t.metrics[name])).length,
              },
            ]),
          ),
        };
      });

      const groups = [...group(objects, (o) => [o.history, o.ttftMs, o.state].join("/"))].map(
        ([name, os]) => {
          const valid = os.filter((o) => o.complete);

          const onTurns = turns.filter(
            (t) => t.phaseProbes && valid.some((o) => o.object === t.object && o.state === t.state),
          );

          const controls = onTurns.map((t) => t.metrics.adjacentControlMs).filter(finite);

          const clockCoverage = Object.fromEntries(
            [
              "receiptToAlarm",
              "receiptToProcess",
              "alarmToProcess",
              "ownershipClaim",
              "postClaimSetup",
              "runSessionTotal",
              "settlementReturnToClient",
              "settlementReturnToAwait",
              "awaitReturnToClient",
            ].map((metric) => [
              metric,
              Object.fromEntries(
                [
                  ...group(onTurns, (t) => t.metricDiagnostics?.[metric]?.status ?? "unresolved"),
                ].map(([status, ts]) => [status, ts.length]),
              ),
            ]),
          );

          return {
            name,
            history: os[0].history,
            ttftMs: os[0].ttftMs,
            state: os[0].state,
            plannedObjects: os.length,
            completeObjects: valid.length,
            onTurns: onTurns.length,
            onTurnClockCoverage: clockCoverage,
            adjacentControlExtrema: {
              validTurns: controls.length,
              minMs: quantile(controls, 0),
              maxMs: quantile(controls, 1),
              maxAbsoluteMs: quantile(controls.map(Math.abs), 1),
            },
            metrics: Object.fromEntries(
              [...phaseMetrics, ...pairedMetrics].map((metric) => [
                metric,
                stats(valid.map((o) => o.metrics[metric])),
              ]),
            ),
          };
        },
      );

      const summary = {
        phase: "attribution",
        status: !rows.length
          ? "awaiting-attribution-data"
          : turns.length === schedule.length && turns.every((t) => t.eligible)
            ? "complete"
            : "partial-or-excluded",
        design: {
          objects: 12,
          objectsPerCondition: 3,
          histories: [50, 250],
          delaysMs: [0, 400],
          turnsPerObject: 8,
          turns: 96,
        },
        inputs,
        build,
        baselineRevision: plan?.baselineRevision ?? null,
        frameworkSource: {
          verified: frameworkSourceVerified,
          contractBaselineRevision: sourceContract?.baselineRevision ?? null,
          contractAllMatch: sourceContract?.allMatch ?? null,
          checkedAt: sourceContract?.checkedAt ?? null,
          matchingBuilds: sourceProofs,
        },
        acceptedWorkerVersions: unique(versions),
        coverage: {
          plannedTurns: schedule.length,
          observedTurns: turns.length,
          eligibleTurns: turns.filter((t) => t.eligible).length,
          completeObjects: (plan?.cohorts ?? []).filter(
            (c) => objects.filter((o) => o.object === c.object && o.complete).length === 3,
          ).length,
          rawMarkers: turns.reduce(
            (n, t) => n + (Array.isArray(t.markers) ? t.markers.length : 0),
            0,
          ),
          markerErrors: turns.reduce(
            (n, t) => n + t.issues.filter((i) => i.kind === "marker-error").length,
            0,
          ),
        },
        limitations,
        groups,
        objects,
        missingTurns: schedule.filter((s) => !returned.has(key(s))),
        unreturnedAttempts: attempts.filter((a) => !returned.has(key(a))),
        excludedTurns: turns
          .filter((t) => !t.eligible)
          .map((t) => ({ key: t.key, reasons: t.reasons })),
        issues: turns.flatMap((t) => t.issues.map((issue) => ({ key: t.key, ...issue }))),
        observations: turns.flatMap((t) =>
          t.observations.map((observation) => ({ key: t.key, ...observation })),
        ),
        failedRequests: requests.filter(
          (r) =>
            r.phase?.startsWith("attribution") && (r.status !== 200 || r.response?.ok !== true),
        ),
        controllerFailures: [
          ...load("attribution-failures.json", []),
          ...load("attribution-seed-failures.json", []),
        ],
      };

      const number = (x) => (finite(x) ? x.toFixed(1) : "—");

      const spread = (s) =>
        s.n
          ? `${number(s.median)} [${number(s.q1)}, ${number(s.q3)}]; ${number(s.min)}…${number(s.max)} (n=${s.n})`
          : "—";

      const bound = (g, prefix) => {
        const a = g.metrics[prefix + "LowerMs"],
          b = g.metrics[prefix + "UpperMs"];

        const clock = g.onTurnClockCoverage[prefix];

        const coloGaps = [
          "probe-colo-mismatch",
          "marker-probe-colo-mismatch",
          "missing-marker-colo",
        ].reduce((sum, status) => sum + (clock[status] ?? 0), 0);

        return `${number(a.median)}…${number(b.median)} (n=${a.n}/${g.plannedObjects}; colo gaps=${coloGaps}/${g.onTurns})`;
      };

      const table = (headers, data) =>
        [
          "| " + headers.join(" | ") + " |",
          "| " + headers.map(() => "---").join(" | ") + " |",
          ...data.map((row) => "| " + row.join(" | ") + " |"),
        ].join("\n");

      const tables = [
        "# Supplemental attribution",
        "",
        summary.status +
          "; " +
          turns.length +
          "/" +
          schedule.length +
          " recorded planned turns; " +
          summary.coverage.completeObjects +
          "/12 complete Objects. Times in ms; no estimates exist before deployed attribution receipts.",
        "",
        "Phase cells: median [Q1, Q3]; min…max across Object medians. n counts Objects with complete compatible evidence for that metric. Bound cells show n/planned Objects and colo gaps/on-turns in completed Object/state sets; full per-metric clock statuses are in summary.json. * Intervals cover beacon-arrival residuals under the stable-offset assumption; transport/output-gate delay remains. Jitter tables use the first-model alarm's adjacent control; all controls are retained per turn. The maximum absolute control difference is an observed turn maximum, separate from Object-median statistics.",
        "",
        table(
          [
            "history/delay/state",
            "Objects",
            "receipt→alarm*",
            "receipt→process*",
            "alarm→process",
            "ownership claim",
            "post-claim setup",
            "RunStorage.claim total",
          ],
          groups.map((g) => [
            g.name,
            g.completeObjects + "/" + g.plannedObjects,
            bound(g, "receiptToAlarm"),
            bound(g, "receiptToProcess"),
            ...[
              "alarmToProcessMs",
              "ownershipClaimMs",
              "postClaimSetupMs",
              "runSessionTotalMs",
            ].map((k) => spread(g.metrics[k])),
          ]),
        ),
        "",
        table(
          [
            "history/delay/state",
            "transaction return→client*",
            "transaction return→await",
            "await return→client*",
            "control jitter signed",
            "control jitter absolute",
            "control absolute max (turn)",
          ],
          groups.map((g) => [
            g.name,
            bound(g, "settlementReturnToClient"),
            spread(g.metrics.settlementReturnToAwaitMs),
            bound(g, "awaitReturnToClient"),
            spread(g.metrics.adjacentControlMs),
            spread(g.metrics.adjacentControlAbsoluteMs),
            `${number(g.adjacentControlExtrema.maxAbsoluteMs)} (n=${g.adjacentControlExtrema.validTurns}/${g.onTurns} turns)`,
          ]),
        ),
        "",
        table(
          [
            "history/delay/state",
            "driver off",
            "driver on",
            "paired on−off",
            "on repeat range",
            "off repeat range",
          ],
          groups.map((g) => [
            g.name,
            ...["offMs", "onMs", "onMinusOffMs", "onRepeatRangeMs", "offRepeatRangeMs"].map((k) =>
              spread(g.metrics[k]),
            ),
          ]),
        ),
        "",
        "Inventory: " +
          summary.issues.length +
          " marker/clock availability issues; " +
          summary.observations.length +
          " signed/order observations (not errors); " +
          summary.excludedTurns.length +
          " excluded turns; " +
          summary.failedRequests.length +
          " failed requests. Full signed values, raw markers, selected indices, claim candidates, and adjacent on/off pairs are in turns.jsonl and summary.json.",
        "",
        ...limitations.map((text) => "- " + text),
        "",
      ].join("\n");

      mkdirSync(outputDir, { recursive: true });
      writeFileSync(join(outputDir, "summary.json"), JSON.stringify(summary, null, 2) + "\n");
      writeFileSync(
        join(outputDir, "turns.jsonl"),
        turns.map((t) => JSON.stringify(t) + "\n").join(""),
      );
      writeFileSync(join(outputDir, "tables.md"), tables);
      console.log(
        JSON.stringify({
          outputDir,
          status: summary.status,
          coverage: summary.coverage,
          issues: summary.issues.length,
        }),
      );

      return summary;
    },
    catch: (cause) => new AnalysisError({ message: String(cause) }),
  });

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2).filter((arg) => arg !== "--"),
    options = {};

  for (let i = 0; i < args.length; i += 2) {
    const name = { "--input-dir": "inputDir", "--out-dir": "outputDir" }[args[i]];

    if (!name || !args[i + 1])
      throw new Error(
        "Usage: analyze-attribution.mjs [--input-dir directory] [--out-dir directory]",
      );
    options[name] = args[i + 1];
  }
  NodeRuntime.runMain(analyzeAttribution(options));
}
