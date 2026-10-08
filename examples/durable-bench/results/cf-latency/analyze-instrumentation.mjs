// Pure offline reduction. No filesystem, telemetry, network, or clock access.
const PHASES = new Set([
  "instrumentation-reset",
  "instrumentation-warmup",
  "instrumentation",
  "instrumentation-release",
]);
const SAMPLES = Array.from({ length: 8 }, (_, i) => `m${i + 6}`);
const QUERY_KEYS = [
  "target",
  "history",
  "object",
  "sample",
  "ttftMs",
  "chunkDelayMs",
  "variant",
  "syncBeforeFetch",
];
const finite = Number.isFinite;
const nonempty = (value) => typeof value === "string" && value.length > 0;
const cohortKey = (row) => `${row.target}/${row.framework}/${row.object}`;
const receipt = (row) => row.response?.thread ?? row.response;
const identity = (value) =>
  value
    ? Object.fromEntries(
        ["objectId", "incarnation", "version", "generation"].map((key) => [
          key,
          value[key] ?? null,
        ]),
      )
    : null;
const validIdentity = (value) =>
  value &&
  ["objectId", "incarnation", "version"].every((key) => nonempty(value[key])) &&
  value.generation === "measure";
const sameIdentity = (a, b) =>
  validIdentity(a) &&
  validIdentity(b) &&
  ["objectId", "incarnation", "version", "generation"].every((key) => a[key] === b[key]);
const queryMatches = (value, query) =>
  value && query && QUERY_KEYS.every((key) => value[key] === query[key]);
const bytes = (value) =>
  value === undefined ? null : Buffer.byteLength(JSON.stringify(value), "utf8");
const group = (items, key) => {
  const result = new Map();
  for (const item of items) {
    const k = key(item);
    if (!result.has(k)) result.set(k, []);
    result.get(k).push(item);
  }
  return result;
};
const stats = (values) => {
  const sorted = values.filter(finite).sort((a, b) => a - b);
  const quantile = (p) => {
    if (!sorted.length) return null;
    const at = (sorted.length - 1) * p;
    const lo = Math.floor(at);
    return sorted[lo] + (sorted[Math.ceil(at)] - sorted[lo]) * (at - lo);
  };
  return {
    n: sorted.length,
    median: quantile(0.5),
    q1: quantile(0.25),
    q3: quantile(0.75),
    min: sorted[0] ?? null,
    max: sorted.at(-1) ?? null,
  };
};
const median = (values) => stats(values).median;
const counts = (values) =>
  Object.fromEntries(
    [...group(values, (value) => (nonempty(value) ? value : "unknown"))].map(([value, entries]) => [
      value,
      entries.length,
    ]),
  );
const colos = (records) => ({
  ingressRequests: counts(records.map((record) => record.ingressColo)),
  providerRequests: counts(records.flatMap((record) => record.providerColos)),
});
const spread = (values) => {
  const absoluteDifferencesMs = values.flatMap((a, i) =>
    values.slice(i + 1).map((b) => Math.abs(a - b)),
  );
  return {
    absoluteDifferencesMs,
    medianAbsoluteDifferenceMs: median(absoluteDifferencesMs),
    rangeMs: Math.max(...values) - Math.min(...values),
  };
};

function requestQuery(row) {
  try {
    const url = new URL(row.path, "https://offline.invalid");
    const q = url.searchParams;
    if ([...new Set(q.keys())].some((key) => q.getAll(key).length !== 1)) return null;
    return {
      path: url.pathname,
      target: q.get("target"),
      history: Number(q.get("history")),
      object: q.get("object"),
      sample: q.get("sample"),
      ttftMs: Number(q.get("ttftMs") ?? 0),
      chunkDelayMs: Number(q.get("chunkDelayMs") ?? 0),
      variant: q.get("variant") ?? "baseline",
      syncBeforeFetch:
        (q.get("syncBeforeFetch") ?? "false") === "false"
          ? false
          : q.get("syncBeforeFetch") === "true"
            ? true
            : null,
      directoryMetrics:
        q.get("directoryMetrics") === "true"
          ? true
          : q.get("directoryMetrics") === "false"
            ? false
            : null,
    };
  } catch {
    return null;
  }
}

function auditRecord(row, rowIndex, expected) {
  const reasons = [];
  const query = requestQuery(row);
  const r = receipt(row);
  const running = row.phase === "instrumentation" || row.phase === "instrumentation-warmup";
  const calls = Array.isArray(r?.calls) ? r.calls : [];
  const providers = calls.map((call) => call?.providerReceipt);
  if (!PHASES.has(row.phase)) reasons.push("unexpected instrumentation phase");
  if (
    row.target !== "primary" ||
    row.framework !== "tardie" ||
    !nonempty(row.worker) ||
    !nonempty(row.object)
  )
    reasons.push("expected primary Tardie Object and Worker identity");
  if (
    !query ||
    query.target !== row.framework ||
    query.object !== row.object ||
    query.sample !== row.sample ||
    ![50, 250].includes(query.history) ||
    query.ttftMs !== 0 ||
    query.chunkDelayMs !== 0 ||
    query.variant !== "baseline" ||
    query.syncBeforeFetch !== false
  )
    reasons.push("request query/row mismatch or unexpected experimental condition");
  if (query?.path !== (running ? "/run" : "/cold")) reasons.push("unexpected request path");
  if (row.status !== 200 || r?.ok !== true || row.error != null || r?.error != null)
    reasons.push("request failed or lacks a successful receipt");
  if (
    !finite(row.clientWallMs) ||
    row.clientWallMs < 0 ||
    !finite(row.startedAt) ||
    !finite(row.endedAt) ||
    row.endedAt < row.startedAt
  )
    reasons.push("missing or invalid controller timing");
  if (!queryMatches(r, query)) reasons.push("response query does not match request");

  const invalidCalls = [];
  let transcriptValid = null;
  if (running) {
    const reference = expected?.[query?.history];
    const hashes = reference?.turns?.[row.sample];
    if (!nonempty(reference?.seed) || r?.seedFingerprint !== reference.seed)
      reasons.push("seed fingerprint mismatch or missing reference");
    if (!validIdentity(r)) reasons.push("missing Thread measurement identity");
    if (query?.directoryMetrics === null || !query)
      reasons.push("explicit observation flag required");
    if (
      row.phase === "instrumentation-warmup" &&
      (!["m6", "m7"].includes(row.sample) || query?.directoryMetrics !== true)
    )
      reasons.push("warmup must be m6/m7 with observation enabled");
    if (row.phase === "instrumentation" && !SAMPLES.slice(2).includes(row.sample))
      reasons.push("measurement must be m8 through m13");
    if (query?.directoryMetrics === true) {
      if (
        !row.response?.thread ||
        !sameIdentity(row.response.directoryStart, row.response.directory) ||
        row.response.directory?.version !== r?.version
      )
        reasons.push("Actor begin/end identity or Actor/Thread version mismatch");
    } else if (
      row.response?.thread !== undefined ||
      row.response?.directory !== undefined ||
      row.response?.directoryStart !== undefined
    ) {
      reasons.push("observation-off response still contains the router observation wrapper");
    }
    transcriptValid =
      calls.length === 9 &&
      Array.isArray(hashes) &&
      hashes.length === 9 &&
      new Set(calls.map((call) => call?.providerRequest)).size === 9;
    calls.forEach((call, index) => {
      const p = call?.providerReceipt;
      const streamValid =
        call?.call === index &&
        call?.status === 200 &&
        call?.sseDone === true &&
        call?.error === undefined &&
        finite(call?.endMs);
      const queryValid = queryMatches(p, query) && p?.call === index;
      const providerValid =
        nonempty(call?.providerRequest) &&
        p?.requestId === call.providerRequest &&
        p?.error === null &&
        finite(p?.arrivalMs) &&
        finite(p?.firstByteMs) &&
        finite(p?.endMs) &&
        p.firstByteMs >= p.arrivalMs &&
        p.endMs >= p.firstByteMs;
      const golden = nonempty(hashes?.[index]) && p?.fingerprint === hashes[index];
      if (!streamValid || !queryValid || !providerValid || !golden) {
        transcriptValid = false;
        invalidCalls.push({
          call: index,
          streamValid,
          queryValid,
          providerValid,
          expected: hashes?.[index] ?? null,
          observed: p?.fingerprint ?? null,
        });
      }
    });
    if (!transcriptValid)
      reasons.push(
        "nine unique native requests with successful SSE completion, matching queries, and golden fingerprints required",
      );
  } else {
    if (
      r?.coldRequested !== true ||
      r?.threadAbort?.expectedAbort !== true ||
      r?.directoryAbort?.expectedAbort !== true
    )
      reasons.push("Thread and Actor explicit abort receipts required");
    if (!validIdentity(r?.before) || !validIdentity(r?.directoryBefore))
      reasons.push("reset/release identity receipt missing");
  }
  return {
    rowIndex,
    raw: row,
    key: cohortKey(row),
    phase: row.phase,
    sample: row.sample,
    history: query?.history ?? null,
    worker: row.worker,
    object: row.object,
    directoryMetrics: running ? (query?.directoryMetrics ?? null) : null,
    thread: running ? identity(r) : null,
    actor: running && query?.directoryMetrics === true ? identity(row.response?.directory) : null,
    clientMs: finite(row.clientWallMs) ? row.clientWallMs : null,
    clientReceiptJsonBytes: bytes(row.response),
    ingressColo:
      typeof row.cfRay === "string" && row.cfRay.includes("-") ? row.cfRay.split("-").at(-1) : null,
    providerColos: providers.map((p) => p?.colo ?? null),
    providerRequestIds: calls.map((call) => call?.providerRequest ?? null),
    transcriptValid,
    invalidCalls,
    eligible: reasons.length === 0,
    reasons,
  };
}

function auditCohort(key, records, completions) {
  const reasons = [];
  const turns = records.filter((record) =>
    ["instrumentation", "instrumentation-warmup"].includes(record.phase),
  );
  const reset = records.filter((record) => record.phase === "instrumentation-reset");
  const release = records.filter((record) => record.phase === "instrumentation-release");
  const bySample = group(turns, (record) => record.sample);
  if (
    records.length !== 10 ||
    turns.length !== 8 ||
    reset.length !== 1 ||
    release.length !== 1 ||
    !SAMPLES.every((sample) => bySample.get(sample)?.length === 1)
  )
    reasons.push(
      "one reset, m6/m7 warmups, six unique m8–m13 measurements, and one release required; retries are not admitted",
    );
  if (records.some((record) => !record.eligible))
    reasons.push("one or more request records failed validation");
  if (
    new Set(records.map((record) => record.worker)).size !== 1 ||
    new Set(records.map((record) => record.history)).size !== 1
  )
    reasons.push("Worker or history changed within cohort");
  if (completions.length !== 1) reasons.push("exactly one completed-cohort record required");
  const completed = completions.length === 1 ? completions[0] : null;
  const first = bySample.get("m6")?.[0];
  const thread = first?.thread;
  const actor = first?.actor;
  if (!validIdentity(thread) || turns.some((record) => !sameIdentity(record.thread, thread)))
    reasons.push("Thread identity/version changed or missing");
  const on = turns.filter((record) => record.directoryMetrics === true);
  if (
    !validIdentity(actor) ||
    on.some((record) => !sameIdentity(record.actor, actor)) ||
    actor?.version !== thread?.version
  )
    reasons.push("Actor identity/version changed or missing across observed turns");
  if (
    !completed ||
    completed.incarnation !== thread?.incarnation ||
    completed.actorIncarnation !== actor?.incarnation ||
    completed.version !== thread?.version
  )
    reasons.push("completed-cohort identity/version does not match receipts");
  const before = reset[0]?.raw.response;
  if (
    before?.before?.objectId !== thread?.objectId ||
    before?.before?.incarnation === thread?.incarnation ||
    before?.directoryBefore?.objectId !== actor?.objectId ||
    before?.directoryBefore?.incarnation === actor?.incarnation
  )
    reasons.push("reset did not preserve both Object IDs and change both incarnations");
  const after = release[0]?.raw.response;
  if (!sameIdentity(after?.before, thread) || !sameIdentity(after?.directoryBefore, actor))
    reasons.push(
      "release does not bracket the final off turn with stable Thread and Actor identities",
    );
  const ordered = [reset[0], ...SAMPLES.map((sample) => bySample.get(sample)?.[0]), release[0]];
  if (
    ordered.some((record) => !record) ||
    ordered.some(
      (record, i) =>
        i > 0 && (!record || !ordered[i - 1] || record.raw.startedAt < ordered[i - 1].raw.endedAt),
    )
  )
    reasons.push("request sequence missing, out of order, or overlapping in controller clock");
  const requestIds = turns.flatMap((record) => record.providerRequestIds);
  if (requestIds.length !== 72 || new Set(requestIds).size !== 72)
    reasons.push("provider request identities reused or missing across cohort");
  const pairs = [];
  for (let repeat = 0; repeat < 3; repeat++) {
    const a = bySample.get(`m${8 + repeat * 2}`)?.[0];
    const b = bySample.get(`m${9 + repeat * 2}`)?.[0];
    if (
      !a ||
      !b ||
      typeof a.directoryMetrics !== "boolean" ||
      typeof b.directoryMetrics !== "boolean" ||
      a.directoryMetrics === b.directoryMetrics
    ) {
      reasons.push(`repeat ${repeat}: one observation-on and one observation-off turn required`);
      continue;
    }
    const enabled = a.directoryMetrics ? a : b;
    const disabled = a.directoryMetrics ? b : a;
    pairs.push({
      repeat,
      onSample: enabled.sample,
      offSample: disabled.sample,
      order: a.directoryMetrics ? "on/off" : "off/on",
      onMinusOffClientMs:
        finite(enabled.clientMs) && finite(disabled.clientMs)
          ? enabled.clientMs - disabled.clientMs
          : null,
      onMinusOffReceiptJsonBytes:
        finite(enabled.clientReceiptJsonBytes) && finite(disabled.clientReceiptJsonBytes)
          ? enabled.clientReceiptJsonBytes - disabled.clientReceiptJsonBytes
          : null,
      onIngressColo: enabled.ingressColo,
      offIngressColo: disabled.ingressColo,
      sameIngressColo:
        enabled.ingressColo && disabled.ingressColo
          ? enabled.ingressColo === disabled.ingressColo
          : null,
      onProviderColos: enabled.providerColos,
      offProviderColos: disabled.providerColos,
    });
  }
  const measurements = turns.filter((record) => record.phase === "instrumentation");
  const enabled = measurements.filter((record) => record.directoryMetrics === true);
  const disabled = measurements.filter((record) => record.directoryMetrics === false);
  if (enabled.length !== 3 || disabled.length !== 3)
    reasons.push("three measured observations per state required");
  const admitted = reasons.length === 0;
  const unit = admitted
    ? {
        clientOnMs: stats(enabled.map((record) => record.clientMs)),
        clientOffMs: stats(disabled.map((record) => record.clientMs)),
        clientOnMinusOffMs:
          median(enabled.map((record) => record.clientMs)) -
          median(disabled.map((record) => record.clientMs)),
        adjacentPairClientOnMinusOffMs: median(pairs.map((pair) => pair.onMinusOffClientMs)),
        sameStateRepeatSpread: {
          on: spread(enabled.map((record) => record.clientMs)),
          off: spread(disabled.map((record) => record.clientMs)),
        },
        receiptOnJsonBytes: stats(enabled.map((record) => record.clientReceiptJsonBytes)),
        receiptOffJsonBytes: stats(disabled.map((record) => record.clientReceiptJsonBytes)),
        receiptOnMinusOffJsonBytes:
          median(enabled.map((record) => record.clientReceiptJsonBytes)) -
          median(disabled.map((record) => record.clientReceiptJsonBytes)),
        colos: { on: colos(enabled), off: colos(disabled) },
      }
    : null;
  return {
    key,
    worker: first?.worker ?? records[0]?.worker ?? null,
    object: first?.object ?? records[0]?.object ?? null,
    history: first?.history ?? records[0]?.history ?? null,
    thread,
    actor,
    completed,
    recordIndices: records.map((record) => record.rowIndex),
    admitted,
    reasons,
    pairs,
    unit,
  };
}

/**
 * expected is analyze.mjs's reference(): { [history]: { seed, turns: { m0..m13: hash[] } } }.
 * completed is instrumentation-completed.json. Inputs are retained, never mutated.
 * Missing/failed/duplicate requests exclude the entire Object, not just a slow arm.
 */
export function analyzeInstrumentation(rows, expected, completed) {
  const records = rows.flatMap((row, rowIndex) =>
    typeof row.phase === "string" && row.phase.startsWith("instrumentation")
      ? [auditRecord(row, rowIndex, expected)]
      : [],
  );
  const byCohort = group(records, (record) => record.key);
  const byCompletion = group(completed, (entry) => entry.key);
  const keys = [...new Set([...byCohort.keys(), ...byCompletion.keys()])];
  const cohorts = keys.map((key) =>
    auditCohort(key, byCohort.get(key) ?? [], byCompletion.get(key) ?? []),
  );
  const admittedCohorts = cohorts.filter((cohort) => cohort.admitted);
  const groups = [50, 250].map((history) => {
    const units = admittedCohorts.filter((cohort) => cohort.history === history);
    const selected = records.filter(
      (record) =>
        record.phase === "instrumentation" && units.some((unit) => unit.key === record.key),
    );
    return {
      history,
      objects: units.length,
      cohortKeys: units.map((unit) => unit.key),
      clientOnMs: stats(units.map(({ unit }) => unit.clientOnMs.median)),
      clientOffMs: stats(units.map(({ unit }) => unit.clientOffMs.median)),
      clientOnMinusOffMs: stats(units.map(({ unit }) => unit.clientOnMinusOffMs)),
      adjacentPairClientOnMinusOffMs: stats(
        units.map(({ unit }) => unit.adjacentPairClientOnMinusOffMs),
      ),
      sameStateRepeatSpreadMs: Object.fromEntries(
        ["on", "off"].map((state) => [
          state,
          {
            medianAbsoluteDifference: stats(
              units.map(({ unit }) => unit.sameStateRepeatSpread[state].medianAbsoluteDifferenceMs),
            ),
            range: stats(units.map(({ unit }) => unit.sameStateRepeatSpread[state].rangeMs)),
          },
        ]),
      ),
      receiptOnJsonBytes: stats(units.map(({ unit }) => unit.receiptOnJsonBytes.median)),
      receiptOffJsonBytes: stats(units.map(({ unit }) => unit.receiptOffJsonBytes.median)),
      receiptOnMinusOffJsonBytes: stats(units.map(({ unit }) => unit.receiptOnMinusOffJsonBytes)),
      pairOrders: counts(units.flatMap((unit) => unit.pairs.map((pair) => pair.order))),
      pairIngressMatches: counts(
        units.flatMap((unit) =>
          unit.pairs.map((pair) =>
            pair.sameIngressColo === null ? null : String(pair.sameIngressColo),
          ),
        ),
      ),
      colos: {
        on: colos(selected.filter((record) => record.directoryMetrics)),
        off: colos(selected.filter((record) => !record.directoryMetrics)),
      },
    };
  });
  return {
    scope:
      "Warm, primary Tardie, TTFT=0/chunkDelay=0, m8–m13 after on-state m6/m7 warmups; separate from the native main matrix",
    definitions: {
      clientOnMinusOffMs:
        "Within each Object: median of three on client times minus median of three off client times; then median [Q1–Q3] across Objects. Positive means observation-on was slower.",
      adjacentPairClientOnMinusOffMs:
        "Within each Object: median of the three adjacent randomized on-minus-off pair differences; then median [Q1–Q3] across Objects.",
      spread:
        "Within-state absolute differences among the three repeats (three comparisons), reduced to an Object median; also report each Object's full range. These are descriptive repeat spread, not confidence intervals.",
      payload:
        "UTF-8 bytes of re-encoded saved response JSON, including the router wrapper on the on arm. Not wire/compressed bytes or isolated RPC payload bytes.",
      colos:
        "Ingress counts use response CF-Ray suffix, one count per measured turn; provider counts use stream receipts, nine per turn. Unknown locations remain explicit.",
      effect:
        "Combined Actor observation RPCs, counter collection, router buffering/serialization, response payload, and associated scheduling. Not a pure two-RTT estimate or a durability wait.",
      accounting:
        "Off turns have no Actor accounting. No CPU, SQL, alarm, or complete Tardie accounting comparison is inferred. Off Actor continuity is bracketed by on receipts and final release identity.",
      admission:
        "All eight transcripts, stable Thread and observed Actor identity/version, explicit reset/release aborts, and a matching completion record are mandatory. This warm comparison makes no cold-start claim.",
      transcript:
        "Provider semantic fingerprints are compared directly with the supplied shared reference projection. This reducer performs no additional normalization and does not assert raw wire or canonical journal byte equality.",
      denominator:
        "Observed cohort keys union completion keys; planned Objects with neither rows nor completion are unavailable to this three-argument reducer.",
    },
    coverage: {
      recordedRows: records.length,
      observedCohorts: byCohort.size,
      completionRecords: completed.length,
      admittedCohorts: admittedCohorts.length,
      admittedMeasuredTurns: admittedCohorts.length * 6,
      excludedCohorts: cohorts.length - admittedCohorts.length,
    },
    records,
    cohorts,
    admittedCohorts,
    groups,
  };
}
