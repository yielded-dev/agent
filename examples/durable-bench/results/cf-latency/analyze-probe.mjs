// Fixed, offline Part 1 reduction. Importable: no filesystem, network, or clock access.
const SIZES = [0, 1024, 16384, 65536, 131072, 524288];
const TRANSACTIONS = [1, 4, 12];
const MODES = ["return", "fetch", "sync-end", "sync-each"];
const REPEATS = [0, 1, 2, 3, 4];
const OBJECTS = Array.from({ length: 8 }, (_, i) => `cf-latency-calibration-${i}`);
const finite = Number.isFinite;
const delta = (end, start) => (finite(end) && finite(start) ? end - start : null);
const counts = (values) => {
  const result = {};
  for (const value of values) result[String(value)] = (result[String(value)] ?? 0) + 1;
  return result;
};
const quantile = (sorted, p) => {
  if (!sorted.length) return null;
  const at = (sorted.length - 1) * p;
  const lo = Math.floor(at);
  return sorted[lo] + (sorted[Math.ceil(at)] - sorted[lo]) * (at - lo);
};
const summary = (values) => {
  const sorted = values.filter(finite).sort((a, b) => a - b);
  return {
    n: sorted.length,
    median: quantile(sorted, 0.5),
    q1: quantile(sorted, 0.25),
    q3: quantile(sorted, 0.75),
    min: sorted[0] ?? null,
    max: sorted.at(-1) ?? null,
  };
};
const group = (items, key) => {
  const result = new Map();
  for (const item of items) {
    const k = key(item);
    if (!result.has(k)) result.set(k, []);
    result.get(k).push(item);
  }
  return result;
};
const invocationKey = (event) => {
  const w = event.$workers;
  const id = event.$metadata?.requestId;
  return w?.scriptName && id ? JSON.stringify([w.scriptName, id]) : null;
};

function telemetryIndex(events) {
  // Invocation identity, not log/span identity. Disagreeing copies remain ambiguous.
  const invocations = new Map();
  let unidentifiedInvocations = 0;
  let duplicateInvocations = 0;
  const conflictingInvocations = new Set();
  for (const event of events) {
    if (event.$metadata?.type !== "cf-worker-event") continue;
    const key = invocationKey(event);
    if (key === null) {
      unidentifiedInvocations++;
      continue;
    }
    const previous = invocations.get(key);
    if (previous) {
      duplicateInvocations++;
      const signature = (e) =>
        JSON.stringify([
          e.$metadata.traceId,
          e.$workers.scriptVersion?.id,
          e.$workers.durableObjectId,
          e.$workers.eventType,
          e.$workers.event,
          e.$workers.cpuTimeMs,
          e.$workers.wallTimeMs,
          e.$workers.outcome,
        ]);
      if (signature(previous) !== signature(event)) conflictingInvocations.add(key);
    } else invocations.set(key, event);
  }
  return {
    invocations,
    alarmLogs: events.filter((e) => e.source?.cfLatency === "alarm-start"),
    logs: group(
      events.filter((e) => e.source?.cfLatency === "probe"),
      (e) => e.source.sample,
    ),
    conflictingInvocations,
    audit: {
      unidentifiedInvocations,
      duplicateInvocations,
      conflictingInvocations: conflictingInvocations.size,
    },
  };
}

function joinRpc(row, index, transport = "rpc") {
  const result = row.response?.result;
  if (!result) return { state: "no-response", invocation: null };
  const nativeFetch = (w) =>
    w?.eventType === "fetch" &&
    w.event?.request?.method === "POST" &&
    w.event.request.url === "https://cf-latency/probe";
  const keys = new Set();
  let conflict = false;
  for (const log of index.logs.get(row.sample) ?? []) {
    const w = log.$workers;
    const m = log.$metadata;
    if (
      w?.scriptName !== row.worker ||
      w?.durableObjectId !== result.objectId ||
      w?.scriptVersion?.id !== result.version ||
      w?.entrypoint !== "ProbeDO" ||
      (transport === "fetch"
        ? !nativeFetch(w)
        : w?.eventType !== "jsrpc" || w?.event?.rpcMethod !== "probe")
    )
      continue;
    const key = invocationKey(log);
    if (index.conflictingInvocations.has(key)) {
      conflict = true;
      continue;
    }
    const event = index.invocations.get(key);
    if (!event) continue;
    const iw = event.$workers;
    if (
      m?.traceId &&
      event.$metadata.traceId === m.traceId &&
      iw.durableObjectId === result.objectId &&
      iw.scriptVersion?.id === result.version &&
      iw.entrypoint === "ProbeDO" &&
      (transport === "fetch"
        ? nativeFetch(iw)
        : iw.eventType === "jsrpc" &&
          iw.event?.rpcCallCount === 1 &&
          iw.event?.rpcMethods?.length === 1 &&
          iw.event.rpcMethods[0] === "probe")
    )
      keys.add(key);
    else conflict = true;
  }
  if (conflict || keys.size > 1) return { state: "ambiguous", invocation: null };
  if (!keys.size) return { state: "missing", invocation: null };
  const event = index.invocations.get([...keys][0]);
  return {
    state: "joined",
    invocation: {
      key: invocationKey(event),
      requestId: event.$metadata.requestId,
      traceId: event.$metadata.traceId,
      objectId: event.$workers.durableObjectId,
      version: event.$workers.scriptVersion.id,
      outcome: event.$workers.outcome ?? "missing",
      cpuMs: finite(event.$workers.cpuTimeMs) ? event.$workers.cpuTimeMs : null,
      wallMs: finite(event.$workers.wallTimeMs) ? event.$workers.wallTimeMs : null,
    },
  };
}

function decodeRow(row) {
  const match =
    /^calibration-o([0-7])-r([0-4])-b(\d+)-t(1|4|12)-(return|fetch|sync-end|sync-each)$/.exec(
      row.sample ?? "",
    );
  if (!match) return null;
  const [, objectIndex, repeat, bytes, transactions, mode] = match;
  const spec = {
    object: OBJECTS[Number(objectIndex)],
    repeat: Number(repeat),
    bytes: Number(bytes),
    transactions: Number(transactions),
    mode,
  };
  if (
    !SIZES.includes(spec.bytes) ||
    row.object !== spec.object ||
    row.input?.sample !== row.sample ||
    row.input.bytes !== spec.bytes ||
    row.input.transactions !== spec.transactions ||
    row.input.mode !== mode
  )
    return null;
  return { ...spec, row };
}

function observation(item, index) {
  const { row } = item;
  const v = row.response?.result;
  const out = v?.outbound;
  const expectedWaits =
    item.mode === "sync-each" ? item.transactions : item.mode === "sync-end" ? 1 : 0;
  const valid =
    row.status === 200 &&
    v?.sample === row.sample &&
    v.bytes === item.bytes &&
    v.transactions === item.transactions &&
    v.mode === item.mode &&
    v.written === item.bytes &&
    typeof v.instance === "string" &&
    typeof v.objectId === "string" &&
    typeof v.version === "string" &&
    finite(row.clientWallMs) &&
    finite(v.startMs) &&
    finite(v.returnMs) &&
    finite(row.response.ingressStartMs) &&
    finite(row.response.stubReturnedMs) &&
    Array.isArray(v.waits) &&
    v.waits.length === expectedWaits &&
    v.waits.every(finite) &&
    (item.mode !== "fetch" ||
      (out?.echo?.sample === row.sample && finite(out.issuedMs) && finite(out.completedMs)));
  const rpc = joinRpc(row, index, item.transport);
  const sync = valid ? v.waits.reduce((a, b) => a + b, 0) : null;
  const fetch = valid ? (item.mode === "fetch" ? delta(out.completedMs, out.issuedMs) : 0) : null;
  const application = valid ? delta(v.returnMs, v.startMs) : null;
  return {
    ...item,
    valid,
    rpc,
    route: valid
      ? JSON.stringify([
          row.response.colo ?? null,
          item.mode === "fetch" ? (out.echo.colo ?? null) : null,
        ])
      : null,
    metrics: {
      clientMs: valid ? row.clientWallMs : null,
      ingressMs: valid ? delta(row.response.stubReturnedMs, row.response.ingressStartMs) : null,
      applicationMs: application,
      syncWaitMs: sync,
      fetchMs: fetch,
      applicationResidualMs: valid ? application - sync - fetch : null,
      rpcCpuMs: rpc.invocation?.cpuMs ?? null,
      rpcWallMs: rpc.invocation?.wallMs ?? null,
    },
  };
}

// Repeats are reduced within Object first; Objects, not requests, are statistical units.
function objectSummary(items, metric) {
  const perObject = [...group(items, (x) => x.object)].map(([object, samples]) => ({
    object,
    ...summary(samples.map(metric)),
  }));
  return { ...summary(perObject.map((x) => x.median)), perObject };
}

function analyzeTransport(rows, index) {
  const selected = rows.filter((r) => r.phase === "transport");
  const bySample = group(selected, (r) => r.sample);
  const cases = [];
  const objects = Array.from({ length: 8 }, (_, o) => {
    const object = `cf-latency-transport-${o}`;
    const own = [];
    for (const repeat of REPEATS)
      for (const bytes of [0, 131072])
        for (const transport of ["rpc", "fetch"]) {
          const sample = `transport-o${o}-r${repeat}-b${bytes}-${transport}`;
          const matches = bySample.get(sample) ?? [];
          const row = matches.length === 1 ? matches[0] : null;
          const observed = row
            ? observation(
                { row, object, repeat, bytes, transactions: 1, mode: "return", transport },
                index,
              )
            : null;
          const result = row?.response?.result;
          const valid = Boolean(
            observed?.valid &&
            row.object === object &&
            typeof row.worker === "string" &&
            row.path?.split("?")[0] === (transport === "rpc" ? "/probe" : "/fetch-probe") &&
            row.input?.sample === sample &&
            row.input.bytes === bytes &&
            row.input.transactions === 1 &&
            row.input.mode === "return",
          );
          const item = {
            object,
            repeat,
            bytes,
            transport,
            sample,
            rows: matches.length,
            valid,
            status: row?.status ?? null,
            identity: valid ? [row.worker, result.objectId, result.instance, result.version] : null,
            colo: row?.response?.colo ?? null,
            invocation: observed?.rpc ?? { state: "no-response", invocation: null },
            metrics: {
              clientMs: valid ? row.clientWallMs : null,
              invocationCpuMs: valid ? (observed.rpc.invocation?.cpuMs ?? null) : null,
              invocationWallMs: valid ? (observed.rpc.invocation?.wallMs ?? null) : null,
            },
          };
          own.push(item);
          cases.push(item);
        }
    const actual = selected.filter((r) => r.object === object);
    const identities = [
      ...new Set(own.filter((x) => x.valid).map((x) => JSON.stringify(x.identity))),
    ].map(JSON.parse);
    return {
      object,
      expectedCases: 20,
      rows: actual.length,
      validCases: own.filter((x) => x.valid).length,
      missing: own.filter((x) => !x.rows).map((x) => x.sample),
      duplicates: own.filter((x) => x.rows > 1).map((x) => x.sample),
      identities,
      complete: actual.length === 20 && own.every((x) => x.valid) && identities.length === 1,
    };
  });
  const eligible = new Set(objects.filter((x) => x.complete).map((x) => x.object));
  const admitted = cases.filter((x) => eligible.has(x.object));
  const metrics = ["clientMs", "invocationCpuMs", "invocationWallMs"];
  const table = [0, 131072].flatMap((bytes) =>
    ["rpc", "fetch"].map((transport) => {
      const items = admitted.filter((x) => x.bytes === bytes && x.transport === transport);
      return {
        bytes,
        transport,
        samples: items.length,
        telemetryCoverage: counts(items.map((x) => x.invocation.state)),
        metrics: Object.fromEntries(
          metrics.map((name) => [name, objectSummary(items, (x) => x.metrics[name])]),
        ),
      };
    }),
  );
  const pairedRpcMinusFetch = [0, 131072].map((bytes) => {
    const pairs = [];
    let routeMismatch = 0;
    let unknownRoute = 0;
    for (const rpc of admitted.filter((x) => x.bytes === bytes && x.transport === "rpc")) {
      const fetch = admitted.find(
        (x) =>
          x.object === rpc.object &&
          x.repeat === rpc.repeat &&
          x.bytes === bytes &&
          x.transport === "fetch",
      );
      if (!rpc.colo || !fetch.colo) {
        unknownRoute++;
        continue;
      }
      if (rpc.colo !== fetch.colo) {
        routeMismatch++;
        continue;
      }
      pairs.push({ object: rpc.object, rpc, fetch });
    }
    return {
      bytes,
      pairs: pairs.length,
      excluded: { routeMismatch, unknownRoute },
      metrics: Object.fromEntries(
        metrics.map((name) => [
          name,
          objectSummary(pairs, (x) => delta(x.rpc.metrics[name], x.fetch.metrics[name])),
        ]),
      ),
    };
  });
  return {
    state: !selected.length ? "not-run" : eligible.size === 8 ? "complete" : "partial",
    scope:
      "Same-Object RPC probe versus native fetch calling probe; 8 Objects x 5 repeats x 2 byte sizes x 2 transports",
    counts: {
      rows: selected.length,
      statuses: counts(selected.map((r) => r.status ?? "transport-error")),
      unrecognizedRows: selected.filter(
        (r) => !cases.some((x) => x.sample === r.sample && x.object === r.object),
      ).length,
      invalidSuccessResponses: cases.filter((x) => x.status === 200 && !x.valid).length,
      telemetryCoverage: counts(cases.filter((x) => x.rows).map((x) => x.invocation.state)),
      joinedOutcomes: counts(
        cases.filter((x) => x.invocation.invocation).map((x) => x.invocation.invocation.outcome),
      ),
    },
    objects,
    analyzedObjects: [...eligible],
    cases: cases.filter((x) => x.rows),
    table,
    pairedRpcMinusFetch,
    limits: [
      "Only complete successful 20-case Objects with one worker/Object/incarnation/version enter distributions; warmups excluded",
      "Pairs match Object, bytes, repeat and known ingress colo; positive deltas mean RPC exceeds fetch",
      "Invocation joins use source probe sample then worker/requestId with Object/version/trace checks; RPC must be one probe call, fetch must be ProbeDO POST https://cf-latency/probe",
      "RPC session wall and fetch invocation wall have different lifetimes; their difference alone is not a transport latency, durability, or output-gate cost",
      "Controller wall is the completed-request comparison, including transport wrappers and JSON serialization/parsing; invocation CPU and wall are separate diagnostics",
      "Missing or sampled telemetry remains unknown; zero CPU is reported millisecond precision, not proof of no CPU work; no application-clock or timestamp interval attribution",
    ],
  };
}

function analyzeAlarms(rows, index) {
  const selected = rows.filter((r) => ["alarm2-arm", "alarm2-probe"].includes(r.phase));
  const pilot = rows.filter((r) => ["alarm-arm", "alarm-probe"].includes(r.phase));
  const bySample = group(selected, (r) => r.sample);
  const outerJoin = (row) => {
    const ray = row.cfRay?.split("-")[0];
    if (!ray) return { state: "missing", event: null };
    const matches = [...index.invocations.values()].filter(
      (e) =>
        e.$workers.scriptName === row.worker &&
        e.$metadata.rayId === ray &&
        e.$workers.eventType === "fetch" &&
        !e.$workers.durableObjectId,
    );
    if (matches.length !== 1 || index.conflictingInvocations.has(invocationKey(matches[0])))
      return { state: matches.length ? "ambiguous" : "missing", event: null };
    return { state: "joined", event: matches[0] };
  };
  const cases = [];
  const objects = Array.from({ length: 8 }, (_, o) => {
    const object = `cf-latency-alarm2-${o}`;
    const own = [];
    for (let repeat = 0; repeat < 4; repeat++) {
      for (const iterations of [0, 20_000_000, 80_000_000]) {
        for (const afterArmMs of [0, 110, 230]) {
          const sample = `alarm2-o${o}-r${repeat}-n${iterations}-d${afterArmMs}`;
          const sampleRows = bySample.get(sample) ?? [];
          const arms = sampleRows.filter((r) => r.phase === "alarm2-arm");
          const probes = sampleRows.filter((r) => r.phase === "alarm2-probe");
          const arm = arms.length === 1 ? arms[0] : null;
          const probe = probes.length === 1 ? probes[0] : null;
          const a = arm?.response?.result;
          const p = probe?.response?.result;
          const observed = probe
            ? observation(
                { row: probe, object, repeat, bytes: 0, transactions: 1, mode: "return" },
                index,
              )
            : null;
          const params = new URLSearchParams(arm?.path?.split("?")[1] ?? "");
          const valid = Boolean(
            arm &&
            probe &&
            arm.object === object &&
            probe.object === object &&
            arm.worker === probe.worker &&
            arm.status === 200 &&
            a?.sample === sample &&
            params.get("sample") === sample &&
            params.get("leadMs") === "200" &&
            params.get("iterations") === String(iterations) &&
            finite(arm.clientWallMs) &&
            observed.valid &&
            p.bytes === 0 &&
            p.transactions === 1 &&
            p.mode === "return" &&
            probe.input?.sample === sample &&
            probe.input.bytes === 0 &&
            probe.input.transactions === 1 &&
            probe.input.mode === "return" &&
            a.objectId === p.objectId &&
            a.version === p.version &&
            typeof a.instance === "string",
          );
          const outer = probe ? outerJoin(probe) : { state: "no-response", event: null };
          const alarmKeys = new Set();
          let alarmConflict = false;
          if (valid)
            for (const log of index.alarmLogs.filter((e) => e.source.sample === sample)) {
              const w = log.$workers;
              if (
                log.source.iterations !== iterations ||
                w?.scriptName !== arm.worker ||
                w.durableObjectId !== a.objectId ||
                w.scriptVersion?.id !== a.version ||
                w.eventType !== "alarm"
              )
                continue;
              const key = invocationKey(log);
              const event = index.invocations.get(key);
              if (index.conflictingInvocations.has(key)) {
                alarmConflict = true;
                continue;
              }
              if (
                event &&
                log.$metadata?.traceId &&
                event.$metadata.traceId === log.$metadata.traceId &&
                event.$workers.eventType === "alarm" &&
                event.$workers.durableObjectId === a.objectId &&
                event.$workers.scriptVersion?.id === a.version
              )
                alarmKeys.add(key);
            }
          const alarms = alarmConflict
            ? []
            : [...alarmKeys].map((key) => index.invocations.get(key));
          const alarmCpu =
            alarms.length && alarms.every((e) => finite(e.$workers.cpuTimeMs))
              ? alarms.reduce((total, e) => total + e.$workers.cpuTimeMs, 0)
              : null;
          const item = {
            object,
            repeat,
            iterations,
            afterArmMs,
            sample,
            valid,
            armRows: arms.length,
            probeRows: probes.length,
            identity: valid ? [arm.worker, p.objectId, p.version] : null,
            armInstance: a?.instance ?? null,
            probeInstance: p?.instance ?? null,
            route: JSON.stringify([arm?.response?.colo ?? null, probe?.response?.colo ?? null]),
            rpc: observed?.rpc ?? { state: "no-response", invocation: null },
            outer: {
              state: outer.state,
              key: outer.event ? invocationKey(outer.event) : null,
              outcome: outer.event?.$workers.outcome ?? null,
            },
            alarm: {
              state: alarmConflict ? "ambiguous" : alarms.length ? "joined" : "missing",
              invocationKeys: alarms.map(invocationKey),
              outcomes: counts(alarms.map((e) => e.$workers.outcome ?? "missing")),
            },
            metrics: {
              clientMs: valid ? probe.clientWallMs : null,
              outerWallMs: valid ? (outer.event?.$workers.wallTimeMs ?? null) : null,
              outerCpuMs: valid ? (outer.event?.$workers.cpuTimeMs ?? null) : null,
              rpcWallMs: valid ? (observed.rpc.invocation?.wallMs ?? null) : null,
              rpcCpuMs: valid ? (observed.rpc.invocation?.cpuMs ?? null) : null,
              observedAlarmCpuMs: alarmCpu,
            },
          };
          own.push(item);
          cases.push(item);
        }
      }
    }
    const identities = [
      ...new Set(own.filter((x) => x.valid).map((x) => JSON.stringify(x.identity))),
    ].map(JSON.parse);
    const actual = selected.filter((r) => r.object === object);
    return {
      object,
      expectedCases: 36,
      validCases: own.filter((x) => x.valid).length,
      rows: actual.length,
      identities,
      complete: actual.length === 72 && own.every((x) => x.valid) && identities.length === 1,
      incarnations: [
        ...new Set(own.flatMap((x) => [x.armInstance, x.probeInstance]).filter(Boolean)),
      ],
    };
  });
  const eligible = new Set(objects.filter((x) => x.complete).map((x) => x.object));
  const admitted = cases.filter((x) => eligible.has(x.object));
  const metrics = [
    "clientMs",
    "outerWallMs",
    "outerCpuMs",
    "rpcWallMs",
    "rpcCpuMs",
    "observedAlarmCpuMs",
  ];
  const table = [0, 20_000_000, 80_000_000].flatMap((iterations) =>
    [0, 110, 230].map((afterArmMs) => {
      const items = admitted.filter(
        (x) => x.iterations === iterations && x.afterArmMs === afterArmMs,
      );
      return {
        iterations,
        afterArmMs,
        samples: items.length,
        metrics: Object.fromEntries(
          metrics.map((name) => [name, objectSummary(items, (x) => x.metrics[name])]),
        ),
      };
    }),
  );
  const pairedBaseline = [20_000_000, 80_000_000].flatMap((iterations) =>
    [0, 110, 230].map((afterArmMs) => {
      const pairs = [];
      let routeMismatch = 0;
      for (const item of admitted.filter(
        (x) => x.iterations === iterations && x.afterArmMs === afterArmMs,
      )) {
        const control = admitted.find(
          (x) =>
            x.object === item.object &&
            x.repeat === item.repeat &&
            x.afterArmMs === afterArmMs &&
            x.iterations === 0,
        );
        if (item.route !== control.route) {
          routeMismatch++;
          continue;
        }
        pairs.push({ object: item.object, item, control });
      }
      return {
        iterations,
        afterArmMs,
        pairs: pairs.length,
        routeMismatch,
        metrics: Object.fromEntries(
          metrics.map((name) => [
            name,
            objectSummary(pairs, (x) => delta(x.item.metrics[name], x.control.metrics[name])),
          ]),
        ),
      };
    }),
  );
  return {
    state: !selected.length ? "not-run" : eligible.size === 8 ? "complete" : "partial",
    scope: "alarm2 only; scheduled-CPU interference, paired against zero-iteration alarms",
    pilot: {
      state: pilot.length ? "failed-pilot-excluded" : "not-run",
      phaseCounts: counts(pilot.map((r) => r.phase)),
      statuses: counts(pilot.map((r) => r.status ?? "transport-error")),
    },
    counts: {
      phaseCounts: counts(selected.map((r) => r.phase)),
      statuses: counts(selected.map((r) => r.status ?? "transport-error")),
      unrecognizedRows: selected.filter(
        (r) => !cases.some((x) => x.sample === r.sample && x.object === r.object),
      ).length,
      rpcCoverage: counts(cases.filter((x) => x.probeRows).map((x) => x.rpc.state)),
      outerCoverage: counts(cases.filter((x) => x.probeRows).map((x) => x.outer.state)),
      alarmCoverage: counts(cases.filter((x) => x.armRows).map((x) => x.alarm.state)),
    },
    objects,
    analyzedObjects: [...eligible],
    cases: cases.filter((x) => x.armRows || x.probeRows),
    table,
    pairedBaseline,
    limits: [
      "Only complete successful 36-case Objects with one worker/Object/version enter distributions; incarnation changes remain visible",
      "afterArmMs is configured controller delay after arm response, not measured alarm/probe overlap or elapsed since alarm scheduling",
      "Zero-iteration baseline still schedules an alarm and timer; contrasts measure scheduled-CPU interference, including scheduling and incarnation effects",
      "Alarm CPU joins by explicit sample source log and invocation identity; retries remain separate invocations; missing telemetry is unknown",
      "No DO Date.now intervals or telemetry timestamp reconstruction; alarm CPU is not attributed to the probe's critical path",
    ],
  };
}

/** Reduce the fixed 8 Objects × 5 repeats × 6 sizes × 3 tx counts × 4 modes plan.
 * rows are parsed requests.jsonl records; telemetryEvents is telemetry-probe.json.events.
 * No mutation of inputs, I/O, timers, fitting, or extrapolation from incomplete Objects.
 */
export function analyzeProbe(rows, telemetryEvents) {
  if (!Array.isArray(rows) || !Array.isArray(telemetryEvents))
    throw new TypeError("Expected rows and telemetryEvents arrays");
  const index = telemetryIndex(telemetryEvents);
  const calibrationRows = rows.filter((r) => r.phase === "calibration");
  const decoded = calibrationRows.map(decodeRow);
  const observations = decoded.filter((x) => x !== null).map((x) => observation(x, index));
  const bySample = group(observations, (x) => x.row.sample);
  const objects = OBJECTS.map((object, i) => {
    const own = observations.filter((x) => x.object === object);
    const expected = SIZES.flatMap((b) =>
      TRANSACTIONS.flatMap((t) =>
        MODES.flatMap((m) => REPEATS.map((r) => `calibration-o${i}-r${r}-b${b}-t${t}-${m}`)),
      ),
    );
    const missing = expected.filter((s) => !bySample.has(s));
    const duplicates = expected.filter((s) => (bySample.get(s)?.length ?? 0) > 1);
    const identities = [
      ...new Set(
        own
          .filter((x) => x.valid)
          .map((x) => {
            const v = x.row.response.result;
            return JSON.stringify([v.objectId, v.instance, v.version]);
          }),
      ),
    ].map(JSON.parse);
    const malformed = calibrationRows.filter(
      (r) => r.object === object && decodeRow(r) === null,
    ).length;
    return {
      object,
      expected: expected.length,
      rows: own.length,
      missing,
      duplicates,
      malformed,
      complete: missing.length === 0 && duplicates.length === 0 && malformed === 0,
      identities,
      statuses: counts(own.map((x) => x.row.status ?? "transport-error")),
      invalidSuccessResponses: own.filter((x) => x.row.status === 200 && !x.valid).length,
      telemetry: counts(own.map((x) => x.rpc.state)),
    };
  });
  const eligible = new Set(
    objects.filter((x) => x.complete && x.identities.length === 1).map((x) => x.object),
  );
  const admitted = observations.filter((x) => eligible.has(x.object));
  const successful = admitted.filter((x) => x.valid);
  const metrics = [
    "clientMs",
    "ingressMs",
    "applicationMs",
    "syncWaitMs",
    "fetchMs",
    "applicationResidualMs",
    "rpcCpuMs",
    "rpcWallMs",
  ];
  const table = SIZES.flatMap((bytes) =>
    TRANSACTIONS.flatMap((transactions) =>
      MODES.map((mode) => {
        const items = successful.filter(
          (x) => x.bytes === bytes && x.transactions === transactions && x.mode === mode,
        );
        const attempted = admitted.filter(
          (x) => x.bytes === bytes && x.transactions === transactions && x.mode === mode,
        );
        return {
          bytes,
          transactions,
          actualTransactionSyncCalls: bytes === 0 ? 0 : transactions,
          mode,
          samples: items.length,
          rejectedSamples: attempted.length - items.length,
          telemetry: counts(items.map((x) => x.rpc.state)),
          metrics: Object.fromEntries(
            metrics.map((name) => [name, objectSummary(items, (x) => x.metrics[name])]),
          ),
        };
      }),
    ),
  );
  const lookup = new Map(
    successful.map((x) => [
      JSON.stringify([x.object, x.repeat, x.bytes, x.transactions, x.mode]),
      x,
    ]),
  );
  const counterpart = (x, bytes, transactions, mode) =>
    lookup.get(JSON.stringify([x.object, x.repeat, bytes, transactions, mode]));
  const pair = (items, findControl) => {
    const pairs = [];
    const excluded = { missingControl: 0, routeMismatch: 0 };
    for (const item of items) {
      const control = findControl(item);
      if (!control) excluded.missingControl++;
      else if (item.route !== control.route) excluded.routeMismatch++;
      else pairs.push({ object: item.object, item, control });
    }
    return { pairs, excluded };
  };
  const noWriteDeltas = SIZES.filter((b) => b > 0).flatMap((bytes) =>
    TRANSACTIONS.flatMap((transactions) =>
      MODES.map((mode) => {
        const { pairs, excluded } = pair(
          successful.filter(
            (x) => x.bytes === bytes && x.transactions === transactions && x.mode === mode,
          ),
          (x) => counterpart(x, 0, transactions, mode),
        );
        return {
          bytes,
          transactions,
          mode,
          pairs: pairs.length,
          excluded,
          metrics: Object.fromEntries(
            metrics.map((name) => [
              name,
              objectSummary(pairs, (x) => delta(x.item.metrics[name], x.control.metrics[name])),
            ]),
          ),
        };
      }),
    ),
  );
  const explicitSyncEffect = SIZES.filter((b) => b > 0).flatMap((bytes) =>
    TRANSACTIONS.map((transactions) => {
      const { pairs, excluded } = pair(
        successful.filter(
          (x) => x.bytes === bytes && x.transactions === transactions && x.mode === "sync-each",
        ),
        (x) => counterpart(x, bytes, transactions, "sync-end"),
      );
      return {
        bytes,
        transactions,
        pairs: pairs.length,
        excluded,
        clientDeltaMs: objectSummary(pairs, (x) =>
          delta(x.item.metrics.clientMs, x.control.metrics.clientMs),
        ),
        rpcCpuDeltaMs: objectSummary(pairs, (x) =>
          delta(x.item.metrics.rpcCpuMs, x.control.metrics.rpcCpuMs),
        ),
        rpcWallDeltaMs: objectSummary(pairs, (x) =>
          delta(x.item.metrics.rpcWallMs, x.control.metrics.rpcWallMs),
        ),
        syncWaitDeltaMs: objectSummary(
          pairs,
          (x) => x.item.metrics.syncWaitMs - x.control.metrics.syncWaitMs,
        ),
        ingressDeltaMs: objectSummary(
          pairs,
          (x) => x.item.metrics.ingressMs - x.control.metrics.ingressMs,
        ),
      };
    }),
  );
  const transactionCountSlopes = SIZES.filter((b) => b > 0).flatMap((bytes) =>
    MODES.map((mode) => {
      // Fixed-total-byte endpoint secant, not a regression or inferred physical flush cost.
      const { pairs, excluded } = pair(
        successful.filter((x) => x.bytes === bytes && x.transactions === 12 && x.mode === mode),
        (x) => counterpart(x, bytes, 1, mode),
      );
      return {
        bytes,
        mode,
        pairs: pairs.length,
        excluded,
        denominator: 11,
        clientMsPerAdditionalTransaction: objectSummary(
          pairs,
          (x) => delta(x.item.metrics.clientMs, x.control.metrics.clientMs) / 11,
        ),
        rpcCpuMsPerAdditionalTransaction: objectSummary(pairs, (x) => {
          const difference = delta(x.item.metrics.rpcCpuMs, x.control.metrics.rpcCpuMs);
          return difference === null ? null : difference / 11;
        }),
        rpcWallMsPerAdditionalTransaction: objectSummary(pairs, (x) => {
          const difference = delta(x.item.metrics.rpcWallMs, x.control.metrics.rpcWallMs);
          return difference === null ? null : difference / 11;
        }),
        ingressMsPerAdditionalTransaction: objectSummary(
          pairs,
          (x) => (x.item.metrics.ingressMs - x.control.metrics.ingressMs) / 11,
        ),
        syncWaitMsPerAdditionalTransaction: objectSummary(
          pairs,
          (x) => (x.item.metrics.syncWaitMs - x.control.metrics.syncWaitMs) / 11,
        ),
      };
    }),
  );
  return {
    schemaVersion: 1,
    scope: "Part 1 fixed calibration; pilots, warmups, native framework measurements excluded",
    statistics:
      "Linear-interpolated Q1/Q3 across Object medians; within-Object repeats reduced first; no confidence intervals",
    units: {
      times: "milliseconds",
      bytes: "total submitted blob payload; not physical disk bytes",
      slopes: "12-versus-1 endpoint secants at fixed total bytes",
    },
    clocks: {
      controller: "clientMs is controller monotonic elapsed time",
      telemetry:
        "rpcCpuMs/rpcWallMs are per-invocation platform durations; export timestamp is not used as invocation start or end",
      application:
        "ingressMs/applicationMs/syncWaitMs/fetchMs and their derivatives are I/O-advancing Date.now observations, not validated wall durations",
      residual:
        "applicationResidualMs is application-clock arithmetic only, not CPU, durability, or unexplained wall time",
    },
    join: "source.cfLatency=probe + source.sample; match worker/Object/version/method; join invocation by (scriptName, requestId) and verify traceId; no spanId or timestamp join",
    objects,
    analyzedObjects: [...eligible],
    counts: {
      inputRows: rows.length,
      calibrationRows: calibrationRows.length,
      malformedCalibrationRows: decoded.filter((x) => x === null).length,
      statuses: counts(calibrationRows.map((r) => r.status ?? "transport-error")),
      invalidSuccessResponses: observations.filter((x) => x.row.status === 200 && !x.valid).length,
      analyzedSuccessfulRows: successful.length,
      telemetryCoverage: counts(observations.map((x) => x.rpc.state)),
      joinedOutcomes: counts(
        observations.filter((x) => x.rpc.invocation).map((x) => x.rpc.invocation.outcome),
      ),
      ...index.audit,
    },
    // Per-request join evidence lets the caller inspect coverage without reconstructing IDs.
    rpcJoins: observations.map((x) => ({ sample: x.row.sample, object: x.object, ...x.rpc })),
    table,
    noWriteDeltas,
    explicitSyncEffect,
    transactionCountSlopes,
    alarms: analyzeAlarms(rows, index),
    transport: analyzeTransport(rows, index),
    limits: [
      "Incomplete, duplicate, malformed-plan, or mixed-incarnation/version Objects excluded from distributions; their counts remain visible",
      "Distributions describe successful responses only; failures and invalid responses remain in coverage",
      "Telemetry absence is unknown; sampled or late exports do not establish complete CPU coverage",
      "RPC wall is an invocation/session metric, not a nested method interval or durability time; CPU is separate and overlaps I/O intervals",
      "Application clocks freeze between I/O and need not catch up to real UTC after CPU; zero elapsed is not zero CPU; no subtraction of absolute timestamps across machines or invocations",
      "Small probe CPU does not validate application-clock accuracy; syncWait is not an exact CPU-independent wait or a proven lower bound; do not add CPU to repair it",
      "Telemetry export timestamps have no assumed start/end semantics; no reconstructed invocation intervals or exact alarm overlap",
      "Paired deltas match Object, repetition and ingress/echo colo; they are incremental write-associated observations, not causal isolation",
      "sync-each/return transaction counts are not physical flush counts; zero-byte cases execute no transactionSync calls",
      "syncWait measures explicit waits only; return/fetch modes may wait at output gates; no per-framework cost extrapolation",
    ],
  };
}
