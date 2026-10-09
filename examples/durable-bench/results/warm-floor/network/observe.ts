import { AsyncLocalStorage } from "node:async_hooks";

import { Effect, Layer, Schema, Tracer } from "effect";
import { DurableObjectState as EffectState } from "effect-cf";

import type { Message } from "../../../src/plan.ts";
import {
  decodeProviderReceipt,
  errorText,
  type Env,
  type ProviderReceipt,
  type Query,
} from "./protocol.ts";

const encoder = new TextEncoder();

export const invocation = new AsyncLocalStorage<{ kind: string; id: string }>();
const activeObservation = new AsyncLocalStorage<Observation>();
export const within = <A>(meter: Observation, entry: { kind: string; id: string }, body: () => A): A =>
  activeObservation.run(meter, () => invocation.run(entry, body));
export const probe = (name: string, detail?: Record<string, unknown>) =>
  activeObservation.getStore()?.note(name, detail);
// Build-only controls selected by the observer, after receipt identity reaches
// the existing endpoint. They are absent from published framework source.
export const candidateEnabled = (candidate: "prearm" | "settlement" | "views") => {
  const meter = activeObservation.getStore();
  const variant = meter?.query?.variant;
  return meter?.env.PHASE === "measure" &&
    (variant === candidate || (variant === "combined" && candidate !== "views"));
};
const markerTransaction = new AsyncLocalStorage<{ settled: boolean }>();

const decodeEcho = Schema.decodeUnknownSync(
  Schema.Struct({
    arrivalMs: Schema.Number,
    colo: Schema.NullOr(Schema.String),
    sample: Schema.NullOr(Schema.String),
  }),
);

const observedSpans = new Set([
  "RunStorage.claim",
  "DoSubmissionLedger.claim",
  "DoSubmissionLedger.publishSettlement",
  "DoSubmissionLedger.finalizeSettlement",
  "DoSubmissionLedger.lookup",
  "DoThreadStore.append",
  "DoJournal.append",
  "DoThreadStore.materialize",
  "DurableAgentRuntime.processThreadHead",
  "DurableAgentRuntime.runRecovery",
]);

/** Observe existing spans; external receipts are confined to the attribution experiment. */
export const timingLayer = Layer.effect(Tracer.Tracer)(
  Effect.gen(function* () {
    const state = yield* EffectState.DurableObjectState;
    const meter = observation(state.raw.storage);

    meter.eventAcquisitions++;

    return Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);

        if (!meter.active || !(observedSpans.has(options.name) || options.name.startsWith("warm-floor."))) return span;
        meter.note(`span.start:${options.name}`);

        const row = {
          name: options.name,
          startMs: Date.now(),
          invocation: invocation.getStore(),
          endMs: undefined as number | undefined,
          outcome: undefined as string | undefined,
        };

        meter.spans.push(row);
        if (options.name === "RunStorage.claim") meter.marker("run-session-start");
        if (options.name === "DoSubmissionLedger.claim") meter.marker("ownership-start");
        if (options.name === "DurableAgentRuntime.processThreadHead") meter.marker("process-start");
        if (options.name === "warm-floor.beginPass") meter.marker("pass-start");
        if (options.name === "DoSubmissionLedger.publishSettlement") meter.marker("settlement-publication-start");
        if (options.name === "DoSubmissionLedger.finalizeSettlement") meter.marker("settlement-finalization-start");
        const end = span.end.bind(span);

        span.end = (at, exit) => {
          row.endMs = Date.now();
          row.outcome = exit._tag;
          meter.note(`span.end:${options.name}`, { outcome: exit._tag });
          if (options.name === "DoSubmissionLedger.claim") meter.marker("ownership-end");
          if (options.name === "RunStorage.claim") meter.marker("run-session-ready");
          if (options.name === "warm-floor.beginMutation") meter.marker("prearm-return");
          if (options.name === "DoSubmissionLedger.publishSettlement") meter.marker("settlement-publication-return");
          if (options.name === "DoSubmissionLedger.finalizeSettlement") meter.marker("settlement-finalization-return");
          if (options.name === "warm-floor.readFinalizedSubmission") meter.marker("waiter-read-return");
          end(at, exit);
        };

        return span;
      },
    });
  }),
);

export const byteLength = (value: unknown): number => {
  if (value === null || value === undefined) return 0;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return value.byteLength;
  if (typeof value === "number" || typeof value === "bigint") return 8;
  if (typeof value === "string") return encoder.encode(value).byteLength;
  throw new Error(`Unexpected SQL value: ${typeof value}`);
};

const zero = () => ({
  statements: 0,
  mutationStatements: 0,
  writeBindingBytes: 0,
  writeSqlTextBytes: 0,
  kvPutCalls: 0,
  kvDeleteCalls: 0,
  kvJsonBytes: 0,
  getAlarmCalls: 0,
  setAlarmCalls: 0,
  deleteAlarmCalls: 0,
  transactions: 0,
  transactionSync: 0,
  writeTransactions: 0,
  writeTransactionSync: 0,
  overlappingTransactionCallbacks: 0,
  transactionRollbacks: 0,
  transactionWindowCrossings: 0,
  transactionFailures: 0,
  syncCalls: 0,
  syncWaitMs: 0,
});

export type Counts = ReturnType<typeof zero>;

export const difference = (after: Counts, before: Counts): Counts =>
  Object.fromEntries(
    Object.keys(after).map((key) => [
      key,
      after[key as keyof Counts] - before[key as keyof Counts],
    ]),
  ) as Counts;

interface Call {
  call: number;
  markerCountAtStart: number;
  startMs: number;
  syncStartedMs?: number;
  syncEndedMs?: number;
  fetchStartedMs?: number;
  headersMs?: number;
  firstByteMs?: number;
  endMs?: number;
  status?: number;
  responseBytes: number;
  providerRequest?: string | null;
  providerReceipt?: ProviderReceipt;
  sseDone?: boolean;
  error?: string;
  sqlAtStart: Counts;
  sqlSincePreviousCall: Counts;
  sqlAtEnd?: Counts;
  invocation?: { kind: string; id: string };
}

/** Logical SQL mutation bindings and JSON-sized KV values; never physical disk/WAL bytes. */
export class Observation {
  constructor(
    readonly env: Env,
    readonly sync: () => Promise<void>,
    readonly keepAlive: (promise: Promise<void>) => void,
  ) {
    this.active = env.PHASE === "measure";
  }
  counts = zero();
  events: Array<Record<string, unknown>> = [];
  eventCounts: Record<string, number> = {};
  invocationEventCounts: Record<string, Record<string, number>> = {};
  workTotals: Record<string, { calls: number; rows: number; bytes: number; views: number }> = {};
  note(name: string, detail?: Record<string, unknown>) {
    if (!this.active || this.env.PHASE !== "measure") return;
    this.eventCounts[name] = (this.eventCounts[name] ?? 0) + 1;
    const kind = invocation.getStore()?.kind ?? "unknown";
    if (name.startsWith("owned.rows.")) {
      const phase = kind === "submit" ? "admission" : kind === "awaitSettlement" ? "waiter"
        : this.calls.at(-1)?.call === 8 && this.calls.at(-1)?.endMs !== undefined ? "tail"
        : this.calls.length > 0 ? "model-loop" : "setup";
      const key = `${phase}/${String(detail?.table)}/${name}`;
      const total = this.workTotals[key] ??= { calls: 0, rows: 0, bytes: 0, views: 0 };
      total.calls++;
      for (const field of ["rows", "bytes", "views"] as const) {
        const value = detail?.[field];
        if (typeof value === "number") total[field] += value;
      }
    }
    const counts = this.invocationEventCounts[kind] ??= {};
    counts[name] = (counts[name] ?? 0) + 1;
    if (!this.query?.object.includes("-diagnostic-") && (!this.query?.object.includes("-map-") || Number(this.query.sample.slice(1)) >= 8)) return;
    this.events.push({ n: this.events.length, name, atMs: Date.now(), calls: this.calls.length, invocation: invocation.getStore(), ...detail });
  }
  calls: Call[] = [];
  syncs: Array<{
    startedMs: number;
    endedMs?: number;
    afterProviderCalls: number;
    error?: string;
  }> = [];
  alarmEvents: Array<{
    alarmId: string;
    edge: "start" | "end";
    active: boolean;
    query?: Query;
    objectId: string;
    incarnation: string;
    version: string;
    atMs: number;
  }> = [];
  spans: Array<{
    name: string;
    startMs: number;
    endMs?: number;
    outcome?: string;
    invocation?: { kind: string; id: string };
  }> = [];
  rpcEvents: Array<{ kind: string; id: string; edge: string; atMs: number; sql: Counts; activeAlarms: number }> = [];
  settlementWrites: Array<{ atMs: number; invocation?: { kind: string; id: string } }> = [];
  waiterReads: Array<{ atMs: number; invocation?: { kind: string; id: string } }> = [];
  phaseProbes = false;
  markers: Array<{
    name: string;
    localMs: number;
    invocation?: { kind: string; id: string };
    provider?: { arrivalMs: number; colo: string | null; sample: string | null };
    error?: string;
  }> = [];
  private markerRequests: Promise<void>[] = [];
  eventAcquisitions = 0;
  measurement?: Record<string, unknown>;
  seen: readonly Message[] = [];
  query?: Query;
  active: boolean;
  private firstRequest = true;
  private alarmStarts = 0;
  private readonly activeAlarms = new Set<string>();
  private alarmsActiveAtStart: string[] = [];
  readonly incarnation = crypto.randomUUID();
  readonly constructedMs = Date.now();
  private previous = zero();
  entry() {
    return {
      firstHarnessRequest: this.firstRequest,
      priorAlarmStarts: this.alarmStarts,
      activeAlarmIds: [...this.activeAlarms],
    };
  }
  initialSql() {
    if (!this.firstRequest) return undefined;
    this.firstRequest = false;
    this.active = false;

    return this.snapshot();
  }
  reset(query: Query) {
    this.query = query;
    this.counts = zero();
    this.events = [];
    this.eventCounts = {};
    this.invocationEventCounts = {};
    this.workTotals = {};
    this.previous = zero();
    this.calls = [];
    this.syncs = [];
    this.alarmEvents = [];
    this.spans = [];
    this.rpcEvents = [];
    this.settlementWrites = [];
    this.waiterReads = [];
    this.phaseProbes = false;
    this.markers = [];
    this.markerRequests = [];
    this.alarmsActiveAtStart = [...this.activeAlarms];
  }
  begin(query: Query, state: DurableObjectState) {
    const entry = this.entry();
    const constructorSql = this.initialSql();

    this.reset(query);
    // Separate attribution experiment only. Alternate on/off in the same Object;
    // neither the main latency matrix nor the public framework is instrumented with probes.
    this.phaseProbes = query.object.includes("-diagnostic-") || (
      query.object.includes("-map-") &&
      Number(query.sample.slice(1)) < 8 &&
      (Number(query.sample.slice(1)) + Number(query.object.match(/-o(\d+)$/)?.[1])) % 2 === 1);
    this.active = true;
    this.measurement = {
      ...query,
      ...this.identity(state, this.env),
      entry,
      constructorSql,
      eventAcquisitionsAtStart: this.eventAcquisitions,
      targetEntryMs: Date.now(),
    };
  }
  rpcEvent(kind: string, id: string, edge: string) {
    const event = { kind, id, edge, atMs: Date.now(), sql: this.snapshot(), activeAlarms: this.activeAlarms.size };

    this.rpcEvents.push(event);
    console.log({
      warmFloor: "target-rpc",
      sample: this.query?.sample,
      object: this.query?.object,
      incarnation: this.incarnation,
      ...event,
    });
    if (kind === "awaitSettlement" && edge === "end") this.marker("await-return");
    if (kind === "submit" && edge === "start") this.marker("submit-entry");
    if (kind === "submit" && edge === "end") this.marker("submit-body-return");
  }
  /** External arrival receipts avoid treating the Object's frozen clock as wall time.
   * These requests keep normal output gates, are owned by the invocation, and are
   * awaited only by the diagnostic /metrics request after the primary timer stops.
   * Their arrivals still include network and output-gate delay; they are estimates.
   */
  marker(name: string) {
    if (!this.phaseProbes || this.env.PHASE !== "measure" || !this.query) return;

    const row: Observation["markers"][number] = {
      name,
      localMs: Date.now(),
      invocation: invocation.getStore(),
    };

    this.markers.push(row);
    const url = new URL("/echo", this.env.PROVIDER_URL);

    url.searchParams.set(
      "sample",
      `${this.query.object}/${this.query.sample}/${name}/${this.markers.length}`,
    );

    const pending = globalThis
      .fetch(url, {
        headers: { authorization: `Bearer ${this.env.TOKEN}` },
        signal: AbortSignal.timeout(10_000),
      })
      .then(async (response) => {
        if (!response.ok) throw new Error(`Phase timestamp receipt failed: ${response.status}`);
        row.provider = decodeEcho(await response.json());
      })
      .catch((cause) => {
        row.error = errorText(cause);
      });

    this.markerRequests.push(pending);
    this.keepAlive(pending);
  }
  async drainMarkers() {
    let count;

    do {
      count = this.markerRequests.length;
      await Promise.all(this.markerRequests);
    } while (count !== this.markerRequests.length);
  }
  snapshot() {
    return { ...this.counts };
  }
  identity(state: DurableObjectState, env: Env) {
    return {
      objectId: state.id.toString(),
      incarnation: this.incarnation,
      constructedMs: this.constructedMs,
      version: env.VERSION.id,
      generation: env.PHASE,
    };
  }
  alarmEvent(alarmId: string, edge: "start" | "end", state: DurableObjectState) {
    if (edge === "start") {
      this.alarmStarts++;
      this.activeAlarms.add(alarmId);
    } else this.activeAlarms.delete(alarmId);
    if (this.env.PHASE !== "measure") return;

    const event = {
      alarmId,
      edge,
      active: this.active,
      query: this.query,
      atMs: Date.now(),
      ...this.identity(state, this.env),
    };

    this.alarmEvents.push(event);
    console.log({ warmFloor: "target-alarm", ...event });
    if (edge === "start") {
      this.marker("alarm-entry");
      this.marker("alarm-entry-control"); // Adjacent dispatches estimate network/order noise.
    } else this.marker("alarm-exit");
  }
  fetch =
    (env: Env): typeof globalThis.fetch =>
    async (input, init) => {
      if (!this.active || !this.query || env.PHASE !== "measure")
        throw new Error("Provider dispatch outside a measured request");
      const query = this.query;
      const original = new Request(input, init);
      const url = new URL(original.url);
      const base = new URL(env.PROVIDER_URL);

      if (
        base.protocol !== "https:" ||
        url.origin !== base.origin ||
        url.pathname !== `${base.pathname.replace(/\/$/, "")}/chat/completions`
      )
        throw new Error(`Unexpected provider destination ${url.origin}${url.pathname}`);
      for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));

      const call: Call = {
        call: this.calls.length,
        markerCountAtStart: this.markers.length,
        invocation: invocation.getStore(),
        startMs: Date.now(),
        responseBytes: 0,
        sqlAtStart: this.snapshot(),
        sqlSincePreviousCall: difference(this.counts, this.previous),
      };

      this.previous = this.snapshot();
      this.calls.push(call);
      this.note("provider.fetch.start", { call: call.call });
      url.searchParams.set("call", String(call.call));
      const headers = new Headers(original.headers);

      headers.set("x-warm-floor-sample", query.sample);
      headers.set("authorization", `Bearer ${env.TOKEN}`);
      const request = new Request(url, new Request(original, { headers }));

      try {
        if (query.syncBeforeFetch) {
          call.syncStartedMs = Date.now();
          await this.sync();
          call.syncEndedMs = Date.now();
        }
        call.fetchStartedMs = Date.now();
        const response = await globalThis.fetch(request);

        call.headersMs = Date.now();
        this.note("provider.headers", { call: call.call });
        call.status = response.status;
        call.providerRequest = response.headers.get("x-warm-floor-request");
        if (!response.body) throw new Error("Provider response has no body");
        const reader = response.body.getReader();
        let sseDone = false;
        let finished = false;
        let cancelled = false;
        let pendingLine = "";
        const decoder = new TextDecoder();

        const finish = (cause?: unknown) => {
          if (finished) return;
          finished = true;
          call.endMs = Date.now();
          call.sseDone = sseDone;
          call.sqlAtEnd = this.snapshot();
          this.note("provider.stream.end", { call: call.call });
          if (cause !== undefined) call.error = errorText(cause);
        };

        const stream = new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              const item = await reader.read();

              if (cancelled) return;

              if (item.done) {
                finish(sseDone ? undefined : "EOF before [DONE]");
                reader.releaseLock();
                controller.close();

                return;
              }
              call.firstByteMs ??= Date.now();
              call.responseBytes += item.value.byteLength;
              pendingLine += decoder.decode(item.value, { stream: true });
              const lines = pendingLine.split("\n");

              pendingLine = lines.pop() ?? "";
              for (const line of lines) {
                if (line === "data: [DONE]") sseDone = true;
                if (line.startsWith(": warm-floor-receipt ")) {
                  if (call.providerReceipt) throw new Error("Duplicate provider stream receipt");
                  call.providerReceipt = decodeProviderReceipt(
                    JSON.parse(line.slice(": warm-floor-receipt ".length)),
                  );
                  if (
                    call.providerReceipt.requestId !== call.providerRequest ||
                    call.providerReceipt.call !== call.call
                  )
                    throw new Error("Provider receipt identity mismatch");
                }
              }
              controller.enqueue(item.value);
            } catch (cause) {
              if (cancelled) return;
              // Effect disposes its HTTP scope after [DONE], which can reject
              // this wrapper's already pending read. The SSE completed first.
              finish(sseDone ? undefined : cause);
              reader.releaseLock();
              if (sseDone) controller.close();
              else controller.error(cause);
            }
          },
          async cancel(reason) {
            cancelled = true;
            finish(sseDone ? undefined : (reason ?? "consumer cancelled before [DONE]"));
            try {
              await reader.cancel(reason);
            } finally {
              reader.releaseLock();
            }
          },
        });

        return new Response(stream, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers,
        });
      } catch (cause) {
        call.endMs = Date.now();
        call.sqlAtEnd = this.snapshot();
        call.error = errorText(cause);
        throw cause;
      }
    };
  finish() {
    return {
      sql: this.snapshot(),
      events: this.events,
      eventCounts: this.eventCounts,
      invocationEventCounts: this.invocationEventCounts,
      workTotals: this.workTotals,
      spans: this.spans,
      rpcEvents: this.rpcEvents,
      settlementWrites: this.settlementWrites,
      waiterReads: this.waiterReads,
      phaseProbes: this.phaseProbes,
      markers: this.markers,
      eventAcquisitions: this.eventAcquisitions,
      calls: this.calls,
      syncs: this.syncs,
      alarmEvents: this.alarmEvents,
      alarmsActiveAtStart: this.alarmsActiveAtStart,
      alarmsActiveAtEnd: [...this.activeAlarms],
      sqlAfterLastCall: difference(this.counts, this.previous),
    };
  }
}

const observations = new WeakMap<DurableObjectStorage, Observation>();

const mutations = (counts: Counts) =>
  counts.mutationStatements +
  counts.kvPutCalls +
  counts.kvDeleteCalls +
  counts.setAlarmCalls +
  counts.deleteAlarmCalls;

export const observation = (storage: DurableObjectStorage): Observation => {
  const found = observations.get(storage);

  if (!found) throw new Error("Storage was not instrumented before runtime initialization");

  return found;
};

/** Wrap the state before super(), so SQL captured by framework constructors is observed too. */
export function instrument(state: DurableObjectState, env: Env): DurableObjectState {
  const meter = new Observation(
    env,
    () => state.storage.sync(),
    (promise) => state.waitUntil(promise),
  );

  let transactionCallbacks = 0;

  const sql = new Proxy(state.storage.sql, {
    get(target, property) {
      if (property !== "exec") {
        const value = Reflect.get(target, property, target);

        return typeof value === "function" ? value.bind(target) : value;
      }

      return (query: string, ...bindings: SqlStorageValue[]) => {
        if (meter.active) {
          meter.counts.statements++;
          meter.note("sql", { query: query.replace(/\s+/g, " ").trim(), bindingCount: bindings.length });
          if (/SET state = 'settled', settled_outcome/.test(query)) {
            meter.settlementWrites.push({ atMs: Date.now(), invocation: invocation.getStore() });
            const transaction = markerTransaction.getStore();

            if (transaction) transaction.settled = true;
          }
          if (/FROM\s+"?[^\s"]*effect_agent_submissions"?\s+WHERE submission_id\s*=/i.test(query))
            meter.waiterReads.push({ atMs: Date.now(), invocation: invocation.getStore() });
          // Includes WITH ... INSERT and predicates in mutation bindings. Queries
          // are the pinned adapters' SQL, not user-supplied arbitrary statements.
          if (/\b(?:INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP)\b/i.test(query)) {
            meter.counts.mutationStatements++;
            meter.counts.writeSqlTextBytes += byteLength(query);
            meter.counts.writeBindingBytes += bindings.reduce<number>(
              (sum, value) => sum + byteLength(value),
              0,
            );
          }
        }

        return target.exec(query, ...bindings);
      };
    },
  });

  const kvMethods = (target: object, property: string | symbol) => {
    const value = Reflect.get(target, property, target);

    if (typeof value !== "function") return value;
    if (property === "sync") {
      return async (...args: unknown[]) => {
        if (!meter.active) return value.apply(target, args);
        const counts = meter.counts;

        const entry: Observation["syncs"][number] = {
          startedMs: Date.now(),
          afterProviderCalls: meter.calls.length,
        };

        counts.syncCalls++;
        meter.syncs.push(entry);
        try {
          return await value.apply(target, args);
        } catch (cause) {
          entry.error = errorText(cause);
          throw cause;
        } finally {
          entry.endedMs = Date.now();
          counts.syncWaitMs += entry.endedMs - entry.startedMs;
        }
      };
    }
    if (property === "getAlarm" || property === "setAlarm" || property === "deleteAlarm") {
      return (...args: unknown[]) => {
        if (meter.active) {
          meter.counts[`${property}Calls`]++;
          meter.note(`kv.${property}`);
        }

        return value.apply(target, args);
      };
    }
    if (property === "put") {
      return (...args: unknown[]) => {
        if (meter.active) {
          meter.counts.kvPutCalls++;
          meter.note("kv.put");
          meter.counts.kvJsonBytes += encoder.encode(JSON.stringify(args)).byteLength;
        }

        return value.apply(target, args);
      };
    }
    if (property === "delete" || property === "deleteAll") {
      return (...args: unknown[]) => {
        if (meter.active) {
          meter.counts.kvDeleteCalls++;
          meter.note(`kv.${property}`);
        }

        return value.apply(target, args);
      };
    }

    return value.bind(target);
  };

  const storage = new Proxy(state.storage, {
    get(target, property) {
      if (property === "sql") return sql;
      if (property === "transactionSync")
        return <T>(callback: () => T) => {
          const counts = meter.active ? meter.counts : undefined;
          const before = counts && mutations(counts);

          if (counts) counts.transactionSync++;
          try {
            const result = target.transactionSync(callback);

            if (counts && before !== undefined && mutations(counts) > before)
              counts.writeTransactionSync++;

            return result;
          } catch (cause) {
            if (meter.active) meter.counts.transactionFailures++;
            throw cause;
          }
        };
      if (property === "transaction")
        return async <T>(callback: (tx: DurableObjectTransaction) => Promise<T>) => {
          const counts = meter.active ? meter.counts : undefined;

          if (counts) counts.transactions++;
          meter.note("transaction.start", { transaction: counts?.transactions });
          let wrote = false;
          let rolledBack = false;
          const settlement = { settled: false };

          try {
            const result = await target.transaction(async (tx) => {
              rolledBack = false;
              settlement.settled = false;
              const before = counts && mutations(counts);

              if (counts && transactionCallbacks > 0) counts.overlappingTransactionCallbacks++;
              transactionCallbacks++;
              meter.note("transaction.callback.start");
              try {
                return await markerTransaction.run(settlement, () =>
                  callback(
                    new Proxy(tx, {
                      get(target, property) {
                        if (property !== "rollback") return kvMethods(target, property);

                        return (...args: unknown[]) => {
                          rolledBack = true;
                          if (counts) counts.transactionRollbacks++;

                          return Reflect.get(target, property, target).apply(target, args);
                        };
                      },
                    }),
                  ),
                );
              } finally {
                transactionCallbacks--;
                meter.note("transaction.callback.end");
                wrote = counts !== undefined && before !== undefined && mutations(counts) > before;
              }
            });

            if (settlement.settled && !rolledBack) meter.marker("settlement-transaction-return");
            meter.note("transaction.end", { wrote, settled: settlement.settled, rolledBack });
            if (counts && wrote && !rolledBack) counts.writeTransactions++;

            return result;
          } catch (cause) {
            if (meter.active) meter.counts.transactionFailures++;
            throw cause;
          } finally {
            if (counts && (meter.counts !== counts || !meter.active))
              counts.transactionWindowCrossings++;
          }
        };

      return kvMethods(target, property);
    },
  });

  observations.set(storage, meter);
  observations.set(state.storage, meter);

  // DurableObjectBase requires the native state's brand in its constructor.
  // Shadow only the JS storage accessor; retain the exact native state object.
  Object.defineProperty(state, "storage", { value: storage, configurable: true });
  if (env.PHASE === "measure") {
    // Cloudflare attaches the triggering invocation identity to this log. This
    // distinguishes request-triggered cold hydration from an earlier alarm wake.
    console.log({ warmFloor: "target-constructed", ...meter.identity(state, env) });
  }

  return state;
}
