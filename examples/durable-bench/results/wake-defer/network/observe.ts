import type { Message } from "../../../src/plan.ts";
import { opaqueDetails, opaqueId } from "./ids.ts";
import { decodeProviderReceipt, errorText, type Env, type ProviderReceipt, type Query } from "./protocol.ts";

const encoder = new TextEncoder();

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
}

/** Logical SQL mutation bindings and JSON-sized KV values; never physical disk/WAL bytes. */
export class Observation {
  constructor(
    readonly env: Env,
    readonly sync: () => Promise<void>,
    readonly objectId: string,
  ) {
    this.active = env.PHASE === "measure";
  }
  counts = zero();
  phase = "entry";
  events: Array<Record<string, unknown>> = [];
  eventsTruncated = false;
  private eventSequence = 0;
  record(event: string, detail: Record<string, unknown> = {}) {
    if (this.env.PHASE !== "measure") return;
    const entry = {
      event, sequence: this.eventSequence++, ioMs: Date.now(), phase: this.phase,
      active: this.active, query: this.query, afterProviderCalls: this.calls.length,
      objectId: this.objectId, incarnation: this.incarnation, version: opaqueId(this.env.VERSION.id),
      activeAlarmIds: [...this.activeAlarms], ...opaqueDetails(detail),
    };
    if (this.events.length < 20_000) this.events.push(entry);
    else this.eventsTruncated = true;
    // Returned receipts retain hot-path observations without one console log per hint.
    // Reports outside the client interval still survive in the platform capture.
    if (!this.active || event === "maintenanceReport" || event === "recovery")
      console.log({ wakeDefer: "mechanism", ...entry });
  }
  calls: Call[] = [];
  syncs: Array<{ startedMs: number; endedMs?: number; afterProviderCalls: number; error?: string }> = [];
  alarmEvents: Array<{
    alarmId: string; edge: "start" | "end"; active: boolean; query?: Query;
    objectId: string; incarnation: string; version: string;
  }> = [];
  seen: readonly Message[] = [];
  query?: Query;
  active: boolean;
  private firstRequest = true;
  private alarmStarts = 0;
  private readonly activeAlarms = new Set<string>();
  private alarmsActiveAtStart: string[] = [];
  readonly incarnation = opaqueId(crypto.randomUUID());
  readonly constructedMs = Date.now();
  private previous = zero();
  entry() {
    return { firstHarnessRequest: this.firstRequest, priorAlarmStarts: this.alarmStarts,
      activeAlarmIds: [...this.activeAlarms] };
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
    this.previous = zero();
    this.calls = [];
    this.syncs = [];
    this.alarmEvents = [];
    this.events = [];
    this.eventsTruncated = false;
    this.phase = "entry";
    this.alarmsActiveAtStart = [...this.activeAlarms];
  }
  snapshot() {
    return { ...this.counts };
  }
  identity(state: DurableObjectState, env: Env) {
    return {
      objectId: this.objectId,
      incarnation: this.incarnation,
      constructedMs: this.constructedMs,
      version: opaqueId(env.VERSION.id),
      generation: env.PHASE,
      buildId: env.BUILD_ID,
      buildMode: env.BUILD_MODE,
    };
  }
  alarmEvent(alarmId: string, edge: "start" | "end", state: DurableObjectState) {
    if (edge === "start") { this.alarmStarts++; this.activeAlarms.add(alarmId); }
    else this.activeAlarms.delete(alarmId);
    if (this.env.PHASE !== "measure") return;
    const event = { alarmId, edge, active: this.active, query: this.query,
      ...this.identity(state, this.env) };
    this.alarmEvents.push(event);
    console.log({ wakeDefer: "target-alarm", ...event });
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
        startMs: Date.now(),
        responseBytes: 0,
        sqlAtStart: this.snapshot(),
        sqlSincePreviousCall: difference(this.counts, this.previous),
      };

      this.previous = this.snapshot();
      this.calls.push(call);
      url.searchParams.set("call", String(call.call));
      const headers = new Headers(original.headers);

      headers.set("x-wake-defer-sample", query.sample);
      headers.set("authorization", `Bearer ${env.TOKEN}`);
      const request = new Request(url, new Request(original, { headers }));

      try {
        call.fetchStartedMs = Date.now();
        const response = await globalThis.fetch(request);

        call.headersMs = Date.now();
        call.status = response.status;
        call.providerRequest = response.headers.get("x-wake-defer-request");
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
                if (line.startsWith(": wake-defer-receipt ")) {
                  if (call.providerReceipt) throw new Error("Duplicate provider stream receipt");
                  call.providerReceipt = decodeProviderReceipt(JSON.parse(line.slice(": wake-defer-receipt ".length)));
                  if (call.providerReceipt.requestId !== call.providerRequest || call.providerReceipt.call !== call.call)
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
            try { await reader.cancel(reason); }
            finally { reader.releaseLock(); }
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
      mechanism: [...this.events],
      mechanismTruncated: this.eventsTruncated,
      sql: this.snapshot(),
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
const mutations = (counts: Counts) => counts.mutationStatements + counts.kvPutCalls + counts.kvDeleteCalls + counts.setAlarmCalls + counts.deleteAlarmCalls;

export const observation = (storage: DurableObjectStorage): Observation => {
  const found = observations.get(storage);

  if (!found) throw new Error("Storage was not instrumented before runtime initialization");

  return found;
};

/** Wrap the state before super(), so SQL captured by framework constructors is observed too. */
export function instrument(state: DurableObjectState, env: Env): DurableObjectState {
  const meter = new Observation(env, () => state.storage.sync(), opaqueId(state.id.toString()));
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
          // Includes WITH ... INSERT and predicates in mutation bindings. Queries
          // are the pinned adapters' SQL, not user-supplied arbitrary statements.
          if (/\b(?:INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP)\b/i.test(query)) {
            meter.counts.mutationStatements++;
            meter.counts.writeSqlTextBytes += byteLength(query);
            meter.counts.writeBindingBytes += bindings.reduce<number>(
              (sum, value) => sum + byteLength(value), 0,
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
        if (meter.active) meter.counts[`${property}Calls`]++;
        if (property !== "getAlarm") meter.record("nativeAlarm", {
          operation: property, deadline: property === "setAlarm" ? Number(args[0]) : null,
        });
        return value.apply(target, args);
      };
    }
    if (property === "put") {
      return (...args: unknown[]) => {
        if (meter.active) {
          meter.counts.kvPutCalls++;
          meter.counts.kvJsonBytes += encoder.encode(JSON.stringify(args)).byteLength;
        }
        return value.apply(target, args);
      };
    }
    if (property === "delete" || property === "deleteAll") {
      return (...args: unknown[]) => {
        if (meter.active) meter.counts.kvDeleteCalls++;
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
            if (counts && before !== undefined && mutations(counts) > before) counts.writeTransactionSync++;
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
          let wrote = false;
          let rolledBack = false;
          try {
            const result = await target.transaction(async (tx) => {
              rolledBack = false;
              const before = counts && mutations(counts);
              if (counts && transactionCallbacks > 0) counts.overlappingTransactionCallbacks++;
              transactionCallbacks++;
              try { return await callback(new Proxy(tx, { get(target, property) {
                if (property !== "rollback") return kvMethods(target, property);
                return (...args: unknown[]) => {
                  rolledBack = true;
                  if (counts) counts.transactionRollbacks++;
                  return Reflect.get(target, property, target).apply(target, args);
                };
              } })); }
              finally {
                transactionCallbacks--;
                wrote = counts !== undefined && before !== undefined && mutations(counts) > before;
              }
            });
            if (counts && wrote && !rolledBack) counts.writeTransactions++;
            return result;
          } catch (cause) {
            if (meter.active) meter.counts.transactionFailures++;
            throw cause;
          } finally {
            if (counts && (meter.counts !== counts || !meter.active)) counts.transactionWindowCrossings++;
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
    console.log({ wakeDefer: "target-constructed", ...meter.identity(state, env) });
  }
  return state;
}
