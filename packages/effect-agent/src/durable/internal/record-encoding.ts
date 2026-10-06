import { DateTime, Predicate, Schema } from "effect";

import type { RunId } from "../../core/Identifiers.ts";
import { utf8ByteLength } from "../../core/internal/utf8.ts";
import { canonicalJson } from "../Digest.ts";
import { ExportedRecord } from "../RecordFormat.ts";
import {
  type CanonicalRecordPayload,
  RecordEnvelope,
  RecordJson,
  type RunContinuation,
} from "../Records.ts";
import {
  executionRunIds,
  isPreContinuationFact,
  isTerminalBudgetFact,
  terminalUsageCharge,
} from "./record-ownership.ts";

const encodeRecord = Schema.encodeSync(RecordEnvelope);
const validateJson = Schema.decodeSync(RecordJson);

export interface RecordEncoding {
  readonly canonical: RecordEnvelope;
  readonly wire: RecordJson;
  readonly json: string;
  readonly bytes: number;
  readonly progress: ProgressAppendRecord;
}

/** Private capture metadata, never caller-supplied wire data or execution authority. */
export interface ProgressAppendRecord {
  readonly recordId: RecordEnvelope["recordId"];
  readonly runIds: ReadonlyArray<RunId>;
  readonly recordBytes: number;
  readonly terminalUsageBytes: number;
  readonly preContinuation: boolean;
  readonly terminal: boolean;
  readonly continuation?: RunContinuation;
}

const progressRecord = (record: RecordEnvelope, bytes: number): ProgressAppendRecord => {
  const payload = record.payload;

  return Object.freeze({
    recordId: record.recordId,
    runIds: Object.freeze(executionRunIds(record)),
    recordBytes: bytes,
    preContinuation: isPreContinuationFact(record),
    terminal: isTerminalBudgetFact(record),
    terminalUsageBytes:
      payload._tag === "ModelResponseRecorded" || payload._tag === "ModelCallAborted"
        ? terminalUsageCharge(payload.modelUsage ?? [])
        : 0,
    ...(payload._tag === "RunContinuation" ? { continuation: payload } : {}),
  });
};

const captured = new WeakMap<RecordEnvelope, RecordEncoding>();

/** Own every fact before suspension; encoded JSON fields can share these private frozen values. */
const capturePayload = (payload: CanonicalRecordPayload) => {
  const copy = { ...payload };

  Object.setPrototypeOf(copy, Object.getPrototypeOf(payload));
  const copies = new WeakMap<object, object>([[payload, copy]]);

  const pending: Array<{ readonly source: object; readonly target: object }> = [
    { source: payload, target: copy },
  ];

  const owned: Array<object> = [copy];

  while (pending.length > 0) {
    const next = pending.pop()!;

    for (const [key, value] of Object.entries(next.source)) {
      if (!Predicate.isObject(value)) continue;
      let child = copies.get(value);

      if (child === undefined) {
        const created: object = Array.isArray(value)
          ? []
          : Object.create(Object.getPrototypeOf(value));

        Object.assign(created, value);
        copies.set(value, created);
        owned.push(created);
        pending.push({ source: value, target: created });
        child = created;
      }
      Reflect.set(next.target, key, child);
    }
  }
  for (const value of owned) Object.freeze(value);

  return { payload: Object.freeze(copy), copies };
};

/** Schema-created structures are private; detach JSON pass-throughs using the same owned copy. */
const ownWire = (wire: RecordJson, copies: WeakMap<object, object>): RecordJson => {
  const pending: Array<object> = Predicate.isObject(wire) ? [wire] : [];

  while (pending.length > 0) {
    const next = pending.pop()!;

    for (const [key, value] of Object.entries(next)) {
      if (!Predicate.isObject(value)) continue;
      const copy = copies.get(value);

      if (copy !== undefined) Reflect.set(next, key, copy);
      else pending.push(value);
    }
    Object.freeze(next);
  }

  return wire;
};

/** Capture at the Schema boundary; only the privately owned result can reuse its encoding. */
export const captureRecord = (input: RecordEnvelope): RecordEncoding => {
  const existing = captured.get(input);

  if (existing !== undefined) return existing;
  const encoded = validateJson(encodeRecord(input));
  const owned = capturePayload(input.payload);
  const wire = ownWire(encoded, owned.copies);
  const json = canonicalJson(wire);
  const createdAt = DateTime.makeUnsafe(DateTime.toEpochMillis(input.createdAt));

  // DateTime fills this cache lazily; populate it before freezing our private copy.
  Object.freeze(DateTime.toPartsUtc(createdAt));
  Object.freeze(createdAt);

  const canonical = Object.freeze(
    new RecordEnvelope(
      {
        ...input,
        createdAt,
        payload: owned.payload,
      },
      { disableChecks: true },
    ),
  );

  const bytes = utf8ByteLength(json);

  const encoding = Object.freeze({
    canonical,
    wire,
    json,
    bytes,
    progress: progressRecord(canonical, bytes),
  });

  captured.set(canonical, encoding);

  return encoding;
};

/** Read evidence retains its original wire; fresh writes use the captured Schema encoding. */
export const recordEncoding = (record: RecordEnvelope): RecordEncoding => {
  const existing = captured.get(record);

  if (existing !== undefined) return existing;
  if (record instanceof ExportedRecord) {
    const json = canonicalJson(record.wire);

    const bytes = utf8ByteLength(json);

    return {
      canonical: record,
      wire: record.wire,
      json,
      bytes,
      progress: progressRecord(record, bytes),
    };
  }

  return captureRecord(record);
};
