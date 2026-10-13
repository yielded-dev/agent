import { DateTime, Predicate, Schema } from "effect";

import type { RunId } from "../../core/Identifiers.ts";
import { utf8ByteLength } from "../../core/internal/utf8.ts";
import { ExportedRecord } from "../RecordFormat.ts";
import {
  type CanonicalRecordPayload,
  MAX_CANONICAL_RECORD_BYTES,
  MAX_PERSISTED_JSON_COLLECTION_LENGTH,
  MAX_PERSISTED_JSON_DEPTH,
  MAX_PERSISTED_JSON_NODES,
  RecordEnvelope,
  type RecordJson,
  type RunContinuation,
} from "../Records.ts";
import { canonicalJson } from "./canonical-json.ts";
import {
  executionRunIds,
  isPreContinuationFact,
  isTerminalBudgetFact,
  terminalUsageCharge,
} from "./record-ownership.ts";

const encodeRecord = Schema.encodeSync(RecordEnvelope);

const recordLimits = {
  depth: MAX_PERSISTED_JSON_DEPTH + 4,
  nodes: MAX_PERSISTED_JSON_NODES * 4,
  collectionLength: MAX_PERSISTED_JSON_COLLECTION_LENGTH,
  // UTF-16 units give a lower bound while serializing; capture checks the exact UTF-8 width.
  textUnits: MAX_CANONICAL_RECORD_BYTES,
};

export interface RecordEncoding {
  readonly canonical: RecordEnvelope;
  readonly wire: RecordJson;
  readonly json: string;
  readonly bytes: number;
  readonly progress: ProgressAppendRecord;
}

interface CapturedRecordEncoding extends RecordEncoding {
  readonly wire: typeof RecordEnvelope.Encoded;
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

const captured = new WeakMap<RecordEnvelope, CapturedRecordEncoding>();

// Define own data properties so valid JSON keys never invoke inherited setters.
const setOwn = (target: object, key: PropertyKey, value: unknown): void => {
  Object.defineProperty(target, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
};

/** JSON arrays use indexed values, even when inherited or non-enumerable. */
const copyOwn = (value: object): object => {
  if (!Array.isArray(value)) {
    const copy = { ...value };

    Object.setPrototypeOf(copy, Object.getPrototypeOf(value));

    return copy;
  }
  const copy: Array<unknown> = [];

  for (let index = 0; index < value.length; index++) setOwn(copy, index, value[index]);
  for (const key of Reflect.ownKeys(value)) {
    if (
      !Object.hasOwn(copy, key) &&
      Object.getOwnPropertyDescriptor(value, key)?.enumerable === true
    )
      setOwn(copy, key, Reflect.get(value, key));
  }

  return copy;
};

/** Own every fact before suspension; encoded JSON fields can share these private frozen values. */
const capturePayload = (payload: CanonicalRecordPayload) => {
  const copy = { ...payload };

  Object.setPrototypeOf(copy, Object.getPrototypeOf(payload));
  const copies = new WeakMap<object, object>([[payload, copy]]);

  const pending: Array<object> = [copy];

  const owned: Array<object> = [copy];

  while (pending.length > 0) {
    const next = pending.pop()!;

    for (const [key, value] of Object.entries(next)) {
      if (!Predicate.isObjectOrArray(value)) continue;
      let child = copies.get(value);

      if (child === undefined) {
        const created = copyOwn(value);

        copies.set(value, created);
        owned.push(created);
        pending.push(created);
        child = created;
      }
      setOwn(next, key, child);
    }
  }
  for (const value of owned) Object.freeze(value);

  return { payload: Object.freeze(copy), copies };
};

/** Schema-created structures are private; detach JSON pass-throughs using the same owned copy. */
const ownWire = <Wire extends RecordJson>(wire: Wire, copies: WeakMap<object, object>): Wire => {
  const pending: Array<object> = Predicate.isObjectOrArray(wire) ? [wire] : [];

  while (pending.length > 0) {
    const next = pending.pop()!;

    for (const [key, value] of Object.entries(next)) {
      if (!Predicate.isObjectOrArray(value)) continue;
      const copy = copies.get(value);

      if (copy !== undefined) setOwn(next, key, copy);
      else pending.push(value);
    }
    Object.freeze(next);
  }

  return wire;
};

/** Capture at the Schema boundary; only the privately owned result can reuse its encoding. */
export const captureRecord = (input: RecordEnvelope): CapturedRecordEncoding => {
  const existing = captured.get(input);

  if (existing !== undefined) return existing;
  const encoded = encodeRecord(input);
  const owned = capturePayload(input.payload);
  const wire = ownWire(encoded, owned.copies);
  const json = canonicalJson(wire, recordLimits);
  const bytes = utf8ByteLength(json);

  if (bytes > MAX_CANONICAL_RECORD_BYTES)
    throw new RangeError("Canonical JSON exceeds its byte bound");
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
