// Task-local transport checks: the pi control must not load Effect just for instrumentation.
// The controller still validates every returned value against the shared Effect Schemas.
import type { Message } from "../../src/plan.ts";
import type {
  BulkFixture,
  Identity,
  IsolateState,
  PaddingRequest,
  ProviderReceipt,
  Query,
  SqlDump,
  Target,
} from "./protocol.ts";

export const object = (input: unknown): Record<string, unknown> => {
  if (typeof input !== "object" || input === null || Array.isArray(input))
    throw new Error("Expected an object");

  return Object.fromEntries(Object.entries(input));
};

export const array = (input: unknown): unknown[] => {
  if (!Array.isArray(input)) throw new Error("Expected an array");

  return input;
};

export const string = (input: unknown): string => {
  if (typeof input !== "string") throw new Error("Expected a string");

  return input;
};

const nonempty = (input: unknown): string => {
  const value = string(input);

  if (!value) throw new Error("Expected a nonempty string");

  return value;
};

const finite = (input: unknown): number => {
  if (typeof input !== "number" || !Number.isFinite(input))
    throw new Error("Expected a finite number");

  return input;
};

export const integer = (input: unknown): number => {
  const value = finite(input);

  if (!Number.isSafeInteger(value)) throw new Error("Expected an integer");

  return value;
};

export const natural = (input: unknown): number => {
  const value = integer(input);

  if (value < 0) throw new Error("Expected a natural number");

  return value;
};

const boolean = (input: unknown): boolean => {
  if (typeof input !== "boolean") throw new Error("Expected a boolean");

  return input;
};

export const parsePaddingRequest = (input: unknown): PaddingRequest => {
  const value = object(input);
  const mib = natural(value.mib);

  if (mib > 64) throw new Error("Padding exceeds the benchmark limit");

  return { mib, read: boolean(value.read) };
};

const target = (input: unknown): Target => {
  if (input !== "yielded" && input !== "pi" && input !== "tardie" && input !== "bare")
    throw new Error("Unknown target");

  return input;
};

const query = (input: unknown): Query => {
  const value = object(input);
  const history = natural(value.history);
  const ttftMs = integer(value.ttftMs);
  const chunkDelayMs = natural(value.chunkDelayMs);

  if ((ttftMs !== 0 && ttftMs !== 400) || chunkDelayMs > 1000)
    throw new Error("Invalid query limits");

  return {
    target: target(value.target),
    object: nonempty(value.object),
    history,
    sample: nonempty(value.sample),
    ttftMs,
    chunkDelayMs,
  };
};

export const readQuery = (url: URL): Query =>
  query({
    target: url.searchParams.get("target"),
    object: url.searchParams.get("object"),
    history: Number(url.searchParams.get("history")),
    sample: url.searchParams.get("sample") ?? "import",
    ttftMs: Number(url.searchParams.get("ttftMs") ?? 0),
    chunkDelayMs: Number(url.searchParams.get("chunkDelayMs") ?? 0),
  });

export const parseIsolate = (input: unknown): IsolateState => {
  const value = object(input);

  return {
    id: nonempty(value.id),
    build: string(value.build),
    statelessFetches: natural(value.statelessFetches),
    durableObjectConstructors: natural(value.durableObjectConstructors),
  };
};

export const parseIdentity = (input: unknown): Identity => {
  const value = object(input);

  return {
    incarnation: nonempty(value.incarnation),
    constructedMs: finite(value.constructedMs),
    firstEntry: boolean(value.firstEntry),
    priorAlarms: natural(value.priorAlarms),
    ...(value.isolate === undefined ? {} : { isolate: parseIsolate(value.isolate) }),
    ...(value.workerIsolate === undefined
      ? {}
      : { workerIsolate: parseIsolate(value.workerIsolate) }),
  };
};

const dump = (input: unknown): SqlDump => {
  const value = object(input);

  return {
    tables: array(value.tables).map((input) => {
      const table = object(input);

      return {
        name: nonempty(table.name),
        sql: nonempty(table.sql),
        columns: array(table.columns).map(string),
        rows: array(table.rows).map((row) =>
          array(row).map((cell) => {
            if (cell === null || typeof cell === "string") return cell;
            if (typeof cell === "number") return finite(cell);

            return { blob: string(object(cell).blob) };
          }),
        ),
      };
    }),
    indexes: array(value.indexes).map(string),
  };
};

export const parseFixture = (input: unknown): BulkFixture => {
  const value = object(input);

  if (value.version !== 1 || value.target !== "pi" || value.mode !== "import")
    throw new Error("Expected a native pi import fixture");
  const history = natural(value.history);

  return {
    version: 1,
    target: "pi",
    history,
    fingerprint: nonempty(value.fingerprint),
    sourceVersion: nonempty(value.sourceVersion),
    mode: "import",
    tables: Object.fromEntries(
      Object.entries(object(value.tables)).map(([key, count]) => [key, natural(count)]),
    ),
    ...(value.thread === undefined ? {} : { thread: dump(value.thread) }),
    ...(value.fallbackReason === undefined
      ? {}
      : { fallbackReason: nonempty(value.fallbackReason) }),
  };
};

export const parseReceipt = (input: unknown): ProviderReceipt => {
  const value = object(input);

  if (value.error !== null) throw new Error("Provider receipt failed");

  return {
    ...query(value),
    call: natural(value.call),
    requestId: nonempty(value.requestId),
    arrivalMs: finite(value.arrivalMs),
    firstByteMs: finite(value.firstByteMs),
    endMs: finite(value.endMs),
    fingerprint: string(value.fingerprint),
    rawWireFingerprint: string(value.rawWireFingerprint),
    requestBytes: natural(value.requestBytes),
    colo: value.colo === null ? null : string(value.colo),
    error: null,
  };
};

export const argument = (input: unknown): number => integer(object(input).n);

export const chatTranscript = (input: unknown): Message[] =>
  array(object(input).messages).flatMap((input) => {
    const message = object(input);

    if (message.role === "system" || message.role === "developer") return [];
    if (message.role !== "user" && message.role !== "assistant" && message.role !== "tool")
      throw new Error("Unknown chat role");

    let text =
      typeof message.content === "string"
        ? message.content
        : message.content === null || message.content === undefined
          ? ""
          : array(message.content)
              .map((input) => {
                const part = object(input);

                string(part.type);

                return part.text === undefined ? "" : string(part.text);
              })
              .join("");

    if (message.role === "tool" && text.startsWith('"')) text = string(JSON.parse(text));

    const calls =
      message.tool_calls === undefined
        ? []
        : array(message.tool_calls).map((input) =>
            argument(JSON.parse(string(object(object(input).function).arguments))),
          );

    return [{ role: message.role, text, ...(calls.length ? { calls } : {}) }];
  });

export const errorText = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause);

export const expectedSeed: Readonly<Record<number, string>> = {
  50: "b017b487524e44a4",
  250: "dcea9f30b0917245",
  1000: "ac520308146f2a8f",
};
