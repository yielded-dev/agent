import { Cause, Effect, Option, Predicate, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { AsyncResult, Atom, AtomHttpApi, Reactivity } from "effect/reactivity";

import { LabApi, Report, VisitorKey, type RunInput } from "./contract.ts";

export class LabClient extends AtomHttpApi.Service<LabClient>()("browser-speed/client", {
  api: LabApi,
  httpClient: FetchHttpClient.layer,
  baseUrl: import.meta.env.BASE_URL.replace(/\/$/, ""),
}) {}

const sessionId = crypto.randomUUID();

export const watchingAtom = Atom.make(false);

export const ClientSample = Schema.Struct({ report: Report, clientElapsedMillis: Schema.Finite });
export type ClientSample = typeof ClientSample.Type;
export const historyAtom = Atom.make<ReadonlyArray<ClientSample>>([]);
export const selectedAtom = Atom.make<string | null>(null);

const headers = { "x-lab-session": sessionId };

/** Visitor keys stay in this browser and travel only as headers on run requests. */
export const ProviderKeys = Schema.Struct({
  openai: Schema.String,
  remember: Schema.Boolean,
});

export type ProviderKeys = typeof ProviderKeys.Type;

const keyStorage = "yielded-browser-use-keys-v1";
const emptyKeys: ProviderKeys = { openai: "", remember: false };
const decodeKeys = Schema.decodeUnknownOption(Schema.fromJsonString(ProviderKeys));
const encodeKeys = Schema.encodeSync(Schema.fromJsonString(ProviderKeys));

// Storage can be missing or blocked (private windows, tests); keys then last for this page only.
const storedKeys = (): ProviderKeys => {
  try {
    const value = localStorage.getItem(keyStorage) ?? sessionStorage.getItem(keyStorage);

    return value === null ? emptyKeys : Option.getOrElse(decodeKeys(value), () => emptyKeys);
  } catch {
    return emptyKeys;
  }
};

export const keysAtom = Atom.make(storedKeys()).pipe(Atom.keepAlive);

export const saveKeysAtom = Atom.fn<ProviderKeys>()(
  Effect.fnUntraced(function* (keys, get) {
    get.set(keysAtom, keys);
    yield* Effect.sync(() => {
      try {
        if (keys.remember) {
          localStorage.setItem(keyStorage, encodeKeys(keys));
          sessionStorage.removeItem(keyStorage);
        } else {
          localStorage.removeItem(keyStorage);
          sessionStorage.setItem(keyStorage, encodeKeys(keys));
        }
      } catch {
        // Keys remain in memory for this page.
      }
    });
  }),
);

export const validKey = (value: string) => Schema.is(VisitorKey)(value.trim());

const keyHeaders = (keys: ProviderKeys) =>
  validKey(keys.openai) ? { "x-lab-openai-key": keys.openai.trim() } : {};

export const accountAtom = LabClient.query("lab", "account", { headers });

/** The travel planner signs visitors in on this origin and returns them to the lab. */
export const signInUrl = `/travel/login?return=${encodeURIComponent(import.meta.env.BASE_URL)}`;

const snapshotQuery = LabClient.query("lab", "snapshot", {
  headers,
  reactivityKeys: ["lab"],
  timeToLive: "5 seconds",
});

const pollingSnapshot = snapshotQuery.pipe(Atom.withRefresh("300 millis"));

export const snapshotAtom = Atom.make((get) =>
  get(get(watchingAtom) ? pollingSnapshot : snapshotQuery),
);

export const runAtom = LabClient.runtime.fn<Omit<RunInput, "id">>()(
  Effect.fnUntraced(function* (input, get) {
    const client = yield* LabClient;

    get.set(watchingAtom, true);
    get.set(selectedAtom, null);
    const start = performance.now();

    return yield* Reactivity.mutation(
      client.lab.run({
        headers: { ...headers, ...keyHeaders(get(keysAtom)) },
        payload: { ...input, id: crypto.randomUUID() },
      }),
      ["lab"],
    ).pipe(
      Effect.tap((report) =>
        Effect.sync(() =>
          get.set(
            historyAtom,
            [...get(historyAtom), { report, clientElapsedMillis: performance.now() - start }].slice(
              -100,
            ),
          ),
        ),
      ),
      Effect.ensuring(
        Effect.sync(() => get.set(watchingAtom, false)).pipe(
          Effect.andThen(Reactivity.invalidate(["lab"])),
        ),
      ),
    );
  }),
);

export const controlAtom = LabClient.runtime.fn<"stop" | "close">()(
  Effect.fnUntraced(function* (action) {
    const client = yield* LabClient;

    yield* Reactivity.mutation(
      action === "stop" ? client.lab.stop({ headers }) : client.lab.close({ headers }),
      ["lab"],
    );
  }),
);

export const exportAtom = Atom.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const samples = get(historyAtom);

    yield* Effect.sync(() => {
      const json = Schema.encodeSync(Schema.fromJsonString(Schema.Array(ClientSample)))(samples);
      const url = URL.createObjectURL(new Blob([json], { type: "application/json" }));
      const link = document.createElement("a");

      link.href = url;
      link.download = "browser-speed-runs.json";
      link.click();
      URL.revokeObjectURL(url);
    });
  }),
);

export const errorMessage = <A, E>(result: AsyncResult.AsyncResult<A, E>) => {
  if (!AsyncResult.isFailure(result)) return null;

  const error = Option.flatMap(
    Cause.findErrorOption(result.cause),
    Schema.decodeUnknownOption(Schema.Struct({ message: Schema.String })),
  );

  if (Option.isSome(error)) return error.value.message;

  return "The connection to the Worker was interrupted. Check the browser status before running again.";
};

/** Durations use the page-ready boundary on the same Worker clock. */
export const fromReady = (report: Report, at: number | null | undefined): number | null =>
  Predicate.isNullish(at) || report.readyAt === null ? null : Math.max(0, at - report.readyAt);

export const verifiedMillis = (report: Report) => fromReady(report, report.verifiedAt);

export const runOutcome = (report: Report) =>
  report.readyAt !== null
    ? report.status
    : report.status === "running"
      ? "preparing"
      : report.status === "cancelled"
        ? "preparation cancelled"
        : "preparation failed";
