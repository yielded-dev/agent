import { Cause, Effect, Option, Predicate, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { AsyncResult, Atom, AtomHttpApi, Reactivity } from "effect/reactivity";

import {
  LabApi,
  LabError,
  Report,
  VisitorKey,
  requiresReasoning,
  serviceTierForModel,
  type BrowserEngine,
  type ModelId,
  type RunInput,
} from "./contract.ts";

export class LabClient extends AtomHttpApi.Service<LabClient>()("browser-speed/client", {
  api: LabApi,
  httpClient: FetchHttpClient.layer,
  baseUrl: import.meta.env.BASE_URL.replace(/\/$/, ""),
}) {}

const sessionId = crypto.randomUUID();

export const watchingAtom = Atom.make(false);
const stopRequestedAtom = Atom.make(false);

export const progressAtom = Atom.make({ current: 0, total: 0 });

export const ClientSample = Schema.Struct({ report: Report, clientElapsedMillis: Schema.Finite });
export type ClientSample = typeof ClientSample.Type;
export const historyAtom = Atom.make<ReadonlyArray<ClientSample>>([]);
export const selectedAtom = Atom.make<string | null>(null);

export const chatDraftAtom = Atom.make("").pipe(Atom.keepAlive);
export const chatIdAtom = Atom.make<string | null>(null).pipe(Atom.keepAlive);

export interface ChatTurn {
  readonly id: string;
  readonly prompt: string;
  readonly pending: boolean;
  readonly connectionLost?: boolean;
  readonly report?: Report;
  readonly error?: string;
}

export const chatTurnsAtom = Atom.make<ReadonlyArray<ChatTurn>>([]).pipe(Atom.keepAlive);

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

/** Shared Agent sign-in returns visitors to the lab. */
export const signInUrl = `/auth/login?provider=yielded&return=${encodeURIComponent(import.meta.env.BASE_URL)}`;

const snapshotQuery = LabClient.query("lab", "snapshot", {
  headers,
  reactivityKeys: ["lab"],
  timeToLive: "5 seconds",
});

const pollingSnapshot = snapshotQuery.pipe(Atom.withRefresh("300 millis"));

export const snapshotAtom = Atom.make((get) => {
  const snapshot = get(snapshotQuery);
  const remote = Option.getOrNull(AsyncResult.value(snapshot));

  // A lost POST does not imply the Worker stopped. Keep its status and Stop control live.
  return get(watchingAtom) || remote?.busy ? get(pollingSnapshot) : snapshot;
});

export const runAtom = LabClient.runtime.fn<
  Omit<RunInput, "id"> & {
    readonly repetitions: number;
    readonly compareModels?: ReadonlyArray<ModelId>;
    readonly compareEngines?: ReadonlyArray<typeof BrowserEngine.Type>;
  }
>()(
  Effect.fnUntraced(function* (input, get) {
    const client = yield* LabClient;

    const {
      repetitions,
      compareModels,
      compareEngines,
      model,
      engine,
      reasoning,
      serviceTier,
      ...request
    } = input;

    const models = compareModels?.length ? compareModels : [model];
    const engines = compareEngines?.length ? compareEngines : [engine ?? "chromium"];
    const variants = models.flatMap((model) => engines.map((engine) => ({ model, engine })));

    // Rotate which configuration runs first each round, keeping requests sequential.
    const attempts = Array.from({ length: repetitions }, (_, round) =>
      variants.map((variant, index) => variants[(index + round) % variants.length] ?? variant),
    ).flat();

    get.set(watchingAtom, true);
    get.set(stopRequestedAtom, false);
    get.set(selectedAtom, null);

    return yield* Effect.gen(function* () {
      for (const [index, variant] of attempts.entries()) {
        if (get(stopRequestedAtom)) break;
        get.set(progressAtom, { current: index + 1, total: attempts.length });
        const start = performance.now();

        const report = yield* Reactivity.mutation(
          client.lab.run({
            headers: { ...headers, ...keyHeaders(get(keysAtom)) },
            payload: {
              ...request,
              engine: variant.engine,
              liveView: request.liveView && variant.engine === "chromium",
              ...(variant.model === undefined ? {} : { model: variant.model }),
              ...(!variant.model?.startsWith("@cf/")
                ? {
                    ...(reasoning === undefined
                      ? {}
                      : {
                          reasoning:
                            reasoning === "none" && requiresReasoning(variant.model ?? "")
                              ? "low"
                              : reasoning,
                        }),
                    ...(serviceTier === undefined
                      ? {}
                      : { serviceTier: serviceTierForModel(variant.model ?? "", serviceTier) }),
                  }
                : {}),
              id: crypto.randomUUID(),
            },
          }),
          ["lab"],
        );

        get.set(chatIdAtom, null);
        get.set(chatTurnsAtom, []);
        get.set(
          historyAtom,
          [
            ...get(historyAtom),
            {
              report,
              clientElapsedMillis: performance.now() - start,
            },
          ].slice(-100),
        );
        if (
          report.status === "cancelled" ||
          report.cleanup === "failed" ||
          (report.status === "failed" && variants.length === 1)
        )
          break;
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => get.set(watchingAtom, false)).pipe(
          Effect.andThen(Reactivity.invalidate(["lab"])),
        ),
      ),
    );
  }),
);

export const controlAtom = LabClient.runtime.fn<"stop" | "close">()(
  Effect.fnUntraced(function* (action, get) {
    const client = yield* LabClient;

    if (action === "stop") {
      get.set(stopRequestedAtom, true);
      get.set(chatIdAtom, null);
    }
    if (action === "close") get.set(chatDraftAtom, "");
    yield* Reactivity.mutation(
      action === "stop" ? client.lab.stop({ headers }) : client.lab.close({ headers }),
      ["lab"],
    );
    if (action === "close") {
      get.set(chatIdAtom, null);
      get.set(chatTurnsAtom, []);
    }
  }),
);

export const sendChatAtom = LabClient.runtime.fn<
  Omit<RunInput, "id" | "scenario" | "prompt" | "chatId">
>()(
  Effect.fnUntraced(function* (settings, get) {
    if (get(watchingAtom)) return;
    const prompt = get(chatDraftAtom).trim();

    if (!prompt) return;
    const client = yield* LabClient;
    const { url, ...options } = settings;
    const id = crypto.randomUUID();
    const chatId = get(chatIdAtom);
    const start = performance.now();

    get.mount(stopRequestedAtom);
    get.set(watchingAtom, true);
    get.set(stopRequestedAtom, false);
    get.set(selectedAtom, null);
    get.set(progressAtom, { current: 1, total: 1 });
    get.set(chatTurnsAtom, [...get(chatTurnsAtom), { id, prompt, pending: true }].slice(-50));
    get.set(chatDraftAtom, "");

    return yield* Effect.gen(function* () {
      // One polling transport, owned by this workflow. Keep the latest matching snapshot
      // even when the POST fails; a previous run's delayed poll cannot populate this turn.
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          get.registry.subscribe(snapshotAtom, (snapshot) => {
            if (!AsyncResult.isSuccess(snapshot)) return;
            const report = snapshot.value.report;

            if (report?.input.id !== id) return;
            get.set(
              chatTurnsAtom,
              get(chatTurnsAtom).map((turn) =>
                turn.id === id &&
                turn.pending &&
                (turn.report === undefined ||
                  (turn.report.status === "running" &&
                    (report.status !== "running" || report.elapsed >= turn.report.elapsed)))
                  ? { ...turn, report }
                  : turn,
              ),
            );
          }),
        ),
        (unsubscribe) => Effect.sync(unsubscribe),
      );

      const report = yield* Reactivity.mutation(
        client.lab.run({
          headers: { ...headers, ...keyHeaders(get(keysAtom)) },
          payload: {
            ...options,
            id,
            scenario: "chat",
            prompt,
            ...(chatId === null ? (url === undefined ? {} : { url }) : { chatId }),
          },
        }),
        ["lab"],
      ).pipe(
        Effect.catchIf(
          (error) =>
            !(
              Schema.is(LabError)(error) &&
              ["configuration", "invalid", "busy", "expired"].includes(error.code)
            ),
          () =>
            Effect.gen(function* () {
              get.set(
                chatTurnsAtom,
                get(chatTurnsAtom).map((turn) =>
                  turn.id === id ? { ...turn, connectionLost: true } : turn,
                ),
              );

              // Match the owner's twelve-minute run limit. A matching report can shorten
              // the wait without synchronized browser/Worker wall clocks.
              const runDeadlineMillis = 12 * 60_000;
              let deadline = start + runDeadlineMillis;

              while (true) {
                if (get(stopRequestedAtom))
                  return yield* LabError.make({
                    code: "interrupted",
                    message:
                      "Stopped checking this response. Admission or completion could not be confirmed. Check the browser status; no actions were replayed.",
                  });
                const snapshot = get(snapshotAtom);

                if (AsyncResult.isSuccess(snapshot)) {
                  const remote = snapshot.value;
                  const report = remote.report;

                  if (report?.input.id === id) {
                    if (!remote.busy && report.status !== "running" && report.cleanup !== "pending")
                      return report;
                    if (remote.busy && remote.canClose)
                      return yield* LabError.make({
                        code: "interrupted",
                        message:
                          "The Worker cannot resolve this run. Choose New chat to close the browser; no actions will be replayed.",
                      });
                    deadline = Math.min(
                      deadline,
                      performance.now() + runDeadlineMillis - report.elapsed,
                    );
                  }
                }
                const remaining = deadline - performance.now();

                if (remaining <= 0)
                  return yield* LabError.make({
                    code: "interrupted",
                    message:
                      "The twelve-minute run deadline was reached without a confirmed result. Check the browser status and close it before starting again; no actions were replayed.",
                  });
                // Read the existing polling atom; never issue another run or status request.
                yield* Effect.sleep(Math.min(300, remaining));
              }
            }),
        ),
      );

      get.set(
        chatIdAtom,
        !get(stopRequestedAtom) && report.cleanup === "retained" ? (chatId ?? id) : null,
      );
      get.set(
        chatTurnsAtom,
        get(chatTurnsAtom).map((turn) =>
          turn.id === id ? { ...turn, report, pending: false, connectionLost: false } : turn,
        ),
      );
      get.set(
        historyAtom,
        [
          ...get(historyAtom),
          {
            report,
            clientElapsedMillis: performance.now() - start,
          },
        ].slice(-100),
      );
    }).pipe(
      Effect.tapCause((cause) =>
        Effect.sync(() => {
          const error = Option.flatMap(
            Cause.findErrorOption(cause),
            Schema.decodeUnknownOption(LabError),
          );

          const uncertain = get(chatTurnsAtom).find((turn) => turn.id === id)?.connectionLost;

          if (uncertain || (Option.isSome(error) && error.value.code === "expired"))
            get.set(chatIdAtom, null);
          get.set(
            chatTurnsAtom,
            get(chatTurnsAtom).map((turn) =>
              turn.id === id
                ? {
                    ...turn,
                    pending: false,
                    error: failureMessage(cause),
                  }
                : turn,
            ),
          );
          if (!uncertain && !get(chatDraftAtom)) get.set(chatDraftAtom, prompt);
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => get.set(watchingAtom, false)).pipe(
          Effect.andThen(Reactivity.invalidate(["lab"])),
        ),
      ),
      Effect.scoped,
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

const failureMessage = <E>(cause: Cause.Cause<E>) => {
  const error = Option.flatMap(
    Cause.findErrorOption(cause),
    Schema.decodeUnknownOption(Schema.Struct({ message: Schema.String })),
  );

  if (Option.isSome(error)) return error.value.message;

  return "The connection to the Worker was interrupted. Check the browser status before running again.";
};

export const errorMessage = <A, E>(result: AsyncResult.AsyncResult<A, E>) =>
  AsyncResult.isFailure(result) ? failureMessage(result.cause) : null;

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

export const flowStarted = (report: Report) => report.readyAt !== null;

const sameWorkload = (value: Report, report: Report) =>
  value.fixture === report.fixture &&
  value.input.scenario === report.input.scenario &&
  value.input.prompt === report.input.prompt &&
  value.input.url === report.input.url &&
  value.input.wikipedia?.start === report.input.wikipedia?.start &&
  value.input.wikipedia?.target === report.input.wikipedia?.target &&
  value.input.screenshots === report.input.screenshots &&
  value.input.liveView === report.input.liveView;

const settingsKey = (report: Report) =>
  JSON.stringify([
    report.input.engine ?? "chromium",
    report.browserVersion ?? "unrecorded",
    report.browserRevision ?? "unrecorded",
    report.commandTimeoutMillis ?? "unrecorded",
    report.input.reasoning ?? "provider-default",
    report.input.serviceTier ?? "provider-default",
  ]);

export const cohort = (samples: ReadonlyArray<ClientSample>, report: Report) =>
  samples.filter(
    ({ report: value }) =>
      sameWorkload(value, report) &&
      value.model === report.model &&
      settingsKey(value) === settingsKey(report),
  );

const servedTiers = (reports: ReadonlyArray<Report>) =>
  [
    ...new Set(
      reports.flatMap((report) => {
        const spans = report.spans.filter((span) => span.phase === "model");

        return spans.length ? spans.map((span) => span.serviceTier ?? "unknown") : ["unknown"];
      }),
    ),
  ]
    .sort()
    .join("+") || "unknown";

/** Compare the same task and capture settings; only verified outcomes contribute timings. */
export const comparisons = (samples: ReadonlyArray<ClientSample>, report: Report) => {
  const groups = new Map<string, Array<Report>>();

  for (const { report: candidate } of samples) {
    if (!sameWorkload(candidate, report)) continue;
    const key = `${candidate.model}/${settingsKey(candidate)}`;
    const group = groups.get(key) ?? [];

    group.push(candidate);
    groups.set(key, group);
  }

  return [...groups.entries()].map(([key, reports]) => ({
    key,
    engine: reports[0]?.input.engine ?? "chromium",
    browserVersion: reports[0]?.browserVersion,
    browserRevision: reports[0]?.browserRevision,
    model: reports[0]?.model ?? "unknown",
    reasoning:
      reports[0]?.input.reasoning ??
      (reports[0]?.model.startsWith("@cf/") ? "n/a" : "provider-default"),
    serviceTier:
      reports[0]?.input.serviceTier ??
      (reports[0]?.model.startsWith("@cf/") ? "n/a" : "provider-default"),
    servedTier: servedTiers(reports),
    count: reports.length,
    started: reports.filter(flowStarted).length,
    preparationFailed: reports.filter((value) => !flowStarted(value) && value.status !== "running")
      .length,
    passed: reports.filter((value) => value.status === "passed").length,
    hops: percentile(
      reports.flatMap((value) =>
        value.status === "passed" && value.race ? [Math.max(0, value.race.path.length - 1)] : [],
      ),
      0.5,
    ),
    median: percentile(
      reports.flatMap((value) => {
        const time = verifiedMillis(value);

        return value.status === "passed" && time !== null ? [time] : [];
      }),
      0.5,
    ),
  }));
};

export const percentile = (values: ReadonlyArray<number>, fraction: number): number | null => {
  const sorted = [...values].sort((a, b) => a - b);

  if (fraction === 0.5 && sorted.length > 0) {
    const middle = Math.floor(sorted.length / 2);

    return ((sorted[Math.ceil(sorted.length / 2) - 1] ?? 0) + (sorted[middle] ?? 0)) / 2;
  }

  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? null;
};
