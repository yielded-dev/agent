import { BrowserCrypto } from "@effect/platform-browser";
import { ThreadObject } from "@yielded/agent-platform-cloudflare";
import { ThreadObjectNamespace } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient } from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import * as Agent from "@yielded/agent/agent";
import { digestDefinitions } from "@yielded/agent/digest";
import { Receipt } from "@yielded/agent/durable-agent-runtime";
import { ThreadId } from "@yielded/agent/identifiers";
import { text } from "@yielded/agent/output";
import { IdempotencyKey, Principal } from "@yielded/agent/receipt";
import { CanonicalSequence, DefinitionDigestInput } from "@yielded/agent/records";
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "effect";
import { RpcTargets } from "effect-cf";
import { LanguageModel, Model, Toolkit } from "effect/ai";

import {
  Batch,
  Fault,
  Identity,
  Payload,
  ThreadCpuControl,
  ThreadCpuSampleResult,
  mark,
  replies,
  type Meta,
  type Result,
} from "./model.ts";

const answer = "done";
const usage = { inputTokens: {}, outputTokens: {} };

const definition = Agent.make("rpc-overhead", {
  input: Payload,
  output: text(Schema.String),
  instructions: "Answer done.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "30 seconds", toolConcurrency: 1 },
});

const model = Model.make(
  "scripted",
  "rpc-overhead",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Effect.succeed([
          { type: "text", text: answer },
          { type: "finish", reason: "stop", usage },
        ]),
      streamText: () =>
        Stream.make(
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: answer },
          { type: "text-end", id: "answer" },
          { type: "finish", reason: "stop", usage },
        ),
    }),
  ),
);

const agent = Agent.withModel(definition, model);

const definitions = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "scripted", name: "rpc-overhead" },
  tools: {},
});

const principal = Principal.make("rpc-overhead");
const zeroSequence = CanonicalSequence.make(0);
const seedKey = "rpc-overhead/seed/v1";

const Seed = Schema.Struct({
  version: Schema.Literal(1),
  state: Schema.Literal("ready"),
  receipt: Receipt,
  tail: CanonicalSequence.check(Schema.isGreaterThan(0)),
});

const SeedState = Schema.Union([
  Schema.Struct({ version: Schema.Literal(1), state: Schema.Literal("seeding") }),
  Seed,
]);

const Control = Schema.Struct({
  batch: Batch,
  identity: Identity,
  token: Schema.String,
  sampling: Schema.Boolean,
});

const decodeControl = Schema.decodeUnknownEffect(Control);
const decodeIdentity = Schema.decodeUnknownEffect(Identity);
const decodeSeed = Schema.decodeUnknownEffect(Seed);
const decodeSeedState = Schema.decodeUnknownEffect(SeedState);
const encodeSeedState = Schema.encodeEffect(SeedState);
const schemaFault = (cause: { readonly message: string }) => Fault.make({ message: cause.message });

const io = <A>(work: () => Promise<A>) =>
  Effect.tryPromise({
    try: work,
    catch: (cause) =>
      Fault.make({
        message: cause instanceof Error ? cause.message : "Thread invocation failed; no retry",
      }),
  });

const name = (object: number) => `rpc-overhead-${object}`;

const isThreadVariant = (variant: Batch["variant"]) =>
  variant === "thread-native" ||
  variant === "thread-status" ||
  variant === "thread-progress" ||
  variant === "thread-submit";

export class BenchThreadDO extends ThreadObject.make(ThreadObject.layer([{ agent, definitions }]), {
  namespaceBinding: "THREADS",
  deploymentId: "rpc-overhead",
  producerPrefix: "rpc-overhead",
}) {
  private readonly incarnation = crypto.randomUUID();
  private readonly build: string;
  private active: typeof Control.Type | undefined;
  private nextIndex = 0;

  constructor(ctx: globalThis.DurableObjectState, env: { BUILD: string }) {
    super(ctx, env);
    this.build = env.BUILD;
  }

  identity(): typeof Identity.Type {
    return { build: this.build, instance: this.incarnation };
  }

  private guard(expected: typeof Identity.Type) {
    return expected.build === this.build && expected.instance === this.incarnation
      ? Effect.void
      : Effect.fail(Fault.make({ message: "Thread build/incarnation mismatch" }));
  }

  reset(raw: unknown): Promise<void> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const expected = yield* decodeIdentity(raw).pipe(Effect.mapError(schemaFault));

        yield* this.guard(expected);
        if (this.active !== undefined)
          return yield* Fault.make({ message: "Thread batch is active" });
        yield* io(() => this.ctx.storage.sync());
        if (this.active !== undefined)
          return yield* Fault.make({ message: "Thread batch started during reset" });
        // Native reset rejects by design; no admission is retried by this file.
        this.ctx.abort("rpc-overhead controlled restart");
      }),
    );
  }

  beginBatch(raw: unknown): Promise<void> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const control = yield* decodeControl(raw).pipe(Effect.mapError(schemaFault));

        yield* this.guard(control.identity);
        if (
          control.batch.build !== this.build ||
          this.ctx.id.name !== name(control.batch.object) ||
          !isThreadVariant(control.batch.variant) ||
          control.batch.size !== 200
        ) {
          return yield* Fault.make({
            message: "Invalid Thread batch target, variant, size or build",
          });
        }
        if (this.active !== undefined && this.active.token !== control.token)
          return yield* Fault.make({ message: "Overlapping Thread batches" });
        this.active = control;
        this.nextIndex = -control.batch.warmup;
      }),
    );
  }

  finishBatch(token: string): typeof Identity.Type {
    const active = this.active;

    if (
      active === undefined ||
      active.token !== token ||
      !active.sampling ||
      this.nextIndex !== active.batch.calls
    )
      throw Fault.make({ message: "Thread sample count or batch ownership mismatch" });

    return this.identity();
  }

  releaseBatch(token: string): void {
    if (this.active?.token === token) this.active = undefined;
  }

  alarmTime(): Promise<number | null> {
    return this.ctx.storage.getAlarm();
  }

  /** Claim once before any public seed submission; a partial seed is never replayed. */
  seedState(): Promise<unknown> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const stored = yield* io(() => this.ctx.storage.get<unknown>(seedKey));

        if (stored !== undefined) {
          const state = yield* decodeSeedState(stored).pipe(Effect.mapError(schemaFault));

          if (state.state !== "ready")
            return yield* Fault.make({
              message: "Partial Thread seed; refusing to retry admission",
            });

          return yield* encodeSeedState(state).pipe(Effect.mapError(schemaFault));
        }

        const started = yield* encodeSeedState({ version: 1, state: "seeding" }).pipe(
          Effect.mapError(schemaFault),
        );

        yield* io(() => this.ctx.storage.put(seedKey, started));
        yield* io(() => this.ctx.storage.sync());

        return undefined;
      }),
    );
  }

  saveSeed(raw: unknown): Promise<void> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const seed = yield* decodeSeed(raw).pipe(Effect.mapError(schemaFault));

        if (seed.receipt.threadId !== this.ctx.id.name)
          return yield* Fault.make({ message: "Seed Receipt belongs to another Thread" });

        const previous = yield* decodeSeedState(
          yield* io(() => this.ctx.storage.get(seedKey)),
        ).pipe(Effect.mapError(schemaFault));

        if (previous.state !== "seeding")
          return yield* Fault.make({ message: "Seed is already complete" });
        const encoded = yield* encodeSeedState(seed).pipe(Effect.mapError(schemaFault));

        yield* io(() => this.ctx.storage.put(seedKey, encoded));
        yield* io(() => this.ctx.storage.sync());
      }),
    );
  }

  private marker(variant?: Batch["variant"]): void {
    const active = this.active;

    if (active === undefined) throw Fault.make({ message: "Missing Thread batch metadata" });
    const measured = active.sampling && active.batch.variant === variant;
    const index = measured ? this.nextIndex++ : -1;

    if (measured && index >= active.batch.calls)
      throw Fault.make({ message: "Unexpected extra Thread sample invocation" });

    const meta: Meta = {
      round: active.batch.round,
      object: active.batch.object,
      variant: active.batch.variant,
      size: active.batch.size,
      phase: measured ? (index < 0 ? "warmup" : "measure") : "prepare",
      index,
    };

    mark("object", meta);
  }

  nativeFloor(_payload: Payload): Payload {
    this.marker("thread-native");

    return replies[200];
  }

  override submitEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    this.marker("thread-submit");

    return super.submitEncoded(encoded, ...trace);
  }

  override submissionStatusEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    this.marker("thread-status");

    return super.submissionStatusEncoded(encoded, ...trace);
  }

  override awaitProgressEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    this.marker("thread-progress");

    return super.awaitProgressEncoded(encoded, ...trace);
  }

  override awaitSettlementEncoded(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    this.marker();

    return super.awaitSettlementEncoded(encoded, ...trace);
  }

  override observePage(encoded: unknown, ...trace: [] | [unknown]): Promise<unknown> {
    this.marker();

    return super.observePage(encoded, ...trace);
  }
}

interface Env {
  readonly BUILD: string;
  readonly THREADS: DurableObjectNamespace<BenchThreadDO>;
}

// Only services/digests are cached across requests. Native targets belong to each batch Scope.
const makeClient = (env: Env) => ({
  runtime: ManagedRuntime.make(
    CloudflareThreadClient.layer.pipe(
      Layer.provide(
        Layer.succeed(ThreadObjectNamespace)({
          get: (threadId) => env.THREADS.getByName(threadId, { locationHint: "wnam" }),
        }),
      ),
      Layer.provideMerge(BrowserCrypto.layer),
    ),
  ),
  digests: Effect.runSync(Effect.cached(digestDefinitions(definitions))),
});

const clients = new WeakMap<Env["THREADS"], Map<string, ReturnType<typeof makeClient>>>();

const clientFor = (env: Env) => {
  let builds = clients.get(env.THREADS);

  if (builds === undefined) {
    builds = new Map();
    clients.set(env.THREADS, builds);
  }
  let client = builds.get(env.BUILD);

  if (client === undefined) {
    client = makeClient(env);
    builds.set(env.BUILD, client);
  }

  return client;
};

const waitIdle = (stub: DurableObjectStub<BenchThreadDO>) =>
  Effect.gen(function* () {
    while ((yield* io(() => stub.alarmTime())) !== null) yield* Effect.sleep("10 millis");
  }).pipe(
    Effect.timeoutOrElse({
      duration: "60 seconds",
      orElse: () => Fault.make({ message: "Thread did not become idle" }),
    }),
  );

type BatchResult = { latencyMs: number[] } & Pick<
  Result,
  "setupMs" | "sameInstance" | "clientSetup"
>;

/** Native Worker boundary. Setup/drains are excluded from every measured public client call. */
export const threadBatch = (env: Env, batch: Batch): Promise<BatchResult> => {
  const setupStart = Date.now();
  const cached = clientFor(env);

  return cached.runtime.runPromise(
    Effect.gen(function* () {
      if (batch.build !== env.BUILD || !isThreadVariant(batch.variant) || batch.size !== 200)
        return yield* Fault.make({ message: "Invalid Thread benchmark batch" });
      const threadId = ThreadId.make(name(batch.object));
      const stub = env.THREADS.getByName(threadId, { locationHint: "wnam" });

      const before = yield* decodeIdentity(yield* io(() => stub.identity())).pipe(
        Effect.mapError(schemaFault),
      );

      if (before.build !== batch.build)
        return yield* Fault.make({ message: "Thread Object build has not propagated" });
      const token = crypto.randomUUID();

      yield* Effect.acquireRelease(
        io(() => stub.beginBatch({ batch, identity: before, token, sampling: false })),
        () => io(() => stub.releaseBatch(token)).pipe(Effect.ignore),
      );
      const client = yield* CloudflareThreadClient;
      const digests = yield* cached.digests;

      const submit = (key: string) =>
        client.submit(agent, replies[200], {
          threadId,
          principal,
          idempotencyKey: IdempotencyKey.make(key),
          definitions: digests,
        });

      const complete = (receipt: Receipt) =>
        client.awaitSettlement(receipt).pipe(
          Effect.flatMap((settlement) =>
            settlement.outcome === "completed"
              ? Effect.void
              : Effect.fail(Fault.make({ message: "Scripted Thread submission did not complete" })),
          ),
          Effect.timeoutOrElse({
            duration: "60 seconds",
            orElse: () => Fault.make({ message: "Thread settlement timed out" }),
          }),
        );

      const stored = yield* io(() => stub.seedState());
      let seed: typeof Seed.Type;

      if (stored === undefined) {
        let last: Receipt | undefined;

        for (let index = 0; index < 8; index++) {
          last = yield* submit(`seed-${index}`);
          yield* complete(last);
        }
        yield* waitIdle(stub);
        const records = yield* client.readAll(threadId);
        const tail = records.at(-1);

        if (last === undefined || tail === undefined || tail.sequence === 0)
          return yield* Fault.make({ message: "Thread seed did not produce canonical history" });
        seed = { version: 1, state: "ready", receipt: last, tail: tail.sequence };
        const encoded = yield* Schema.encodeEffect(Seed)(seed).pipe(Effect.mapError(schemaFault));

        yield* io(() => stub.saveSeed(encoded));
      } else {
        seed = yield* decodeSeed(stored).pipe(Effect.mapError(schemaFault));
      }
      if (seed.receipt.threadId !== threadId)
        return yield* Fault.make({ message: "Stored seed Receipt targets another Thread" });
      yield* waitIdle(stub);
      const status = yield* client.submissionStatus(seed.receipt);

      if (status._tag !== "settled" || status.settlement.outcome !== "completed")
        return yield* Fault.make({ message: "Stored seed Receipt is not completed" });

      const canonical = yield* client.readPage(threadId, {
        afterSequence: CanonicalSequence.make(seed.tail - 1),
        limit: 1,
      });

      if (canonical[0]?.sequence !== seed.tail)
        return yield* Fault.make({ message: "Stored seed tail is absent from canonical history" });
      yield* io(() => stub.beginBatch({ batch, identity: before, token, sampling: true }));
      const setupMs = Date.now() - setupStart;
      const latencyMs: number[] = [];

      for (let index = -batch.warmup; index < batch.calls; index++) {
        // Prepare the fresh input/key before admission timing; never retry uncertain admission.
        const admission = submit(`${token}-${index}`);
        const start = Date.now();

        switch (batch.variant) {
          case "thread-native": {
            const value = yield* io(() => stub.nativeFloor(replies[200]));
            const elapsed = Date.now() - start;

            if (value.version !== 1 || value.text !== replies[200].text)
              return yield* Fault.make({ message: "Thread native response mismatch" });
            if (index >= 0) latencyMs.push(elapsed);
            break;
          }
          case "thread-status": {
            const observed = yield* client.submissionStatus(seed.receipt);
            const elapsed = Date.now() - start;

            if (observed._tag !== "settled" || observed.settlement.outcome !== "completed")
              return yield* Fault.make({ message: "Seed status changed during the batch" });
            if (index >= 0) latencyMs.push(elapsed);
            break;
          }
          case "thread-progress": {
            yield* client.awaitProgress(threadId, zeroSequence);
            if (index >= 0) latencyMs.push(Date.now() - start);
            break;
          }
          case "thread-submit": {
            const receipt = yield* admission;
            const elapsed = Date.now() - start;

            if (index >= 0) latencyMs.push(elapsed);
            yield* complete(receipt);
            yield* waitIdle(stub);
            break;
          }
          default:
            return yield* Fault.make({ message: "Invalid Thread variant" });
        }
      }

      const after = yield* decodeIdentity(yield* io(() => stub.finishBatch(token))).pipe(
        Effect.mapError(schemaFault),
      );

      if (after.build !== before.build || after.instance !== before.instance)
        return yield* Fault.make({ message: "Thread batch changed Object build/incarnation" });

      return {
        latencyMs,
        setupMs,
        sameInstance: true,
        clientSetup:
          "cached ManagedRuntime per namespace/build; invocation-scoped targets; eight public seed turns and drains excluded",
      };
    }).pipe(RpcTargets.withScope, Effect.scoped),
  );
};

const decodeCpuControl = Schema.decodeUnknownEffect(ThreadCpuControl);
const encodeCpuControl = Schema.encodeEffect(ThreadCpuControl);
const encodeCpuSample = Schema.encodeEffect(ThreadCpuSampleResult);
const decodeCpuIndex = Schema.decodeUnknownEffect(Schema.Natural);
const decodeCpuBatch = Schema.decodeUnknownEffect(Batch);
const decodeCpuReceipt = Schema.decodeUnknownEffect(Receipt);

const validateCpuControl = Effect.fnUntraced(function* (env: Env, raw: unknown) {
  const control = yield* decodeCpuControl(raw).pipe(Effect.mapError(schemaFault));
  const { batch, identity, receipt } = control;

  if (
    batch.build !== env.BUILD ||
    identity.build !== env.BUILD ||
    !isThreadVariant(batch.variant) ||
    batch.size !== 200 ||
    batch.warmup !== 0 ||
    receipt.threadId !== name(batch.object)
  )
    return yield* Fault.make({ message: "Invalid Thread CPU control" });

  return control;
});

/** Prepare in a separate HTTP invocation; the target batch remains armed until finish. */
export const threadCpuPrepare = (
  env: Env,
  raw: unknown,
): Promise<typeof ThreadCpuControl.Encoded> => {
  const cached = clientFor(env);

  return cached.runtime.runPromise(
    Effect.gen(function* () {
      const requested = yield* decodeCpuBatch(raw).pipe(Effect.mapError(schemaFault));

      if (
        requested.build !== env.BUILD ||
        !isThreadVariant(requested.variant) ||
        requested.size !== 200
      )
        return yield* Fault.make({ message: "Invalid Thread CPU preparation batch" });
      const batch: Batch = { ...requested, warmup: 0 };

      // Reuse the unchanged baseline's seeding, canonical validation and idle checks.
      // Its one status call is attributed to a different round, never to this CPU cohort.
      yield* io(() =>
        threadBatch(env, {
          ...batch,
          round: batch.round + "-prepare",
          variant: "thread-status",
          calls: 1,
        }),
      );
      const stub = env.THREADS.getByName(name(batch.object), { locationHint: "wnam" });

      const identity = yield* decodeIdentity(yield* io(() => stub.identity())).pipe(
        Effect.mapError(schemaFault),
      );

      const seed = yield* decodeSeed(yield* io(() => stub.seedState())).pipe(
        Effect.mapError(schemaFault),
      );

      const control = yield* validateCpuControl(env, {
        batch,
        identity,
        token: crypto.randomUUID(),
        receipt: seed.receipt,
      });

      const encoded = yield* encodeCpuControl(control).pipe(Effect.mapError(schemaFault));

      yield* io(() =>
        stub.beginBatch({
          batch,
          identity,
          token: control.token,
          sampling: true,
        }),
      ).pipe(Effect.onError(() => io(() => stub.releaseBatch(control.token)).pipe(Effect.ignore)));

      return encoded;
    }).pipe(RpcTargets.withScope, Effect.scoped),
  );
};

/**
 * One actual operation and driver marker per HTTP invocation, after control/index validation.
 * Call sequentially, once per index, without retries. No setup/probe/drain/finish RPC occurs here.
 * Whole invocation CPU still includes request validation, dispatch, and response encoding.
 */
export const threadCpuSample = (
  env: Env,
  raw: unknown,
  index: number,
): Promise<typeof ThreadCpuSampleResult.Encoded> => {
  const cached = clientFor(env);
  // Report whether this Worker had already acquired the cached runtime on entry.
  const clientWarm = cached.runtime.cachedContext !== undefined;

  return cached.runtime.runPromise(
    Effect.gen(function* () {
      const control = yield* validateCpuControl(env, raw);
      const sampleIndex = yield* decodeCpuIndex(index).pipe(Effect.mapError(schemaFault));

      if (sampleIndex >= control.batch.calls)
        return yield* Fault.make({ message: "Thread CPU sample index exceeds the prepared batch" });
      mark(
        "driver",
        {
          round: control.batch.round,
          object: control.batch.object,
          variant: control.batch.variant,
          size: control.batch.size,
          phase: clientWarm ? "measure" : "prepare",
          index: sampleIndex,
        },
        1,
        0,
      );
      const client = yield* CloudflareThreadClient;
      const threadId = control.receipt.threadId;
      let receipt: Receipt | null = null;
      let latencyMs: number;

      switch (control.batch.variant) {
        case "thread-native": {
          const stub = env.THREADS.getByName(threadId, { locationHint: "wnam" });
          const start = Date.now();
          const response = yield* io(() => stub.nativeFloor(replies[200]));

          latencyMs = Date.now() - start;
          if (response.version !== 1 || response.text !== replies[200].text)
            return yield* Fault.make({ message: "Thread native response mismatch" });
          break;
        }
        case "thread-status": {
          const start = Date.now();
          const status = yield* client.submissionStatus(control.receipt);

          latencyMs = Date.now() - start;
          if (status._tag !== "settled" || status.settlement.outcome !== "completed")
            return yield* Fault.make({ message: "Seed status changed during the CPU cohort" });
          break;
        }
        case "thread-progress": {
          const start = Date.now();

          yield* client.awaitProgress(threadId, zeroSequence);
          latencyMs = Date.now() - start;
          break;
        }
        case "thread-submit": {
          const options = {
            threadId,
            principal,
            idempotencyKey: IdempotencyKey.make(control.token + "-" + sampleIndex),
            definitions: yield* cached.digests,
          };

          const admission = client.submit(agent, replies[200], options);
          const start = Date.now();

          receipt = yield* admission;
          latencyMs = Date.now() - start;
          break;
        }
        default:
          return yield* Fault.make({ message: "Invalid Thread CPU variant" });
      }

      return yield* encodeCpuSample({ latencyMs, receipt, clientWarm }).pipe(
        Effect.mapError(schemaFault),
      );
    }).pipe(RpcTargets.withScope, Effect.scoped),
  );
};

/** Settle exactly the returned admission in a later HTTP invocation, then wait for idle. */
export const threadCpuDrain = (
  env: Env,
  rawControl: unknown,
  rawReceipt: unknown,
): Promise<void> => {
  const cached = clientFor(env);

  return cached.runtime.runPromise(
    Effect.gen(function* () {
      const control = yield* validateCpuControl(env, rawControl);
      const receipt = yield* decodeCpuReceipt(rawReceipt).pipe(Effect.mapError(schemaFault));

      if (
        control.batch.variant !== "thread-submit" ||
        receipt.threadId !== control.receipt.threadId ||
        receipt.queueSequence <= control.receipt.queueSequence
      )
        return yield* Fault.make({
          message: "Drain requires a new admission to the prepared Thread",
        });
      const client = yield* CloudflareThreadClient;

      const settlement = yield* client.awaitSettlement(receipt).pipe(
        Effect.timeoutOrElse({
          duration: "60 seconds",
          orElse: () => Fault.make({ message: "Thread CPU settlement timed out" }),
        }),
      );

      if (settlement.outcome !== "completed")
        return yield* Fault.make({ message: "Scripted Thread CPU admission did not complete" });
      const stub = env.THREADS.getByName(receipt.threadId, { locationHint: "wnam" });

      yield* waitIdle(stub);
    }).pipe(RpcTargets.withScope, Effect.scoped),
  );
};

/** Verify the whole cohort, then release its metadata. Failed verification still attempts release. */
export const threadCpuFinish = (
  env: Env,
  raw: unknown,
): Promise<{ readonly sameInstance: true }> => {
  const cached = clientFor(env);

  return cached.runtime.runPromise(
    Effect.gen(function* () {
      const control = yield* validateCpuControl(env, raw);
      const stub = env.THREADS.getByName(control.receipt.threadId, { locationHint: "wnam" });
      const release = io(() => stub.releaseBatch(control.token));

      yield* Effect.gen(function* () {
        const after = yield* decodeIdentity(yield* io(() => stub.finishBatch(control.token))).pipe(
          Effect.mapError(schemaFault),
        );

        if (after.build !== control.identity.build || after.instance !== control.identity.instance)
          return yield* Fault.make({
            message: "Thread CPU cohort changed Object build/incarnation",
          });
      }).pipe(Effect.onError(() => release.pipe(Effect.ignore)));
      yield* release;

      return { sameInstance: true } as const;
    }).pipe(RpcTargets.withScope, Effect.scoped),
  );
};
