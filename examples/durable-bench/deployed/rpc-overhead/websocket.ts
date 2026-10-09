import {
  Cause,
  Clock,
  Config,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Predicate,
  Queue,
  Schedule,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import type { DurableObjectWebSocket } from "effect-cf";
import { DurableObject, DurableObjectRpcWebSocket, DurableObjectState } from "effect-cf";
import { Rpc, RpcClient, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc";
import * as Socket from "effect/socket/Socket";

import {
  type Payload,
  Batch,
  Fault,
  Rpcs,
  mark,
  replies,
  type Meta,
  PushOptions,
  type BurstMetrics,
  type IdleProbe,
  type NativeMetrics,
  type NativeCancellation,
  type WebSocketMetrics,
  type ReconnectMetrics,
  type PushResult,
} from "./model.ts";

const tag = "rpc-overhead";
const attachmentKey = "rpcOverhead";

const Attachment = Schema.Struct({
  nonce: Schema.String,
  rpcOverhead: Schema.Struct({ clientId: Schema.Natural }),
});

const Hello = Schema.Struct({ build: Schema.String });
const Cursor = Schema.Natural.check(Schema.isLessThanOrEqualTo(64));
const Frame = Schema.Struct({ sequence: Cursor, text: Schema.String });

type Frame = typeof Frame.Type;
const SourceRequest = Schema.Struct({ build: Schema.String, source: Schema.String });
const NativeWatch = Schema.Struct({ ...SourceRequest.fields, after: Cursor });
const Watch = Schema.Struct({ ...NativeWatch.fields, subscriptionKey: Schema.String });

const Publish = Schema.Struct({
  ...SourceRequest.fields,
  from: Cursor,
  count: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 8 })),
});

const Published = Schema.Struct({ last: Cursor, instance: Schema.String });

const Checkpoint = Schema.Struct({
  ...Hello.fields,
  subscriptionKey: Schema.String,
  cursor: Cursor,
});

const Source = Schema.Struct({
  version: Schema.Literal(1),
  source: Schema.String,
  frames: Schema.Array(Frame).check(Schema.isMaxLength(64)),
});

const sourceKey = "rpc-overhead/frame-source/v1";
const decodeWatch = Schema.decodeUnknownOption(Watch);
const decodeFrame = Schema.decodeUnknownOption(Frame);
const frameText = (sequence: number) => `frame-${sequence}`;

const resumable = DurableObjectRpcWebSocket.resumableStream({
  id: "rpc-overhead-frames-v1",
  rpcTag: "watch",
  resumeDescriptorSchema: SourceRequest,
  checkpointSchema: Cursor,
  identify: (request) =>
    Option.map(decodeWatch(request.payload), (watch) => ({
      subscriptionKey: watch.subscriptionKey,
      resumeDescriptor: { build: watch.build, source: watch.source },
      acknowledgedCheckpoint: watch.after,
    })),
  rebuild: ({ subscriptionKey, resumeDescriptor, acknowledgedCheckpoint }) => ({
    payload: { ...resumeDescriptor, subscriptionKey, after: acknowledgedCheckpoint },
  }),
  checkpointFromValue: (value) => Option.map(decodeFrame(value), (frame) => frame.sequence),
  checkpointToken: String,
});

const Connection = Schema.Struct({
  build: Schema.String,
  instance: Schema.String,
  connectionNonce: Schema.String,
});

const Wire = Rpcs.merge(
  RpcGroup.make(
    Rpc.make("hello", { payload: Hello, success: Connection, error: Fault }),
    Rpc.make("watch", { payload: Watch, success: Frame, stream: true, error: Fault }),
    Rpc.make("publish", { payload: Publish, success: Published, error: Fault }),
    Rpc.make("checkpoint", { payload: Checkpoint, success: Schema.Boolean, error: Fault }),
  ),
);

const decodeAttachment = Schema.decodeUnknownSync(Attachment);

// Diagnostic state only: warm batches require an unchanged constructor. No attachment writes.
const unaryDiagnostics = new WeakMap<WebSocket, { readonly batch: Batch; nextIndex: number }>();
const BatchHeader = Schema.fromJsonString(Batch);

class ObjectContext extends Context.Service<
  ObjectContext,
  {
    readonly build: string;
    readonly instance: string;
    readonly state: DurableObjectState.DurableObjectState["Service"];
  }
>()("rpc-overhead/WebSocketObject") {}

const checkBuild = Effect.fnUntraced(function* (build: string) {
  const object = yield* ObjectContext;

  if (build !== object.build) return yield* new Fault({ message: "Object build mismatch" });

  return object;
});

const io = <A>(operation: string, work: () => Promise<A>) =>
  Effect.tryPromise({
    try: work,
    catch: (cause) => new Fault({ message: `${operation}: ${describeFailure(cause)}` }),
  });

class Frames extends Context.Service<
  Frames,
  {
    readonly prepare: (request: typeof SourceRequest.Type) => Effect.Effect<void, Fault>;
    readonly publish: (request: typeof Publish.Type) => Effect.Effect<typeof Published.Type, Fault>;
    readonly watch: (request: typeof NativeWatch.Type) => Stream.Stream<Frame, Fault>;
    readonly observerCount: Effect.Effect<number>;
  }
>()("rpc-overhead/Frames") {
  static readonly layer = Layer.effect(
    Frames,
    Effect.gen(function* () {
      const object = yield* ObjectContext;
      const gate = yield* Semaphore.make(1);
      const listeners = new Set<Queue.Queue<void>>();

      const requireBuild = (build: string) =>
        build === object.build
          ? Effect.void
          : Effect.fail(new Fault({ message: "Object build mismatch" }));

      const read = Effect.fnUntraced(function* (source: string) {
        const raw = yield* io("read source", () =>
          object.state.raw.storage.get<unknown>(sourceKey),
        );

        const stored = yield* Schema.decodeUnknownEffect(Source)(raw).pipe(
          Effect.mapError(() => new Fault({ message: "Source missing or invalid" })),
        );

        if (stored.source !== source)
          return yield* new Fault({ message: "Source identity mismatch" });

        return stored;
      });

      const write = Effect.fnUntraced(function* (source: typeof Source.Type) {
        const encoded = yield* Schema.encodeEffect(Source)(source).pipe(
          Effect.mapError(() => new Fault({ message: "Invalid source write" })),
        );

        yield* io("write source", () => object.state.raw.storage.put(sourceKey, encoded));
      });

      const prepare = Effect.fnUntraced(
        function* (request: typeof SourceRequest.Type) {
          yield* requireBuild(request.build);
          if (listeners.size > 0 || (yield* object.state.getWebSockets(tag)).length > 0) {
            return yield* new Fault({ message: "Cannot replace an observed source" });
          }

          yield* write({ version: 1, source: request.source, frames: [] });
        },
        (effect) => gate.withPermit(effect),
      );

      const publish = Effect.fnUntraced(
        function* (request: typeof Publish.Type) {
          yield* requireBuild(request.build);
          const stored = yield* read(request.source);

          if (
            request.from !== stored.frames.length + 1 ||
            stored.frames.length + request.count > 64
          ) {
            return yield* new Fault({ message: "Publish cursor or bound mismatch" });
          }

          const added = Array.from({ length: request.count }, (_, index) => ({
            sequence: request.from + index,
            text: frameText(request.from + index),
          }));

          yield* write({ ...stored, frames: [...stored.frames, ...added] });
          // Coalesced in-memory hints; the committed source is always re-read.
          for (const listener of listeners) Queue.offerUnsafe(listener, undefined);

          return { last: stored.frames.length + request.count, instance: object.instance };
        },
        (effect) => gate.withPermit(effect),
      );

      const watch = (request: typeof NativeWatch.Type) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* requireBuild(request.build);
            if (listeners.size >= 4)
              return yield* new Fault({ message: "Too many active frame observers" });
            const wake = yield* Queue.bounded<void>(1);

            listeners.add(wake);
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                listeners.delete(wake);
              }).pipe(Effect.andThen(Queue.shutdown(wake))),
            );

            // Register before reading, then park without timers when no committed frame exists.
            return Stream.paginate(request.after, (after) =>
              Effect.gen(function* () {
                while (true) {
                  const stored = yield* read(request.source);

                  if (after > stored.frames.length)
                    return yield* new Fault({ message: "Cursor beyond source" });
                  // Emit the available page immediately, including a one-frame replay.
                  // Bound it below the RPC client's 16-value receive buffer.
                  const available = stored.frames.slice(after, after + 8);
                  const last = available.at(-1);

                  if (last !== undefined) return [available, Option.some(last.sequence)] as const;
                  yield* Queue.take(wake);
                }
              }),
            );
          }),
        );

      return { prepare, publish, watch, observerCount: Effect.sync(() => listeners.size) };
    }),
  );
}

const handlers = Wire.toLayer({
  watch: (request) => Stream.unwrap(Effect.map(Frames, (frames) => frames.watch(request))),
  publish: (request) => Effect.flatMap(Frames, (frames) => frames.publish(request)),
  checkpoint: Effect.fnUntraced(function* (request, { client }) {
    yield* checkBuild(request.build);
    const rpc = yield* Transport;

    return yield* rpc
      .checkpoint(resumable, {
        clientId: client.id,
        subscriptionKey: request.subscriptionKey,
        checkpoint: request.cursor,
      })
      .pipe(
        Effect.mapError((cause) => new Fault({ message: `Checkpoint: ${describeFailure(cause)}` })),
      );
  }),
  noop: Effect.fnUntraced(function* (call) {
    yield* checkBuild(call.build);

    return replies[call.meta.size];
  }),
  hello: Effect.fnUntraced(function* ({ build }, { client }) {
    const object = yield* checkBuild(build);
    const sockets = yield* object.state.getWebSockets(tag);

    for (const socket of sockets) {
      const attachment = yield* socket.deserializeAttachment.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Attachment)),
        Effect.mapError(() => new Fault({ message: "Invalid connection attachment" })),
      );

      if (attachment.rpcOverhead.clientId === client.id) {
        return { build, instance: object.instance, connectionNonce: attachment.nonce };
      }
    }

    return yield* new Fault({ message: "Connection attachment missing" });
  }),
});

const transport = DurableObjectRpcWebSocket.layer({
  tag,
  attachmentKey,
  resumableStreams: [resumable],
}).pipe(Layer.provide(RpcSerialization.layerJson));

const constructors = new WeakMap<globalThis.DurableObjectState, string>();

/** Call once in the owning DO constructor, before its first event; use that DO's nonce. */
export const wsConstructed = (state: globalThis.DurableObjectState, instance: string) => {
  constructors.set(state, instance);
  console.log({
    kind: "rpc-overhead-ws-constructor",
    instance,
    connections: state
      .getWebSockets(tag)
      .map((socket) => decodeAttachment(socket.deserializeAttachment()).nonce),
  });
};

/** Merge into the same instance layer as the native/schema/HTTP controls. */
export const wsLayer = RpcServer.layer(Wire, { concurrency: 8 }).pipe(
  Layer.provide(handlers),
  Layer.provideMerge(transport),
  Layer.provideMerge(Frames.layer),
  Layer.provideMerge(
    Layer.effect(
      ObjectContext,
      Effect.gen(function* () {
        const state = yield* DurableObjectState.DurableObjectState;
        const build = yield* Config.String("BUILD");
        const instance = constructors.get(state.raw);

        if (instance === undefined)
          return yield* new Fault({ message: "Missing wsConstructed registration" });

        return { state, build, instance };
      }),
    ),
  ),
);

const Transport = DurableObjectRpcWebSocket.DurableObjectRpcWebSocket;

/** Native controls share the exact same persisted source as the WS handlers. */
export const wsRpc = {
  frameObservers: () => Effect.flatMap(Frames, (frames) => frames.observerCount),
  prepare: Effect.fnUntraced(function* (raw: unknown) {
    const request = yield* Schema.decodeUnknownEffect(SourceRequest)(raw);
    const frames = yield* Frames;

    yield* frames.prepare(request);
  }),
  publish: Effect.fnUntraced(function* (raw: unknown) {
    const request = yield* Schema.decodeUnknownEffect(Publish)(raw);
    const frames = yield* Frames;

    return yield* frames.publish(request);
  }),
  // The returned ReadableStream owns its producer; the driver must cancel it.
  nativeStream: Effect.fnUntraced(function* (raw: unknown) {
    const request = yield* Schema.decodeUnknownEffect(NativeWatch)(raw);

    yield* checkBuild(request.build);
    const frames = yield* Frames;
    const encode = Schema.encodeEffect(Schema.fromJsonString(Frame));
    const encoder = new TextEncoder();

    return yield* frames.watch(request).pipe(
      Stream.mapEffect((frame) => Effect.map(encode(frame), (text) => encoder.encode(`${text}\n`))),
      Stream.toReadableStreamEffect({ strategy: { highWaterMark: 1 } }),
    );
  }),
};

export const wsUpgrade = Effect.fnUntraced(function* (request: Request) {
  yield* checkBuild(request.headers.get("x-bench-build") ?? "");
  if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket", { status: 426 });
  }
  const diagnosticHeader = request.headers.get("x-bench-batch");

  const batch =
    diagnosticHeader === null
      ? undefined
      : yield* Schema.decodeEffect(BatchHeader)(diagnosticHeader);

  if (batch !== undefined) yield* checkBuild(batch.build);
  const rpc = yield* Transport;
  const accepted = yield* rpc.acceptUpgrade({ attachment: { nonce: crypto.randomUUID() } });

  if (batch !== undefined) {
    unaryDiagnostics.set(accepted.server.raw, { batch, nextIndex: -batch.warmup });
  }

  return accepted.response;
});

/** Spread into DurableObject.make options, preserving native hibernation callbacks. */
export const wsEvents = {
  webSocketMessage: (
    socket: DurableObjectWebSocket.DurableWebSocket,
    message: string | ArrayBuffer,
  ) => {
    const diagnostic = unaryDiagnostics.get(socket.raw);

    // RpcSerialization.json emits text with this exact tag. Classify only for diagnostics;
    // the RPC server remains responsible for the sole JSON/schema decode of the Call.
    if (
      diagnostic !== undefined &&
      typeof message === "string" &&
      message.includes('"tag":"noop"')
    ) {
      const { batch } = diagnostic;
      const index = diagnostic.nextIndex++;

      mark("object", {
        round: batch.round,
        object: batch.object,
        variant: batch.variant,
        size: batch.size,
        phase: index < 0 ? "warmup" : "measure",
        index,
      });
    }

    return Effect.flatMap(Transport, (rpc) => rpc.message(socket, message));
  },
  webSocketClose: (socket: DurableObjectWebSocket.DurableWebSocket) => {
    unaryDiagnostics.delete(socket.raw);

    return Effect.flatMap(Transport, (rpc) => rpc.close(socket));
  },
  webSocketError: (socket: DurableObjectWebSocket.DurableWebSocket, cause: unknown) => {
    unaryDiagnostics.delete(socket.raw);

    return Effect.flatMap(Transport, (rpc) => rpc.error(socket, cause));
  },
};

/** Standalone fixture only; paired comparisons should merge wsLayer into MicroDO. */
export class WebSocketDO extends DurableObject.make(wsLayer, { ...wsEvents, rpc: wsRpc }) {
  constructor(state: globalThis.DurableObjectState, env: { BUILD: string }) {
    super(state, env);
    wsConstructed(state, crypto.randomUUID());
  }

  override fetch(request: Request): Promise<Response> {
    return this[DurableObject.RunSymbol](wsUpgrade(request), { event: "fetch" });
  }
}

const isFault = Schema.is(Fault);
const safeName = (value: string) => value.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 80);

const describeFailure = (value: unknown, depth = 0): string => {
  if (isFault(value)) return value.message.slice(0, 256);
  if (!Predicate.isObject(value)) return "unclassified failure";

  const name =
    Predicate.hasProperty(value, "_tag") && Predicate.isString(value._tag)
      ? safeName(value._tag)
      : Predicate.hasProperty(value, "name") && Predicate.isString(value.name)
        ? safeName(value.name)
        : "defect";

  const code =
    Predicate.hasProperty(value, "code") && Predicate.isNumber(value.code) ? `:${value.code}` : "";

  if (depth >= 3) return `${name}${code}`;

  // Retain causal tags/codes, never arbitrary transport messages, bodies, URLs or stacks.
  const nested = Predicate.hasProperty(value, "reason")
    ? value.reason
    : Predicate.hasProperty(value, "cause")
      ? value.cause
      : undefined;

  return `${name}${code}${nested === undefined ? "" : `/${describeFailure(nested, depth + 1)}`}`;
};

const sanitized = (cause: Cause.Cause<unknown>) =>
  cause.reasons
    .map((reason) =>
      reason._tag === "Fail"
        ? describeFailure(reason.error)
        : reason._tag === "Die"
          ? describeFailure(reason.defect)
          : "Interrupted",
    )
    .join("; ")
    .slice(0, 512);

type FetchStub = Pick<DurableObjectStub, "fetch">;

const session = Effect.fnUntraced(function* (stub: FetchStub, build: string, batch?: Batch) {
  let upgradeCount = 0;
  const headers: Record<string, string> = { Upgrade: "websocket", "x-bench-build": build };

  if (batch !== undefined)
    headers["x-bench-batch"] = yield* Schema.encodeEffect(BatchHeader)(batch);

  const openError = (cause: unknown) =>
    new Socket.SocketError({ reason: new Socket.SocketOpenError({ kind: "Unknown", cause }) });

  const acquire = Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        upgradeCount++;

        const response = await stub.fetch("https://rpc-overhead/rpc", {
          headers,
        });

        if (response.status !== 101 || response.webSocket === null)
          throw new Fault({ message: `WebSocket upgrade rejected (${response.status})` });

        return response.webSocket;
      },
      catch: openError,
    }),
    (socket) => Effect.sync(() => socket.close(1000)),
  ).pipe(
    Effect.tap((socket) =>
      Effect.try({
        try: () => {
          socket.binaryType = "arraybuffer";
          socket.accept();
        },
        catch: openError,
      }),
    ),
  );

  const socket = Layer.effect(Socket.Socket, Socket.fromWebSocket(acquire));

  // layerProtocolSocket does not expose retryPolicy in Effect 4.0.0.
  const protocol = Layer.effect(
    RpcClient.Protocol,
    RpcClient.makeProtocolSocket({ retryPolicy: Schedule.recurs(0) }),
  ).pipe(Layer.provide(socket), Layer.provide(RpcSerialization.layerJson));

  const services = yield* Layer.build(protocol);

  const client = yield* RpcClient.make(Wire).pipe(Effect.provideContext(services));

  const hello = yield* client.hello({ build }).pipe(Effect.timeout("15 seconds"));

  return { client, hello, upgradeCount: () => upgradeCount };
});

const validatePayload = (value: Payload, size: Batch["size"]) =>
  value.version === replies[size].version && value.text === replies[size].text
    ? Effect.void
    : Effect.fail(new Fault({ message: "Response mismatch" }));

/** One invocation owns the connection; upgrade and successful hello precede excluded warmup. */
export const unaryBatch = Effect.fnUntraced(
  function* (stub: FetchStub, batch: Batch) {
    yield* Schema.decodeEffect(Batch)(batch);
    const started = yield* Clock.currentTimeMillis;
    const connection = yield* session(stub, batch.build, batch);
    const setupMs = (yield* Clock.currentTimeMillis) - started;
    const latencyMs: number[] = [];

    for (let index = -batch.warmup; index < batch.calls; index++) {
      const meta: Meta = {
        round: batch.round,
        object: batch.object,
        variant: batch.variant,
        size: batch.size,
        phase: index < 0 ? "warmup" : "measure",
        index,
      };

      const start = yield* Clock.currentTimeMillis;

      const value = yield* connection.client
        .noop({ meta, build: batch.build, payload: replies[batch.size] })
        .pipe(Effect.timeout("15 seconds"));

      const elapsed = (yield* Clock.currentTimeMillis) - start;

      yield* validatePayload(value, batch.size);
      if (index >= 0) latencyMs.push(elapsed);
    }

    const after = yield* connection.client
      .hello({ build: batch.build })
      .pipe(Effect.timeout("15 seconds"));

    if (
      connection.upgradeCount() !== 1 ||
      after.connectionNonce !== connection.hello.connectionNonce
    ) {
      return yield* new Fault({ message: "Connection changed during unary batch" });
    }

    return {
      latencyMs,
      setupMs,
      sameInstance: after.instance === connection.hello.instance,
      clientSetup: `request-scoped; upgrade+hello excluded; warmup excluded; retries=0; upgradeCount=${connection.upgradeCount()}`,
    };
  },
  Effect.scoped,
  Effect.catchCause((cause) =>
    Cause.hasInterrupts(cause)
      ? Effect.interrupt
      : Effect.fail(new Fault({ message: sanitized(cause) })),
  ),
);

/** A single stub must supply every control; the experiment never chooses another Object. */
type PushStub = FetchStub & {
  readonly frameObservers: () => Promise<number>;
  readonly prepare: (request: typeof SourceRequest.Type) => Promise<void>;
  readonly publish: (request: typeof Publish.Type) => Promise<typeof Published.Type>;
  readonly nativeStream: (request: typeof NativeWatch.Type) => Promise<ReadableStream<Uint8Array>>;
};
type ObservedFrame = { readonly frame: Frame; readonly receivedAt: number };

// Time frames in the consuming driver fiber, before publish's reply necessarily arrives.
const observe = <E, R>(stream: Stream.Stream<Frame, E, R>) =>
  stream.pipe(
    Stream.mapEffect((frame) =>
      Effect.map(Clock.currentTimeMillis, (receivedAt) => ({ frame, receivedAt })),
    ),
    Stream.toQueue({ capacity: 64 }),
  );

const expectedFrame = Effect.fnUntraced(function* <E>(
  queue: Queue.Dequeue<ObservedFrame, E>,
  sequence: number,
) {
  const observed = yield* Queue.take(queue).pipe(Effect.timeout("15 seconds"));

  if (observed.frame.sequence !== sequence || observed.frame.text !== frameText(sequence)) {
    return yield* new Fault({
      message: `Frame order/content mismatch; expected ${sequence}, received ${observed.frame.sequence}`,
    });
  }

  return observed;
});

const noQueuedFrame = Effect.fnUntraced(function* <E>(queue: Queue.Dequeue<ObservedFrame, E>) {
  const unexpected = yield* Queue.poll(queue);

  if (Option.isSome(unexpected)) {
    return yield* new Fault({ message: `Unexpected frame ${unexpected.value.frame.sequence}` });
  }
});

const nativeQueue = Effect.fnUntraced(function* (
  stub: PushStub,
  request: typeof NativeWatch.Type,
  cancellations: Array<typeof NativeCancellation.Type>,
) {
  // Retain both handles. workerd cannot release a reader with a pending read:
  // cancel through that reader first, then unlock and dispose the RPC session.
  let pending: Promise<ReadableStream<Uint8Array>> | undefined;
  let readable: ReadableStream<Uint8Array> | undefined;
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let closed = false;

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      yield* Effect.sync(() => {
        closed = true;
      });
      const stream = readable;
      const start = yield* Clock.currentTimeMillis;

      if (stream !== undefined) {
        const result = yield* io("native cancel", () =>
          reader === undefined ? stream.cancel() : reader.cancel(),
        ).pipe(Effect.interruptible, Effect.timeout("1 second"), Effect.exit);

        const released = yield* Effect.try({
          try: () => reader?.releaseLock(),
          catch: (cause) => new Fault({ message: `Reader release: ${describeFailure(cause)}` }),
        }).pipe(Effect.exit);

        cancellations.push({
          readerUnlocked: !stream.locked,
          succeeded: Exit.isSuccess(result) && Exit.isSuccess(released),
          error: Exit.isFailure(result)
            ? sanitized(result.cause)
            : Exit.isFailure(released)
              ? sanitized(released.cause)
              : null,
          elapsedMs: (yield* Clock.currentTimeMillis) - start,
        });
      } else {
        cancellations.push({
          readerUnlocked: false,
          succeeded: false,
          error: "Stream not opened",
          elapsedMs: (yield* Clock.currentTimeMillis) - start,
        });
      }
      yield* Effect.sync(() => {
        if (pending !== undefined && Predicate.hasProperty(pending, Symbol.dispose)) {
          const dispose = pending[Symbol.dispose];

          if (typeof dispose === "function") dispose.call(pending);
        }
      });
    }),
  );

  const stream = yield* io("native stream", () => {
    pending = stub.nativeStream(request);

    return pending.then(async (value) => {
      readable = value;
      if (closed && !value.locked) await value.cancel();

      return value;
    });
  });

  const decode = Schema.decodeEffect(Schema.fromJsonString(Frame));
  const currentReader = stream.getReader();

  reader = currentReader;

  return yield* observe(
    Stream.fromPull(
      Effect.succeed(
        io("native stream read", () => currentReader.read()).pipe(
          Effect.flatMap(({ done, value }) =>
            done ? Cause.done() : Effect.succeed([value] as const),
          ),
        ),
      ),
    ).pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.mapEffect((line) => decode(line)),
    ),
  );
});

/**
 * Bounded same-Object experiment. Native I/O is cancelled before WS idle begins.
 * A 45s wait is an observation window, never a claim that eviction must occur.
 * No DO timers: publish wakes queues; resumable checkpoints release chunk Acks.
 */
export const pushExperiment = Effect.fnUntraced(function* (
  initialStub: PushStub,
  options: PushOptions,
  restartForCleanup: () => Promise<PushStub>,
) {
  let stub = initialStub;

  const validated = yield* Schema.decodeEffect(PushOptions)(options).pipe(
    Effect.mapError(() => new Fault({ message: "Invalid push options" })),
  );

  const count = validated.framesPerBurst ?? 4;
  const build = validated.build;
  let stage = "native prepare";
  let native: typeof NativeMetrics.Type | null = null;
  let websocket: typeof WebSocketMetrics.Type | null = null;
  let reconnect: typeof ReconnectMetrics.Type | null = null;
  const failures: Array<{ stage: string; cause: string }> = [];

  const run = Effect.gen(function* () {
    const nativeSource = crypto.randomUUID();
    const cancellations: Array<typeof NativeCancellation.Type> = [];

    yield* io("prepare native source", () => stub.prepare({ build, source: nativeSource }));

    // Cancel in the driver scope; verify remote release separately below.
    const activeNative = yield* Effect.scoped(
      Effect.gen(function* () {
        stage = "native open";
        const started = yield* Clock.currentTimeMillis;

        const queue = yield* nativeQueue(
          stub,
          { build, source: nativeSource, after: 0 },
          cancellations,
        );

        const setupMs = (yield* Clock.currentTimeMillis) - started;
        const bursts: Array<typeof BurstMetrics.Type> = [];
        const instances: string[] = [];

        for (let burst = 0; burst < 2; burst++) {
          if (burst > 0) {
            stage = "native idle";
            yield* Effect.sleep("45 seconds");
          }
          stage = `native burst ${burst + 1}`;
          const from = burst * count + 1;
          const publishStart = yield* Clock.currentTimeMillis;

          const published = yield* io("native publish", () =>
            stub.publish({ build, source: nativeSource, from, count }),
          );

          const publishRttMs = (yield* Clock.currentTimeMillis) - publishStart;

          if (published.last !== from + count - 1)
            return yield* new Fault({ message: "Native publish cursor mismatch" });
          instances.push(published.instance);
          const sequences: number[] = [];
          const publishToReceiveMs: number[] = [];

          for (let sequence = from; sequence < from + count; sequence++) {
            const observed = yield* expectedFrame(queue, sequence);

            sequences.push(observed.frame.sequence);
            publishToReceiveMs.push(observed.receivedAt - publishStart);
          }
          yield* noQueuedFrame(queue);
          bursts.push({ from, sequences, publishRttMs, publishToReceiveMs });
        }

        return { setupMs, bursts, sameInstance: instances[0] === instances[1] };
      }),
    );

    stage = "native fresh stream";
    const savedCursor = count * 2 - 1;

    const freshNative = yield* Effect.scoped(
      Effect.gen(function* () {
        const start = yield* Clock.currentTimeMillis;

        const queue = yield* nativeQueue(
          stub,
          { build, source: nativeSource, after: savedCursor },
          cancellations,
        );

        const observed = yield* expectedFrame(queue, savedCursor + 1);

        yield* noQueuedFrame(queue);

        return {
          freshStreamSetupAndFirstFrameMs: observed.receivedAt - start,
          replayedSequence: observed.frame.sequence,
        };
      }),
    );

    native = { ...activeNative, ...freshNative, savedCursor, cancellations, release: null };

    stage = "native release confirmation";

    const waitForRelease = Effect.gen(function* () {
      while ((yield* io("native observer count", () => stub.frameObservers())) !== 0)
        yield* Effect.sleep("25 millis");
    }).pipe(Effect.timeout("5 seconds"));

    yield* waitForRelease.pipe(Effect.ignore);
    const observersAfterCancel = yield* io("native observer count", () => stub.frameObservers());
    let releaseAfterWriteMs: number | null = null;
    let diagnosticFrames = 0;

    if (observersAfterCancel > 0) {
      // Diagnostic only, excluded from delivery samples. Older workerd RPC streams
      // propagate an idle receiver's cancellation only on the origin's next write.
      stage = "native cancellation diagnostic write";
      const started = yield* Clock.currentTimeMillis;

      diagnosticFrames = 8;
      yield* io("native diagnostic publish", () =>
        stub.publish({ build, source: nativeSource, from: count * 2 + 1, count: diagnosticFrames }),
      );
      const released = yield* waitForRelease.pipe(Effect.result);

      if (released._tag === "Success")
        releaseAfterWriteMs = (yield* Clock.currentTimeMillis) - started;
    }
    const observersAfterWrite = yield* io("native observer count", () => stub.frameObservers());

    if (observersAfterWrite > 0) {
      // Isolate a failed native teardown from the WS experiment. This reset occurs
      // before opening the socket and never counts as observed WS hibernation.
      stage = "native cleanup restart";
      stub = yield* io("native cleanup restart", restartForCleanup);
    }
    native = {
      ...native,
      release: {
        observersAfterCancel,
        diagnosticFrames,
        observersAfterWrite,
        resetRequired: observersAfterWrite > 0,
        releaseAfterWriteMs,
        observersAfterRelease: yield* io("native observer count", () => stub.frameObservers()),
      },
    };
    if (native.release?.observersAfterRelease !== 0)
      return yield* new Fault({ message: "Native cleanup left active observers" });

    stage = "websocket prepare";
    const source = crypto.randomUUID();

    yield* io("prepare websocket source", () => stub.prepare({ build, source }));

    const activeWebSocket = yield* Effect.scoped(
      Effect.gen(function* () {
        stage = "websocket open";
        const start = yield* Clock.currentTimeMillis;
        const connection = yield* session(stub, build);
        const setupMs = (yield* Clock.currentTimeMillis) - start;
        const subscriptionKey = crypto.randomUUID();

        const queue = yield* observe(
          connection.client.watch({ build, source, after: 0, subscriptionKey }),
        );

        const bursts: Array<typeof BurstMetrics.Type> = [];
        const probes: Array<typeof IdleProbe.Type> = [];
        let current = connection.hello;

        const checkpoint = Effect.fnUntraced(function* (cursor: number) {
          const advanced = yield* connection.client
            .checkpoint({ build, subscriptionKey, cursor })
            .pipe(Effect.timeout("15 seconds"));

          if (!advanced)
            return yield* new Fault({ message: `Checkpoint ${cursor} did not advance` });
        });

        const idle = Effect.fnUntraced(function* (held: number) {
          stage = `websocket idle after ${held}`;
          yield* Effect.sleep("45 seconds");
          stage = `websocket wake after ${held}`;
          const wakeStart = yield* Clock.currentTimeMillis;

          const after = yield* connection.client
            .hello({ build })
            .pipe(Effect.timeout("15 seconds"));

          const wakeHelloMs = (yield* Clock.currentTimeMillis) - wakeStart;

          const sameSocket =
            connection.upgradeCount() === 1 &&
            after.connectionNonce === connection.hello.connectionNonce;

          if (!sameSocket) return yield* new Fault({ message: "Connection changed during idle" });
          const constructorChanged = after.instance !== current.instance;
          let replayedSequence: number | null = null;
          let wakeAndFirstReplayMs: number | null = null;

          if (constructorChanged) {
            replayedSequence = (yield* expectedFrame(queue, held)).frame.sequence;
            // Complete hello plus availability/validation of the replay, even if buffered first.
            wakeAndFirstReplayMs = (yield* Clock.currentTimeMillis) - wakeStart;
          }
          yield* noQueuedFrame(queue);

          const probe: typeof IdleProbe.Type = {
            idleMs: 45_000,
            wakeHelloMs,
            wakeAndFirstReplayMs,
            upgradeCount: connection.upgradeCount(),
            constructorChanged,
            sameSocket,
            replayedSequence,
          };

          probes.push(probe);
          console.log({
            kind: "rpc-overhead-ws-hibernation",
            round: options.round,
            object: options.object,
            ...probe,
            beforeInstance: current.instance,
            afterInstance: after.instance,
            connectionNonce: after.connectionNonce,
          });
          current = after;
        });

        for (let burst = 0; burst < 2; burst++) {
          if (burst > 0) {
            yield* idle(count);
            // Leave exactly one frame unacknowledged over idle; release it before publishing again.
            yield* checkpoint(count);
          }
          stage = `websocket burst ${burst + 1}`;
          const from = burst * count + 1;
          const publishStart = yield* Clock.currentTimeMillis;

          // The same native control triggers both transports. Compare delivery paths,
          // with publication/storage work identical and one clock in the driver.
          const published = yield* io("websocket source publish", () =>
            stub.publish({ build, source, from, count }),
          );

          const publishRttMs = (yield* Clock.currentTimeMillis) - publishStart;

          if (published.last !== from + count - 1)
            return yield* new Fault({ message: "WebSocket publish cursor mismatch" });
          const sequences: number[] = [];
          const publishToReceiveMs: number[] = [];

          for (let sequence = from; sequence < from + count; sequence++) {
            const observed = yield* expectedFrame(queue, sequence);

            sequences.push(observed.frame.sequence);
            publishToReceiveMs.push(observed.receivedAt - publishStart);
          }
          yield* checkpoint(from + count - 2);
          yield* noQueuedFrame(queue);
          bursts.push({ from, sequences, publishRttMs, publishToReceiveMs });
        }
        if (!probes.some((probe) => probe.constructorChanged)) yield* idle(count * 2);
        if (connection.upgradeCount() !== 1)
          return yield* new Fault({ message: "Unexpected automatic redial" });

        return {
          connectionNonce: connection.hello.connectionNonce,
          metrics: {
            setupMs,
            bursts,
            probes,
            savedCursor,
            recreationObserved: probes.some(
              (probe) => probe.constructorChanged && probe.sameSocket,
            ),
            upgradeCount: connection.upgradeCount(),
          },
        };
      }),
    );

    websocket = activeWebSocket.metrics;

    stage = "explicit websocket reconnect";
    const previous = activeWebSocket;

    reconnect = yield* Effect.scoped(
      Effect.gen(function* () {
        const start = yield* Clock.currentTimeMillis;
        const connection = yield* session(stub, build);
        const setupMs = (yield* Clock.currentTimeMillis) - start;
        const subscriptionKey = crypto.randomUUID();

        const queue = yield* observe(
          connection.client.watch({ build, source, after: savedCursor, subscriptionKey }),
        );

        const observed = yield* expectedFrame(queue, savedCursor + 1);
        const setupAndFirstFrameMs = observed.receivedAt - start;

        if (
          connection.upgradeCount() !== 1 ||
          connection.hello.connectionNonce === previous.connectionNonce
        ) {
          return yield* new Fault({
            message: "Explicit reconnect did not create exactly one new connection",
          });
        }

        const advanced = yield* connection.client
          .checkpoint({ build, subscriptionKey, cursor: observed.frame.sequence })
          .pipe(Effect.timeout("15 seconds"));

        if (!advanced) return yield* new Fault({ message: "Reconnect checkpoint did not advance" });
        yield* noQueuedFrame(queue);

        return {
          setupMs,
          setupAndFirstFrameMs,
          replayedSequence: observed.frame.sequence,
          newConnection: connection.hello.connectionNonce !== previous.connectionNonce,
          upgradeCount: connection.upgradeCount(),
          totalUpgradeCount: previous.metrics.upgradeCount + connection.upgradeCount(),
        };
      }),
    );
  });

  yield* run.pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause)
        ? Effect.interrupt
        : Effect.sync(() => {
            failures.push({ stage, cause: sanitized(cause) });
          }),
    ),
  );

  return {
    ok: failures.length === 0,
    round: options.round,
    object: options.object,
    framesPerBurst: count,
    idleMs: 45_000 as const,
    native,
    websocket,
    reconnect,
    failures,
  } satisfies PushResult;
}, Effect.scoped);
