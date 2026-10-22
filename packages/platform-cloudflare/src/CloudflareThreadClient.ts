import { AgentInputError } from "@yielded/agent/agent-error";
import {
  type DurableSubmitAgent,
  type DurableSubmitOptions,
} from "@yielded/agent/durable-agent-runtime";
import { type ThreadId } from "@yielded/agent/identifiers";
import { type Receipt } from "@yielded/agent/receipt";
import {
  type CanonicalRecordEnvelope,
  type CanonicalSequence,
  PersistedJson,
  type SubmissionSettledRecord,
} from "@yielded/agent/records";
import {
  type AbortCommand,
  type AbortIntent,
  type ApprovalDecisionCommand,
  type ApprovalDecisionIntent,
  type Settlement,
  type UnknownResolutionCommand,
  type UnknownResolutionIntent,
} from "@yielded/agent/submission-ledger";
import { type SubmissionStatus } from "@yielded/agent/submission-status";
import { RpcTargets, RpcTracing } from "effect-cf";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  callThreadObject,
  ThreadObjectNamespace,
  type ThreadObjectRpc,
} from "./CloudflareBindings.ts";
import { cloudflareFailureSignals, safeCauseMessage } from "./internal/boundary.ts";
import { cloudflareCryptoLayer } from "./internal/crypto.ts";
import { boundHostDiagnostic, HostProtocolError } from "./internal/host-protocol-error.ts";
import {
  AwaitSettlementRequest,
  AwaitProgressRequest,
  CancelProgressRequest,
  ObservePageRequest,
  decodeHostResponse,
  hostOperations,
  type HostFailure,
  type HostSuccess,
  type HostOperation,
  type ProgressCancelled,
  type SettlementReached,
  type SettlementRecordReached,
} from "./internal/host-protocol.ts";
import { Observation as LiveTextObservation } from "./internal/live-text-observation.ts";
import {
  liveTextProtocolFailure,
  Request as WatchTextRequest,
} from "./internal/live-text-protocol.ts";
import { decodeLiveText, type LiveTextFrame } from "./LiveText.ts";

export { boundHostDiagnostic, HostProtocolError } from "./internal/host-protocol-error.ts";

export {
  SubmitRequest,
  ObservePageRequest,
  AwaitSettlementRequest,
  AwaitProgressRequest,
  CancelProgressRequest,
  HostFailure,
  SubmitSucceeded,
  SubmissionStatusResponse,
  SettlementReached,
  SettlementRecordReached,
  ObservedPage,
  ProgressObserved,
  ProgressCancelled,
  AbortRecorded,
  ApprovalRecorded,
  UnknownResolutionRecorded,
  HostFailed,
  HostResponse,
  decodeSubmitRequest,
  encodeSubmitRequest,
  decodeReceipt,
  encodeReceipt,
  decodeAwaitSettlementRequest,
  encodeAwaitSettlementRequest,
  decodeObservePageRequest,
  encodeObservePageRequest,
  decodeAwaitProgressRequest,
  encodeAwaitProgressRequest,
  decodeCancelProgressRequest,
  encodeCancelProgressRequest,
  decodeAbortCommand,
  encodeAbortCommand,
  decodeApprovalDecisionCommand,
  encodeApprovalDecisionCommand,
  decodeUnknownResolutionCommand,
  encodeUnknownResolutionCommand,
  encodeHostResponse,
  decodeHostResponse,
} from "./internal/host-protocol.ts";

export { decodeLiveText, LiveTextFrame } from "./LiveText.ts";

/**
 * The Worker↔Thread-Object host protocol (plan §1.4): Schema envelopes for the host
 * entry points (`submitEncoded`, `awaitSettlementEncoded`, `observePage`, `abortEncoded`,
 * `resolveApprovalEncoded`, `resolveUnknownEncoded`) plus the Worker-side client that speaks
 * it. Mirrors the WP2 port protocol: requests and responses are closed Schema unions, typed
 * failures travel as their own tagged classes and RE-DECODE to identical tags on the caller
 * (error-tag fidelity), and protocol anomalies are answered typed instead of thrown.
 */

const PROGRESS_CANCELLATION_TIMEOUT = "1 second";

/** The Worker-side stub call itself failed (RPC rejection, overload, eviction mid-call). */
export class ThreadClientError extends Schema.TaggedError<ThreadClientError>()(
  "ThreadClientError",
  {
    threadId: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
    /** Cloudflare's own classification for a failure safe to retry with a fresh stub. */
    retryable: Schema.optionalKey(Schema.Boolean),
    /** Cloudflare overloads are surfaced immediately instead of adding retry pressure. */
    overloaded: Schema.optionalKey(Schema.Boolean),
  },
) {}

export type ClientSubmitFailure = typeof hostOperations.submit.failure.Type | ThreadClientError;

export type ClientAwaitFailure =
  | typeof hostOperations.awaitSettlement.failure.Type
  | ThreadClientError;

export type ClientObserveFailure =
  | typeof hostOperations.observePage.failure.Type
  | ThreadClientError;

export type ClientProgressFailure = ClientObserveFailure;
export type ClientAbortFailure = typeof hostOperations.abort.failure.Type | ThreadClientError;

export type ClientApprovalFailure =
  | typeof hostOperations.resolveApproval.failure.Type
  | ThreadClientError;

export type ClientUnknownFailure =
  | typeof hostOperations.resolveUnknown.failure.Type
  | ThreadClientError;

const outOfContract = (threadId: string, operation: string, observed: string): ThreadClientError =>
  ThreadClientError.make({
    threadId,
    message: boundHostDiagnostic(
      `The Thread Object answered ${operation} with the out-of-contract ${observed}.`,
    ),
  });

/** Worker-side client over the Thread Object namespace (DEPLOY-010). */
export class CloudflareThreadClient extends Context.Service<
  CloudflareThreadClient,
  {
    /** Encode the input client-side, then durably submit to the owning Object. */
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<Receipt, ClientSubmitFailure, InputSchema["EncodingServices"]>;
    /** Wake-hinted, poll-guaranteed settlement wait executed inside the owning Object. */
    readonly submissionStatus: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionStatus, ClientAwaitFailure>;
    /**
     * Wait for settlement; interruption spends at most one second cancelling the remote
     * observation best effort. Cancellation never aborts the admitted Run.
     */
    readonly awaitSettlement: (receipt: Receipt) => Effect.Effect<Settlement, ClientAwaitFailure>;
    /**
     * Wait for finalization and return this Receipt's canonical terminal record in one RPC.
     * Requires both settlement and observation authority. Decode an ordinary completed
     * record's result through the Agent output Schema; joined completion may have no result.
     * Interruption uses the same bounded observation cancellation as `awaitSettlement`.
     */
    readonly awaitSettlementRecord: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionSettledRecord, ClientAwaitFailure>;
    /**
     * Wait without polling until progress after `afterSequence` is already durable or hinted.
     * The result is deliberately void: canonical records remain authoritative and must be read.
     * Interruption waits at most one second for best-effort remote cancellation.
     */
    readonly awaitProgress: (
      threadId: ThreadId,
      afterSequence: CanonicalSequence,
    ) => Effect.Effect<void, ClientProgressFailure>;
    /**
     * Current provisional text followed by native deltas. Reset starts a replacement snapshot,
     * including when a slow consumer exceeds the fixed backlog. Reconcile by
     * Run/Turn identity to canonical records; discard drafts on Discard, AttemptEnded,
     * Reset or stream failure. Snapshots exist only in the Object incarnation's memory.
     * Scope the stream: interruption cancels its remote subscription.
     */
    readonly watchText: (threadId: ThreadId) => Stream.Stream<LiveTextFrame, ClientObserveFailure>;
    /**
     * Forward producer NDJSON bytes without decoding or re-encoding frames.
     * Blank keepalive lines every 15 seconds expose idle transport disconnects.
     * The final consumer must use `decodeLiveText` to validate untrusted frames.
     * Interruption cancels the remote subscription; buffers remain producer-bounded.
     */
    readonly watchTextBytes: (
      threadId: ThreadId,
    ) => Stream.Stream<Uint8Array, ClientObserveFailure>;
    /** One bounded page of canonical records. */
    readonly readPage: (
      threadId: ThreadId,
      options?: {
        readonly afterSequence?: CanonicalSequence | undefined;
        readonly limit?: number | undefined;
      },
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ClientObserveFailure>;
    /**
     * Every canonical record up to the CURRENT committed tail, via repeated pages. A
     * snapshot read, not a live observation — callers wanting liveness re-read after
     * `awaitSettlement`.
     */
    readonly readAll: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ClientObserveFailure>;
    /**
     * Submission-addressed operations take the owning Thread explicitly (from the
     * Receipt): minted Submission identities stay OPAQUE outside the storage adapter that
     * minted them (D-P6-5), so the client never parses one to find the lane.
     */
    readonly abort: (
      threadId: ThreadId,
      command: AbortCommand,
    ) => Effect.Effect<AbortIntent, ClientAbortFailure>;
    readonly resolveApproval: (
      threadId: ThreadId,
      command: ApprovalDecisionCommand,
    ) => Effect.Effect<ApprovalDecisionIntent, ClientApprovalFailure>;
    readonly resolveUnknown: (
      threadId: ThreadId,
      command: UnknownResolutionCommand,
    ) => Effect.Effect<UnknownResolutionIntent, ClientUnknownFailure>;
  }
>()("@effect-agent/platform-cloudflare/CloudflareThreadClient") {
  /**
   * Assemble the client from a resolved namespace and the platform Crypto implementation.
   * rpcTracing is the binding name and remains opt-in. Caller authentication, principals,
   * idempotency keys, and definition digests are still explicit submission inputs.
   */
  static layerFromBinding(options: {
    readonly namespace: DurableObjectNamespace<ThreadObjectRpc>;
    readonly rpcTracing?: string;
  }): Layer.Layer<CloudflareThreadClient> {
    return CloudflareThreadClient.layer.pipe(
      Layer.provide([
        ThreadObjectNamespace.layer(options.namespace, options),
        cloudflareCryptoLayer,
      ]),
    );
  }

  static readonly layer: Layer.Layer<
    CloudflareThreadClient,
    never,
    ThreadObjectNamespace | Crypto.Crypto
  > = Layer.effect(CloudflareThreadClient)(
    Effect.gen(function* () {
      const namespace = yield* ThreadObjectNamespace;
      const { rpcTracing } = namespace;
      const crypto = yield* Crypto.Crypto;

      const call = Effect.fnUntraced(
        function* <Request, Success extends HostSuccess, Failure extends HostFailure>(
          threadId: ThreadId,
          contract: HostOperation<Request, Success, Failure>,
          request: Request,
        ): Effect.fn.Return<Success, Failure | ThreadClientError | HostProtocolError> {
          const operation = contract.name;

          const encoded = yield* contract.encodeRequest(request).pipe(
            Effect.mapError((error) =>
              HostProtocolError.make({
                message: boundHostDiagnostic(
                  `${operation} request encode failed: ${error.message}`,
                ),
              }),
            ),
          );

          // Empty arguments preserve native arity. Passing `undefined` still adds an argument.
          const traceArgs =
            rpcTracing === undefined ? [] : yield* RpcTracing.withRpcTraceContext([]);

          const raw = yield* callThreadObject(
            threadId,
            (stub) => stub[contract.method](encoded, ...traceArgs),
            (cause) =>
              ThreadClientError.make({
                threadId,
                message: boundHostDiagnostic(
                  `${operation} did not reach the Thread Object: ${safeCauseMessage(
                    cause,
                    "the RPC failed without a diagnostic",
                  )}`,
                ),
                cause,
                ...cloudflareFailureSignals(cause),
              }),
          ).pipe(Effect.provideService(ThreadObjectNamespace, namespace));

          const response = yield* contract.decodeResponse(raw).pipe(
            // Valid replies take one decode. The broad fallback only distinguishes malformed
            // envelopes from a valid result/failure belonging to another host operation.
            Effect.catch(() =>
              decodeHostResponse(raw).pipe(
                Effect.mapError((error) =>
                  HostProtocolError.make({
                    message: boundHostDiagnostic(
                      `The ${operation} answer could not be decoded: ${error.message}`,
                    ),
                  }),
                ),
                Effect.flatMap((observed) =>
                  Effect.fail(
                    outOfContract(
                      threadId,
                      operation,
                      observed._tag === "HostFailed"
                        ? `failure ${observed.failure._tag}`
                        : `result ${observed._tag}`,
                    ),
                  ),
                ),
              ),
            ),
          );

          if (response._tag === "HostFailed") return yield* Effect.fail(response.failure);

          return response;
        },
        (effect, threadId, contract) =>
          rpcTracing === undefined
            ? Effect.withSpan(effect, "CloudflareThreadClient.call", {
                attributes: { threadId, operation: contract.name },
              })
            : RpcTracing.withRpcClientSpan(effect, rpcTracing, contract.method),
      );

      const readPage = Effect.fnUntraced(function* (
        threadId: ThreadId,
        options?: {
          readonly afterSequence?: CanonicalSequence | undefined;
          readonly limit?: number | undefined;
        },
      ) {
        const request = ObservePageRequest.make({
          ...(options?.afterSequence === undefined ? {} : { afterSequence: options.afterSequence }),
          limit: options?.limit ?? 256,
        });

        const page = yield* call(threadId, hostOperations.observePage, request);

        return page.records;
      });

      const cancelProgress = (threadId: ThreadId, waiterId: string): Effect.Effect<void> =>
        call<CancelProgressRequest, ProgressCancelled, HostProtocolError>(
          threadId,
          hostOperations.cancelProgress,
          CancelProgressRequest.make({ waiterId }),
        ).pipe(
          // Finalizers are uninterruptible; only the foreign RPC branch must remain
          // interruptible so a lost cancellation reply cannot prevent local shutdown.
          Effect.interruptible,
          Effect.timeout(PROGRESS_CANCELLATION_TIMEOUT),
          Effect.asVoid,
          Effect.ignore,
        );

      const awaitSettlementCall = Effect.fnUntraced(function* <
        Success extends SettlementReached | SettlementRecordReached,
      >(
        receipt: Receipt,
        contract: HostOperation<
          AwaitSettlementRequest,
          Success | ProgressCancelled,
          typeof hostOperations.awaitSettlement.failure.Type
        >,
      ): Effect.fn.Return<Success, ClientAwaitFailure> {
        const waiterId = yield* crypto.randomUUIDv4().pipe(
          Effect.mapError((error) =>
            HostProtocolError.make({
              message: boundHostDiagnostic(
                `${contract.name} cancellation identity generation failed: ${error.message}`,
              ),
            }),
          ),
        );

        const response = yield* call(
          receipt.threadId,
          contract,
          AwaitSettlementRequest.make({ ...receipt, waiterId }),
        ).pipe(Effect.onInterrupt(() => cancelProgress(receipt.threadId, waiterId)));

        if (response._tag === "ProgressCancelled")
          return yield* outOfContract(receipt.threadId, contract.name, "result ProgressCancelled");

        return response;
      });

      const watchTextBytes = (
        threadId: ThreadId,
      ): Stream.Stream<Uint8Array, ClientObserveFailure> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const traceArgs =
              rpcTracing === undefined ? [] : yield* RpcTracing.withRpcTraceContext([]);

            const openingFailure = (cause: unknown) =>
              ThreadClientError.make({
                threadId,
                message: "Provisional text observation could not reach the Thread Object",
                cause,
                ...cloudflareFailureSignals(cause),
              });

            // Retain the original RPC promise: resolving through callThreadObject would
            // erase its optional native disposer before an interrupted opening can use it.
            const target = yield* RpcTargets.get(namespace, threadId, () =>
              namespace.get(threadId),
            ).pipe(Effect.mapError(openingFailure));

            let request: Promise<unknown> | undefined;
            let received: unknown;
            let closed = false;

            const cancel = () => {
              const value = received;

              received = undefined;

              const observation = Schema.is(LiveTextObservation)(value) ? value : undefined;

              // Native RPC disposes the capability with the request; also release
              // it for application-owned endpoints and late opening responses.
              try {
                observation?.lifetime[Symbol.dispose]();
              } catch {
                // A request disposer may already have revoked this stub.
              }
              const stream = observation?.stream ?? value;

              return stream instanceof ReadableStream && !stream.locked
                ? stream.cancel().catch(() => undefined)
                : Promise.resolve();
            };

            // Install ownership before invoking foreign code, not after its promise settles.
            yield* Effect.addFinalizer(() =>
              Effect.try({
                try: () => {
                  closed = true;
                  if (Predicate.hasProperty(request, Symbol.dispose)) {
                    const dispose = request[Symbol.dispose];

                    if (typeof dispose === "function") dispose.call(request);
                  }
                },
                catch: () => undefined,
              }).pipe(
                Effect.ignore,
                Effect.andThen(
                  Effect.tryPromise({ try: cancel, catch: () => undefined }).pipe(
                    Effect.interruptible,
                    Effect.timeout(PROGRESS_CANCELLATION_TIMEOUT),
                    Effect.ignore,
                  ),
                ),
              ),
            );

            const raw = yield* Effect.tryPromise({
              try: () => {
                request = target.watchTextEncoded(
                  Schema.encodeSync(WatchTextRequest)({ schemaVersion: 1 }),
                  ...traceArgs,
                );

                return request.then((value) => {
                  received = value;

                  // A custom endpoint may return a plain, non-cancellable promise. Keep
                  // late cleanup on that native promise chain; it starts no Effect fiber.
                  return closed ? cancel().then(() => value) : value;
                });
              },
              catch: openingFailure,
            }).pipe(
              Effect.interruptible,
              Effect.tapCause(() => RpcTargets.invalidate(target)),
            );

            const stream = Schema.is(LiveTextObservation)(raw) ? raw.stream : raw;

            if (stream instanceof ReadableStream)
              return Stream.fromReadableStream<Uint8Array, ThreadClientError>({
                evaluate: () => stream,
                // The subscription Scope cancels after the reader releases its lock.
                releaseLockOnEnd: true,
                onError: (cause) =>
                  ThreadClientError.make({
                    threadId,
                    message:
                      "Provisional text disconnected; discard drafts and read canonical records",
                    cause,
                  }),
              });

            const response = yield* decodeHostResponse(raw).pipe(
              Effect.mapError(liveTextProtocolFailure),
            );

            if (
              response._tag === "HostFailed" &&
              Schema.is(hostOperations.observePage.failure)(response.failure)
            )
              return yield* response.failure;

            return yield* liveTextProtocolFailure();
          }),
        );

      return CloudflareThreadClient.of({
        submit: Effect.fnUntraced(function* <InputSchema extends Schema.Top>(
          agent: DurableSubmitAgent<InputSchema>,
          input: InputSchema["Type"],
          options: DurableSubmitOptions,
        ) {
          // Client-side half of `DurableAgentRuntime.submit`'s input boundary: encode
          // through the Agent's input schema and prove the canonical persistence bounds.
          const encodedInput = yield* Schema.encodeEffect(agent.definition.input)(input).pipe(
            Effect.mapError((cause) =>
              AgentInputError.make({ message: `Unable to encode Agent input: ${cause.message}` }),
            ),
          );

          const inputPayload = yield* Schema.decodeUnknownEffect(PersistedJson)(encodedInput).pipe(
            Effect.mapError(() =>
              AgentInputError.make({
                message: "Agent input does not satisfy the canonical persistence bounds",
              }),
            ),
          );

          const request = {
            agentId: agent.definition.id,
            principal: options.principal,
            idempotencyKey: options.idempotencyKey,
            ...(options.admissionGroup === undefined
              ? {}
              : { admissionGroup: options.admissionGroup }),
            ...(options.admissionFence === undefined
              ? {}
              : { admissionFence: options.admissionFence }),
            ...(options.workerAdmission === undefined
              ? {}
              : { workerAdmission: options.workerAdmission }),
            ...(options.messageAdmission === undefined
              ? {}
              : { messageAdmission: options.messageAdmission }),
            definitions: options.definitions,
            inputPayload,
          };

          const succeeded = yield* call(options.threadId, hostOperations.submit, request);

          return succeeded.receipt;
        }),

        submissionStatus: Effect.fnUntraced(function* (receipt) {
          const result = yield* call(receipt.threadId, hostOperations.submissionStatus, receipt);

          return result.status;
        }),
        awaitSettlement: Effect.fnUntraced(function* (receipt) {
          const settled = yield* awaitSettlementCall(receipt, hostOperations.awaitSettlement);

          return settled.settlement;
        }),

        awaitSettlementRecord: Effect.fnUntraced(function* (receipt) {
          const settled = yield* awaitSettlementCall(receipt, hostOperations.awaitSettlementRecord);

          return settled.record;
        }),

        awaitProgress: Effect.fnUntraced(function* (threadId, afterSequence) {
          const waiterId = yield* crypto.randomUUIDv4().pipe(
            Effect.mapError((error) =>
              HostProtocolError.make({
                message: boundHostDiagnostic(
                  `awaitProgress cancellation identity generation failed: ${error.message}`,
                ),
              }),
            ),
          );

          const request = AwaitProgressRequest.make({ afterSequence, waiterId });

          const attempt = (retry: number): Effect.Effect<void, ClientProgressFailure> =>
            call(threadId, hostOperations.awaitProgress, request).pipe(
              Effect.asVoid,
              Effect.catchTag("ThreadClientError", (error) =>
                error.retryable === true && error.overloaded !== true && retry < 5
                  ? Effect.sleep(Duration.millis(10 * 2 ** retry)).pipe(
                      Effect.andThen(attempt(retry + 1)),
                    )
                  : Effect.fail(error),
              ),
            );

          yield* attempt(0).pipe(Effect.onInterrupt(() => cancelProgress(threadId, waiterId)));
        }),

        watchText: (threadId) => decodeLiveText(threadId, watchTextBytes(threadId)),
        watchTextBytes,

        readPage,

        readAll: Effect.fnUntraced(function* (threadId) {
          const all: Array<CanonicalRecordEnvelope> = [];
          let after: CanonicalSequence | undefined;

          for (;;) {
            const page = yield* readPage(threadId, { afterSequence: after, limit: 1_024 });

            all.push(...page);
            const last = page.at(-1);

            if (page.length < 1_024 || last === undefined) return all;
            after = last.sequence;
          }
        }),

        abort: Effect.fnUntraced(function* (threadId, command) {
          const recorded = yield* call(threadId, hostOperations.abort, command);

          return recorded.intent;
        }),

        resolveApproval: Effect.fnUntraced(function* (threadId, command) {
          const recorded = yield* call(threadId, hostOperations.resolveApproval, command);

          return recorded.intent;
        }),

        resolveUnknown: Effect.fnUntraced(function* (threadId, command) {
          const recorded = yield* call(threadId, hostOperations.resolveUnknown, command);

          return recorded.intent;
        }),
      });
    }),
  );
}
