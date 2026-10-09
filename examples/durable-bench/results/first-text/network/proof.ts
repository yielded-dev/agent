import { BrowserCrypto } from "@effect/platform-browser";
import {
  CloudflareThreadClient,
  LiveTextFrame,
} from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import { digestDefinitions } from "@yielded/agent/digest";
import { ThreadId } from "@yielded/agent/identifiers";
import { OperationDenied } from "@yielded/agent/operation-authorizer";
import type { Event } from "@yielded/agent/provisional-text";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import {
  CanonicalSequence,
  SubmissionSettledRecord,
  type CanonicalRecordEnvelope,
} from "@yielded/agent/records";
import { AbortCommand } from "@yielded/agent/submission-ledger";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect";

import { MEASURED_TOOLS, turn } from "../../../src/plan.ts";
import type { Env } from "./protocol.ts";
import { agent, definitions, type NetworkYieldedDO } from "./yielded.ts";

type Bindings = Env & { YIELDED: DurableObjectNamespace<NetworkYieldedDO> };
const Request = Schema.Struct({
  mode: Schema.Literals(["abort", "cancel", "slow", "wide", "denied"]),
  object: Schema.NonEmptyString,
  sample: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
});
const Metrics = Schema.Struct({
  ok: Schema.Literal(true),
  version: Schema.String,
  calls: Schema.Array(
    Schema.Struct({
      call: Schema.Int,
      status: Schema.optionalKey(Schema.Int),
      sseDone: Schema.optionalKey(Schema.Boolean),
      error: Schema.optionalKey(Schema.String),
      responseBytes: Schema.Int,
      invocation: Schema.optionalKey(Schema.Struct({ kind: Schema.String })),
      providerReceipt: Schema.optionalKey(
        Schema.Struct({
          firstTextMs: Schema.Finite,
          lastTextMs: Schema.Finite,
          endMs: Schema.Finite,
          requestBytes: Schema.Int,
          modelVisibleFingerprint: Schema.String,
        }),
      ),
    }),
  ),
});
class ProofFailure extends Schema.TaggedError<ProofFailure>()("FirstTextProofFailure", {
  stage: Schema.String,
  reason: Schema.String,
}) {}
const requireEvidence = (condition: boolean, reason: string) =>
  condition ? Effect.void : Effect.fail(ProofFailure.make({ stage: "assertion", reason }));
const encoder = new TextEncoder();
const hash = (text: string) =>
  Effect.tryPromise({
    try: async () =>
      Array.from(
        new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(text))),
        (byte) => byte.toString(16).padStart(2, "0"),
      ).join(""),
    catch: () => ProofFailure.make({ stage: "digest", reason: "sha256-failed" }),
  });
type TextEvent = Extract<Event, { _tag: "Text" }>;
type SeenText = { event: TextEvent; sequence: number; atMs: number };
type Control = { sequence: number; atMs: number };
const modelKey = (event: Exclude<Event, { _tag: "AttemptEnded" }>) =>
  JSON.stringify([
    event.threadId,
    event.submissionId,
    event.attemptId,
    event.runId,
    event.turnId,
    event.generation,
  ]);

/** Deployed native workflow proof. Caller supplies fresh, separate proof Objects and saves the result. */
export const runProof = (url: URL, env: Bindings) =>
  Effect.suspend(() => {
    let stage = "query";
    const startedMs = Date.now();
    return Effect.gen(function* () {
      const query = yield* Schema.decodeUnknownEffect(Request)({
        mode: url.searchParams.get("mode"),
        object: url.searchParams.get("object"),
        sample: url.searchParams.get("sample"),
      });
      yield* requireEvidence(
        env.PHASE === "measure" &&
          query.object.includes("-proof-") &&
          /-h50-d400-o0/.test(query.object),
        "measure-proof-object-required",
      );
      const client = yield* CloudflareThreadClient;
      const threadId = ThreadId.make(query.object);
      const stub = env.YIELDED.getByName(query.object, { locationHint: "wnam" });
      const common = {
        mode: query.mode,
        versionSha256: yield* hash(env.VERSION.id),
        startedMs,
        objectSha256: yield* hash(query.object),
        sampleSha256: yield* hash(query.sample),
      };

      if (query.mode === "denied") {
        stage = "denied-observation";
        yield* requireEvidence(query.object.includes("-proof-denied-"), "denied-object-required");
        let resets = 0;
        const operations = [
          client.watchText(threadId).pipe(
            Stream.runForEach((frame) =>
              Effect.sync(() => {
                if (frame._tag === "Reset") resets++;
              }),
            ),
          ),
          client.readPage(threadId, { afterSequence: CanonicalSequence.make(0), limit: 1 }),
          client.awaitProgress(threadId, CanonicalSequence.make(0)),
        ];
        const denied: boolean[] = [];
        for (const operation of operations) {
          const exit = yield* operation.pipe(Effect.timeout("10 seconds"), Effect.exit);
          const failure = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();
          denied.push(Option.isSome(failure) && Schema.is(OperationDenied)(failure.value));
        }
        yield* requireEvidence(
          denied.every(Boolean) && resets === 0,
          "all-observation-apis-must-deny-before-reset",
        );
        return {
          ok: true,
          ...common,
          expected: "OperationDenied",
          assertions: {
            watchTextDenied: denied[0],
            readPageDenied: denied[1],
            awaitProgressDenied: denied[2],
            resetFrames: resets,
          },
          turns: [],
          provider: null,
          finishedMs: Date.now(),
        };
      }

      const digests = yield* digestDefinitions(definitions);
      const wide = query.mode === "slow" || query.mode === "wide";
      const prefix = wide ? "proof-wide-" : `proof-${query.mode}-`;
      const sampleBase = query.sample.startsWith(prefix) ? query.sample : prefix + query.sample;
      let after = CanonicalSequence.make(0);
      const submit = (index: number) =>
        Effect.gen(function* () {
          stage = "submit";
          const input = turn(`${sampleBase}-${index}`, MEASURED_TOOLS);
          const submitStartedMs = Date.now();
          const receipt = yield* client.submit(agent, input.text, {
            threadId,
            principal: Principal.make("bench"),
            idempotencyKey: IdempotencyKey.make(input.id),
            definitions: digests,
          });
          return { receipt, sample: input.id, submitStartedMs, receiptMs: Date.now() };
        }).pipe(Effect.timeout("30 seconds"));

      const finish = (
        accepted: { receipt: Receipt; sample: string; submitStartedMs: number; receiptMs: number },
        expected: "completed" | "aborted",
      ) =>
        Effect.gen(function* () {
          stage = "settlement";
          const settlement = yield* client.awaitSettlement(accepted.receipt);
          const settlementMs = Date.now();
          yield* requireEvidence(settlement.outcome === expected, "native-settlement-outcome");
          stage = "canonical";
          const records: CanonicalRecordEnvelope[] = [];
          let terminal: SubmissionSettledRecord | undefined;
          while (terminal === undefined) {
            const page = yield* client.readPage(threadId, { afterSequence: after, limit: 64 });
            for (const envelope of page) {
              after = envelope.sequence;
              records.push(envelope);
              const payload = envelope.record.payload;
              if (
                payload._tag === "SubmissionSettled" &&
                payload.submissionId === accepted.receipt.submissionId
              )
                terminal = payload;
            }
            yield* requireEvidence(records.length <= 512, "canonical-record-bound");
            if (terminal === undefined && page.length === 0)
              yield* client.awaitProgress(threadId, after);
          }
          yield* requireEvidence(
            terminal.outcome === expected &&
              terminal.receiptId === accepted.receipt.receiptId &&
              terminal.runId !== undefined,
            "canonical-settlement-identity",
          );
          const responses = records.flatMap(({ record }) =>
            record.payload._tag === "ModelResponseRecorded" &&
            record.payload.runId === terminal.runId
              ? [record.payload]
              : [],
          );
          const lookups = records.flatMap(({ record }) =>
            record.payload._tag === "ToolCallSettled" &&
            record.payload.runId === terminal.runId &&
            record.payload.toolName === "lookup"
              ? [record.payload]
              : [],
          );
          if (expected === "completed")
            yield* requireEvidence(
              responses.length === 9 &&
                terminal.usageSummary?.modelCalls === 9 &&
                lookups.length === 8 &&
                lookups.every((value) => !value.isFailure),
              "completed-nine-model-calls-eight-lookups",
            );
          else
            yield* requireEvidence(
              responses.length === 0 &&
                lookups.length === 0 &&
                records.some(
                  ({ record }) =>
                    record.payload._tag === "AbortRequested" &&
                    record.payload.submissionId === accepted.receipt.submissionId,
                ),
              "abort-before-first-response-commit",
            );
          const encodedReceipt = yield* Schema.encodeEffect(Schema.fromJsonString(Receipt))(
            accepted.receipt,
          );
          const encodedTerminal = yield* Schema.encodeEffect(
            Schema.fromJsonString(SubmissionSettledRecord),
          )(terminal);
          const canonicalMs = Date.now();
          stage = "provider-metrics";
          const metricsUrl = new URL("https://first-text/metrics");
          for (const [key, value] of Object.entries({
            target: "yielded",
            object: query.object,
            sample: accepted.sample,
            history: "50",
            ttftMs: "400",
            chunkDelayMs: "25",
            variant: "production",
          }))
            metricsUrl.searchParams.set(key, value);
          const metricsJson = yield* Effect.tryPromise({
            try: async (signal) => {
              const response = await stub.fetch(
                new globalThis.Request(metricsUrl, {
                  signal,
                  headers: { authorization: `Bearer ${env.TOKEN}` },
                }),
              );
              if (!response.ok) throw new Error("metrics-http-status");
              return response.json();
            },
            catch: () =>
              ProofFailure.make({ stage: "provider-metrics", reason: "metrics-request-failed" }),
          }).pipe(Effect.timeout("15 seconds"));
          const metrics = yield* Schema.decodeUnknownEffect(Metrics)(metricsJson);
          yield* requireEvidence(
            metrics.version === env.VERSION.id &&
              metrics.calls.every((call) => call.invocation?.kind === "alarm"),
            "native-alarm-and-version",
          );
          if (expected === "completed")
            yield* requireEvidence(
              metrics.calls.length === 9 &&
                metrics.calls.every(
                  (call) =>
                    call.status === 200 &&
                    call.sseDone === true &&
                    call.error === undefined &&
                    call.providerReceipt !== undefined,
                ),
              "native-provider-nine-completed-calls",
            );
          return {
            receipt: {
              sha256: yield* hash(encodedReceipt),
              receiptIdSha256: yield* hash(accepted.receipt.receiptId),
              submissionIdSha256: yield* hash(accepted.receipt.submissionId),
              queueSequence: accepted.receipt.queueSequence,
            },
            canonical: {
              outcome: terminal.outcome,
              runIdSha256: yield* hash(terminal.runId ?? ""),
              settlementSha256: yield* hash(encodedTerminal),
              records: records.length,
              afterSequence: after,
              modelCalls: terminal.usageSummary?.modelCalls ?? null,
              modelResponses: responses.length,
              lookups: lookups.length,
              failedLookups: lookups.filter((value) => value.isFailure).length,
              messagesDigests: responses.map((value) => value.messagesDigest),
            },
            provider: {
              calls: metrics.calls.map((call) => ({
                call: call.call,
                status: call.status ?? null,
                sseDone: call.sseDone === true,
                streamFailedOrCancelled: call.error !== undefined,
                responseBytes: call.responseBytes,
                ...call.providerReceipt,
              })),
            },
            timing: {
              submitStartedMs: accepted.submitStartedMs,
              receiptMs: accepted.receiptMs,
              settlementMs,
              canonicalMs,
            },
          };
        }).pipe(Effect.timeout("60 seconds"));

      const watch = Effect.gen(function* () {
        const ready = yield* Deferred.make<number, ProofFailure>();
        const first = yield* Deferred.make<SeenText, ProofFailure>();
        const discarded = yield* Deferred.make<Control, ProofFailure>();
        const ended = yield* Deferred.make<Control, ProofFailure>();
        let target: SeenText | undefined;
        const counts = {
          resets: 0,
          starts: 0,
          deltas: 0,
          ends: 0,
          textChars: 0,
          discards: 0,
          attemptEnds: 0,
        };
        const fiber = yield* client.watchText(threadId).pipe(
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              const atMs = Date.now();
              if (frame._tag === "Reset") {
                counts.resets++;
                yield* Deferred.succeed(ready, atMs);
                return;
              }
              const event = frame.event;
              if (event._tag === "AttemptEnded") {
                counts.attemptEnds++;
                if (
                  target &&
                  event.attemptId === target.event.attemptId &&
                  event.submissionId === target.event.submissionId
                )
                  yield* Deferred.succeed(ended, { sequence: frame.sequence, atMs });
              } else if (event._tag === "Discard") {
                counts.discards++;
                if (target && modelKey(event) === modelKey(target.event))
                  yield* Deferred.succeed(discarded, { sequence: frame.sequence, atMs });
              } else if (event.part.type === "text-start") counts.starts++;
              else if (event.part.type === "text-end") counts.ends++;
              else {
                counts.deltas++;
                counts.textChars += event.part.delta.length;
                if (wide)
                  yield* requireEvidence(event.part.delta.length === 4096, "wide-fragment-width");
                if (event.part.delta.length > 0 && target === undefined) {
                  target = { event, sequence: frame.sequence, atMs };
                  yield* Deferred.succeed(first, target);
                }
              }
            }),
          ),
          Effect.onExit((exit) =>
            Exit.isFailure(exit)
              ? Effect.all(
                  [
                    Deferred.fail(
                      ready,
                      ProofFailure.make({ stage: "watch", reason: "watch-ended" }),
                    ),
                    Deferred.fail(
                      first,
                      ProofFailure.make({ stage: "watch", reason: "watch-ended" }),
                    ),
                    Deferred.fail(
                      discarded,
                      ProofFailure.make({ stage: "watch", reason: "watch-ended" }),
                    ),
                    Deferred.fail(
                      ended,
                      ProofFailure.make({ stage: "watch", reason: "watch-ended" }),
                    ),
                  ],
                  { discard: true },
                )
              : Effect.void,
          ),
          Effect.forkScoped,
        );
        return { ready, first, discarded, ended, fiber, counts };
      });

      if (query.mode === "slow") {
        stage = "raw-watch-open";
        const reader = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: async () => {
              const raw = await stub.watchTextEncoded({ schemaVersion: 1 });
              if (!(raw instanceof ReadableStream)) throw new Error("not-a-byte-stream");
              return raw.getReader();
            },
            catch: () =>
              ProofFailure.make({ stage: "raw-watch-open", reason: "rpc-stream-required" }),
          }).pipe(Effect.interruptible, Effect.timeout("15 seconds")),
          (reader) =>
            Effect.tryPromise({
              try: () => reader.cancel(),
              catch: () => ProofFailure.make({ stage: "raw-watch-close", reason: "cancel-failed" }),
            }).pipe(
              Effect.interruptible,
              Effect.timeout("3 seconds"),
              Effect.ignore,
              Effect.ensuring(Effect.sync(() => reader.releaseLock())),
            ),
        );
        const decode = Schema.decodeUnknownSync(Schema.fromJsonString(LiveTextFrame));
        const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
        let pending = "";
        let bytesRead = 0;
        let framesRead = 0;
        let deltasRead = 0;
        let expectedSequence = 1;
        const nextFrame = Effect.gen(function* () {
          while (!pending.includes("\n")) {
            const chunk = yield* Effect.tryPromise({
              try: () => reader.read(),
              catch: () => ProofFailure.make({ stage: "raw-read", reason: "read-failed" }),
            });
            if (chunk.done || !(chunk.value instanceof Uint8Array))
              return yield* ProofFailure.make({ stage: "raw-read", reason: "byte-stream-ended" });
            bytesRead += chunk.value.byteLength;
            pending += decoder.decode(chunk.value, { stream: true });
            yield* requireEvidence(pending.length <= 8 * 1024 * 1024, "raw-buffer-bound");
          }
          const newline = pending.indexOf("\n");
          const line = pending.slice(0, newline);
          pending = pending.slice(newline + 1);
          yield* requireEvidence(encoder.encode(line).byteLength <= 32 * 1024, "raw-frame-bound");
          const frame = yield* Effect.try({
            try: () => decode(line),
            catch: () => ProofFailure.make({ stage: "raw-read", reason: "invalid-native-frame" }),
          });
          framesRead++;
          return frame;
        }).pipe(Effect.timeout("10 seconds"));
        const reset = yield* nextFrame;
        if (reset._tag !== "Reset" || reset.threadId !== threadId || reset.sequence !== 0)
          return yield* ProofFailure.make({
            stage: "raw-reset",
            reason: "reset-required-before-submit",
          });
        const readyMs = Date.now();
        const drain = (receipt: Receipt) =>
          Effect.gen(function* () {
            stage = "raw-resume";
            for (let count = 0; count < 2000; count++) {
              const frame = yield* nextFrame;
              if (
                frame._tag !== "Event" ||
                frame.streamId !== reset.streamId ||
                frame.event.threadId !== threadId
              )
                return yield* ProofFailure.make({ stage, reason: "raw-stream-identity" });
              if (frame.event._tag === "Text" && frame.event.part.type === "text-delta") {
                deltasRead++;
                yield* requireEvidence(
                  frame.event.part.delta.length === 4096,
                  "raw-wide-fragment-width",
                );
              }
              yield* requireEvidence(frame.sequence >= expectedSequence, "raw-sequence-reversed");
              if (frame.sequence > expectedSequence)
                return {
                  expected: expectedSequence,
                  observed: frame.sequence,
                  missing: frame.sequence - expectedSequence,
                  atMs: Date.now(),
                };
              expectedSequence++;
              if (
                frame.event._tag === "AttemptEnded" &&
                frame.event.submissionId === receipt.submissionId
              )
                return undefined;
            }
            return yield* ProofFailure.make({ stage, reason: "raw-frame-count-bound" });
          }).pipe(Effect.timeout("20 seconds"));
        const firstAccepted = yield* submit(0);
        const turns = [yield* finish(firstAccepted, "completed")];
        const resumedMs = [Date.now()];
        let gap = yield* drain(firstAccepted.receipt);
        const heldBatches = [1];
        if (gap === undefined) {
          // After a gap-free first drain, accumulate three turns without any reader pulls.
          let lastReceipt = firstAccepted.receipt;
          for (let index = 1; index < 4; index++) {
            const accepted = yield* submit(index);
            turns.push(yield* finish(accepted, "completed"));
            lastReceipt = accepted.receipt;
          }
          heldBatches.push(3);
          resumedMs.push(Date.now());
          gap = yield* drain(lastReceipt);
        }
        yield* requireEvidence(deltasRead > 0, "wide-native-deltas-observed");
        return {
          ok: gap !== undefined,
          ...common,
          expected: "sequence-gap",
          assertions: {
            settledBeforeResume: true,
            sequenceGap: gap !== undefined,
            sameSubscription: true,
          },
          turns,
          observation: {
            readyMs,
            resumedMs,
            heldBatches,
            bytesRead,
            framesRead,
            deltasRead,
            gap: gap ?? null,
          },
          finishedMs: Date.now(),
        };
      }

      stage = "watch-ready";
      const observer = yield* watch;
      const readyMs = yield* Deferred.await(observer.ready).pipe(Effect.timeout("10 seconds"));
      const accepted = yield* submit(0);
      stage = "first-text";
      const first = yield* Deferred.await(observer.first).pipe(Effect.timeout("15 seconds"));
      yield* requireEvidence(
        first.event.submissionId === accepted.receipt.submissionId,
        "first-text-native-receipt",
      );
      const identitySha256 = yield* hash(modelKey(first.event));
      const attemptSha256 = yield* hash(first.event.attemptId);
      const firstText = {
        sequence: first.sequence,
        atMs: first.atMs,
        generation: first.event.generation,
        identitySha256,
        attemptSha256,
      };

      if (query.mode === "cancel") {
        stage = "watch-cancel";
        yield* Fiber.interrupt(observer.fiber).pipe(Effect.timeout("5 seconds"));
        const cancelled = yield* Fiber.await(observer.fiber);
        yield* requireEvidence(
          Exit.isFailure(cancelled) && Cause.hasInterrupts(cancelled.cause),
          "watcher-interrupted",
        );
        const cancelledMs = Date.now();
        for (let index = 0; index < 10; index++)
          yield* Effect.scoped(
            Effect.gen(function* () {
              const reopened = yield* watch;
              yield* Deferred.await(reopened.ready);
            }),
          ).pipe(Effect.timeout("10 seconds"));
        const completed = yield* finish(accepted, "completed");
        return {
          ok: true,
          ...common,
          expected: "watcher-interrupted-turn-completed",
          assertions: { watcherInterrupted: true, reopenReadyClosed: 10, canonicalCompleted: true },
          turns: [completed],
          observation: { readyMs, firstText, cancelledMs, ...observer.counts },
          finishedMs: Date.now(),
        };
      }

      if (query.mode === "abort") {
        stage = "native-abort";
        const abortRequestedMs = Date.now();
        yield* client
          .abort(
            threadId,
            AbortCommand.make({
              submissionId: accepted.receipt.submissionId,
              author: "first-text-proof",
              reason: "abort after first nonempty provisional delta",
            }),
          )
          .pipe(Effect.timeout("15 seconds"));
        const results = yield* Effect.all(
          {
            turn: finish(accepted, "aborted"),
            discard: Deferred.await(observer.discarded).pipe(Effect.timeout("20 seconds")),
            ended: Deferred.await(observer.ended).pipe(Effect.timeout("20 seconds")),
          },
          { concurrency: "unbounded" },
        );
        yield* requireEvidence(
          results.discard.sequence > first.sequence &&
            results.ended.sequence > results.discard.sequence,
          "discard-before-attempt-ended",
        );
        return {
          ok: true,
          ...common,
          expected: "aborted",
          assertions: { canonicalAborted: true, matchingDiscard: true, matchingAttemptEnded: true },
          turns: [results.turn],
          observation: {
            readyMs,
            firstText,
            abortRequestedMs,
            discard: { ...results.discard, identitySha256 },
            attemptEnded: { ...results.ended, attemptSha256 },
            ...observer.counts,
          },
          finishedMs: Date.now(),
        };
      }

      const completed = yield* finish(accepted, "completed");
      const ended = yield* Deferred.await(observer.ended).pipe(Effect.timeout("10 seconds"));
      yield* requireEvidence(
        observer.counts.starts === 9 &&
          observer.counts.ends === 9 &&
          observer.counts.deltas === 432 &&
          observer.counts.textChars === 4096 * 432 &&
          observer.counts.discards === 0,
        "wide-public-decoder-complete",
      );
      return {
        ok: true,
        ...common,
        expected: "wide-consumed-completed",
        assertions: { publicDecoderComplete: true, canonicalCompleted: true, sequenceGap: false },
        turns: [completed],
        observation: {
          readyMs,
          firstText,
          attemptEnded: { ...ended, attemptSha256 },
          ...observer.counts,
        },
        finishedMs: Date.now(),
      };
    }).pipe(
      Effect.scoped,
      Effect.timeout("180 seconds"),
      Effect.provide([
        CloudflareThreadClient.layerFromBinding({ namespace: env.YIELDED }),
        BrowserCrypto.layer,
      ]),
      Effect.catchCause((cause) => {
        const error = Cause.findErrorOption(cause);
        if (Option.isSome(error) && Schema.is(ProofFailure)(error.value))
          return Effect.fail(error.value);
        return Effect.fail(
          ProofFailure.make({
            stage,
            reason: Cause.hasInterrupts(cause)
              ? "interrupted"
              : "native-operation-or-deadline-failed",
          }),
        );
      }),
    );
  });
