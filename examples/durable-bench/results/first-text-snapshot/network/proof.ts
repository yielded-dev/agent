import { BrowserCrypto } from "@effect/platform-browser";
import {
  CloudflareThreadClient,
  type LiveTextFrame,
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
import { Prompt } from "effect/ai";

import { MEASURED_TOOLS, turn } from "../../../src/plan.ts";
import type { Env } from "./protocol.ts";
import { textFragments } from "./text.ts";
import { agent, definitions, type NetworkYieldedDO } from "./yielded.ts";

type Bindings = Env & { YIELDED: DurableObjectNamespace<NetworkYieldedDO> };
const Request = Schema.Struct({
  mode: Schema.Literals(["abort", "cancel", "slow", "wide", "denied", "late"]),
  object: Schema.NonEmptyString,
  sample: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
});
const Metrics = Schema.Struct({
  ok: Schema.Literal(true),
  version: Schema.String,
  metricsObservedMs: Schema.Finite,
  activeAlarmCount: Schema.Int,
  calls: Schema.Array(
    Schema.Struct({
      call: Schema.Int,
      status: Schema.optionalKey(Schema.Int),
      sseDone: Schema.optionalKey(Schema.Boolean),
      error: Schema.optionalKey(Schema.String),
      responseBytes: Schema.Int,
      firstByteMs: Schema.optionalKey(Schema.Finite),
      endMs: Schema.optionalKey(Schema.Finite),
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
type SeenText = { event: TextEvent; streamId: string; sequence: number; atMs: number };
type Control = { sequence: number; atMs: number };
type Draft = {
  event: TextEvent;
  call: number;
  text: string;
  expected: string;
  ended: boolean;
};
type Prefix = SeenText & { text: string; referenceChars: number; resetMs: number };
const modelKey = (event: Exclude<Event, { _tag: "AttemptEnded" }>) =>
  JSON.stringify([
    event.threadId,
    event.submissionId,
    event.attemptId,
    event.runId,
    event.turnId,
    event.generation,
  ]);
const partKey = (event: TextEvent) => JSON.stringify([modelKey(event), event.part.id]);

/** Deployed native workflow proof. Caller supplies fresh, separate proof Objects and saves the result. */
export const runProof = (url: URL, env: Bindings) =>
  Effect.suspend(() => {
    let stage = "query";
    let mode: (typeof Request.Type)["mode"] | undefined;
    const diagnostics: Record<string, Schema.Json> = {};
    const startedMs = Date.now();
    return Effect.gen(function* () {
      const query = yield* Schema.decodeUnknownEffect(Request)({
        mode: url.searchParams.get("mode"),
        object: url.searchParams.get("object"),
        sample: url.searchParams.get("sample"),
      });
      mode = query.mode;
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
      diagnostics.identity = common;

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
      const readMetrics = Effect.fnUntraced(function* (sample: string) {
        const requestMs = Date.now();
        const metricsUrl = new URL("https://first-text/metrics");
        for (const [key, value] of Object.entries({
          target: "yielded",
          object: query.object,
          sample,
          history: "50",
          ttftMs: "400",
          chunkDelayMs: "25",
          variant: "production",
        }))
          metricsUrl.searchParams.set(key, value);
        const value = yield* Effect.tryPromise({
          try: async (signal) => {
            const response = await stub.fetch(
              new globalThis.Request(metricsUrl, {
                signal,
                headers: { authorization: `Bearer ${env.TOKEN}` },
              }),
            );
            diagnostics.metricsHttp = {
              requestMs,
              receivedMs: Date.now(),
              status: response.status,
            };
            if (!response.ok) throw new Error(`metrics-http-${response.status}`);
            return response.json();
          },
          catch: () =>
            ProofFailure.make({ stage: "provider-metrics", reason: "metrics-request-failed" }),
        }).pipe(Effect.timeout("5 seconds"));
        return yield* Schema.decodeUnknownEffect(Metrics)(value);
      });
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
        projections: readonly { name: string; parts: ReadonlyMap<string, Draft> }[] = [],
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
          const visibleText = [];
          for (const [call, response] of responses.entries()) {
            const prompt = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(response.messages);
            const text = prompt.content
              .flatMap((message) =>
                message.role === "assistant"
                  ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                  : [],
              )
              .join("");
            for (const projection of projections) {
              const parts = [...projection.parts.values()].filter(
                (part) =>
                  part.event.runId === response.runId && part.event.turnId === response.turnId,
              );
              yield* requireEvidence(
                parts.length === 1 && parts[0]!.ended && parts[0]!.text === text,
                `${projection.name}-canonical-text-call-${call}`,
              );
            }
            visibleText.push({ call, characters: text.length, sha256: yield* hash(text) });
          }
          stage = "provider-metrics";
          const metrics = yield* readMetrics(accepted.sample);
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
              visibleText,
              matchedProjections: projections.map((projection) => projection.name),
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

      const projection = (name: string) => {
        const parts = new Map<string, Draft>();
        const models = new Map<string, number>();
        let characters = 0;
        const apply = Effect.fnUntraced(function* (frame: LiveTextFrame) {
          if (frame._tag === "Reset") {
            parts.clear();
            characters = 0;
            return;
          }
          const event = frame.event;
          if (event._tag === "AttemptEnded") return;
          if (event._tag === "Discard") {
            for (const [key, part] of parts)
              if (modelKey(part.event) === modelKey(event)) {
                characters -= part.text.length;
                parts.delete(key);
              }
            return;
          }
          const key = partKey(event);
          if (event.part.type === "text-start") {
            const model = modelKey(event);
            const call = models.get(model) ?? models.size;
            yield* requireEvidence(call < 9 && parts.size < 32, `${name}-part-bound`);
            models.set(model, call);
            const expected = textFragments(
              call < 8 ? { call: call + 1 } : { answer: "done after 8 lookups" },
            )
              .map((fragment) => (wide ? fragment.padEnd(4096, ".") : fragment))
              .join("");
            const draft = { event, call, text: "", expected, ended: false };
            parts.set(key, draft);
            return draft;
          }
          const draft = parts.get(key);
          if (!draft)
            return yield* ProofFailure.make({ stage: name, reason: "text-without-start" });
          diagnostics[name + "Projection"] = {
            call: draft.call,
            generation: event.generation,
            part: event.part.type,
            characters: draft.text.length,
            expectedCharacters: draft.expected.length,
          };
          if (event.part.type === "text-delta") {
            const delta = event.part.delta;
            yield* requireEvidence(
              !draft.ended &&
                draft.expected.slice(draft.text.length, draft.text.length + delta.length) === delta,
              `${name}-prefix-mismatch-call-${draft.call}`,
            );
            draft.text += delta;
            characters += delta.length;
            yield* requireEvidence(characters <= 2 * 1024 * 1024, `${name}-text-bound`);
          } else {
            yield* requireEvidence(
              draft.text === draft.expected,
              `${name}-incomplete-text-call-${draft.call}`,
            );
            draft.ended = true;
          }
          return draft;
        });
        return { name, parts, apply };
      };

      const watch = Effect.fnUntraced(function* (
        onFrame: (frame: LiveTextFrame, atMs: number) => Effect.Effect<void, ProofFailure> = () =>
          Effect.void,
      ) {
        const ready = yield* Deferred.make<number, ProofFailure>();
        const first = yield* Deferred.make<SeenText, ProofFailure>();
        const discarded = yield* Deferred.make<Control, ProofFailure>();
        const ended = yield* Deferred.make<Control, ProofFailure>();
        const failed = yield* Deferred.make<never, ProofFailure>();
        const resets: { streamId: string; atMs: number }[] = [];
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
                yield* requireEvidence(counts.resets <= 32, "reset-count-bound");
                resets.push({ streamId: frame.streamId, atMs });
                yield* Deferred.succeed(ready, atMs);
                yield* onFrame(frame, atMs);
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
                  target = { event, streamId: frame.streamId, sequence: frame.sequence, atMs };
                  yield* Deferred.succeed(first, target);
                }
              }
              yield* onFrame(frame, atMs);
            }),
          ),
          Effect.onExit((exit) => {
            if (Exit.isSuccess(exit)) return Effect.void;
            const error = Cause.findErrorOption(exit.cause);
            const tag = Option.isSome(error)
              ? Schema.decodeOption(Schema.Struct({ _tag: Schema.String }))(error.value)
              : Option.none();
            const failure =
              Option.isSome(error) && Schema.is(ProofFailure)(error.value)
                ? error.value
                : ProofFailure.make({
                    stage: "watch",
                    reason: Option.isSome(tag)
                      ? `native-watch-failed:${tag.value._tag}`
                      : "native-watch-interrupted-or-defected",
                  });
            return Effect.all(
              [
                Deferred.fail(ready, failure),
                Deferred.fail(first, failure),
                Deferred.fail(discarded, failure),
                Deferred.fail(ended, failure),
                Deferred.fail(failed, failure),
              ],
              { discard: true },
            );
          }),
          Effect.forkScoped,
        );
        return { ready, first, discarded, ended, failed, fiber, counts, resets };
      });

      if (query.mode === "late") {
        stage = "late-submit-without-watchers";
        const accepted = yield* submit(0);
        stage = "late-provider-inflight";
        let trigger: typeof Metrics.Type | undefined;
        for (let poll = 0; poll < 64; poll++) {
          const metrics = yield* readMetrics(accepted.sample);
          const call = metrics.calls[0];
          diagnostics.latePoll = {
            poll,
            calls: metrics.calls.length,
            metricsObservedMs: metrics.metricsObservedMs,
            activeAlarmCount: metrics.activeAlarmCount,
            firstByteMs: call?.firstByteMs ?? null,
            responseBytes: call?.responseBytes ?? 0,
            endMs: call?.endMs ?? null,
            sseDone: call?.sseDone === true,
          };
          yield* requireEvidence(metrics.version === env.VERSION.id, "late-provider-version");
          yield* requireEvidence(
            metrics.calls.length <= 1 &&
              call?.endMs === undefined &&
              call?.providerReceipt === undefined &&
              call?.error === undefined,
            "late-first-call-window-missed",
          );
          if (
            call?.firstByteMs !== undefined &&
            call.status === 200 &&
            call.responseBytes > 0 &&
            metrics.activeAlarmCount > 0 &&
            metrics.metricsObservedMs - call.firstByteMs >= 200
          ) {
            trigger = metrics;
            break;
          }
          yield* Effect.sleep("25 millis");
        }
        if (!trigger) return yield* ProofFailure.make({ stage, reason: "inflight-poll-bound" });
        const state = projection("late");
        const prefixReady = yield* Deferred.make<Prefix, ProofFailure>();
        const continued = yield* Deferred.make<Control, ProofFailure>();
        let prefix: Prefix | undefined;
        let resetMs = 0;
        const maxFragment = Math.max(
          ...textFragments({ call: 1 }).map((fragment) => fragment.length),
        );
        const connectStartedMs = Date.now();
        stage = "late-connect";
        const observer = yield* watch(
          Effect.fnUntraced(function* (frame: LiveTextFrame, atMs: number) {
            const draft = yield* state.apply(frame);
            if (frame._tag === "Reset") {
              resetMs = atMs;
              yield* requireEvidence(prefix === undefined, "late-reset-after-prefix");
              return;
            }
            const event = frame.event;
            if (event._tag !== "Text" || event.part.type !== "text-delta" || !draft) return;
            yield* requireEvidence(
              event.submissionId === accepted.receipt.submissionId,
              "late-receipt-identity",
            );
            if (!prefix) {
              yield* requireEvidence(
                draft.call === 0 &&
                  event.part.delta.length > maxFragment &&
                  draft.text.length < draft.expected.length,
                "late-nonempty-coalesced-active-prefix",
              );
              prefix = {
                event,
                streamId: frame.streamId,
                sequence: frame.sequence,
                atMs,
                text: draft.text,
                referenceChars: draft.text.length,
                resetMs,
              };
              diagnostics.latePrefix = {
                atMs,
                sequence: frame.sequence,
                characters: draft.text.length,
                generation: event.generation,
                connectToPrefixMs: atMs - connectStartedMs,
              };
              yield* Deferred.succeed(prefixReady, prefix);
            } else if (partKey(event) === partKey(prefix.event)) {
              yield* requireEvidence(frame.streamId === prefix.streamId, "late-continuation-epoch");
              yield* Deferred.succeed(continued, { sequence: frame.sequence, atMs });
            }
          }),
        );
        diagnostics.lateObserver = observer.counts;
        const seen = yield* Effect.all(
          {
            prefix: Deferred.await(prefixReady),
            continuation: Deferred.await(continued),
          },
          { concurrency: "unbounded" },
        ).pipe(Effect.raceFirst(Deferred.await(observer.failed)), Effect.timeout("10 seconds"));
        const completed = yield* finish(accepted, "completed", [state]).pipe(
          Effect.raceFirst(Deferred.await(observer.failed)),
        );
        const ended = yield* Deferred.await(observer.ended).pipe(
          Effect.timeout("10 seconds"),
          Effect.raceFirst(Deferred.await(observer.failed)),
        );
        return {
          ok: true,
          ...common,
          expected: "zero-subscriber-prefix-and-live-continuation",
          assertions: {
            zeroSubscribersBeforeConnect: true,
            coalescedPrefix: true,
            sameGenerationContinuation: true,
            canonicalTextMatches: true,
            canonicalCompleted: true,
          },
          turns: [completed],
          observation: {
            trigger: diagnostics.latePoll,
            connectStartedMs,
            resetMs: seen.prefix.resetMs,
            prefixMs: seen.prefix.atMs,
            connectToPrefixMs: seen.prefix.atMs - connectStartedMs,
            submitToVisibleMs: seen.prefix.atMs - accepted.submitStartedMs,
            submitToSettlementMs: completed.timing.settlementMs - accepted.submitStartedMs,
            prefixCharacters: seen.prefix.text.length,
            prefixSha256: yield* hash(seen.prefix.text),
            identitySha256: yield* hash(modelKey(seen.prefix.event)),
            attemptSha256: yield* hash(seen.prefix.event.attemptId),
            streamIdSha256: yield* hash(seen.prefix.streamId),
            generation: seen.prefix.event.generation,
            continuation: seen.continuation,
            attemptEnded: ended,
            ...observer.counts,
          },
          finishedMs: Date.now(),
        };
      }

      if (query.mode === "slow") {
        const referenceState = projection("reference");
        const slowState = projection("slow");
        const resume = yield* Deferred.make<void, ProofFailure>();
        type Watermark = SeenText & { characters: number; call: number };
        const pressure = yield* Deferred.make<Watermark, ProofFailure>();
        const caughtUp = yield* Deferred.make<
          { prefix: Prefix; continuation: Control & { characters: number } },
          ProofFailure
        >();
        let watermark: Watermark | undefined;
        let referenceEnded = false;
        let referenceDeltas = 0;
        let referenceResets = 0;
        stage = "slow-reference-open";
        const reference = yield* watch(
          Effect.fnUntraced(function* (frame: LiveTextFrame, atMs: number) {
            const draft = yield* referenceState.apply(frame);
            if (frame._tag === "Reset") {
              yield* requireEvidence(++referenceResets === 1, "reference-observer-overflow");
              return;
            }
            const event = frame.event;
            if (event._tag === "AttemptEnded") referenceEnded = true;
            if (event._tag !== "Text" || event.part.type !== "text-delta" || !draft) return;
            if (++referenceDeltas === 350) {
              watermark = {
                event,
                streamId: frame.streamId,
                sequence: frame.sequence,
                atMs,
                characters: draft.text.length,
                call: draft.call,
              };
              yield* Deferred.succeed(pressure, watermark);
            }
          }),
        );
        yield* Deferred.await(reference.ready).pipe(Effect.timeout("10 seconds"));
        let initialStreamId: string | undefined;
        let recovery: { streamId: string; atMs: number } | undefined;
        let candidate: Prefix | undefined;
        let catchupPublished = false;
        const connectStartedMs = Date.now();
        stage = "slow-paused-open";
        const observer = yield* watch(
          Effect.fnUntraced(function* (frame: LiveTextFrame, atMs: number) {
            const draft = yield* slowState.apply(frame);
            if (frame._tag === "Reset") {
              if (initialStreamId === undefined) {
                initialStreamId = frame.streamId;
                yield* Deferred.await(resume);
                return;
              }
              yield* requireEvidence(
                frame.streamId !== (recovery?.streamId ?? initialStreamId),
                "slow-recovery-stream-id",
              );
              yield* requireEvidence(!referenceEnded, "slow-reset-after-attempt-ended");
              recovery = { streamId: frame.streamId, atMs };
              candidate = undefined;
              diagnostics.slowReset = { atMs, referenceDeltas, referenceEnded };
              return;
            }
            const event = frame.event;
            if (event._tag === "AttemptEnded" && !catchupPublished)
              return yield* ProofFailure.make({
                stage: "slow-catchup",
                reason: "attempt-ended-before-active-catchup",
              });
            if (
              !recovery ||
              !watermark ||
              !draft ||
              event._tag !== "Text" ||
              event.part.type !== "text-delta" ||
              partKey(event) !== partKey(watermark.event) ||
              catchupPublished
            )
              return;
            const source = referenceState.parts.get(partKey(event));
            if (!candidate && draft.text.length >= watermark.characters) {
              yield* requireEvidence(
                !referenceEnded &&
                  source !== undefined &&
                  !source.ended &&
                  source.text.length < source.expected.length &&
                  draft.text.length > 4096 &&
                  draft.text.length < draft.expected.length,
                "slow-snapshot-must-catch-active-nonempty-generation",
              );
              candidate = {
                event,
                streamId: frame.streamId,
                sequence: frame.sequence,
                atMs,
                text: draft.text,
                referenceChars: Math.max(draft.text.length, source!.text.length),
                resetMs: recovery.atMs,
              };
              diagnostics.slowPrefix = {
                atMs,
                characters: candidate.text.length,
                referenceChars: candidate.referenceChars,
                generation: event.generation,
              };
            } else if (
              candidate &&
              draft.text.length > candidate.referenceChars &&
              atMs > candidate.atMs
            ) {
              yield* requireEvidence(
                frame.streamId === candidate.streamId && !referenceEnded,
                "slow-live-continuation-identity",
              );
              catchupPublished = true;
              yield* Deferred.succeed(caughtUp, {
                prefix: candidate,
                continuation: { sequence: frame.sequence, atMs, characters: draft.text.length },
              });
            }
          }),
        );
        diagnostics.slowReference = reference.counts;
        diagnostics.slowObserver = observer.counts;
        const readyMs = yield* Deferred.await(observer.ready).pipe(Effect.timeout("10 seconds"));
        const failed = Effect.raceFirst(
          Deferred.await(reference.failed),
          Deferred.await(observer.failed),
        );
        const accepted = yield* submit(0);
        stage = "slow-wait-for-pressure";
        const trigger = yield* Deferred.await(pressure).pipe(
          Effect.raceFirst(failed),
          Effect.timeout("30 seconds"),
        );
        yield* requireEvidence(
          trigger.event.submissionId === accepted.receipt.submissionId,
          "slow-reference-receipt",
        );
        const resumedMs = Date.now();
        diagnostics.slowTrigger = {
          atMs: trigger.atMs,
          resumedMs,
          call: trigger.call,
          characters: trigger.characters,
          referenceDeltas,
        };
        stage = "slow-active-catchup";
        yield* Deferred.succeed(resume, undefined);
        const seen = yield* Deferred.await(caughtUp).pipe(
          Effect.raceFirst(failed),
          Effect.timeout("15 seconds"),
        );
        const completed = yield* finish(accepted, "completed", [referenceState, slowState]).pipe(
          Effect.raceFirst(failed),
        );
        const ended = yield* Effect.all(
          {
            reference: Deferred.await(reference.ended),
            observer: Deferred.await(observer.ended),
          },
          { concurrency: "unbounded" },
        ).pipe(Effect.raceFirst(failed), Effect.timeout("10 seconds"));
        yield* requireEvidence(
          observer.counts.resets >= 2 && reference.counts.deltas === 432,
          "slow-fresh-reset-and-native-reference",
        );
        return {
          ok: true,
          ...common,
          expected: "active-snapshot-catchup",
          assertions: {
            publicDecoder: true,
            freshReset: true,
            newStreamId: true,
            attemptActiveAtCatchup: true,
            nonemptyPrefix: true,
            sameGenerationContinuation: true,
            canonicalTextMatches: true,
            canonicalCompleted: true,
            sameSubscription: true,
          },
          turns: [completed],
          observation: {
            connectStartedMs,
            readyMs,
            resumedMs,
            trigger: diagnostics.slowTrigger,
            resetMs: seen.prefix.resetMs,
            prefixMs: seen.prefix.atMs,
            resumeToResetMs: seen.prefix.resetMs - resumedMs,
            resumeToPrefixMs: seen.prefix.atMs - resumedMs,
            submitToCatchupPrefixMs: seen.prefix.atMs - accepted.submitStartedMs,
            submitToSettlementMs: completed.timing.settlementMs - accepted.submitStartedMs,
            prefixCharacters: seen.prefix.text.length,
            prefixSha256: yield* hash(seen.prefix.text),
            referenceCharactersAtCatchup: seen.prefix.referenceChars,
            identitySha256: yield* hash(modelKey(seen.prefix.event)),
            attemptSha256: yield* hash(seen.prefix.event.attemptId),
            initialStreamIdSha256: yield* hash(initialStreamId ?? ""),
            streamIdSha256: yield* hash(seen.prefix.streamId),
            generation: seen.prefix.event.generation,
            continuation: seen.continuation,
            attemptEnded: ended,
            reference: reference.counts,
            observer: observer.counts,
          },
          finishedMs: Date.now(),
        };
      }

      stage = "watch-ready";
      const observer = yield* watch();
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
              const reopened = yield* watch();
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
        const failure =
          Option.isSome(error) && Schema.is(ProofFailure)(error.value) ? error.value : undefined;
        const tagged = Option.isSome(error)
          ? Schema.decodeOption(Schema.Struct({ _tag: Schema.String }))(error.value)
          : Option.none();
        return Effect.succeed({
          ok: false,
          mode: mode ?? "invalid",
          stage: failure?.stage ?? stage,
          reason:
            failure?.reason ??
            (Cause.hasInterrupts(cause) ? "interrupted" : "native-operation-or-deadline-failed"),
          nativeErrorTag: Option.isSome(tagged) ? tagged.value._tag : null,
          diagnostics,
          startedMs,
          finishedMs: Date.now(),
        });
      }),
    );
  });
