import * as OpenAiClient from "@effect/ai-openai-compat/OpenAiClient";
import * as OpenAiLanguageModel from "@effect/ai-openai-compat/OpenAiLanguageModel";
import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto";
import {
  HostResponse,
  LiveTextFrame,
  SubmitRequest,
} from "@yielded/agent-platform-cloudflare/cloudflare-thread-client";
import * as ThreadObject from "@yielded/agent-platform-cloudflare/thread-object";
import * as Agent from "@yielded/agent/agent";
import { digestDefinitions } from "@yielded/agent/digest";
import { text } from "@yielded/agent/output";
import { IdempotencyKey, Principal, Receipt } from "@yielded/agent/receipt";
import { DefinitionDigestInput } from "@yielded/agent/records";
import { DurableObjectState, Rpc } from "effect-cf";
import * as Model from "effect/ai/Model";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { alarmEntered, constructed, identity, probe } from "./identity.ts";
import { encode, lines, type Env } from "./protocol.ts";

class CaptureError extends Schema.TaggedError<CaptureError>()("CaptureError", {
  message: Schema.String,
}) {}

const Observation = Schema.Struct({
  stream: Schema.instanceOf(ReadableStream<Uint8Array>),
  lifetime: Schema.declare(Rpc.isDisposable),
});

const decodeFrame = Schema.decodeUnknownSync(Schema.fromJsonString(LiveTextFrame));
const instances = new WeakMap<globalThis.DurableObjectState, { env: Env; calls: number }>();

const definition = Agent.make("hello", {
  input: Schema.String,
  inputPrompt: (input) => input,
  output: text(Schema.String),
  instructions: "Reply briefly.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "30 seconds" },
});

const selected = Model.make(
  "scripted",
  "scripted-1",
  Layer.unwrap(
    Effect.gen(function* () {
      const native = yield* DurableObjectState.DurableObjectState;
      const state = instances.get(native.raw);

      if (!state)
        return yield* Effect.die(CaptureError.make({ message: "Missing Object environment" }));

      return OpenAiLanguageModel.layer({ model: "scripted-1" }).pipe(
        Layer.provide(OpenAiClient.layer({ apiUrl: state.env.PROVIDER_URL })),
        Layer.provide(FetchHttpClient.layer),
        Layer.provide(
          Layer.succeed(FetchHttpClient.Fetch, (input, init) => {
            state.calls++;
            const request = new Request(input, init);

            request.headers.set("authorization", `Bearer ${state.env.TOKEN}`);

            return fetch(request);
          }),
        ),
      );
    }),
  ),
);

const definitions = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "scripted", name: "scripted-1" },
  tools: {},
});

const digests = Effect.runSync(
  Effect.cached(digestDefinitions(definitions).pipe(Effect.provide(BrowserCrypto.layer))),
);

const Base = ThreadObject.make(
  ThreadObject.layer([{ agent: Agent.withModel(definition, selected), definitions }]),
  { namespaceBinding: "THREAD", deploymentId: "cold-start", producerPrefix: "cold-start" },
);

export class YieldedObject extends Base {
  private readonly state;
  private readonly environment: Env;

  constructor(ctx: globalThis.DurableObjectState, env: Env) {
    const state = { env, calls: 0 };

    instances.set(ctx, state);
    super(ctx, env);
    this.state = state;
    this.environment = env;
    constructed();
  }
  override alarm(info?: AlarmInvocationInfo) {
    alarmEntered();

    return super.alarm(info);
  }
  touch() {
    return identity(this.environment.VERSION.id);
  }
  probe(sample: string) {
    return probe(this.environment.VERSION.id, () => this.run(sample));
  }
  async run(sample: string): Promise<ReadableStream<Uint8Array>> {
    Schema.decodeSync(Schema.NonEmptyString)(sample);

    const observation = Schema.decodeUnknownSync(Observation)(
      await super.watchTextEncoded({ schemaVersion: 1 }),
    );

    const reader = observation.stream.getReader();
    const startCalls = this.state.calls;
    let cancelled = false;
    const dispose = () => observation.lifetime[Symbol.dispose]();

    return new ReadableStream<Uint8Array>({
      start: async (controller) => {
        const emit = (type: string, value: unknown) => {
          if (!cancelled) controller.enqueue(encode(type, value));
        };

        const textLines = lines((line) => {
          if (!line) return;
          const frame = decodeFrame(line);

          if (
            frame._tag === "Event" &&
            frame.event._tag === "Text" &&
            frame.event.part.type === "text-delta" &&
            frame.event.part.delta
          )
            emit("text", { text: frame.event.part.delta });
        });

        const reading = (async () => {
          try {
            for (;;) {
              const next = await reader.read();

              if (next.done) break;
              textLines.write(next.value);
            }
            textLines.finish();
          } finally {
            reader.releaseLock();
          }
        })();

        // Handle a rejected reader immediately while admission/settlement is pending.
        const observed = reading.catch((cause: unknown) => {
          emit("error", String(cause));
        });

        try {
          const submitted = Schema.decodeUnknownSync(HostResponse)(
            await super.submitEncoded(
              Schema.encodeSync(SubmitRequest)(
                SubmitRequest.make({
                  agentId: definition.id,
                  principal: Principal.make("example"),
                  idempotencyKey: IdempotencyKey.make(sample),
                  definitions: await Effect.runPromise(digests),
                  inputPayload: "Say hello.",
                }),
              ),
            ),
          );

          if (submitted._tag !== "SubmitSucceeded")
            // oxlint-disable-next-line effect/no-untyped-throw -- Native stream callback reports this typed failure on its wire.
            throw CaptureError.make({ message: `Admission: ${submitted._tag}` });

          const settled = Schema.decodeUnknownSync(HostResponse)(
            await super.awaitSettlementEncoded(Schema.encodeSync(Receipt)(submitted.receipt)),
          );

          if (settled._tag !== "SettlementReached" || settled.settlement.outcome !== "completed")
            // oxlint-disable-next-line effect/no-untyped-throw -- Native stream callback reports this typed failure on its wire.
            throw CaptureError.make({ message: "Turn did not complete" });
          dispose();
          await observed;
          emit("done", { outcome: "completed", calls: this.state.calls - startCalls });
        } catch (cause) {
          emit("error", String(cause));
        } finally {
          dispose();
          await observed;
          if (!cancelled) controller.close();
        }
      },
      async cancel() {
        cancelled = true;
        try {
          await reader.cancel();
        } finally {
          dispose();
        }
      },
    });
  }
}
