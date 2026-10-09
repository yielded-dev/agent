import { Schema } from "effect";

import { fingerprint, next } from "../../../src/plan.ts";
import {
  chatTranscript,
  decodeChat,
  errorText,
  readQuery,
  type Env,
  type ProviderReceipt,
} from "./protocol.ts";
import { textFragments } from "./text.ts";

const encoder = new TextEncoder();
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const canonical = (value: unknown): string =>
  JSON.stringify(value, (_key, item) =>
    item !== null && typeof item === "object" && !Array.isArray(item)
      ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)))
      : item,
  );
const digest = async (value: string) =>
  Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(value))), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");

export default {
  async fetch(
    request: Request,
    env: Pick<Env, "TOKEN">,
    context: ExecutionContext,
  ): Promise<Response> {
    const arrivalMs = Date.now();

    if (!env.TOKEN || request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    const sample = url.searchParams.get("sample");

    if (url.pathname === "/echo" && request.method === "GET") {
      return Response.json(
        { arrivalMs, colo: request.cf?.colo ?? null, sample },
        { headers: { "cache-control": "no-store" } },
      );
    }
    if (url.pathname !== "/v1/chat/completions" || request.method !== "POST")
      return new Response("not found", { status: 404 });
    try {
      const query = readQuery(url);
      const body = await request.text();
      const bodyBytes = encoder.encode(body);
      const raw = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(
        JSON.parse(body),
      );
      const chat = decodeChat(raw);

      if (!chat.stream) throw new Error("This benchmark requires stream:true");
      const transcript = chatTranscript(chat);
      const user = transcript.findLast((message) => message.role === "user");

      if (!user || !/^turn .+ tools=8$/.test(user.text))
        throw new Error("Measured provider requires the exact eight-lookup turn");
      const step = next(transcript);
      const id = `chatcmpl-first-text-${crypto.randomUUID()}`;
      const created = Math.floor(arrivalMs / 1000);

      const envelope = (delta: object, finish: string | null = null) => ({
        id,
        object: "chat.completion.chunk",
        created,
        model: chat.model,
        choices: [{ index: 0, delta, finish_reason: finish }],
      });

      // The first frame contains text; a role-only header is not a first text token.
      const fragments = textFragments(step).map((fragment) =>
        query.sample.startsWith("proof-wide-") ? fragment.padEnd(4096, ".") : fragment,
      );
      const chunks: object[] = fragments.map((content, index) =>
        envelope({ ...(index === 0 ? { role: "assistant" } : {}), content }),
      );

      if ("call" in step) {
        const args = JSON.stringify({ n: step.call });

        chunks.push(
          envelope({
            tool_calls: [
              {
                index: 0,
                id: `call-${step.call}`,
                type: "function",
                function: { name: "lookup", arguments: "" },
              },
            ],
          }),
        );
        for (const fragment of [args.slice(0, 5), args.slice(5)])
          chunks.push(envelope({ tool_calls: [{ index: 0, function: { arguments: fragment } }] }));
        chunks.push(envelope({}, "tool_calls"));
      } else {
        chunks.push(envelope({}, "stop"));
      }
      chunks.push({
        id,
        object: "chat.completion.chunk",
        created,
        model: chat.model,
        choices: [],
        usage: {
          prompt_tokens: 1,
          completion_tokens: fragments.length + ("call" in step ? 3 : 0),
          total_tokens: 1 + fragments.length + ("call" in step ? 3 : 0),
        },
      });

      const frames = [
        ...chunks.map((chunk) => encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`)),
        encoder.encode("data: [DONE]\n\n"),
      ];

      // The terminal frame waits for hashing. CPU spent hashing need not appear
      // in the provider's I/O-clock interval, especially with no timer delays.
      const computeHashes = () =>
        Promise.all([
          fingerprint(transcript),
          crypto.subtle
            .digest("SHA-256", bodyBytes)
            .then((digest) =>
              Array.from(new Uint8Array(digest).slice(0, 8), (byte) =>
                byte.toString(16).padStart(2, "0"),
              ).join(""),
            ),
          digest(
            canonical({
              model: raw.model,
              messages: raw.messages,
              tools: raw.tools,
              tool_choice: raw.tool_choice,
              max_tokens: raw.max_tokens,
              max_completion_tokens: raw.max_completion_tokens,
              temperature: raw.temperature,
              top_p: raw.top_p,
            }),
          ),
        ]);

      let hashes: ReturnType<typeof computeHashes> | undefined;

      let firstByteMs: number | null = null;
      let firstTextMs: number | null = null;
      let lastTextMs: number | null = null;
      let lastTokenMs: number | null = null;
      let endMs: number | null = null;
      let failure: string | null = null;
      let bytes = 0;
      let index = 0;
      let cancelled = false;
      let receipt: ProviderReceipt | undefined;
      let finish!: () => void;

      const completed = new Promise<void>((resolve) => {
        finish = resolve;
      });

      const stream = new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            const delay =
              index === 0
                ? Math.max(0, query.ttftMs - (Date.now() - arrivalMs))
                : query.chunkDelayMs;

            if (delay > 0) await sleep(delay);
            if (cancelled) return;
            let frame = frames[index++];

            if (!frame) throw new Error("SSE frame exhausted before close");
            firstByteMs ??= Date.now();
            if (index <= fragments.length) {
              firstTextMs ??= Date.now();
              lastTextMs = Date.now();
            }
            // All content / argument fragments precede finish, usage, and [DONE].
            if (index <= frames.length - 3) lastTokenMs = Date.now();
            if (index === frames.length) {
              const [fingerprint, rawWireFingerprint, modelVisibleFingerprint] = await (hashes ??=
                computeHashes());

              endMs = Date.now();
              receipt = {
                ...query,
                call: Number(url.searchParams.get("call")),
                requestId: id,
                arrivalMs,
                firstByteMs,
                firstTextMs: firstTextMs!,
                lastTextMs: lastTextMs!,
                lastTokenMs: lastTokenMs!,
                endMs,
                fingerprint,
                rawWireFingerprint,
                framing: JSON.stringify(
                  chat.messages.filter(
                    (message) => message.role === "system" || message.role === "developer",
                  ),
                ),
                tools: JSON.stringify(raw.tools),
                modelVisibleFingerprint,
                messageShape: chat.messages.map((message) => message.role[0]).join(""),
                messageTail: JSON.stringify(chat.messages.slice(-4)),
                requestBytes: bodyBytes.byteLength,
                colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
                error: null,
              };
              // Same final frame and delay as before. Comments are ignored by
              // all three native SSE parsers, and precede their [DONE] disposal.
              frame = encoder.encode(
                `: first-text-receipt ${JSON.stringify(receipt)}\n\ndata: [DONE]\n\n`,
              );
            }
            controller.enqueue(frame);
            bytes += frame.byteLength;
            hashes ??= computeHashes();
            if (index === frames.length) {
              controller.close();
              finish();
            }
          } catch (cause) {
            failure = errorText(cause);
            endMs = Date.now();
            controller.error(cause);
            finish();
          }
        },
        cancel(reason) {
          cancelled = true;
          failure = `cancelled: ${String(reason)}`;
          endMs = Date.now();
          finish();
        },
      });

      context.waitUntil(
        completed
          .then(async () => {
            console.log({
              firstText: "provider",
              ...query,
              call: url.searchParams.get("call"),
              requestId: id,
              sampleHeader: request.headers.get("x-first-text-sample"),
              arrivalMs,
              firstByteMs,
              endMs,
              fingerprint: receipt?.fingerprint ?? (await (hashes ??= computeHashes()))[0],
              rawWireFingerprint:
                receipt?.rawWireFingerprint ?? (await (hashes ??= computeHashes()))[1],
              step,
              bytes,
              requestBytes: bodyBytes.byteLength,
              colo: request.cf?.colo ?? null,
              error: failure,
            });
          })
          .catch((cause) =>
            console.error({ firstText: "provider-log-failure", sample, error: errorText(cause) }),
          ),
      );

      return new Response(stream, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-store",
          "x-first-text-request": id,
          "x-first-text-arrival": String(arrivalMs),
        },
      });
    } catch (cause) {
      const error = errorText(cause);

      console.error({ firstText: "provider-failure", sample, arrivalMs, endMs: Date.now(), error });

      return Response.json({ error }, { status: 400 });
    }
  },
};
