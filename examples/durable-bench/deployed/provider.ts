import { Schema } from "effect";

import { fingerprint, history, MEASURED_TOOLS, next, turn } from "../src/plan.ts";
import { chatTranscript, decodeChat, readQuery, type ProviderReceipt } from "./worker/protocol.ts";

const encoder = new TextEncoder();
const sleep = (ms: number) => new Promise<void>((done) => setTimeout(done, ms));
const Header = Schema.Struct({ model: Schema.String, stream: Schema.Literal(true) });

export default {
  async fetch(
    request: Request,
    env: { BENCH_TOKEN: string; BUILD: string },
    context: ExecutionContext,
  ): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);

    if (url.pathname === "/health") return Response.json({ ok: true, build: env.BUILD });
    if (url.pathname !== "/v1/chat/completions" || request.method !== "POST")
      return new Response("not found", { status: 404 });
    const arrivalMs = Date.now();

    try {
      const query = readQuery(url);
      const raw = await request.text();
      const json: unknown = JSON.parse(raw);
      const { model } = Schema.decodeUnknownSync(Header)(json);
      const messages = chatTranscript(decodeChat(json));

      const seed = /^h(\d+)$/.exec(query.sample);
      const seedIndex = Number(seed?.[1]);

      if (
        seed &&
        (query.target !== "yielded" ||
          seedIndex >= query.history ||
          query.ttftMs !== 0 ||
          query.chunkDelayMs !== 0)
      )
        throw new Error("Seed replay requires a historical turn and the instant provider.");

      const expected = seed
        ? history(seedIndex, seedIndex + 1)[0]?.text
        : turn(query.sample, MEASURED_TOOLS).text;

      if (messages.findLast((m) => m.role === "user")?.text !== expected)
        throw new Error("Unexpected benchmark workload.");
      const step = next(messages);
      const id = `chatcmpl-durable-bench-${crypto.randomUUID()}`;

      const envelope = (delta: object, finish: string | null = null) => ({
        id,
        object: "chat.completion.chunk",
        created: Math.floor(arrivalMs / 1000),
        model,
        choices: [{ index: 0, delta, finish_reason: finish }],
      });

      const chunks: object[] = [envelope({ role: "assistant" })];

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
        for (const part of [args.slice(0, 5), args.slice(5)])
          chunks.push(envelope({ tool_calls: [{ index: 0, function: { arguments: part } }] }));
        chunks.push(envelope({}, "tool_calls"));
      } else {
        chunks.push(
          envelope({ content: "done after " }),
          envelope({ content: step.answer.slice("done after ".length) }),
          envelope({}, "stop"),
        );
      }
      chunks.push({
        id,
        object: "chat.completion.chunk",
        created: Math.floor(arrivalMs / 1000),
        model,
        choices: [],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      });
      const wire = encoder.encode(raw);

      const computeHashes = () =>
        Promise.all([fingerprint(messages), crypto.subtle.digest("SHA-256", wire)]);

      let hashes: ReturnType<typeof computeHashes> | undefined;
      let index = 0;
      let firstByteMs = 0;
      let cancelled = false;
      let finish = () => {};

      const completed = new Promise<void>((resolve) => {
        finish = resolve;
      });

      context.waitUntil(completed);

      const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            const delay =
              index === 0
                ? Math.max(0, query.ttftMs - (Date.now() - arrivalMs))
                : query.chunkDelayMs;

            if (delay) await sleep(delay);
            if (cancelled) return;
            firstByteMs ||= Date.now();
            if (index < chunks.length) {
              controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunks[index++])}\n\n`));
              hashes ??= computeHashes();

              return;
            }
            const [digest, rawHash] = await (hashes ??= computeHashes());

            const receipt: ProviderReceipt = {
              ...query,
              call: Number(url.searchParams.get("call")),
              requestId: id,
              arrivalMs,
              firstByteMs,
              endMs: Date.now(),
              fingerprint: digest,
              rawWireFingerprint: Array.from(new Uint8Array(rawHash).slice(0, 8), (b) =>
                b.toString(16).padStart(2, "0"),
              ).join(""),
              requestBytes: wire.length,
              colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
              error: null,
            };

            controller.enqueue(
              encoder.encode(`: rebench-receipt ${JSON.stringify(receipt)}\n\ndata: [DONE]\n\n`),
            );
            controller.close();
            finish();
          } catch (error) {
            controller.error(error);
            finish();
          }
        },
        cancel() {
          cancelled = true;
          finish();
        },
      });

      return new Response(body, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-store",
          "x-rebench-request": id,
        },
      });
    } catch {
      return Response.json({ error: "Invalid benchmark model request." }, { status: 400 });
    }
  },
};
