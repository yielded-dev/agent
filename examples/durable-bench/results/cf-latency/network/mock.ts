import { fingerprint, next } from "../../../src/plan.ts";
import { chatTranscript, decodeChat, errorText, readQuery, type Env, type ProviderReceipt } from "./protocol.ts";

const encoder = new TextEncoder();
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

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
      const chat = decodeChat(JSON.parse(body));

      if (!chat.stream) throw new Error("This benchmark requires stream:true");
      const transcript = chatTranscript(chat);
      const user = transcript.findLast((message) => message.role === "user");

      if (!user || !/^turn .+ tools=8$/.test(user.text))
        throw new Error("Measured provider requires the exact eight-lookup turn");
      const step = next(transcript);
      const id = `chatcmpl-cf-latency-${crypto.randomUUID()}`;
      const created = Math.floor(arrivalMs / 1000);

      const envelope = (delta: object, finish: string | null = null) => ({
        id,
        object: "chat.completion.chunk",
        created,
        model: chat.model,
        choices: [{ index: 0, delta, finish_reason: finish }],
      });

      // A role-only header carries no extra model-visible text message. Effect's
      // native adapter preserves content:"" as a separate empty assistant message,
      // which would diverge from the durable-bench transcript after the first call.
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
        for (const fragment of [args.slice(0, 5), args.slice(5)])
          chunks.push(envelope({ tool_calls: [{ index: 0, function: { arguments: fragment } }] }));
        chunks.push(envelope({}, "tool_calls"));
      } else {
        for (const fragment of ["done after ", "8 lookups"])
          chunks.push(envelope({ content: fragment }));
        chunks.push(envelope({}, "stop"));
      }
      chunks.push({
        id,
        object: "chat.completion.chunk",
        created,
        model: chat.model,
        choices: [],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      });

      const frames = [
        ...chunks.map((chunk) => encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`)),
        encoder.encode("data: [DONE]\n\n"),
      ];

      // Hashing is now included in provider time, since the final stream frame
      // returns a reliable receipt. No framework transcript or adapter changes.
      const computeHashes = () => Promise.all([
        fingerprint(transcript),
        crypto.subtle.digest("SHA-256", bodyBytes).then((digest) =>
          Array.from(new Uint8Array(digest).slice(0, 8),
            (byte) => byte.toString(16).padStart(2, "0")).join("")),
      ]);
      let hashes: ReturnType<typeof computeHashes> | undefined;

      let firstByteMs: number | null = null;
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
            if (index === frames.length) {
              const [fingerprint, rawWireFingerprint] = await (hashes ??= computeHashes());
              endMs = Date.now();
              receipt = {
                ...query, call: Number(url.searchParams.get("call")), requestId: id,
                arrivalMs, firstByteMs, endMs, fingerprint, rawWireFingerprint,
                requestBytes: bodyBytes.byteLength,
                colo: typeof request.cf?.colo === "string" ? request.cf.colo : null,
                error: null,
              };
              // Same final frame and delay as before. Comments are ignored by
              // all three native SSE parsers, and precede their [DONE] disposal.
              frame = encoder.encode(`: cf-latency-receipt ${JSON.stringify(receipt)}\n\ndata: [DONE]\n\n`);
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
              cfLatency: "provider",
              ...query,
              call: url.searchParams.get("call"),
              requestId: id,
              sampleHeader: request.headers.get("x-cf-latency-sample"),
              arrivalMs,
              firstByteMs,
              endMs,
              fingerprint: receipt?.fingerprint ?? (await (hashes ??= computeHashes()))[0],
              rawWireFingerprint: receipt?.rawWireFingerprint ?? (await (hashes ??= computeHashes()))[1],
              step,
              bytes,
              requestBytes: bodyBytes.byteLength,
              colo: request.cf?.colo ?? null,
              error: failure,
            });
          })
          .catch((cause) =>
            console.error({ cfLatency: "provider-log-failure", sample, error: errorText(cause) }),
          ),
      );

      return new Response(stream, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-store",
          "x-cf-latency-request": id,
          "x-cf-latency-arrival": String(arrivalMs),
        },
      });
    } catch (cause) {
      const error = errorText(cause);

      console.error({ cfLatency: "provider-failure", sample, arrivalMs, endMs: Date.now(), error });

      return Response.json({ error }, { status: 400 });
    }
  },
};
