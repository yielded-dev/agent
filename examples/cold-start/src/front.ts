import { identity } from "./identity.ts";
import { encode, type Env } from "./protocol.ts";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const worker = identity(env.VERSION.id);

    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response(null, { status: 401 });
    const url = new URL(request.url);

    // ID lookup is used only by the separate profiling command; it never visits the Object.
    if (url.pathname === "/id")
      return Response.json({ actorId: env.THREAD.idFromName("thread").toString() });
    const stub = env.THREAD.getByName("thread", { locationHint: env.LOCATION_HINT });

    if (url.pathname === "/touch") return Response.json(await stub.touch());
    if (url.pathname !== "/capture") return new Response(null, { status: 404 });
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;

    return new Response(
      new ReadableStream<Uint8Array>({
        async start(controller) {
          controller.enqueue(encode("worker", worker));
          try {
            reader = (await stub.probe(url.searchParams.get("sample") ?? "first")).getReader();
            for (;;) {
              const next = await reader.read();

              if (next.done) break;
              controller.enqueue(next.value);
            }
          } catch (cause) {
            controller.enqueue(encode("error", String(cause)));
          } finally {
            reader?.releaseLock();
            controller.close();
          }
        },
        async cancel() {
          await reader?.cancel();
        },
      }),
      { headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" } },
    );
  },
};
