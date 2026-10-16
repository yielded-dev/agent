/** One deterministic Chat Completions stream. No model, tools, or artificial delay. */
export default {
  async fetch(request: Request, env: { TOKEN: string }): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`)
      return new Response(null, { status: 401 });
    if (new URL(request.url).pathname === "/health") return new Response("ready");
    await request.arrayBuffer();

    const chunk = (delta: object, finish: string | null) =>
      `data: ${JSON.stringify({
        id: "scripted",
        object: "chat.completion.chunk",
        created: 0,
        model: "scripted-1",
        choices: [{ index: 0, delta, finish_reason: finish }],
      })}\n\n`;

    return new Response(
      chunk({ role: "assistant", content: "hello" }, null) + chunk({}, "stop") + "data: [DONE]\n\n",
      { headers: { "content-type": "text/event-stream", "cache-control": "no-store" } },
    );
  },
};
