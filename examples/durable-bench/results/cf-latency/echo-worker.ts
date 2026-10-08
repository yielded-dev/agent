type Env = { TOKEN: string };

export default {
  fetch(request: Request, env: Env): Response {
    if (request.headers.get("authorization") !== `Bearer ${env.TOKEN}`) {
      return new Response("unauthorized", { status: 401 });
    }
    const arrivalMs = Date.now();
    const sample = new URL(request.url).searchParams.get("sample");
    return Response.json(
      { arrivalMs, sample, colo: request.cf?.colo ?? null },
      { headers: { "cache-control": "no-store" } },
    );
  },
};
