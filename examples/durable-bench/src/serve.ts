import type { Stats, Turn } from "./plan.ts";

export interface Bench {
  wake(): Promise<void>;
  turn(input: Turn): Promise<void>;
  seed(turns: readonly Turn[]): Promise<string>;
  stats(): Promise<Stats>;
}

export const tables = (sql: SqlStorage): Record<string, number> => {
  const names = sql
    .exec<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'",
    )
    .toArray();

  return Object.fromEntries(
    names.map(({ name }) => [
      name,
      sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM "${name}"`).one().n,
    ]),
  );
};

export const serve = <Env>(bench: (env: Env) => Bench, setup?: (env: Env) => Promise<void>) => ({
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const target = bench(env);
      const body = request.method === "POST" ? await request.json() : undefined;
      const path = new URL(request.url).pathname;

      if (path === "/setup") return Response.json((await setup?.(env)) ?? null);
      if (path === "/wake") return Response.json((await target.wake()) ?? null);
      if (path === "/turn") return Response.json((await target.turn(body as Turn)) ?? null);
      if (path === "/seed") return Response.json(await target.seed(body as readonly Turn[]));
      if (path === "/stats") return Response.json(await target.stats());

      return new Response("not found", { status: 404 });
    } catch (error) {
      const message = error instanceof Error ? (error.stack ?? error.message) : String(error);

      console.error(message);

      return new Response(message, { status: 500 });
    }
  },
});
