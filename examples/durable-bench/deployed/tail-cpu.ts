import { Effect } from "effect";

import { request } from "./cloudflare.ts";
import { type Invocation } from "./model.ts";
import { TailPage } from "./tail.ts";

export const tailCpu = Effect.fnUntraced(function* (
  endpoint: string,
  token: string,
  object: string,
  target: string,
) {
  const invocations: Invocation[] = [];
  let cursor: string | undefined;

  do {
    const url = new URL(endpoint);

    url.searchParams.set("object", object);
    url.searchParams.set("target", target);
    if (cursor !== undefined) url.searchParams.set("cursor", cursor);
    const page = (yield* request(url, token, TailPage)).value;

    invocations.push(
      ...page.records.map((row) => ({ ...row.record, invocationId: row.invocation })),
    );
    cursor = page.cursor;
  } while (cursor !== undefined);

  return invocations;
});
