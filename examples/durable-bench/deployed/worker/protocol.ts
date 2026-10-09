import { Receipt } from "@yielded/agent/receipt";
import { Settlement } from "@yielded/agent/submission-ledger";
import * as Schema from "effect/Schema";

// Keep receipt schemas on the Yielded edge; shared Worker helpers use the neutral protocol.
export * from "../isolate/protocol.ts";

export const SubmitResult = Schema.Struct({
  ok: Schema.Literal(true),
  receipt: Schema.toEncoded(Receipt),
});

export type SubmitResult = typeof SubmitResult.Type;

export const AwaitResult = Schema.Struct({
  ok: Schema.Literal(true),
  settlement: Schema.toEncoded(Settlement),
});

export type AwaitResult = typeof AwaitResult.Type;
