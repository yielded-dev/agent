import { Rpc } from "effect-cf";
import * as Schema from "effect/Schema";

/** The RPC lifetime survives an idle byte stream and is disposed on caller disconnect. */
export const Observation = Schema.Struct({
  stream: Schema.instanceOf(ReadableStream<Uint8Array>),
  lifetime: Schema.declare(Rpc.isDisposable),
});
