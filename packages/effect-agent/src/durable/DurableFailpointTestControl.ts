/**
 * Scoped test control for replacing and clearing the durable coordinator's active failpoint
 * handler.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointHandler,
} from "./DurableFailpoint.ts";

const noFailpoint: DurableRuntimeFailpointHandler = () => Effect.void;

/**
 * Test-only control for replacing the active coordinator failpoint handler.
 *
 * @category services
 * @since 0.1.0
 */
export class DurableRuntimeFailpointTestControl extends Context.Service<
  DurableRuntimeFailpointTestControl,
  {
    readonly clear: Effect.Effect<void>;
    readonly setHandler: (handler: DurableRuntimeFailpointHandler) => Effect.Effect<void>;
  }
>()("@effect-agent/thread/DurableRuntimeFailpointTestControl") {
  /** Reusable test Layer with a control service backed by the same handler Ref. */
  static readonly layer = Layer.effectContext(
    Effect.gen(function* () {
      const handler = yield* Ref.make<DurableRuntimeFailpointHandler>(noFailpoint);

      return Context.make(
        DurableRuntimeFailpoint,
        DurableRuntimeFailpoint.of({
          hit: (location) => Ref.get(handler).pipe(Effect.flatMap((current) => current(location))),
        }),
      ).pipe(
        Context.add(
          DurableRuntimeFailpointTestControl,
          DurableRuntimeFailpointTestControl.of({
            clear: Ref.set(handler, noFailpoint),
            setHandler: (next) => Ref.set(handler, next),
          }),
        ),
      );
    }),
  );
}
