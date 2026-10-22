/**
 * Explicit fault-injection authority for Durable Object storage boundaries.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { DoStorageFailpointError, DoStorageFailpointLocation } from "./DoStorageError.ts";

/**
 * Callback invoked at an explicit Durable Object storage fault boundary.
 *
 * @category utility types
 * @since 0.1.0
 */
export type DoStorageFailpointHandler = (
  location: DoStorageFailpointLocation,
) => Effect.Effect<void, DoStorageFailpointError>;

const noFailpoint: DoStorageFailpointHandler = () => Effect.void;

/** @internal Select a plain native body only for the library's no-op handler. */
export const isDefaultDoStorageFailpoint = (handler: DoStorageFailpointHandler): boolean =>
  handler === noFailpoint;

/**
 * Explicit fault-injection authority used at Durable Object storage operation boundaries.
 *
 * @category services
 * @since 0.1.0
 */
export class DoStorageFailpoint extends Context.Service<
  DoStorageFailpoint,
  {
    readonly hit: DoStorageFailpointHandler;
  }
>()("@effect-agent/storage-cloudflare/DoStorageFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });
}
