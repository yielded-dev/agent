import { DurableObject } from "cloudflare:workers";

type ObjectConstructor = new (ctx: globalThis.DurableObjectState, env: never) => object;
type MethodKeys<T> = {
  [K in keyof T]-?: T[K] extends (...args: never[]) => unknown ? K : never;
}[keyof T] &
  string;
type AsyncMethods<T, K extends keyof T> = {
  [P in K]: T[P] extends (...args: infer A) => infer R
    ? (...args: A) => Promise<Awaited<R>>
    : never;
};
interface NativeHandlers {
  fetch(request: Request): Promise<Response>;
  alarm(info?: AlarmInvocationInfo): Promise<void>;
  webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void>;
  webSocketClose(socket: WebSocket, code: number, reason: string, clean: boolean): Promise<void>;
  webSocketError(socket: WebSocket, error: unknown): Promise<void>;
}
const delegates = new WeakMap<object, Promise<object>>();

const reserved = new Set([
  "constructor",
  "prototype",
  "__proto__",
  "then",
  "fetch",
  "alarm",
  "webSocketMessage",
  "webSocketClose",
  "webSocketError",
]);

const invoke = async (self: object, method: PropertyKey, args: unknown[]): Promise<unknown> => {
  const delegate = await delegates.get(self);

  if (!delegate) throw new TypeError("Lazy Object delegate is unavailable");
  const operation = Reflect.get(delegate, method, delegate);

  if (typeof operation !== "function")
    throw new TypeError(`Object has no ${String(method)} method`);

  return Reflect.apply(operation, delegate, args);
};

/**
 * Load an implementation on first Object construction behind a native facade.
 * The implementation receives the original state and environment and retains
 * ownership of its runtime, storage, constructor gates and resource scopes.
 *
 * Import this module directly from the Worker entry. Emit the implementation as
 * a separate uploaded module, with its dependencies outside the eager entry graph.
 * Keep loading and construction local and bounded inside blockConcurrencyWhile.
 * RPC names are an explicit prototype whitelist. Native fetch, alarm and
 * hibernating WebSocket hooks are forwarded without changing their arguments.
 * The facade does not manage a returned Response body's lifetime or cancellation.
 */
export function lazyObject<
  C extends ObjectConstructor,
  const Keys extends readonly MethodKeys<InstanceType<C>>[],
>(
  load: () => Promise<C>,
  rpcNames: Keys,
): new (
  ctx: globalThis.DurableObjectState,
  env: ConstructorParameters<C>[1],
) => NativeHandlers &
  DurableObject<ConstructorParameters<C>[1]> &
  AsyncMethods<InstanceType<C>, Keys[number]> {
  class LazyObject extends DurableObject<ConstructorParameters<C>[1]> {
    constructor(ctx: globalThis.DurableObjectState, env: ConstructorParameters<C>[1]) {
      super(ctx, env);

      const ready = ctx.blockConcurrencyWhile(async () => {
        const Implementation = await load();

        return new Implementation(ctx, env);
      });

      delegates.set(this, ready);
      ctx.waitUntil(ready);
    }
    fetch(request: Request): Promise<Response> {
      return invoke(this, "fetch", [request]) as Promise<Response>;
    }
    async alarm(...args: [info?: AlarmInvocationInfo]): Promise<void> {
      await invoke(this, "alarm", args);
    }
    async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
      await invoke(this, "webSocketMessage", [socket, message]);
    }
    async webSocketClose(
      socket: WebSocket,
      code: number,
      reason: string,
      clean: boolean,
    ): Promise<void> {
      await invoke(this, "webSocketClose", [socket, code, reason, clean]);
    }
    async webSocketError(socket: WebSocket, error: unknown): Promise<void> {
      await invoke(this, "webSocketError", [socket, error]);
    }
  }
  for (const name of rpcNames) {
    if (reserved.has(name) || Object.hasOwn(LazyObject.prototype, name))
      throw new TypeError("Invalid or duplicate RPC name: " + name);
    Object.defineProperty(LazyObject.prototype, name, {
      value: function (this: LazyObject, ...args: unknown[]) {
        return invoke(this, name, args);
      },
      writable: true,
      configurable: true,
    });
  }

  // The exact listed names are installed above; this is native dispatch, not a schema boundary.
  return LazyObject as new (
    ctx: globalThis.DurableObjectState,
    env: ConstructorParameters<C>[1],
  ) => NativeHandlers &
    DurableObject<ConstructorParameters<C>[1]> &
    AsyncMethods<InstanceType<C>, Keys[number]>;
}
