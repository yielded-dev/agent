import { Context, Effect, Exit, Schema, Semaphore, Stream } from "effect";

/** Fixed, synchronous workloads. Only deployed invocation telemetry times these. */
export const cases = [
  "empty", "sync", "sync-reused", "gen8", "map8", "flatMap8", "errors-success",
  "fn-untraced", "fn-traced", "service8", "stream8", "schema-decode",
  "schema-encode", "sql", "scope", "semaphore", "interrupt-mask", "span",
  "failpoint", "allocate",
] as const;

export type Case = (typeof cases)[number];
export type Sql = { exec(query: string, value: number): { one(): { n: number } } };

class Service extends Context.Service<Service, { value: number }>()("effect-eval-cost/Service") {}
class Problem extends Schema.TaggedError<Problem>()("Problem", { value: Schema.Number }) {}

const Record = Schema.Struct({
  id: Schema.String,
  revision: Schema.Int,
  content: Schema.Array(Schema.Union([
    Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
    Schema.Struct({ type: Schema.Literal("tool"), name: Schema.String, value: Schema.Int }),
  ])),
});
const decode = Schema.decodeEffect(Record);
const encode = Schema.encodeEffect(Record);
const decodeSync = Schema.decodeSync(Record);
const encodeSync = Schema.encodeSync(Record);
const record = Record.make({ id: "sample", revision: 7, content: [
  { type: "text", text: "small canonical record" },
  { type: "tool", name: "lookup", value: 42 },
] });
const values = [1, 2, 3, 4, 5, 6, 7, 8];
const service = { value: 3 };
const add = (n: number) => (n + 1) & 255;
const helper = (n: number) => Effect.sync(() => add(n));
const untraced = Effect.fnUntraced(function* (n: number) { return yield* helper(n); });
const traced = Effect.fn("effect-eval-cost.helper")(function* (n: number) { return yield* helper(n); });
const eight = Effect.fnUntraced(function* (n: number) {
  for (let j = 0; j < 8; j++) n = yield* helper(n);
  return n;
});
const lookup = Effect.fnUntraced(function* (n: number) {
  for (let j = 0; j < 8; j++) n = (n + (yield* Service).value) & 255;
  return n;
});
const permit = Semaphore.makeUnsafe(1);
const noFailpoint = () => Effect.void;

/** The result is consumed and compared, preventing dead-work elimination. */
export const runCase = (name: Case, mode: "effect" | "plain", n: number, sql: Sql): number => {
  let sum = 0;
  if (name === "empty") {
    for (let i = 0; i < n; i++) sum = (sum + (i & 255)) | 0;
    return sum;
  }
  if (name === "allocate") {
    const ring: Array<{ value: number }> = Array.from({ length: 4096 }, () => ({ value: 0 }));
    for (let i = 0; i < n; i++) {
      const index = i & 4095;
      sum = (sum + ring[index]!.value) | 0;
      // Exit Success objects are deliberately allocated but never interpreted.
      if (mode === "effect") {
        const allocated = Exit.succeed(i);
        if (Exit.isSuccess(allocated)) ring[index] = allocated;
      } else ring[index] = { value: i };
    }
    for (const value of ring) sum = (sum + value.value) | 0;
    return sum;
  }
  if (mode === "plain") {
    for (let i = 0; i < n; i++) {
      let value = i & 255;
      switch (name) {
        case "gen8": case "map8": case "flatMap8":
          for (let j = 0; j < 8; j++) value = add(value);
          break;
        case "service8":
          for (let j = 0; j < 8; j++) value = (value + service.value) & 255;
          break;
        case "stream8":
          value = 0;
          for (const item of values) value += item;
          break;
        case "schema-decode": value = decodeSync(record).revision; break;
        case "schema-encode": value = encodeSync(record).revision; break;
        case "sql": value = sql.exec("SELECT ? AS n", value).one().n; break;
        case "scope": {
          try { value = add(value); } finally { sum = (sum + 1) | 0; }
          break;
        }
        case "failpoint": break;
        default: value = add(value);
      }
      sum = (sum + value) | 0;
    }
    return sum;
  }
  return Effect.runSync(Effect.gen(function* () {
    let current = 0;
    const reused = Effect.sync(() => add(current));
    for (let i = 0; i < n; i++) {
      current = i & 255;
      let value = current;
      switch (name) {
        case "sync": value = yield* helper(value); break;
        case "sync-reused": value = yield* reused; break;
        case "gen8": value = yield* eight(value); break;
        case "map8": {
          let program = Effect.succeed(value);
          for (let j = 0; j < 8; j++) program = Effect.map(program, add);
          value = yield* program;
          break;
        }
        case "flatMap8": {
          let program = Effect.succeed(value);
          for (let j = 0; j < 8; j++) program = Effect.flatMap(program, (x) => Effect.succeed(add(x)));
          value = yield* program;
          break;
        }
        case "errors-success":
          value = yield* helper(value).pipe(
            Effect.mapError(() => new Problem({ value })),
            Effect.catchTag("Problem", (error) => Effect.succeed(error.value)),
          );
          break;
        case "fn-untraced": value = yield* untraced(value); break;
        case "fn-traced": value = yield* traced(value); break;
        case "service8": value = yield* lookup(value); break;
        case "stream8": {
          value = 0;
          yield* Stream.runForEach(Stream.fromIterable(values), (item) => Effect.sync(() => { value += item; }));
          break;
        }
        case "schema-decode": value = (yield* decode(record)).revision; break;
        case "schema-encode": value = (yield* encode(record)).revision; break;
        case "sql": value = yield* Effect.sync(() => sql.exec("SELECT ? AS n", current).one().n); break;
        case "scope":
          value = yield* Effect.scoped(Effect.acquireRelease(helper(value), () => Effect.sync(() => { sum = (sum + 1) | 0; })));
          break;
        case "semaphore": value = yield* permit.withPermits(1)(helper(value)); break;
        case "interrupt-mask": value = yield* Effect.uninterruptible(helper(value)); break;
        case "span": value = yield* Effect.withSpan(helper(value), "effect-eval-cost.span"); break;
        case "failpoint": yield* noFailpoint(); break;
      }
      sum = (sum + value) | 0;
    }
    return sum;
  }).pipe(Effect.provideService(Service, service)));
};
