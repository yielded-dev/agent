import { Effect, Option, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { BenchError } from "./platform.ts";
import { run, teardown } from "./run.ts";
import { History, Target } from "./worker/protocol.ts";

const positive = (name: string) =>
  Flag.Int(name).pipe(Flag.withSchema(Schema.Int.check(Schema.isGreaterThan(0))));

export const command = Command.make(
  "deployed",
  {
    targets: Flag.String("targets").pipe(
      Flag.optional,
      Flag.withDescription("Comma-separated targets (isolated: yielded,pi; otherwise all three)"),
    ),
    sizes: Flag.String("sizes").pipe(
      Flag.withDefault("50,250"),
      Flag.withDescription("Comma-separated seeded history lengths"),
    ),
    ttft: Flag.String("ttft").pipe(
      Flag.withDefault("0,400"),
      Flag.withDescription("Provider time to first token, in ms: 0,400"),
    ),
    objects: positive("objects").pipe(
      Flag.withDefault(7),
      Flag.withDescription("Objects per target/cell (default: 7)"),
    ),
    repeats: positive("repeats").pipe(
      Flag.optional,
      Flag.withDescription("Warm turns per Object (quick: 4; rigorous: 6 per build pass)"),
    ),
    concurrency: positive("concurrency").pipe(
      Flag.withDefault(6),
      Flag.withDescription("Concurrent Objects (default: 6); turns stay sequential within each"),
    ),
    cold: Flag.Boolean("cold").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Include the first turn after Object abort, before warmup"),
    ),
    cpu: Flag.Boolean("cpu").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Enable invocation logs and query Cloudflare CPU telemetry"),
    ),
    keep: Flag.Boolean("keep").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Retain this run's target stack until --teardown"),
    ),
    rigorous: Flag.Boolean("rigorous").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Cold + CPU + same-Object balanced A/B; requires both refs"),
    ),
    isolate: Flag.Boolean("isolate").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Separate Yielded/pi Workers; reset old code before each fresh upload"),
    ),
    coldMode: Flag.Literals("cold-mode", ["object", "fresh"]).pipe(Flag.withDefault("fresh")),
    order: Flag.Literals("order", ["ABBA", "BAAB", "AABB"]).pipe(
      Flag.optional,
      Flag.withDescription("Isolated rigorous pass order (default: randomly choose ABBA or BAAB)"),
    ),
    baseline: Flag.String("baseline").pipe(Flag.optional),
    candidate: Flag.String("candidate").pipe(Flag.optional),
    storageProbe: Flag.Literals("storage-probe", ["none", "untouched", "touched"]).pipe(
      Flag.withDefault("none"),
    ),
    paddingMiB: positive("padding-mib").pipe(Flag.withDefault(32)),
    teardown: Flag.Boolean("teardown").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Destroy all owned target stacks and shared infrastructure, then verify the account",
      ),
    ),
  },
  Effect.fnUntraced(function* (flags) {
    if (flags.teardown) return yield* teardown;
    const baseline = Option.getOrUndefined(flags.baseline);
    const candidate = Option.getOrUndefined(flags.candidate);
    const order = Option.getOrUndefined(flags.order);

    if (order !== undefined && !(flags.isolate && flags.rigorous))
      return yield* new BenchError({ message: "--order requires --isolate --rigorous." });

    if (
      flags.rigorous ? !baseline || !candidate : baseline !== undefined || candidate !== undefined
    )
      return yield* new BenchError({
        message: "Use --rigorous with both --baseline <ref> and --candidate <ref>.",
      });

    const targets = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Target))(
      Option.getOrElse(flags.targets, () =>
        flags.isolate ? "yielded,pi" : "yielded,pi,tardie",
      ).split(","),
    );

    const sizes = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(History))(
      flags.sizes.split(",").map(Number),
    );

    const ttft = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Schema.Literals([0, 400])))(
      flags.ttft.split(",").map(Number),
    );

    if (flags.rigorous && !targets.includes("yielded"))
      return yield* new BenchError({ message: "Rigorous A/B requires the yielded target." });
    if (flags.isolate && targets.includes("tardie"))
      return yield* new BenchError({ message: "Isolated mode supports only yielded and pi." });
    if (flags.isolate && sizes.some((size) => ![0, 50, 250, 1000].includes(size)))
      return yield* new BenchError({ message: "Isolated mode uses 0/50/250/1000 histories." });
    if (flags.storageProbe !== "none") {
      if (!flags.isolate || !flags.rigorous || baseline !== candidate)
        return yield* new BenchError({
          message: "Storage probes require isolated rigorous mode with identical framework refs.",
        });
      if (flags.objects < 20 || flags.objects % 2 !== 0)
        return yield* new BenchError({
          message:
            "Storage probes need an even Object count of at least 20: half grow, half stay small.",
        });
      if (order !== undefined && order !== "AABB")
        return yield* new BenchError({
          message: "Fresh allocation probes use AABB so every Object starts small.",
        });
    }

    return yield* run({
      targets: [...new Set(targets)],
      sizes: [...new Set(sizes)],
      ttft: [...new Set(ttft)],
      objects: flags.objects,
      repeats: Option.getOrElse(flags.repeats, () => (flags.rigorous ? 6 : 4)),
      concurrency: flags.concurrency,
      cold: flags.cold || flags.rigorous || flags.isolate,
      cpu: flags.cpu || flags.rigorous,
      keep: flags.keep,
      rigorous: flags.rigorous,
      isolate: flags.isolate,
      coldMode: flags.coldMode,
      storageProbe: flags.storageProbe,
      paddingMiB: flags.paddingMiB,
      ...(order === undefined ? {} : { order }),
      ...(baseline === undefined ? {} : { baseline }),
      ...(candidate === undefined ? {} : { candidate }),
    });
  }),
).pipe(
  Command.withDescription(
    "Bulk-import fixtures and compare real Cloudflare Durable Objects from a nearby driver Worker.",
  ),
);
