import { Effect, Option, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { BenchError } from "./platform.ts";
import { run, teardown } from "./run.ts";
import { History, Target } from "./worker/protocol.ts";

const positive = (name: string) => Flag.Int(name).pipe(Flag.withSchema(History));

export const command = Command.make(
  "deployed",
  {
    targets: Flag.String("targets").pipe(
      Flag.withDefault("yielded,pi,tardie"),
      Flag.withDescription("Comma-separated targets"),
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

    if (
      flags.rigorous ? !baseline || !candidate : baseline !== undefined || candidate !== undefined
    )
      return yield* new BenchError({
        message: "Use --rigorous with both --baseline <ref> and --candidate <ref>.",
      });

    const targets = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Target))(
      flags.targets.split(","),
    );

    const sizes = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(History))(
      flags.sizes.split(",").map(Number),
    );

    const ttft = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Schema.Literals([0, 400])))(
      flags.ttft.split(",").map(Number),
    );

    if (flags.rigorous && !targets.includes("yielded"))
      return yield* new BenchError({ message: "Rigorous A/B requires the yielded target." });

    if (flags.storageProbe !== "none" && (!flags.rigorous || baseline !== candidate))
      return yield* new BenchError({
        message: "Storage probes require rigorous mode with identical refs.",
      });

    return yield* run({
      targets: [...new Set(targets)],
      sizes: [...new Set(sizes)],
      ttft: [...new Set(ttft)],
      objects: flags.objects,
      repeats: Option.getOrElse(flags.repeats, () => (flags.rigorous ? 6 : 4)),
      concurrency: flags.concurrency,
      cold: flags.cold || flags.rigorous,
      cpu: flags.cpu || flags.rigorous,
      keep: flags.keep,
      rigorous: flags.rigorous,
      storageProbe: flags.storageProbe,
      paddingMiB: flags.paddingMiB,
      ...(baseline === undefined ? {} : { baseline }),
      ...(candidate === undefined ? {} : { candidate }),
    });
  }),
).pipe(
  Command.withDescription(
    "Bulk-import fixtures and compare real Cloudflare Durable Objects from a nearby driver Worker.",
  ),
);
