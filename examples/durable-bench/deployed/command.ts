import { Effect, Option, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { ProfileType } from "./model.ts";
import { BenchError } from "./platform.ts";
import { run, teardown } from "./run.ts";
import { History, Target } from "./worker/protocol.ts";

const positive = (name: string) => Flag.Int(name).pipe(Flag.withSchema(History));

export const command = Command.make(
  "deployed",
  {
    buildHistory: Flag.Boolean("build-history").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Build real history in deployed Objects, then measure open + first turn and 9 warm turns",
      ),
    ),
    targets: Flag.String("targets").pipe(
      Flag.withDefault("yielded,pi,tardie"),
      Flag.withDescription("Comma-separated targets"),
    ),
    sizes: Flag.String("sizes").pipe(
      Flag.optional,
      Flag.withDescription("History lengths (quick: 50,250; build-history: 50,250,1000,3500)"),
    ),
    ttft: Flag.String("ttft").pipe(
      Flag.optional,
      Flag.withDescription("Provider TTFT in ms (quick: 0,400; build-history: 0)"),
    ),
    textStreaming: Flag.Boolean("text-streaming").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Stream an initial preamble and final reply at 40 fragments/s in the 400 ms cell",
      ),
    ),
    objects: positive("objects").pipe(
      Flag.optional,
      Flag.withDescription("Objects per target/cell (quick: 7; build-history: 3)"),
    ),
    repeats: positive("repeats").pipe(
      Flag.optional,
      Flag.withDescription("Warm turns per Object (quick: 4; rigorous: 6; build-history: 9)"),
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
    profile: Flag.String("profile").pipe(
      Flag.mapTryCatch(
        (value) => Schema.decodeUnknownSync(Schema.NonEmptyArray(ProfileType))(value.split(",")),
        () => "Expected cpu or memory, comma-separated.",
      ),
      Flag.atLeast(0),
      Flag.withDescription(
        "Capture cpu or memory profiles of one Yielded/pi Object per cell; repeat or comma-separate",
      ),
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
      Option.getOrElse(flags.sizes, () => (flags.buildHistory ? "50,250,1000,3500" : "50,250"))
        .split(",")
        .map(Number),
    );

    const ttft = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Schema.Literals([0, 400])))(
      Option.getOrElse(flags.ttft, () => (flags.buildHistory ? "0" : "0,400"))
        .split(",")
        .map(Number),
    );

    if (flags.rigorous && !targets.includes("yielded"))
      return yield* new BenchError({ message: "Rigorous A/B requires the yielded target." });

    const profiles = [...new Set(flags.profile.flat())];

    if (flags.buildHistory && (flags.rigorous || flags.textStreaming || profiles.length > 0))
      return yield* new BenchError({
        message:
          "--build-history measures completion without observers, profiling or A/B redeploys.",
      });

    if (profiles.length > 0 && !targets.some((target) => target === "yielded" || target === "pi"))
      return yield* new BenchError({ message: "--profile requires the yielded or pi target." });

    return yield* run({
      buildHistory: flags.buildHistory,
      targets: [...new Set(targets)],
      sizes: [...new Set(sizes)],
      ttft: [...new Set(ttft)],
      textStreaming: flags.textStreaming,
      objects: Option.getOrElse(flags.objects, () => (flags.buildHistory ? 3 : 7)),
      repeats: Option.getOrElse(flags.repeats, () =>
        flags.buildHistory ? 9 : flags.rigorous ? 6 : 4,
      ),
      concurrency: flags.concurrency,
      cold: flags.cold || flags.rigorous || flags.buildHistory,
      cpu: flags.cpu || flags.rigorous,
      profiles,
      keep: flags.keep,
      rigorous: flags.rigorous,
      ...(baseline === undefined ? {} : { baseline }),
      ...(candidate === undefined ? {} : { candidate }),
    });
  }),
).pipe(
  Command.withDescription(
    "Build or import histories and compare deployed Durable Objects from a nearby driver Worker.",
  ),
);
