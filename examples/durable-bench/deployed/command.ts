import { Effect, Option, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { ProfileType } from "./model.ts";
import { BenchError } from "./platform.ts";
import { resume, run, teardown } from "./run.ts";
import { History, RESTORE_BYTES, Target } from "./worker/protocol.ts";

const positive = (name: string) => Flag.Int(name).pipe(Flag.withSchema(History));

export const command = Command.make(
  "deployed",
  {
    preset: Flag.Literals("preset", ["quick", "full"]).pipe(
      Flag.withDefault("quick"),
      Flag.withDescription("Paired screening (12 Objects) or balanced final matrix (30 Objects)"),
    ),
    restoreBytes: Flag.Int("restore-bytes").pipe(
      Flag.withSchema(
        Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: RESTORE_BYTES })),
      ),
      Flag.withDefault(RESTORE_BYTES),
      Flag.withDescription("Maximum bytes per snapshot request (default: 4 MiB)"),
    ),
    restoreEachPass: Flag.Boolean("restore-each-pass").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Restore identical saved history before every pass (automatic for different storage formats)",
      ),
    ),
    fixtureDirectory: Flag.String("fixture-directory").pipe(
      Flag.optional,
      Flag.withDescription("Read saved fixtures from this directory"),
    ),
    buildHistory: Flag.Boolean("build-history").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Build real history in deployed Objects, then measure open + first turn and 9 warm turns",
      ),
    ),
    targets: Flag.String("targets").pipe(
      Flag.optional,
      Flag.withDescription("Comma-separated targets (quick/full: yielded; build-history: all)"),
    ),
    sizes: Flag.String("sizes").pipe(
      Flag.optional,
      Flag.withDescription("History lengths (quick/full: 1000; build-history: 50,250,1000,3500)"),
    ),
    ttft: Flag.String("ttft").pipe(
      Flag.optional,
      Flag.withDescription("Provider TTFT in ms (default: 0)"),
    ),
    textStreaming: Flag.Boolean("text-streaming").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Stream an initial preamble and final reply at 40 fragments/s in the 400 ms cell",
      ),
    ),
    objects: positive("objects").pipe(
      Flag.optional,
      Flag.withDescription("Objects per target/cell (quick: 12; full: 30; build-history: 3)"),
    ),
    repeats: positive("repeats").pipe(
      Flag.optional,
      Flag.withDescription("Warm turns per Object (quick: 4; rigorous: 6; build-history: 9)"),
    ),
    concurrency: positive("concurrency").pipe(
      Flag.optional,
      Flag.withDescription("Concurrent measured Objects (default: 6); turns stay sequential"),
    ),
    buildConcurrency: positive("build-concurrency").pipe(
      Flag.optional,
      Flag.withDescription(
        "Concurrent histories (build-history: all selected Objects; quick: --concurrency)",
      ),
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
        "Capture cpu or memory profiles of one Object per cell; repeat or comma-separate",
      ),
    ),
    profileAfter: positive("profile-after").pipe(
      Flag.optional,
      Flag.withDescription(
        "Profile 10 seconds after this real-history checkpoint (multiple of 50)",
      ),
    ),
    keep: Flag.Boolean("keep").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Retain this run's target stack until --teardown"),
    ),
    rigorous: Flag.Boolean("rigorous").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Alias for --preset full: cold + CPU + same-Object balanced A/B"),
    ),
    baseline: Flag.String("baseline").pipe(Flag.optional),
    candidate: Flag.String("candidate").pipe(Flag.optional),
    resume: Flag.String("resume").pipe(
      Flag.optional,
      Flag.withDescription(
        "Resume a saved run with its options; preserve verified history checkpoints and retire unknown in-flight or partially measured Objects",
      ),
    ),
    teardown: Flag.Boolean("teardown").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Destroy all owned target stacks and shared infrastructure, then verify the account",
      ),
    ),
  },
  Effect.fnUntraced(function* (flags) {
    if (flags.teardown) return yield* teardown;
    const runName = Option.getOrUndefined(flags.resume);

    if (runName !== undefined) return yield* resume(runName);
    const rigorous = flags.rigorous || flags.preset === "full";

    const baseline = flags.buildHistory
      ? undefined
      : Option.getOrElse(flags.baseline, () => "origin/main");

    const candidate = flags.buildHistory
      ? undefined
      : Option.getOrElse(flags.candidate, () => "HEAD");

    const targets = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Target))(
      Option.getOrElse(flags.targets, () =>
        flags.buildHistory ? "yielded,pi,tardie" : "yielded",
      ).split(","),
    );

    const sizes = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(History))(
      Option.getOrElse(flags.sizes, () => (flags.buildHistory ? "50,250,1000,3500" : "1000"))
        .split(",")
        .map(Number),
    );

    const ttft = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Schema.Literals([0, 400])))(
      Option.getOrElse(flags.ttft, () => "0")
        .split(",")
        .map(Number),
    );

    if (!flags.buildHistory && !targets.includes("yielded"))
      return yield* new BenchError({ message: "Paired A/B requires the yielded target." });

    const profiles = [...new Set(flags.profile.flat())];
    const profileAfter = Option.getOrUndefined(flags.profileAfter);

    const objects = Option.getOrElse(flags.objects, () =>
      flags.buildHistory ? 3 : rigorous ? 30 : 12,
    );

    const concurrency = Option.getOrElse(flags.concurrency, () => 6);

    const buildConcurrency = Option.getOrElse(flags.buildConcurrency, () =>
      flags.buildHistory
        ? new Set(targets).size * new Set(sizes).size * new Set(ttft).size * objects
        : concurrency,
    );

    if (flags.buildHistory && (rigorous || flags.textStreaming))
      return yield* new BenchError({
        message: "--build-history measures completion without observers or A/B redeploys.",
      });

    if (
      profileAfter !== undefined &&
      (!flags.buildHistory ||
        profiles.length === 0 ||
        profileAfter % 50 !== 0 ||
        sizes.length !== 1 ||
        profileAfter >= sizes[0] ||
        targets.length !== 1 ||
        ttft.length !== 1 ||
        objects !== 1 ||
        concurrency !== 1 ||
        buildConcurrency !== 1)
    )
      return yield* new BenchError({
        message:
          "--profile-after requires --build-history, --profile, one target/size/TTFT/Object, concurrency and build-concurrency 1, and a checkpoint divisible by 50 before the final turn.",
      });
    if (flags.buildHistory && profiles.length > 0 && profileAfter === undefined)
      return yield* new BenchError({ message: "History profiling requires --profile-after." });

    return yield* run({
      ...(Option.isSome(flags.fixtureDirectory)
        ? { fixtureDirectory: flags.fixtureDirectory.value }
        : {}),
      restoreBytes: flags.restoreBytes,
      restoreEachPass: flags.restoreEachPass,
      buildHistory: flags.buildHistory,
      targets: [...new Set(targets)],
      sizes: [...new Set(sizes)],
      ttft: [...new Set(ttft)],
      textStreaming: flags.textStreaming,
      objects,
      repeats: Option.getOrElse(flags.repeats, () => (flags.buildHistory ? 9 : rigorous ? 6 : 4)),
      concurrency,
      buildConcurrency,
      cold: true,
      cpu: flags.cpu || rigorous,
      profiles,
      ...(profileAfter === undefined ? {} : { profileAfter }),
      keep: flags.keep,
      rigorous,
      ...(baseline === undefined ? {} : { baseline }),
      ...(candidate === undefined ? {} : { candidate }),
    });
  }),
).pipe(
  Command.withDescription(
    "Build or import histories and compare deployed Durable Objects from a nearby driver Worker.",
  ),
);
