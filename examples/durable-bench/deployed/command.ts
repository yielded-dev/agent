import { Effect, Option, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { DEFAULT_ROW_LIMIT } from "./budget.ts";
import { Cloudflare } from "./cloudflare.ts";
import { type Options, ProfileType } from "./model.ts";
import { BenchError } from "./platform.ts";
import { previewCost, resume, run, teardown } from "./run.ts";
import { History, RESTORE_BYTES, Target } from "./worker/protocol.ts";

const positive = (name: string) => Flag.Int(name).pipe(Flag.withSchema(History));

export const command = Command.make(
  "deployed",
  {
    preset: Flag.Literals("preset", ["screen", "quick", "full"]).pipe(
      Flag.withDefault("screen"),
      Flag.withDescription("Exploratory screen (4 Objects, history 50), quick (12), or full (30)"),
    ),
    costOnly: Flag.Boolean("cost-only").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Print the write reservation estimate without building or deploying"),
    ),
    writes: Flag.Boolean("writes").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Diagnostic native write counters; excludes timing conclusions"),
    ),
    maxWrittenRows: positive("max-written-rows").pipe(
      Flag.withDefault(DEFAULT_ROW_LIMIT),
      Flag.withDescription("Maximum estimated row writes admitted for this run (default: 1000000)"),
    ),
    dailyWrittenRows: positive("daily-written-rows").pipe(
      Flag.withDefault(DEFAULT_ROW_LIMIT),
      Flag.withDescription(
        "Shared local account reservation limit over 24 hours (default: 1000000)",
      ),
    ),
    budgetReason: Flag.String("budget-reason").pipe(
      Flag.optional,
      Flag.withDescription("Decision supported by a large run or raised write budget"),
    ),
    restoreBytes: Flag.Int("restore-bytes").pipe(
      Flag.withSchema(
        Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: RESTORE_BYTES })),
      ),
      Flag.withDefault(RESTORE_BYTES),
      Flag.withDescription("Maximum bytes per snapshot request (default: 4 MiB)"),
    ),
    restoreEachPass: Flag.Boolean("restore-each-pass").pipe(
      Flag.optional,
      Flag.withDescription("Restore identical history before every comparison pass (required)"),
    ),
    verifyIncarnationRecovery: Flag.Boolean("verify-incarnation-recovery").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Diagnostic: abort one settled Object per target before metrics, then verify sample recovery",
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
      Flag.withDescription(
        "Targets: yielded, pi, tardie; opt into effect-ai-floor for a warm native AI comparison",
      ),
    ),
    sizes: Flag.String("sizes").pipe(
      Flag.optional,
      Flag.withDescription(
        "History lengths (screen: 50; quick/full: 1000; build-history: 50,250,1000,3500)",
      ),
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
      Flag.withDescription(
        "Objects per target/cell (screen: 4; quick: 12; full: 30; build-history: 2)",
      ),
    ),
    repeats: positive("repeats").pipe(
      Flag.optional,
      Flag.withDescription(
        "Warm turns per Object (screen: 3; quick: 12; full: 6; build-history: 9)",
      ),
    ),
    concurrency: positive("concurrency").pipe(
      Flag.optional,
      Flag.withDescription(
        "Concurrent measured Objects (quick/full: 12, minimum 2); pairs stay together",
      ),
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
    isolateCold: Flag.Boolean("isolate-cold").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Upload a fresh version before each cold boundary without readiness probes",
      ),
    ),
    coldSamples: Flag.Int("cold-samples").pipe(
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 2, maximum: 12 }))),
      Flag.withDefault(2),
      Flag.withDescription(
        "Cold turns per Object/epoch with --isolate-cold; extra cold turns follow the warm batch",
      ),
    ),
    cpu: Flag.Boolean("cpu").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Collect per-turn Object CPU through a Tail Worker and temporary R2 bucket",
      ),
    ),
    profile: Flag.String("profile").pipe(
      Flag.mapTryCatch(
        (value) => Schema.decodeUnknownSync(Schema.NonEmptyArray(ProfileType))(value.split(",")),
        () => "Expected cpu or memory, comma-separated.",
      ),
      Flag.atLeast(0),
      Flag.withDescription(
        "Snapshot: one diagnostic cpu capture at first cold open; history: cpu or memory with --profile-after",
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
      Flag.withDescription("Alias for --preset full: cold + same-Object balanced A/B"),
    ),
    seed: Flag.String("seed").pipe(
      Flag.optional,
      Flag.withDescription("Replay all native arm and scheduling orders"),
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
    if (flags.costOnly && (flags.teardown || Option.isSome(flags.resume)))
      return yield* BenchError.make({
        message: "--cost-only cannot be combined with --resume or --teardown.",
      });
    if (flags.teardown) return yield* teardown.pipe(Effect.provide(Cloudflare.layer));
    const runName = Option.getOrUndefined(flags.resume);

    if (runName !== undefined) return yield* resume(runName).pipe(Effect.provide(Cloudflare.layer));
    const rigorous = flags.rigorous || flags.preset === "full";
    const screen = !rigorous && flags.preset === "screen";

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
      Option.getOrElse(flags.sizes, () =>
        flags.buildHistory ? "50,250,1000,3500" : screen ? "50" : "1000",
      )
        .split(",")
        .map(Number),
    );

    const ttft = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(Schema.Literals([0, 400])))(
      Option.getOrElse(flags.ttft, () => "0")
        .split(",")
        .map(Number),
    );

    if (
      targets.includes("effect-ai-floor") &&
      (flags.buildHistory ||
        flags.verifyIncarnationRecovery ||
        !Option.getOrElse(flags.restoreEachPass, () => true))
    )
      return yield* BenchError.make({
        message:
          "effect-ai-floor requires snapshot comparison with --restore-each-pass and cannot build or recover durable history.",
      });

    if (
      flags.verifyIncarnationRecovery &&
      (flags.buildHistory ||
        flags.profile.flat().length > 0 ||
        !Option.getOrElse(flags.restoreEachPass, () => true))
    )
      return yield* BenchError.make({
        message:
          "Recovery diagnostics require unprofiled, snapshot-backed passes with --restore-each-pass.",
      });

    const profiles = [...new Set(flags.profile.flat())];

    if (
      flags.writes &&
      (targets.length !== 1 || targets[0] !== "yielded" || profiles.length > 0 || flags.cpu)
    )
      return yield* BenchError.make({
        message:
          "--writes requires a single Yielded target without CPU or profiler instrumentation.",
      });

    if (flags.coldSamples !== 2 && !flags.isolateCold)
      return yield* BenchError.make({ message: "Extra cold samples require --isolate-cold." });

    if (
      flags.isolateCold &&
      (flags.buildHistory ||
        flags.verifyIncarnationRecovery ||
        profiles.length > 0 ||
        targets.some((target) => target !== "yielded" && target !== "pi"))
    )
      return yield* BenchError.make({
        message:
          "--isolate-cold requires an unprofiled Yielded/pi snapshot comparison without recovery diagnostics.",
      });

    if (!flags.buildHistory && Option.getOrElse(flags.restoreEachPass, () => true) === false)
      return yield* BenchError.make({
        message: "Snapshot runs require identical restored histories.",
      });
    const profileAfter = Option.getOrUndefined(flags.profileAfter);

    const objects = Option.getOrElse(flags.objects, () =>
      flags.buildHistory ? 2 : rigorous ? 30 : screen ? 4 : 12,
    );

    if (
      !flags.buildHistory &&
      profiles.length > 0 &&
      (profiles.length !== 1 ||
        profiles[0] !== "cpu" ||
        targets.length !== 1 ||
        !["yielded", "pi"].includes(targets[0]) ||
        sizes.length !== 1 ||
        ttft.length !== 1 ||
        objects !== 2 ||
        rigorous ||
        flags.cpu ||
        profileAfter !== undefined)
    )
      return yield* BenchError.make({
        message:
          "Snapshot --profile cpu requires one Yielded/pi target, size and TTFT, --objects 2, quick mode, no --cpu and no --profile-after; it stops after the selected candidate Object's first cold turn.",
      });

    if (!flags.buildHistory && objects % 2 !== 0)
      return yield* BenchError.make({
        message: "Counterbalanced comparison requires an even Object count.",
      });

    const concurrency = Option.getOrElse(flags.concurrency, () =>
      flags.buildHistory ? 6 : screen ? 4 : 12,
    );

    if (!flags.buildHistory && concurrency < 2)
      return yield* BenchError.make({
        message: "Paired comparison requires concurrency of at least 2.",
      });

    const buildConcurrency = Option.getOrElse(flags.buildConcurrency, () =>
      flags.buildHistory
        ? new Set(targets).size * new Set(sizes).size * new Set(ttft).size * objects
        : concurrency,
    );

    if (flags.buildHistory && (rigorous || flags.textStreaming))
      return yield* BenchError.make({
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
      return yield* BenchError.make({
        message:
          "--profile-after requires --build-history, --profile, one target/size/TTFT/Object, concurrency and build-concurrency 1, and a checkpoint divisible by 50 before the final turn.",
      });
    if (flags.buildHistory && profiles.length > 0 && profileAfter === undefined)
      return yield* BenchError.make({ message: "History profiling requires --profile-after." });

    const options: Options = {
      writes: flags.writes,
      costOnly: flags.costOnly,
      maxWrittenRows: flags.maxWrittenRows,
      dailyWrittenRows: flags.dailyWrittenRows,
      ...(Option.isSome(flags.budgetReason) ? { budgetReason: flags.budgetReason.value } : {}),
      ...(Option.isSome(flags.seed) ? { seed: flags.seed.value } : {}),
      ...(Option.isSome(flags.fixtureDirectory)
        ? { fixtureDirectory: flags.fixtureDirectory.value }
        : {}),
      restoreBytes: flags.restoreBytes,
      restoreEachPass: Option.getOrElse(flags.restoreEachPass, () => !flags.buildHistory),
      verifyIncarnationRecovery: flags.verifyIncarnationRecovery,
      buildHistory: flags.buildHistory,
      targets: [...new Set(targets)],
      sizes: [...new Set(sizes)],
      ttft: [...new Set(ttft)],
      textStreaming: flags.textStreaming,
      objects,
      repeats: Option.getOrElse(flags.repeats, () =>
        flags.buildHistory ? 9 : rigorous ? 6 : screen ? 3 : 12,
      ),
      concurrency,
      buildConcurrency,
      cold: true,
      isolateCold: flags.isolateCold,
      coldSamples: flags.coldSamples,
      cpu: flags.cpu,
      profiles,
      ...(profileAfter === undefined ? {} : { profileAfter }),
      keep: flags.keep,
      rigorous,
      ...(baseline === undefined ? {} : { baseline }),
      ...(candidate === undefined ? {} : { candidate }),
    };

    return yield* flags.costOnly
      ? previewCost(options)
      : run(options).pipe(Effect.provide(Cloudflare.layer));
  }),
).pipe(
  Command.withDescription(
    "Build or import histories and compare deployed Durable Objects from a nearby driver Worker.",
  ),
);
