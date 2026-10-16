import { Cause, Clock, Console, Effect, Exit, Fiber, FileSystem, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { Cloudflare, type Run, withRun } from "./cloudflare.ts";
import { Frame, Identity } from "./evidence.ts";
import { BenchError, check, gunzip, save } from "./platform.ts";

const ProfileResult = Schema.Struct({
  label: Schema.String,
  version: Schema.String,
  startedAt: Schema.Finite,
  finishedAt: Schema.Finite,
  status: Schema.optionalKey(Schema.Int),
  bytes: Schema.optionalKey(Schema.Int),
  gzipValid: Schema.optionalKey(Schema.Boolean),
  error: Schema.optionalKey(Schema.String),
  body: Schema.optionalKey(Schema.String),
  retryAfter: Schema.optionalKey(Schema.NullOr(Schema.String)),
});

const Settings = Schema.Struct({
  bindings: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      namespace_id: Schema.optionalKey(Schema.String),
    }),
  ),
});

const seconds = Clock.currentTimeMillis.pipe(Effect.map((ms) => ms / 1000));

const capture = Effect.fnUntraced(function* (
  run: Pick<Run, "out" | "uploads" | "redact">,
  name: string,
  label: string,
  namespace?: string,
  actor?: string,
) {
  const cf = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;
  const version = run.uploads[name]!.version;
  const startedAt = yield* seconds;

  const request = yield* Effect.exit(
    Effect.gen(function* () {
      const response = yield* cf.profile(name, version, {
        duration_ms: 5000,
        profile_type: "cpu",
        ...(namespace ? { namespace_id: namespace, actor_id: actor! } : {}),
      });

      if (response.status < 200 || response.status >= 300)
        return {
          status: response.status,
          body: run.redact(new TextDecoder().decode(response.bytes)),
          retryAfter: response.retryAfter,
        };
      yield* fs.writeFile(`${run.out}/${label}.pprof.gz`, response.bytes, { mode: 0o600 });
      const inflated = yield* gunzip(response.bytes);

      return {
        status: response.status,
        bytes: response.bytes.length,
        gzipValid: inflated.length > 0,
      };
    }),
  );

  const result: typeof ProfileResult.Type = {
    label,
    version,
    startedAt,
    finishedAt: yield* seconds,
    ...(Exit.isSuccess(request)
      ? request.value
      : { error: run.redact(Cause.pretty(request.cause)) }),
  };

  yield* save(`${run.out}/${label}.json`, result);
  yield* Console.error(label, result.status ?? "transport failure");
  if (result.status === 429) {
    yield* Console.error("Waiting after profiler rate limit");
    const delay = Number(result.retryAfter ?? "300");

    yield* Effect.sleep((Number.isFinite(delay) && delay > 0 ? delay : 300) * 1000);
  }

  return result;
});

const profile = (out: string) =>
  withRun(out, (run) =>
    Effect.gen(function* () {
      const cf = yield* Cloudflare;
      const results: (typeof ProfileResult.Type)[] = [];

      yield* run.initialize;
      const provider = yield* run.upload("provider", "provider");

      yield* run.ready(provider);

      const targets = [
        {
          arm: "yielded",
          name: yield* run.upload("yielded", "yielded", "Thread", undefined, true),
        },
        { arm: "pi", name: yield* run.upload("pi", "pi", "Thread", undefined, true) },
      ];

      yield* Console.error("Waiting 180 seconds without target traffic");
      yield* Effect.sleep("180 seconds");
      for (const { arm, name } of targets) {
        yield* check(
          (yield* cf.version(name)) === run.uploads[name]!.version,
          "Deployed version does not match upload",
        );
        results.push(yield* capture(run, name, arm + "-before-module-load"));

        const { actorId } = yield* run.invoke(
          name,
          "/id",
          Schema.Struct({ actorId: Schema.NonEmptyString }),
        );

        const settings = yield* cf.api(`workers/scripts/${name}/settings`, Settings);

        const namespace = settings.bindings.find(
          (binding) => binding.name === "THREAD",
        )?.namespace_id;

        if (!namespace)
          return yield* BenchError.make({
            message: "Thread namespace missing from deployed settings",
          });
        results.push(yield* capture(run, name, arm + "-before-object-load", namespace, actorId));
        const touches: (typeof Identity.Type)[] = [];

        for (let index = 0; index < 8; index++) {
          touches.push(yield* run.invoke(name, "/touch", Identity));
          yield* Effect.sleep("1 second");
        }
        yield* check(
          touches.every((touch) => touch.version === run.uploads[name]!.version) &&
            new Set(touches.map((touch) => touch.moduleId)).size === 1,
          "Object identity or version changed during no-op touches",
        );
        yield* save(`${run.out}/${arm}-touches.json`, touches);
        yield* Effect.scoped(
          Effect.gen(function* () {
            // Issuing the request is not an attachment acknowledgement. Preserve both clocks.
            const recording = yield* capture(
              run,
              name,
              arm + "-first-turn",
              namespace,
              actorId,
            ).pipe(Effect.forkScoped);

            yield* Effect.sleep("300 millis");
            const startedAt = yield* seconds;
            const turn = yield* Effect.exit(run.invokeText(name, "/capture?sample=first"));

            yield* save(`${run.out}/${arm}-turn.json`, {
              startedAt,
              finishedAt: yield* seconds,
              ...(Exit.isSuccess(turn)
                ? { raw: turn.value }
                : { error: run.redact(Cause.pretty(turn.cause)) }),
            });
            results.push(yield* Fiber.join(recording));
            yield* save(`${run.out}/summary.json`, results);
            const raw = yield* turn;

            const frames = yield* Effect.forEach(raw.split("\n").filter(Boolean), (line) =>
              Schema.decodeEffect(Schema.fromJsonString(Frame))(line, {
                onExcessProperty: "error",
              }),
            );

            yield* check(
              !frames.some((frame) => frame.type === "error"),
              "First turn returned an error frame",
            );
            yield* check(
              frames
                .flatMap((frame) => (frame.type === "text" ? [frame.value.text] : []))
                .join("") === "hello",
              "First turn did not return hello",
            );
            const last = frames.at(-1);

            yield* check(
              last?.type === "done" && last.value.outcome === "completed" && last.value.calls === 1,
              "First turn did not complete with one provider call",
            );

            const objects = frames.flatMap((frame) =>
              frame.type === "object" ? [frame.value] : [],
            );

            yield* check(
              objects.length === 1 &&
                objects[0]!.version === run.uploads[name]!.version &&
                objects[0]!.moduleId === touches.at(-1)!.moduleId &&
                objects[0]!.calls === touches.at(-1)!.calls + 1,
              "First turn did not use the touched Object identity and version",
            );
          }),
        );
        if (name !== targets.at(-1)!.name) {
          yield* Console.error("Spacing framework profile attempts by five minutes");
          yield* Effect.sleep("5 minutes");
        }
      }
      yield* save(`${run.out}/summary.json`, results);
    }),
  );

export const command = Command.make(
  "profile",
  {
    out: Flag.String("out").pipe(Flag.withDescription("New private output directory")),
  },
  ({ out }) => profile(out),
).pipe(
  Command.withDescription("Try module-load and first-turn CPU profiling in a separate deployment"),
);
