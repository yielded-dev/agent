import { Console, Effect, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { qualifyRun } from "./analyze.ts";
import { Cloudflare, makeRun, withRun } from "./cloudflare.ts";
import type { Cell } from "./evidence.ts";
import { arms, Capture, Health } from "./evidence.ts";
import { check, revision, save } from "./platform.ts";

const measure = Effect.fnUntraced(function* (out: string, blocks: number) {
  yield* withRun(out, (run) =>
    Effect.gen(function* () {
      const cf = yield* Cloudflare;

      yield* run.initialize;
      const provider = yield* run.upload("provider", "provider");
      const driver = yield* run.upload("driver", "driver", "Driver");

      yield* run.ready(provider);

      const health = yield* Schema.decodeEffect(Schema.fromJsonString(Health))(
        yield* run.ready(driver),
      );

      const cells: (typeof Cell.Type)[] = [];

      for (let block = 0; block < blocks; block++) {
        const offset = Math.floor(block / 2) % arms.length;
        const order = [...arms.slice(offset), ...arms.slice(0, offset)];

        if (block % 2) order.reverse();
        for (const arm of order) {
          const framework = arm.startsWith("yielded-") ? "yielded" : "pi";
          const klass = arm.endsWith("-empty") ? "EmptyObject" : "Thread";
          const suffix = `b${block}-${arm}`;
          const host = yield* run.upload(suffix, framework, klass);

          const front = arm.endsWith("-single")
            ? host
            : yield* run.upload(suffix + "-front", "front", klass, host);

          cells.push({ block, arm, host, front });
        }
      }
      yield* save(`${run.out}/plan.json`, {
        cells,
        driver: health,
        blocks,
        state: "brand-new Thread",
        locationHint: "wnam",
        propagationSeconds: 180,
        commit: yield* revision,
      });
      yield* Console.error("All targets untouched; waiting 180 seconds after final upload");
      yield* Effect.sleep("180 seconds");
      const confirmations: Record<string, string | null> = {};

      for (const [name, uploaded] of Object.entries(run.uploads)) {
        confirmations[name] = yield* cf.version(name);
        yield* check(
          confirmations[name] === uploaded.version,
          "Deployed version does not match upload",
        );
      }
      yield* save(`${run.out}/confirmed-versions.json`, confirmations);
      for (const cell of cells) {
        const file = `${run.out}/capture-b${cell.block}-${cell.arm}.json`;

        yield* run.invoke(driver, "/capture", Capture, { name: cell.front, sample: "first" }, file);

        yield* Console.error("Captured", cell.block, cell.arm);
      }
    }),
  );
  yield* qualifyRun(out);
  yield* Console.error("Freshness and completed captures verified");
});

export const command = Command.make(
  "measure",
  {
    action: Argument.Literals("action", ["run", "cleanup"]),
    out: Flag.String("out").pipe(
      Flag.withDescription("Private output directory; cleanup reuses it"),
    ),
    blocks: Flag.Int("blocks").pipe(
      Flag.withSchema(
        Schema.Int.check(
          Schema.makeFilter((value) => [2, 4, 6].includes(value) || "Expected 2, 4, or 6"),
        ),
      ),
      Flag.withDefault(6),
      Flag.withDescription(
        "Pairs per deployment; run two six-block deployments for reported intervals",
      ),
    ),
  },
  Effect.fnUntraced(function* ({ action, out, blocks }) {
    if (action === "cleanup") yield* (yield* makeRun(out, true)).cleanup;
    else yield* measure(out, blocks);
  }),
).pipe(
  Command.withDescription("Measure fresh Cloudflare isolates or finish a recorded run's teardown"),
);
