import { Console, Effect, FileSystem, Path, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { arms, Capture, Cleanup, Plan, Uploads } from "./evidence.ts";
import { BenchError, check as requireEvidence, hash, read, save } from "./platform.ts";

const Window = Schema.Struct({ worker: Schema.Finite, object: Schema.Finite });

const Turn = Schema.Struct({
  ...Window.fields,
  firstText: Schema.Finite,
  afterObject: Schema.Finite,
});

const Block = Schema.Struct({
  "yielded-single": Turn,
  "pi-single": Turn,
  "yielded-split": Turn,
  "pi-split": Turn,
  "yielded-empty": Window,
  "pi-empty": Window,
});

type Block = typeof Block.Type;

// Qualification must not strip extra terminal fields before checking the evidence.
const QualifiedCapture = Capture.annotate({ parseOptions: { onExcessProperty: "error" } });

/** Qualify one cohort, including small smoke runs. Shared sets detect reuse across cohorts. */
export const qualifyRun = Effect.fn("qualifyRun")(function* (
  root: string,
  seen = new Set<string>(),
  bundles = new Map<string, string>(),
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const plan = yield* read(path.join(root, "plan.json"), Plan);
  const uploads = yield* read(path.join(root, "uploads.json"), Uploads);

  const confirmed = yield* read(
    path.join(root, "confirmed-versions.json"),
    Schema.Record(Schema.String, Schema.String),
  );

  for (const uploaded of Object.values(uploads)) {
    const previous = bundles.get(uploaded.bundle);

    yield* requireEvidence(
      previous === undefined || previous === uploaded.bundleSha256,
      "Mixed source bundles",
    );
    bundles.set(uploaded.bundle, uploaded.bundleSha256);
  }
  const blocks = new Map<number, Record<string, typeof Window.Type | typeof Turn.Type>>();
  let driverInstance: string | undefined;

  for (const cell of plan.cells) {
    const result = yield* read(
      path.join(root, `capture-b${cell.block}-${cell.arm}.json`),
      QualifiedCapture,
    );

    yield* requireEvidence(!("error" in result), "Capture failed");
    // Readiness can precede an eviction. Only collection must share an incarnation.
    driverInstance ??= result.instance;
    yield* requireEvidence(
      result.instance === driverInstance,
      "Driver incarnation changed during collection",
    );
    const rows = result.frames;

    yield* requireEvidence(
      rows.every(
        (row, index) =>
          row.frame.type !== "error" &&
          row.at >= 0 &&
          (index === 0 || rows[index - 1].at <= row.at),
      ),
      "Invalid frames or arrival order",
    );
    yield* requireEvidence(
      rows[0]?.frame.type === "worker" && rows[1]?.frame.type === "object",
      "Expected Worker and Object markers first",
    );
    const markers = { worker: 0, object: 0 };

    for (const [kind, name] of [
      ["worker", cell.front],
      ["object", cell.host],
    ] as const) {
      const matches = rows.filter((row) => row.frame.type === kind);
      const marker = matches[0];

      if (
        matches.length !== 1 ||
        !marker ||
        (marker.frame.type !== "worker" && marker.frame.type !== "object")
      ) {
        return yield* BenchError.make({ message: `Expected exactly one ${kind} marker` });
      }
      const value = marker.frame.value;
      const uploaded = uploads[name];

      yield* requireEvidence(
        value.calls === 1 &&
          value.alarms === 0 &&
          value.constructions === (kind === "object" ? 1 : 0),
        `Contaminated ${kind} identity`,
      );
      yield* requireEvidence(
        uploaded !== undefined &&
          value.version === uploaded.version &&
          value.version === confirmed[name],
        "Version mismatch",
      );
      yield* requireEvidence(!seen.has(value.moduleId), "Reused module instance");
      seen.add(value.moduleId);
      const bytes = yield* fs.readFile(path.join(root, `${name}.mjs`));

      yield* requireEvidence(hash(bytes) === uploaded.sha256, "Uploaded source hash mismatch");
      markers[kind] = marker.at;
    }

    let values: typeof Window.Type | typeof Turn.Type = {
      worker: markers.worker,
      object: markers.object - markers.worker,
    };

    if (cell.arm.endsWith("empty")) {
      yield* requireEvidence(rows.length === 2, "Empty Object performed a turn");
    } else {
      const texts = rows.flatMap((row) =>
        row.frame.type === "text" && row.frame.value.text
          ? [{ at: row.at, text: row.frame.value.text }]
          : [],
      );

      const done = rows.filter((row) => row.frame.type === "done");
      const terminal = done[0]?.frame;

      yield* requireEvidence(
        texts.map((row) => row.text).join("") === "hello",
        "Expected exactly the scripted hello text",
      );
      yield* requireEvidence(
        rows.at(-1)?.frame.type === "done" &&
          done.length === 1 &&
          terminal?.type === "done" &&
          terminal.value.outcome === "completed" &&
          terminal.value.calls === 1 &&
          Object.keys(terminal.value).length === 2,
        "Expected one completed turn with one provider call",
      );
      values = { ...values, firstText: texts[0].at, afterObject: texts[0].at - markers.object };
    }
    const block = blocks.get(cell.block) ?? {};

    yield* requireEvidence(!(cell.arm in block), "Duplicate arm in pair block");
    block[cell.arm] = values;
    blocks.set(cell.block, block);
  }
  yield* requireEvidence(blocks.size === plan.blocks, "Block count mismatch");

  const qualified = yield* Effect.forEach(blocks.values(), (block) =>
    Schema.decodeUnknownEffect(Block)(block).pipe(
      Effect.mapError((cause) => BenchError.make({ message: "Incomplete pair block", cause })),
    ),
  );

  const cleanup = yield* read(path.join(root, "cleanup.json"), Cleanup);

  yield* requireEvidence(cleanup.verified, "Teardown unverified");

  return qualified;
});

/** Only the CPython Random(204) / choice sequence needed by this bootstrap.
 * Integer seeds use init_by_array, and choice rejects getrandbits(bit_length(n)).
 */
const bootstrapChoice = () => {
  const mt = new Uint32Array(624);

  mt[0] = 19650218;
  for (let i = 1; i < 624; i++)
    mt[i] = (Math.imul(mt[i - 1] ^ (mt[i - 1] >>> 30), 1812433253) + i) >>> 0;
  let index = 1;

  for (let k = 624; k > 0; k--) {
    mt[index] =
      ((mt[index] ^ Math.imul(mt[index - 1] ^ (mt[index - 1] >>> 30), 1664525)) + 204) >>> 0;
    if (++index >= 624) {
      mt[0] = mt[623];
      index = 1;
    }
  }
  for (let k = 623; k > 0; k--) {
    mt[index] =
      ((mt[index] ^ Math.imul(mt[index - 1] ^ (mt[index - 1] >>> 30), 1566083941)) - index) >>> 0;
    if (++index >= 624) {
      mt[0] = mt[623];
      index = 1;
    }
  }
  mt[0] = 0x80000000;
  index = 624;

  const next = () => {
    if (index === 624) {
      for (let i = 0; i < 624; i++) {
        const y = (mt[i] & 0x80000000) | (mt[(i + 1) % 624] & 0x7fffffff);

        mt[i] = mt[(i + 397) % 624] ^ (y >>> 1) ^ (y & 1 ? 0x9908b0df : 0);
      }
      index = 0;
    }
    let y = mt[index++];

    y ^= y >>> 11;
    y ^= (y << 7) & 0x9d2c5680;
    y ^= (y << 15) & 0xefc60000;
    y ^= y >>> 18;

    return y >>> 0;
  };

  return (group: readonly number[]) => {
    const bits = 32 - Math.clz32(group.length);
    let picked: number;

    do {
      picked = next() >>> (32 - bits);
    } while (picked >= group.length);

    return group[picked];
  };
};

const mean = (values: readonly number[]) =>
  values.reduce((sum, value) => sum + value, 0) / values.length;

const median = (values: readonly number[]) => {
  const sorted = [...values].sort((a, b) => a - b);

  return (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2;
};

const estimates = (cohorts: readonly (readonly Block[])[], pick: (block: Block) => number) => {
  const values = cohorts.map((cohort) => cohort.map(pick));
  const choice = bootstrapChoice();
  const count = values.reduce((n, group) => n + group.length, 0);

  const boot = Array.from({ length: 20000 }, () => {
    let total = 0;

    for (const group of values) for (let i = 0; i < group.length; i++) total += choice(group);

    return total / count;
  }).sort((a, b) => a - b);

  return { mean: mean(values.flat()), low: boot[500], high: boot[19499] };
};

export const analyze = Effect.fn("analyze")(function* (runs: readonly string[]) {
  const seen = new Set<string>();
  const bundles = new Map<string, string>();
  const cohorts = yield* Effect.forEach(runs, (root) => qualifyRun(root, seen, bundles));
  const pairs = cohorts.reduce((count, cohort) => count + cohort.length, 0);

  yield* requireEvidence(
    cohorts.length >= 2 && cohorts.every((cohort) => cohort.length > 0) && pairs >= 12,
    "Need >=12 pairs in >=2 deployments",
  );

  const window = (pick: (block: Block) => typeof Window.Type) => ({
    worker: estimates(cohorts, (block) => pick(block).worker),
    object: estimates(cohorts, (block) => pick(block).object),
  });

  const turn = (pick: (block: Block) => typeof Turn.Type) => ({
    ...window(pick),
    firstText: estimates(cohorts, (block) => pick(block).firstText),
    afterObject: estimates(cohorts, (block) => pick(block).afterObject),
  });

  const gap = (a: typeof Window.Type, b: typeof Window.Type) => ({
    worker: a.worker - b.worker,
    object: a.object - b.object,
  });

  const turnGap = (a: typeof Turn.Type, b: typeof Turn.Type) => ({
    ...gap(a, b),
    firstText: a.firstText - b.firstText,
    afterObject: a.afterObject - b.afterObject,
  });

  return {
    pairs,
    deployments: cohorts.length,
    arms: {
      "yielded-single": turn((b) => b["yielded-single"]),
      "pi-single": turn((b) => b["pi-single"]),
      "yielded-split": turn((b) => b["yielded-split"]),
      "pi-split": turn((b) => b["pi-split"]),
      "yielded-empty": window((b) => b["yielded-empty"]),
      "pi-empty": window((b) => b["pi-empty"]),
    },
    contrasts: {
      single: turn((b) => turnGap(b["yielded-single"], b["pi-single"])),
      split: turn((b) => turnGap(b["yielded-split"], b["pi-split"])),
      empty: window((b) => gap(b["yielded-empty"], b["pi-empty"])),
      "yielded-split-minus-single": estimates(
        cohorts,
        (b) => b["yielded-split"].firstText - b["yielded-single"].firstText,
      ),
      "pi-split-minus-single": estimates(
        cohorts,
        (b) => b["pi-split"].firstText - b["pi-single"].firstText,
      ),
    },
    medians: {
      emptyObjectPairedGapMs: median(
        cohorts.flat().map((b) => b["yielded-empty"].object - b["pi-empty"].object),
      ),
      yieldedEmptyObjectWindowMs: median(cohorts.flat().map((b) => b["yielded-empty"].object)),
    },
  };
});

// Python's .0f uses ties-to-even, including the saved 129.5 / 2494.5 endpoints.
const rounded = (value: number) => {
  const floor = Math.floor(value);
  const n = value - floor === 0.5 ? (floor % 2 === 0 ? floor : floor + 1) : Math.round(value);

  return n === 0 && value < 0 ? "-0" : String(n);
};

const fmt = (value: ReturnType<typeof estimates>) =>
  `${rounded(value.mean)} [${rounded(value.low)}, ${rounded(value.high)}]`;

export const command = Command.make(
  "analyze",
  {
    runs: Argument.String("runs").pipe(
      Argument.variadic({ min: 1 }),
      Argument.withDescription("Private saved deployment directories"),
    ),
    out: Flag.String("out").pipe(Flag.withDescription("Private analysis JSON output")),
  },
  Effect.fn(function* ({ runs, out }) {
    const result = yield* analyze(runs);

    yield* save(out, result);
    yield* Console.log(
      "| Layout | Yielded first text (ms) | pi first text (ms) | Paired gap (ms) |",
    );
    yield* Console.log("| --- | ---: | ---: | ---: |");
    for (const shape of ["single", "split"] as const) {
      yield* Console.log(
        `| ${shape} | ${fmt(result.arms[`yielded-${shape}`].firstText)} | ${fmt(result.arms[`pi-${shape}`].firstText)} | ${fmt(result.contrasts[shape].firstText)} |`,
      );
    }
    yield* Console.log("Empty Object window gap:", fmt(result.contrasts.empty.object));
    yield* Console.log("After Object marker gap:", fmt(result.contrasts.split.afterObject));
  }),
).pipe(
  Command.withDescription(
    `Validate first use of ${arms.length} arms and report paired, deployment-stratified 95% CIs.`,
  ),
);
