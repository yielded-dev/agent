// Task-local, explicitly requested proof: no clocks or timing measurements.
// Run from the repository root:
// vp node examples/durable-bench/results/effect-eval-cost/calibration-race/proof.mjs > examples/durable-bench/results/effect-eval-cost/calibration-race/proof.json
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { Effect } from "effect";
import { cases, runCase } from "./cases.ts";

const sha256 = (url) => createHash("sha256").update(readFileSync(url)).digest("hex");
const lifecycle = [];
for (const name of ["race-winner-first", "race-loser-first"]) {
  const iterations = 32;
  const winners = Array(iterations).fill(0);
  const starts = Array(iterations).fill(0);
  const finalizers = Array(iterations).fill(0);
  const values = Effect.runSync(Effect.gen(function* () {
    const output = [];
    for (let i = 0; i < iterations; i++) {
      const winner = Effect.sync(() => { winners[i]++; return i + 1; });
      // Counters decorate only the proof loser, never the timed fixture.
      const loser = Effect.ensuring(
        Effect.andThen(Effect.sync(() => { starts[i]++; }), Effect.never),
        Effect.sync(() => { finalizers[i]++; }),
      );
      const value = yield* (name === "race-winner-first"
        ? Effect.raceFirst(winner, loser)
        : Effect.raceFirst(loser, winner));
      assert.equal(value, i + 1);
      assert.equal(winners[i], 1);
      assert.equal(starts[i], name === "race-loser-first" ? 1 : 0);
      assert.equal(finalizers[i], name === "race-loser-first" ? 1 : 0);
      output.push(value);
    }
    return output;
  }));
  assert.deepEqual(values, Array.from({ length: iterations }, (_, i) => i + 1));
  lifecycle.push({ case: name, iterations, winners, loserStarts: starts, loserFinalizers: finalizers });
}

const sql = { exec() { throw new Error("Race supplement must not use SQL"); } };
const checksums = [];
for (const iterations of [0, 1, 17, 257, 1000]) {
  for (const name of cases) {
    const plain = runCase(name, "plain", iterations, sql);
    const effect = runCase(name, "effect", iterations, sql);
    assert.equal(effect, plain, `${name}/${iterations}`);
    if (name.startsWith("race-")) assert.equal(effect, runCase("sync", "effect", iterations, sql));
    checksums.push({ case: name, iterations, plain, effect });
  }
}
assert.notEqual(runCase("empty", "plain", 256, sql), runCase("sync", "plain", 256, sql));

console.log(JSON.stringify({
  passed: true,
  node: process.version,
  effectVersion: JSON.parse(readFileSync(new URL("../../../../../node_modules/effect/package.json", import.meta.url), "utf8")).version,
  casesSha256: sha256(new URL("./cases.ts", import.meta.url)),
  proofSha256: sha256(new URL("./proof.mjs", import.meta.url)),
  effectPatchSha256: sha256(new URL("../../../../../patches/effect@4.0.0.patch", import.meta.url)),
  lifecycle,
  checksums,
}, null, 2));
