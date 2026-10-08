import { Effect, Exit } from "effect";

/** Separate synchronous race supplement; timing belongs to deployed telemetry. */
export const cases = [
  "empty", "sync", "race-winner-first", "race-loser-first", "allocate",
] as const;

export type Case = (typeof cases)[number];
export type Sql = { exec(query: string, value: number): { one(): { n: number } } };

const body = (value: number) => (value + 1) & 255;
// Preserve result order: a cyclic shift must not disappear into a simple sum.
const mix = (checksum: number, value: number) => Math.imul(checksum ^ value, 16_777_619) >>> 0;

export const runCase = (
  name: Case,
  mode: "effect" | "plain",
  iterations: number,
  _sql: Sql,
): number => {
  let checksum = 0;
  if (name === "allocate") {
    // Retained for the inherited GC controller; no race or interpreter is involved.
    const ring: Array<{ value: number }> = Array.from({ length: 4096 }, () => ({ value: 0 }));
    for (let i = 0; i < iterations; i++) {
      const index = i & 4095;
      checksum = mix(checksum, ring[index]!.value);
      if (mode === "effect") {
        const allocated = Exit.succeed(i);
        if (Exit.isSuccess(allocated)) ring[index] = allocated;
      } else ring[index] = { value: i };
    }
    for (const value of ring) checksum = mix(checksum, value.value);
    return checksum;
  }
  if (name === "empty" || mode === "plain") {
    for (let i = 0; i < iterations; i++) {
      const input = i & 255;
      checksum = mix(checksum, name === "empty" ? input : body(input));
    }
    return checksum;
  }
  return Effect.runSync(Effect.gen(function* () {
    for (let i = 0; i < iterations; i++) {
      const input = i & 255;
      const winner = Effect.sync(() => body(input));
      // Winner-first can finish before the loser starts. Reversing the operands
      // arms the never-ending child and exercises interruption before returning.
      const value = yield* (name === "race-winner-first"
        ? Effect.raceFirst(winner, Effect.never)
        : name === "race-loser-first"
          ? Effect.raceFirst(Effect.never, winner)
          : winner);
      checksum = mix(checksum, value);
    }
    return checksum;
  }));
};
