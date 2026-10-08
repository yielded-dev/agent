import process from "node:process";
import { cpus, loadavg } from "node:os";
import { Effect, Exit } from "effect";
import { writeEvidence } from "./evidence.js";

const ticks = () => cpus().reduce((sum, { times }) => {
  const total = times.user + times.nice + times.sys + times.irq + times.idle;
  return { total: sum.total + total, busy: sum.busy + total - times.idle };
}, { total: 0, busy: 0 });

// The stock profiling mode selects the continuous loop without per-operation clocks.
export const makeCpuProfiler = Effect.succeed({
  capture: (operation, filename) => Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
    const hostBefore = ticks();
    const loadBefore = loadavg();
    const start = process.hrtime.bigint();
    const cpuBefore = process.cpuUsage();
    const result = yield* restore(operation).pipe(Effect.exit);
    const cpu = process.cpuUsage(cpuBefore);
    const wallMs = Number(process.hrtime.bigint() - start) / 1e6;
    const hostAfter = ticks();
    yield* writeEvidence(filename, JSON.stringify({
      measurement: "kom433-process-cpu-v1",
      capture: "resident checked operation loop only; no Inspector and no primitive/stage hooks",
      cpu: { userMicros: cpu.user, systemMicros: cpu.system, totalMicros: cpu.user + cpu.system },
      wallMs,
      node: process.version,
      execPath: process.execPath,
      pid: process.pid,
      success: Exit.isSuccess(result),
      host: { loadBefore, loadAfter: loadavg(), busyFraction: (hostAfter.busy - hostBefore.busy) / (hostAfter.total - hostBefore.total) },
    }, null, 2) + "\n");
    if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause);
    return { value: result.value, profileDurationMs: null };
  })),
});
