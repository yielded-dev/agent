import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Clock, Effect, FileSystem, Schema } from "effect";

import type { CostPlan, Options } from "./model.ts";
import { BenchError, hash, stateDirectory } from "./platform.ts";
import type { BulkFixture } from "./worker/protocol.ts";

export const DEFAULT_ROW_LIMIT = 1_000_000;
const DAY = 86_400_000;

/** Admission estimate, not an invoice. Reserve retries and recovery before any deployment.
 * Index entries and implicit UNIQUE/PRIMARY KEY indexes count as writes. The extra import
 * copy funds one complete Object/pass recovery; runtime writes have a per-turn allowance.
 * Native counters in the result are evidence, not a substitute for Cloudflare billing.
 */
export const planCost = (
  options: Options,
  fixtures: readonly BulkFixture[],
  estimates: ReadonlyMap<string, number>,
): typeof CostPlan.Type => {
  const passes = options.buildHistory ? 1 : options.rigorous ? 4 : options.baseline ? 2 : 1;
  const copies = options.objects * options.ttft.length;
  let oneImport = 0;

  for (const fixture of fixtures) {
    if (fixture.target === "effect-ai-floor") continue;
    oneImport += estimates.get(`${fixture.target}-${fixture.history}`) ?? Number.POSITIVE_INFINITY;
    // Tardie's directory is tiny; conservatively allow every row in every explicit index.
    oneImport += (fixture.actor?.tables ?? []).reduce(
      (sum, table) => sum + table.rows.length * (2 + (fixture.actor?.indexes.length ?? 0)),
      0,
    );
    // DDL, chunk acknowledgements, and constructor bookkeeping.
    oneImport += 1_000;
  }

  const imports =
    fixtures.filter((fixture) => fixture.target !== "effect-ai-floor").length * copies * passes;

  const importRows = oneImport * copies * passes * 8; // initial attempt + three retries, two copies each

  const measuredTurns =
    options.targets.length *
    options.sizes.length *
    copies *
    passes *
    (options.repeats + (options.buildHistory ? 1 : 1 + (options.coldSamples ?? 2)));

  const historyTurns = options.buildHistory
    ? options.targets.length * copies * options.sizes.reduce((sum, size) => sum + size, 0)
    : 0;

  const turns = measuredTurns + historyTurns;
  const turnRows = turns * 2_000 * 2; // allowance includes driver receipts and one recovery

  return { imports, importRows, turns, turnRows, reservedRows: importRows + turnRows + 1_000 };
};

export const checkCost = (options: Options, plan: typeof CostPlan.Type) => {
  const limit = options.maxWrittenRows ?? DEFAULT_ROW_LIMIT;
  const daily = options.dailyWrittenRows ?? DEFAULT_ROW_LIMIT;

  if (
    (limit > DEFAULT_ROW_LIMIT ||
      daily > DEFAULT_ROW_LIMIT ||
      options.rigorous ||
      options.sizes.some((size) => size > 1_000)) &&
    !options.budgetReason?.trim()
  )
    return Effect.fail(
      BenchError.make({
        message:
          "Large runs and raised write budgets require --budget-reason describing the decision this run will support.",
      }),
    );
  if (plan.reservedRows > limit || plan.reservedRows > daily)
    return Effect.fail(
      BenchError.make({
        message: `Write budget refused: ${plan.reservedRows.toLocaleString("en-US")} reserved rows (${plan.importRows.toLocaleString("en-US")} setup; ${plan.turnRows.toLocaleString("en-US")} turns), above the run/24h limit. Use --preset screen or explicitly raise --max-written-rows and --daily-written-rows with --budget-reason.`,
      }),
    );

  return Effect.void;
};

/** All prefixes/worktrees on this machine share an account ledger. Reservations never refund:
 * killed processes, unknown outcomes, retries, and resumed runs cannot erase their admission.
 * A SQLite transaction serializes admission; it does not serialize benchmark execution.
 */
export const reserveCost = Effect.fnUntraced(function* (
  account: string,
  run: string,
  options: Options,
  plan: typeof CostPlan.Type,
) {
  yield* checkCost(options, plan);
  const fs = yield* FileSystem.FileSystem;
  const directory = stateDirectory("durable-bench-budget");

  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  const now = yield* Clock.currentTimeMillis;

  const result = yield* Effect.try({
    try: () => {
      const db = new DatabaseSync(join(directory, "reservations.sqlite"));

      try {
        db.exec(
          "PRAGMA busy_timeout = 5000; CREATE TABLE IF NOT EXISTS reservations (id INTEGER PRIMARY KEY, account TEXT NOT NULL, run TEXT NOT NULL, at INTEGER NOT NULL, rows INTEGER NOT NULL, reason TEXT)",
        );
        db.exec("BEGIN IMMEDIATE");
        const key = hash(account);

        const { rows } = Schema.decodeUnknownSync(Schema.Struct({ rows: Schema.Natural }))(
          db
            .prepare(
              "SELECT COALESCE(SUM(rows), 0) AS rows FROM reservations WHERE account = ? AND at > ?",
            )
            .get(key, now - DAY),
        );

        const limit = options.dailyWrittenRows ?? DEFAULT_ROW_LIMIT;

        if (rows + plan.reservedRows > limit)
          return BenchError.make({
            message: `Rolling 24h write budget exhausted: ${rows} already reserved; ${plan.reservedRows} requested; limit ${limit}. Failed and interrupted runs retain their reservations.`,
          });

        const inserted = db
          .prepare(
            "INSERT INTO reservations (account, run, at, rows, reason) VALUES (?, ?, ?, ?, ?)",
          )
          .run(key, run, now, plan.reservedRows, options.budgetReason ?? null);

        db.exec("COMMIT");

        return {
          id: String(inserted.lastInsertRowid),
          rows: plan.reservedRows,
          rollingRows: rows + plan.reservedRows,
          limit,
          at: now,
        };
      } finally {
        db.close();
      }
    },
    catch: (cause) =>
      Schema.is(BenchError)(cause)
        ? cause
        : BenchError.make({
            message: "Cannot reserve the shared write budget; deployment refused.",
            cause,
          }),
  });

  if (Schema.is(BenchError)(result)) return yield* result;

  return result;
});
