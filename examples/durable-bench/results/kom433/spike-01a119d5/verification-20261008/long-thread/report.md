Manual public-package diagnostics. Elapsed wall milliseconds, median [Q1–Q3]; marks are inclusive. No CPU or provider latency claim.

Status: setup; 0/4 complete batches.
Failure: BenchmarkError: Diagnostics require clean exact-commit checkouts
    at <anonymous> (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/.bun/effect@4.0.0/node_modules/effect/dist/Schema.js:8958:86)
    at transform (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/.bun/effect@4.0.0/node_modules/effect/dist/internal/schema/interpreter.js:21:130)
    at <anonymous> (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/.bun/effect@4.0.0/node_modules/effect/dist/SchemaParser.js:947:38)
    at <anonymous> (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/tooling/runtime-benchmark/src/contracts.ts:11:48)
    at benchmark.check (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/runtime-diagnostics.ts:282:12)
    at benchmark.check (definition) (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/tooling/runtime-benchmark/src/contracts.ts:10:29)
    at diagnostic.compare (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/runtime-diagnostics.ts:488:12)
    at diagnostic.compare (definition) (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/runtime-diagnostics.ts:160:42)
    at Effect.fn (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/node_modules/.bun/effect@4.0.0/node_modules/effect/dist/cli/Command.js:1103:31)
    at Effect.fn (definition) (/Users/dan/dev/effect-agent/.worktrees/dan-KOM-433/scripts/runtime-diagnostics.ts:470:10)

| Case / metric | Base | Head |
| --- | ---: | ---: |
| long-thread-aging-256-131328 / total | n/a | n/a |
| long-thread-store-size / total | n/a | n/a |
