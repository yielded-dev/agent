# Error handling and reporting

Read the installed Effect guidance first. These are Effect 4 shapes; check the
installed implementation before adopting them. Domain operations and helpers
below stand for the project's existing code.

Keep expected failures typed in `E`, preserve original causes internally, and
sanitize at the public or telemetry boundary. `E = never` does not rule out
defects or interruption.

## Translate the whole Cause

Use this specialized pattern when a composed boundary must retain every
failure reason. Keep ordinary `mapError` when its semantics suffice.

```ts
// Avoid when mapError selects only the first typed failure.
work.pipe(Effect.mapError(toDomainError));

// Prefer: map typed failures, retaining defects, interruption and annotations.
work.pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, toDomainError))));
```

Effect's [`wrapEncoding`](https://github.com/Effect-TS/effect/blob/bc07bd4e86ac2b7336844040b2b11e8781b2dee3/packages/effect/src/internal/schema/interpreter.ts#L70-L81)
uses this composition with `failCauseSync`. Keep `toDomainError` pure and
non-throwing. Check installed `orDie` and tag-based recovery for first-error
behavior; do not blanket-rewrite every catch.

## Recover only an entirely expected failure

A `NotFound` plus a cleanup defect must not become successful absence.

```ts
// Avoid at a boundary that can produce mixed Causes.
lookup.pipe(Effect.catchTag("NotFound", () => Effect.succeed(undefined)));

// Prefer: recover only when every reason is NotFound.
lookup.pipe(
  Effect.catchCause((cause) =>
    cause.reasons.length > 0 &&
    cause.reasons.every((reason) => Cause.isFailReason(reason) && reason.error._tag === "NotFound")
      ? Effect.succeed(undefined)
      : Effect.failCause(cause),
  ),
);
```

Keep verification recovery around verification, before calling the protected
handler. Its failures must not become authentication denial.

## Report without swallowing or leaking

After expected domain outcomes have been handled:

```ts
// Avoid: logs private values and turns failure into success.
work.pipe(Effect.catchCause((cause) => Effect.logError(cause)));

// Prefer: observe at the owning boundary and retain the original failure.
work.pipe(
  Effect.onError((cause) =>
    Cause.hasInterruptsOnly(cause) ? Effect.void : reportSafeFailure(toSafeDiagnostic(cause)),
  ),
);
```

`toSafeDiagnostic` allows only finite categories and approved codes: no raw
messages, credentials, SQL values, payloads, or arbitrary URLs.
`reportSafeFailure` owns best-effort sink handling; its failure must not replace
the operation's cause. Report once, while allowing newly added cleanup failures
to be reported. Project a safe public error before crossing the public boundary.

## Attempt independent cleanup

Use Scope ownership for resources. Within an owned cleanup sequence:

```ts
// Avoid: closeFile failing prevents releaseLock.
Effect.gen(function* () {
  yield* closeFile;
  yield* releaseLock;
});

// Prefer: attempt both and retain their failures.
closeFile.pipe(Effect.ensuring(releaseLock));
```

Only use this ordering when releasing the lock is safe even if closing fails.

## Inspect stopped work's result

When stopping an owned fiber whose result has not already been consumed:

```ts
// Avoid: interrupt waits, but does not propagate the child's failure.
Fiber.interrupt(fiber);

// Prefer: also inspect the completed Exit; pure cancellation stays quiet.
Effect.gen(function* () {
  yield* Fiber.interrupt(fiber);
  const exit = yield* Fiber.await(fiber);
  if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)) {
    return yield* Effect.failCause(exit.cause);
  }
});
```

Keep this inside the owner's cleanup/finalizer. Prefer existing scoped
combinators over new lifetime wrappers; verify timeout cleanup too.

## Check the remaining boundaries

- Verify the actual runtime observes failed promises, `Exit`s, Atom results and
  disposal. Providing a Logger alone does not do this; see [Atom lifecycle](atom/effect-atom-lifecycle.md).
- Retry only classified safe failures. A timeout does not prove an external
  side effect rolled back; preserve receipts and unknown outcomes.
- A downstream catch cannot recover a cause swallowed by a dependency. Fix its
  owner and verify published consumers receive the fixed release.
- Follow repository testing policy. Reuse the smallest failing workflow and an
  expected-recovery control; retain evidence and coverage limits in the PR.
  Lint and tsgo diagnostics identify suspects, not proof of cause preservation,
  privacy, reporter installation, or safe retries.
