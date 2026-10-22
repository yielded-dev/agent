import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import type { ThreadId } from "../../core/Identifiers.ts";
import { type Claim, LedgerError, OwnershipLost } from "../SubmissionLedger.ts";

/**
 * Runs an Effect under the physical storage owner's shared mutation serialization.
 *
 * @category models
 * @since 0.1.0
 */
export type MutationGate = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;

/**
 * Local capability only. It is never encoded, persisted, or reconstructed from an Attempt ID.
 *
 * @category models
 * @since 0.1.0
 */
export interface OwnerGeneration {
  readonly claim: Claim;
  readonly identity: object;
  readonly checkActive: Effect.Effect<void, OwnershipLost>;
  readonly release: Effect.Effect<void>;
  readonly use: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | OwnershipLost, R>;
}

/**
 * Shared process-local ownership registry and mutation/progress gates for all facets of one
 * physical storage owner.
 *
 * @category models
 * @since 0.1.0
 */
export interface StorageOwnerService {
  readonly identity: object;
  readonly withMutation: MutationGate;
  readonly withProgress: <A, E, R>(
    threadId: ThreadId,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly isClosed: () => boolean;
  readonly hasOwner: (threadId: string, submissionId?: string) => boolean;
  /** Trusted administrative retirement, while holding the same mutation gate. */
  readonly closeSubmission: (threadId: string, submissionId: string) => void;
  readonly acquire: <E, R>(
    threadId: ThreadId,
    select: Effect.Effect<Option.Option<Claim>, E, R>,
  ) => Effect.Effect<Option.Option<OwnerGeneration>, E | LedgerError, R | Scope.Scope>;
}

/**
 * Provides the shared local ownership capability used to fence claims and storage mutations.
 *
 * @category services
 * @since 0.1.0
 */
export class StorageOwner extends Context.Service<StorageOwner, StorageOwnerService>()(
  "@effect-agent/durable/StorageOwner",
) {}

const ActiveOwner = Context.Reference<object | undefined>(
  "@effect-agent/durable/internal/ActiveStorageOwner",
  { defaultValue: () => undefined },
);

const ActiveProgress = Context.Reference<
  { readonly owner: object; readonly threadId: ThreadId } | undefined
>("@effect-agent/durable/internal/ActiveProgress", { defaultValue: () => undefined });

/**
 * Reuse the physical owner's gate; reentrant calls remain in the same mutation.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeStorageOwner = Effect.fnUntraced(function* (gate: MutationGate) {
  const identity = {};
  let closed = false;
  const progress = new Map<ThreadId, { readonly gate: Semaphore.Semaphore; users: number }>();
  const active = new Map<string, { readonly claim: Claim; closed: boolean }>();

  const withMutation: MutationGate = (effect) =>
    Effect.withFiber((fiber) => {
      const current = fiber.getRef(ActiveOwner);

      return current === identity
        ? effect
        : gate(Effect.provideService(effect, ActiveOwner, identity));
    });

  yield* Effect.addFinalizer(() =>
    withMutation(
      Effect.sync(() => {
        closed = true;
        for (const entry of active.values()) entry.closed = true;
        active.clear();
      }),
    ),
  );

  return StorageOwner.of({
    identity,
    withMutation,
    withProgress: (threadId, effect) =>
      Effect.withFiber((fiber) => {
        const current = fiber.getRef(ActiveProgress);

        if (current?.owner === identity && current.threadId === threadId) return effect;

        return Effect.suspend(() => {
          let entry = progress.get(threadId);

          if (entry === undefined) {
            entry = { gate: Semaphore.makeUnsafe(1), users: 0 };
            progress.set(threadId, entry);
          }
          const retained = entry;

          retained.users++;

          return retained.gate
            .withPermits(1)(
              Effect.provideService(effect, ActiveProgress, { owner: identity, threadId }),
            )
            .pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  retained.users--;
                  if (retained.users === 0 && progress.get(threadId) === retained)
                    progress.delete(threadId);
                }),
              ),
            );
        });
      }),
    isClosed: () => closed,
    hasOwner: (threadId, submissionId) => {
      const entry = active.get(threadId);

      return (
        !closed &&
        entry !== undefined &&
        !entry.closed &&
        (submissionId === undefined || entry.claim.submissionId === submissionId)
      );
    },
    closeSubmission: (threadId, submissionId) => {
      const entry = active.get(threadId);

      if (entry?.claim.submissionId !== submissionId) return;
      entry.closed = true;
      active.delete(threadId);
    },
    acquire: (threadId, select) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const selected = yield* withMutation(
            Effect.gen(function* () {
              if (closed)
                return yield* LedgerError.make({
                  operation: "claim",
                  message: "Storage owner is closed",
                });
              if (active.has(threadId)) return Option.none<OwnerGeneration>();
              const selected = yield* select;

              if (Option.isNone(selected)) return Option.none<OwnerGeneration>();
              const entry = { claim: selected.value, closed: false };

              active.set(threadId, entry);

              const checkActive = Effect.suspend(() =>
                closed || entry.closed || active.get(threadId) !== entry
                  ? Effect.fail(OwnershipLost.make({ submissionId: entry.claim.submissionId }))
                  : Effect.void,
              );

              const release = withMutation(
                Effect.sync(() => {
                  entry.closed = true;
                  if (active.get(threadId) === entry) active.delete(threadId);
                }),
              );

              return Option.some<OwnerGeneration>({
                claim: entry.claim,
                identity: entry,
                checkActive,
                release,
                use: (effect) => withMutation(Effect.andThen(checkActive, effect)),
              });
            }),
          );

          if (Option.isNone(selected)) return selected;
          // A closed Scope runs this immediately. Never register while holding a nonlocal gate.
          yield* Effect.addFinalizer(() => selected.value.release);

          return yield* restore(Effect.succeed(selected));
        }),
      ),
  });
});
