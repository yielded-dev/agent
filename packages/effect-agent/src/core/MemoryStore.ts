/**
 * Authoritative memory documents with conditional writes, durable idempotency receipts, and
 * terminal withdrawal. Reader and writer capabilities remain separate.
 *
 * @since 0.1.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as MemoryNamespace from "./MemoryNamespace.ts";
import { MemoryContent, MemorySourceReference } from "./MemoryReference.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const Timestamp = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * Host-defined recall visibility label. A scope identifies access policy; it does not grant it.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MemoryScope = Identity.pipe(Schema.brand("@effect-agent/core/MemoryScope"));
/**
 * Decoded value of {@link MemoryScope}.
 *
 * @category schemas
 * @since 0.1.0
 */
export type MemoryScope = typeof MemoryScope.Type;

/** A host-selected namespace prevents accidental cross-tenant document lookup. */
class MemoryKeyWire extends Schema.Class<MemoryKeyWire>("@effect-agent/core/MemoryKey")({
  namespace: MemoryNamespace.Any,
  id: MemorySourceReference.fields.id,
}) {}

export type {
  /**
   * Portable memory key decoded by {@link MemoryKey.Wire} before namespace restoration.
   *
   * @category models
   * @since 0.1.0
   */
  MemoryKeyWire,
};

/**
 * Type produced by {@link MemoryKey}.
 *
 * @category utility types
 * @since 0.1.0
 */
export type MemoryKey<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> = Omit<
  MemoryKeyWire,
  "namespace"
> & { readonly namespace: Namespace };

/**
 * Schema and constructor pairing a typed namespace with an authoritative source identifier.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MemoryKey = {
  Wire: MemoryKeyWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryKey<Namespace>,
  ): MemoryKey<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryKeyWire)(fields), { namespace: fields.namespace }),
};

const KnownSource = Schema.Struct({
  ...MemorySourceReference.fields,
  revision: Identity,
});

const AccessScopes = Schema.Array(MemoryScope).check(
  Schema.isMaxLength(128),
  Schema.makeFilter((scopes) => new Set(scopes).size === scopes.length, {
    expected: "unique access scopes",
  }),
);

const DocumentFields = {
  version: Schema.Literal(1),
  key: MemoryKey.Wire,
  source: KnownSource,
  generation: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER })),
  predecessor: Schema.NullOr(MemorySourceReference),
  modifiedAt: Timestamp,
};

class ActiveMemoryDocumentWire extends Schema.TaggedClass<ActiveMemoryDocumentWire>()(
  "ActiveMemoryDocument",
  {
    ...DocumentFields,
    content: MemoryContent,
    /** Explicit application-defined scope names. Empty grants no recall access. */
    scopes: AccessScopes,
  },
) {}

/** Terminal tombstone. Reusing its ID cannot restore the source through delayed work. */
class WithdrawnMemoryDocumentWire extends Schema.TaggedClass<WithdrawnMemoryDocumentWire>()(
  "WithdrawnMemoryDocument",
  {
    ...DocumentFields,
    reason: Schema.String.check(Schema.isMaxLength(4_096)),
  },
) {}

export type {
  /**
   * Active document decoded by {@link ActiveMemoryDocument.Wire} before namespace restoration.
   *
   * @category models
   * @since 0.1.0
   */
  ActiveMemoryDocumentWire,
  /**
   * Withdrawal decoded by {@link WithdrawnMemoryDocument.Wire} before namespace restoration.
   *
   * @category models
   * @since 0.1.0
   */
  WithdrawnMemoryDocumentWire,
};

/**
 * Type produced by {@link ActiveMemoryDocument}.
 *
 * @category utility types
 * @since 0.1.0
 */
export type ActiveMemoryDocument<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> =
  Omit<ActiveMemoryDocumentWire, "key"> & { readonly key: MemoryKey<Namespace> };

/**
 * Schema and constructor for current authoritative content, revision lineage, and explicit recall
 * access scopes.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ActiveMemoryDocument = {
  Wire: ActiveMemoryDocumentWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: Omit<ActiveMemoryDocument<Namespace>, "_tag"> & {
      readonly _tag?: "ActiveMemoryDocument";
    },
  ): ActiveMemoryDocument<Namespace> =>
    Object.assign(
      Schema.decodeUnknownSync(ActiveMemoryDocumentWire)({
        _tag: "ActiveMemoryDocument",
        ...fields,
      }),
      { key: MemoryKey.make(fields.key) },
    ),
};

/**
 * Type produced by {@link WithdrawnMemoryDocument}.
 *
 * @category utility types
 * @since 0.1.0
 */
export type WithdrawnMemoryDocument<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> =
  Omit<WithdrawnMemoryDocumentWire, "key"> & { readonly key: MemoryKey<Namespace> };

/**
 * Schema and constructor for a terminal source tombstone. Delayed writes cannot restore a withdrawn
 * source.
 *
 * @category schemas
 * @since 0.1.0
 */
export const WithdrawnMemoryDocument = {
  Wire: WithdrawnMemoryDocumentWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: Omit<WithdrawnMemoryDocument<Namespace>, "_tag"> & {
      readonly _tag?: "WithdrawnMemoryDocument";
    },
  ): WithdrawnMemoryDocument<Namespace> =>
    Object.assign(
      Schema.decodeUnknownSync(WithdrawnMemoryDocumentWire)({
        _tag: "WithdrawnMemoryDocument",
        ...fields,
      }),
      { key: MemoryKey.make(fields.key) },
    ),
};

/**
 * Type produced by {@link MemoryDocument}.
 *
 * @category utility types
 * @since 0.1.0
 */
export type MemoryDocument<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> =
  | ActiveMemoryDocument<Namespace>
  | WithdrawnMemoryDocument<Namespace>;

/**
 * Schemas for active and withdrawn documents, with restoration that validates the caller’s
 * namespace before typed use.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MemoryDocument = {
  Wire: Schema.Union([ActiveMemoryDocument.Wire, WithdrawnMemoryDocument.Wire]),
  restore: Effect.fnUntraced(function* <Namespace extends MemoryNamespace.Any>(
    namespace: Namespace,
    input: unknown,
  ): Effect.fn.Return<MemoryDocument<Namespace>, MemoryStorageError> {
    const document = yield* Schema.decodeUnknownEffect(MemoryDocument.Wire)(input).pipe(
      Effect.mapError(() =>
        MemoryStorageError.make({ operation: "restore memory document", reason: "corrupt" }),
      ),
    );

    if (!MemoryNamespace.equals(namespace, document.key.namespace))
      return yield* MemoryStorageError.make({
        operation: "restore memory namespace",
        reason: "corrupt",
      });
    const key = MemoryKey.make({ ...document.key, namespace });

    return document._tag === "ActiveMemoryDocument"
      ? ActiveMemoryDocument.make({ ...document, key })
      : WithdrawnMemoryDocument.make({ ...document, key });
  }),
};

const WriteFields = {
  key: MemoryKey.Wire,
  /** The same operation ID must always carry exactly the same Schema-encoded command. */
  operationId: Identity,
};

const MemoryWriteWire = Schema.Union([
  Schema.TaggedStruct("Put", {
    ...WriteFields,
    expectedRevision: Schema.NullOr(Identity),
    locator: MemorySourceReference.fields.locator,
    content: MemoryContent,
    scopes: AccessScopes,
  }),
  Schema.TaggedStruct("Withdraw", {
    ...WriteFields,
    expectedRevision: Identity,
    reason: WithdrawnMemoryDocument.Wire.fields.reason,
  }),
]);

/**
 * Decoded value of {@link MemoryWrite}.
 *
 * @category schemas
 * @since 0.1.0
 */
export type MemoryWrite<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> = (
  | Omit<(typeof MemoryWriteWire.members)[0]["Type"], "key">
  | Omit<(typeof MemoryWriteWire.members)[1]["Type"], "key">
) & { readonly key: MemoryKey<Namespace> };

/**
 * Schema and constructor for conditional Put or Withdraw commands. Reusing an operation ID requires
 * the exact same encoded command.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MemoryWrite = {
  Wire: MemoryWriteWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryWrite<Namespace>,
  ): MemoryWrite<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryWriteWire)(fields), {
      key: MemoryKey.make(fields.key),
    }),
};

/**
 * A conditional write’s expected revision differs from the current source revision.
 *
 * @category errors
 * @since 0.1.0
 */
export class MemoryConflict extends Schema.TaggedError<MemoryConflict>()("MemoryConflict", {
  key: MemoryKey.Wire,
  expectedRevision: Schema.NullOr(Identity),
  actualRevision: Schema.NullOr(Identity),
}) {}

/**
 * The source is permanently withdrawn and cannot accept another write.
 *
 * @category errors
 * @since 0.1.0
 */
export class MemoryWithdrawn extends Schema.TaggedError<MemoryWithdrawn>()("MemoryWithdrawn", {
  key: MemoryKey.Wire,
  revision: Identity,
}) {}

/**
 * An operation ID was reused for a different encoded memory command.
 *
 * @category errors
 * @since 0.1.0
 */
export class MemoryOperationConflict extends Schema.TaggedError<MemoryOperationConflict>()(
  "MemoryOperationConflict",
  {
    key: MemoryKey.Wire,
    operationId: Identity,
  },
) {}

/**
 * An authoritative memory operation failed validation, storage availability, integrity checks, or
 * format compatibility.
 *
 * @category errors
 * @since 0.1.0
 */
export class MemoryStorageError extends Schema.TaggedError<MemoryStorageError>()(
  "MemoryStorageError",
  {
    operation: Schema.NonEmptyString,
    reason: Schema.Literals(["invalid-input", "unavailable", "corrupt", "incompatible"]),
  },
) {}

/**
 * Schema for fault-injection boundaries during memory-store initialization and mutation.
 *
 * @category schemas
 * @since 0.1.0
 */
export const MemoryMutationPoint = Schema.Literals([
  "memory:initialize:before",
  "memory:initialize:before-accounting",
  "memory:initialize:after-accounting",
  "memory:initialize:after",
  "memory:change:before",
  "memory:change:after-state",
  "memory:change:after-receipt",
  "memory:change:after",
]);

/**
 * Decoded value of {@link MemoryMutationPoint}.
 *
 * @category models
 * @since 0.1.0
 */
export type MemoryMutationPoint = typeof MemoryMutationPoint.Type;

/**
 * A configured memory fault-injection boundary interrupted an operation.
 *
 * @category errors
 * @since 0.1.0
 */
export class MemoryMutationFailure extends Schema.TaggedError<MemoryMutationFailure>()(
  "MemoryMutationFailure",
  {
    point: MemoryMutationPoint,
  },
) {}

/**
 * Explicit fault-injection seam; production adapters install the no-op Layer.
 *
 * @category services
 * @since 0.1.0
 */
export class MemoryMutationFailpoint extends Context.Service<
  MemoryMutationFailpoint,
  {
    readonly hit: (point: MemoryMutationPoint) => Effect.Effect<void, MemoryMutationFailure>;
  }
>()("@effect-agent/core/MemoryMutationFailpoint") {
  static readonly layer = Layer.succeed(this, { hit: () => Effect.void });
}

/**
 * Expected storage, conditional-write, withdrawn-source, idempotency, and fault-injection failures
 * from a memory write.
 *
 * @category models
 * @since 0.1.0
 */
export type MemoryWriteError =
  | MemoryStorageError
  | MemoryConflict
  | MemoryWithdrawn
  | MemoryOperationConflict
  | MemoryMutationFailure;

/**
 * Read capability independent of editing. A get begun after a committed write must observe
 * that write or a later revision. An adapter unable to provide that view must fail typed.
 * Host code binds namespace and access scope; cached candidates do not implement this port.
 *
 * @category services
 * @since 0.1.0
 */
export class MemoryReader extends Context.Service<
  MemoryReader,
  {
    readonly get: <Namespace extends MemoryNamespace.Any>(
      key: MemoryKey<Namespace>,
    ) => Effect.Effect<MemoryDocument<Namespace> | null, MemoryStorageError>;
  }
>()("@effect-agent/core/MemoryReader") {
  static fromAdapter(adapter: {
    readonly get: (key: MemoryKey) => Effect.Effect<MemoryDocument | null, MemoryStorageError>;
  }): MemoryReader["Service"] {
    return {
      get: Effect.fnUntraced(function* <Namespace extends MemoryNamespace.Any>(
        key: MemoryKey<Namespace>,
      ) {
        const document = yield* adapter.get(key);

        return document === null ? null : yield* MemoryDocument.restore(key.namespace, document);
      }),
    };
  }
}

/**
 * Optional conditional writer. Implement only where atomic expected-revision checks and
 * durable idempotency receipts can be guaranteed. Apply authorization before calling it.
 * Reconcile an existing operation receipt before evaluating tombstones or expected revisions:
 * identical commands return the original result; changed commands fail MemoryOperationConflict.
 * Original results include content, even for a historical Put replayed after withdrawal;
 * replay does not change the current head. Revalidate a source before using recalled content.
 * A failed acknowledgement can hide a committed change: retain and retry the exact command.
 * Successful withdrawal excludes checks begun afterward; already captured views may finish.
 * Receipts and original Thread history are separate retention concerns.
 *
 * @category services
 * @since 0.1.0
 */
export class MemoryWriter extends Context.Service<
  MemoryWriter,
  {
    readonly change: <Namespace extends MemoryNamespace.Any>(
      write: MemoryWrite<Namespace>,
    ) => Effect.Effect<MemoryDocument<Namespace>, MemoryWriteError>;
  }
>()("@effect-agent/core/MemoryWriter") {
  static fromAdapter(adapter: {
    readonly change: (write: MemoryWrite) => Effect.Effect<MemoryDocument, MemoryWriteError>;
  }): MemoryWriter["Service"] {
    return {
      change: Effect.fnUntraced(function* <Namespace extends MemoryNamespace.Any>(
        write: MemoryWrite<Namespace>,
      ) {
        const document = yield* adapter.change(write);

        return yield* MemoryDocument.restore(write.key.namespace, document);
      }),
    };
  }
}

/**
 * Transition mechanics for an authoritative document store. Revisions identify this readable
 * source, not the original activity from which a consumer extracted it. The adapter reconciles
 * receipts first and owns atomicity and the clock. Original evidence belongs in provenance.
 *
 * @category operations
 * @since 0.1.0
 */
export const applyMemoryWrite = Effect.fnUntraced(function* <Namespace extends MemoryNamespace.Any>(
  current: MemoryDocument<NoInfer<Namespace>> | null,
  write: MemoryWrite<Namespace>,
  modifiedAt: number,
) {
  if (
    current !== null &&
    (!MemoryNamespace.equals(current.key.namespace, write.key.namespace) ||
      current.key.id !== write.key.id ||
      current.source.id !== write.key.id)
  ) {
    return yield* MemoryStorageError.make({
      operation: "memory transition identity",
      reason: "corrupt",
    });
  }
  if (current?._tag === "WithdrawnMemoryDocument") {
    return yield* MemoryWithdrawn.make({ key: write.key, revision: current.source.revision });
  }
  const actualRevision = current?.source.revision ?? null;

  if (actualRevision !== write.expectedRevision) {
    return yield* MemoryConflict.make({
      key: write.key,
      expectedRevision: write.expectedRevision,
      actualRevision,
    });
  }
  const generation = (current?.generation ?? 0) + 1;

  const fields = {
    version: 1 as const,
    key: write.key,
    source: {
      id: write.key.id,
      locator: write._tag === "Put" ? write.locator : (current?.source.locator ?? ""),
      revision: String(generation),
    },
    generation,
    predecessor: current?.source ?? null,
    modifiedAt,
  };

  const next =
    write._tag === "Put"
      ? {
          _tag: "ActiveMemoryDocument" as const,
          ...fields,
          content: write.content,
          scopes: write.scopes,
        }
      : { _tag: "WithdrawnMemoryDocument" as const, ...fields, reason: write.reason };

  const document = yield* Schema.decodeUnknownEffect(MemoryDocument.Wire)(next).pipe(
    Effect.mapError(() =>
      MemoryStorageError.make({ operation: "memory transition", reason: "invalid-input" }),
    ),
  );

  return yield* MemoryDocument.restore(write.key.namespace, document);
});
