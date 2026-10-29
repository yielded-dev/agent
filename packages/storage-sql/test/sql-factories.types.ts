import { expectTypeOf } from "@effect/vitest";
import type { AppendConflict, ThreadStoreError } from "@yielded/agent/thread-store";
import * as Context from "effect/Context";
import type * as CryptoDigest from "effect/crypto/Digest";
import type * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as SqlClient from "effect/sql/SqlClient";

import type { makeSqlJournal, SqlJournal } from "../src/SqlJournal.ts";
import type { makeSqlSubmissionLedger } from "../src/SqlSubmissionLedger.ts";
import type { makeSqlThreadStore } from "../src/SqlThreadStore.ts";

class StorageFailure extends Schema.TaggedError<StorageFailure>()("StorageFailure", {}) {}
class Corruption extends Schema.TaggedError<Corruption>()("Corruption", {}) {}
class Contention extends Schema.TaggedError<Contention>()("Contention", {}) {}
class InjectedFailure extends Schema.TaggedError<InjectedFailure>()("InjectedFailure", {}) {}
class BodyFailure extends Schema.TaggedError<BodyFailure>()("BodyFailure", {}) {}
class RequiredService extends Context.Service<RequiredService, {}>()("RequiredService") {}

type Journal = SqlJournal<StorageFailure, Corruption, Contention, InjectedFailure>;
type JournalConstruction = ReturnType<
  typeof makeSqlJournal<StorageFailure, Corruption, Contention, InjectedFailure>
>;
type ThreadConstruction = ReturnType<
  typeof makeSqlThreadStore<StorageFailure, Corruption, Contention, InjectedFailure>
>;
type LedgerConstruction = ReturnType<
  typeof makeSqlSubmissionLedger<StorageFailure, Corruption, Contention, InjectedFailure>
>;

expectTypeOf<Effect.Error<JournalConstruction>>().toEqualTypeOf<StorageFailure>();
expectTypeOf<Effect.Services<JournalConstruction>>().toEqualTypeOf<
  SqlClient.SqlClient | CryptoDigest.Digest | Scope.Scope
>();
expectTypeOf<Effect.Error<ReturnType<Journal["append"]>>>().toEqualTypeOf<
  StorageFailure | Corruption | Contention | InjectedFailure | AppendConflict | ThreadStoreError
>();
expectTypeOf<Effect.Services<ThreadConstruction>>().toEqualTypeOf<
  SqlClient.SqlClient | CryptoDigest.Digest
>();
expectTypeOf<Effect.Error<ThreadConstruction>>().toEqualTypeOf<StorageFailure | Corruption>();
expectTypeOf<Effect.Services<LedgerConstruction>>().toEqualTypeOf<
  SqlClient.SqlClient | CryptoDigest.Digest | SecureRandom.SecureRandom
>();
expectTypeOf<Effect.Error<LedgerConstruction>>().toEqualTypeOf<never>();

export const preservesTransactionChannels = (journal: Journal) => {
  const transaction = journal.withWriteTransaction("typed body")(
    Effect.andThen(RequiredService, Effect.fail(BodyFailure.make({}))),
  );

  expectTypeOf<Effect.Error<typeof transaction>>().toEqualTypeOf<
    BodyFailure | StorageFailure | Contention
  >();
  expectTypeOf<Effect.Services<typeof transaction>>().toEqualTypeOf<RequiredService>();
};
