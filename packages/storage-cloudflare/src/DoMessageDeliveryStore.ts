/**
 * Durable message delivery obligations stored beside their owning Thread.
 *
 * @since 0.1.0
 */
import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto";
import { SqlLifecycleSource } from "@yielded/agent-storage-sql/sql-lifecycle-publication";
import {
  makeSqlMessageDeliveryStore,
  registerCanonicalMessageWriter,
} from "@yielded/agent-storage-sql/sql-message-delivery-store";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import { SqlStorageOwner } from "@yielded/agent/sql-storage-owner";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { DoStorageConfig } from "./DoStorageConfig.ts";
import { DoStorageFailpoint } from "./DoStorageFailpoint.ts";
import { initializeDoJournal } from "./internal/do-journal.ts";

/**
 * Message obligations paired with the Thread Object's physical storage owner. Canonical
 * publication uses this configured writer inside the source transaction. The platform must
 * retain delivery maintenance with that transaction and while nextDeadline is present.
 *
 * @category layers
 * @since 0.1.0
 */
export const doMessageDeliveryStoreLayer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* DoStorageConfig;
      const failpoint = yield* DoStorageFailpoint;

      const journal = yield* initializeDoJournal(sql, failpoint.hit, config.maxStoredValueBytes);

      const store = yield* makeSqlMessageDeliveryStore(journal.state.owner, limits, {
        maxStoredValueBytes: config.maxStoredValueBytes,
      }).pipe(
        Effect.provideService(SqlStorageOwner, journal.owner),
        Effect.provideService(SqlLifecycleSource, {
          beforeRetain: (threadId) => journal.flushCanonical(threadId),
          beforePending: journal.flushPublications(),
        }),
      );

      yield* registerCanonicalMessageWriter(journal.state.owner, store);

      return store;
    }),
  ).pipe(Layer.provide(BrowserCrypto.layer));
