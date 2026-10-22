/**
 * SQLite storage for source-owned message delivery obligations.
 *
 * @since 0.1.0
 */
import {
  makeSqlMessageDeliveryStore,
  registerCanonicalMessageWriter,
} from "@yielded/agent-storage-sql/sql-message-delivery-store";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";

/**
 * Source-owned message obligations in the host's existing SQLite database.
 *
 * @category layers
 * @since 0.1.0
 */
export const messageDeliveryStoreLayer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const journal = yield* initializeSqliteJournal();
      const store = yield* makeSqlMessageDeliveryStore(journal.owner, limits);

      yield* registerCanonicalMessageWriter(journal.owner, store);

      return store;
    }),
  );
