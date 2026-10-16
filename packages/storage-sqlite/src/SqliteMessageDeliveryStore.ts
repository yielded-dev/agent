import { makeSqlMessageDeliveryStore } from "@yielded/agent-storage-sql/sql-message-delivery-store";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";

/** Source-owned message obligations in the host's existing SQLite database. */
export const messageDeliveryStoreLayer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      yield* initializeSqliteJournal();

      return yield* makeSqlMessageDeliveryStore(limits);
    }),
  );
