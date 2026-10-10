import { utf8ByteLength } from "@yielded/agent/digest";
import {
  MAX_PROMPT_CHECKPOINT_BYTES,
  PromptCheckpoint,
  PromptCheckpointHead,
  PromptCheckpointRead,
  ThreadStoreError,
  type PromptCheckpoints,
} from "@yielded/agent/thread-store";
import { Effect, Option, Schema } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import type { DoJournal } from "./do-journal.ts";

const Header = Schema.Struct({
  ...PromptCheckpointHead.fields,
  chunks: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
});

const decodeHeader = Schema.decodeEffect(Schema.fromJsonString(Header));
const encodeHeader = Schema.encodeEffect(Schema.fromJsonString(Header));

const decodeRows = Schema.decodeUnknownEffect(
  Schema.Array(
    Schema.Struct({
      key: Schema.String,
      value: Schema.String.check(Schema.isMaxLength(2_000_000)),
    }),
  ).check(Schema.isMaxLength(65)),
);

const decodeWrite = Schema.decodeEffect(PromptCheckpoint);
const decodeRead = Schema.decodeEffect(PromptCheckpointRead);
const prefixFor = (threadId: string) => `prompt-checkpoint/${JSON.stringify(threadId)}/`;
const keyFor = (prefix: string, chunk: number) => prefix + String(chunk).padStart(5, "0");

/**
 * Versioned derivative keys use the adapter's existing metadata store. Canonical layout and
 * export are unchanged. Chunking keeps SQL values below the DO limit; loading is one bounded
 * query followed by one parse of the native prompt. Only the latest checkpoint is retained.
 */
export const makePromptCheckpoints = (
  sql: SqlClient,
  journal: DoJournal,
  maxValueBytes: number,
): PromptCheckpoints => ({
  load: (input) =>
    journal.state
      .read(
        Effect.gen(function* () {
          const request = yield* decodeRead(input);
          const prefix = prefixFor(request.threadId);

          const rows = yield* decodeRows(
            yield* sql`
              SELECT key, value FROM effect_agent_meta
              WHERE key >= ${prefix} AND key < ${prefix + "~"}
                AND (SELECT sum(length(CAST(key AS BLOB)) + length(CAST(value AS BLOB)))
                  FROM effect_agent_meta
                  WHERE key >= ${prefix} AND key < ${prefix + "~"})
                    <= ${MAX_PROMPT_CHECKPOINT_BYTES + 16_384}
              ORDER BY key LIMIT 66`,
          );

          const first = rows[0];

          if (first === undefined || first.key !== keyFor(prefix, 0)) return Option.none();
          const header = yield* decodeHeader(first.value);

          if (
            header.threadId !== request.threadId ||
            header.producerEpoch + 1 !== request.producerEpoch ||
            header.chunks !== rows.length - 1 ||
            rows.some((row, index) => row.key !== keyFor(prefix, index))
          )
            return Option.none();
          const [head] = yield* journal.getThread(request.threadId);

          if (
            head === undefined ||
            head.producer_epoch !== request.producerEpoch ||
            head.tail_sequence !== header.throughSequence ||
            head.tail_digest !== header.tailDigest
          )
            return Option.none();
          let bytes = 0;

          for (const row of rows) {
            bytes += utf8ByteLength(row.value);
            if (bytes > MAX_PROMPT_CHECKPOINT_BYTES + 16_384) return Option.none();
          }

          return Option.some({
            ...header,
            promptJson: rows
              .slice(1)
              .map((row) => row.value)
              .join(""),
          });
        }),
      )
      .pipe(Effect.orElseSucceed(() => Option.none())),
  save: (input) =>
    Effect.gen(function* () {
      const checkpoint = yield* decodeWrite(input);

      if (utf8ByteLength(checkpoint.promptJson) > MAX_PROMPT_CHECKPOINT_BYTES) return;
      const prefix = prefixFor(checkpoint.threadId);
      const chunks: Array<string> = [];
      // Three UTF-8 bytes per UTF-16 code unit is a conservative bound. Keep surrogate pairs
      // together so SQLite never normalizes an isolated half while persisting a chunk.
      const chars = Math.floor(Math.min(maxValueBytes - 4_096, 1_000_000) / 3);

      if (chars < 1) return;
      for (let offset = 0; offset < checkpoint.promptJson.length;) {
        let end = Math.min(offset + chars, checkpoint.promptJson.length);
        const last = checkpoint.promptJson.charCodeAt(end - 1);

        if (last >= 0xd800 && last <= 0xdbff) end--;
        if (end <= offset || chunks.length >= 64) return;
        chunks.push(checkpoint.promptJson.slice(offset, end));
        offset = end;
      }
      const header = yield* encodeHeader({ ...checkpoint, chunks: chunks.length });

      yield* journal.state
        .transaction(
          Effect.gen(function* () {
            const [head] = yield* journal.getThread(checkpoint.threadId);

            if (
              head === undefined ||
              head.producer_epoch !== checkpoint.producerEpoch ||
              head.tail_sequence !== checkpoint.throughSequence ||
              head.tail_digest !== checkpoint.tailDigest
            )
              return;
            yield* sql`DELETE FROM effect_agent_meta WHERE key >= ${prefix} AND key < ${prefix + "~"}`;
            for (const [index, value] of [header, ...chunks].entries())
              yield* sql`INSERT INTO effect_agent_meta (key, value) VALUES (${keyFor(prefix, index)}, ${value})`;
          }),
        )
        .pipe(Effect.provideService(SqlClient, sql));
    }).pipe(
      Effect.mapError((cause) =>
        ThreadStoreError.make({
          operation: "save prompt checkpoint",
          message: "Unable to retain a disposable prompt checkpoint",
          cause,
        }),
      ),
    ),
});
