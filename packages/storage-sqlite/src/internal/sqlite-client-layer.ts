import { SqliteClient } from "@effect/sql-sqlite-node";
import { Effect, Layer } from "effect";

import { SqliteStorageError } from "../SqliteStorageError.ts";

export const sqliteClientLayer = (
  options: Parameters<typeof SqliteClient.layer>[0],
  operation: string,
) =>
  SqliteClient.layer(options).pipe(
    Layer.catch((cause) =>
      Layer.effectContext<
        Layer.Success<ReturnType<typeof SqliteClient.layer>>,
        SqliteStorageError,
        never
      >(Effect.fail(SqliteStorageError.make({ operation, message: cause.message, cause }))),
    ),
  );
