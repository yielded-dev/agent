import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SqliteStorageError } from "../SqliteStorageError.ts";

/** @internal */
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
