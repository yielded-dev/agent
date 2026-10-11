import {
  requiredSessionConstraints,
  type StatefulSessionMapping,
} from "@yielded/auth-persistence-drizzle";
import { TokenDigest } from "@yielded/auth/Schema";
import {
  SessionId,
  SessionMetadata,
  SessionAuthenticationProvenance,
  SessionCredentialVersion,
} from "@yielded/auth/Sessions";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { DateTime, Effect, Schema } from "effect";

import type { subject, credential } from "./schema";
import { subjectMapping, credentialMapping, subjectId, clock, mappingError } from "./schema";

export const Claims = Schema.Struct({ displayName: Schema.String });

const Record = Schema.Struct({
  ...SessionMetadata.fields,
  claims: Claims,
  digest: TokenDigest,
  provenance: SessionAuthenticationProvenance,
  credentialVersion: SessionCredentialVersion,
});

const recordJson = Schema.fromJsonString(Record);

export const session = sqliteTable(
  "auth_session",
  {
    id: text().primaryKey(),
    subjectId: text().notNull(),
    digest: text().notNull(),
    revision: text().notNull(),
    issuedAt: integer().notNull(),
    expiresAt: integer().notNull(),
    absoluteExpiresAt: integer().notNull(),
    record: text().notNull(),
  },
  (t) => [uniqueIndex("auth_session_digest_unique").on(t.digest)],
);

export const sessionsMapping = (
  moduleId: string,
): StatefulSessionMapping<
  typeof Claims.Type,
  typeof subject,
  typeof credential,
  typeof session,
  never,
  string,
  string
> => ({
  moduleId,
  clock,
  subject: subjectMapping,
  credential: credentialMapping,
  subjectId,
  sessionId: {
    toNative: (id) => Effect.succeed(id),
    toSession: (id) => Schema.decodeEffect(SessionId)(id).pipe(Effect.mapError(mappingError)),
    equals: (a, b) => a === b,
  },
  constraints: requiredSessionConstraints,
  isConstraintConflict: () => false,
  session: {
    table: session,
    sessionId: "id",
    subjectId: "subjectId",
    digest: "digest",
    securityRevision: "revision",
    issuedAt: "issuedAt",
    expiresAt: "expiresAt",
    absoluteExpiresAt: "absoluteExpiresAt",
    encodeInstant: DateTime.toEpochMillis,
    encodeInsert: (record, ids) => ({
      id: ids.sessionId,
      subjectId: ids.subjectId,
      digest: record.digest,
      revision: record.securityRevision,
      issuedAt: DateTime.toEpochMillis(record.issuedAt),
      expiresAt: DateTime.toEpochMillis(record.expiresAt),
      absoluteExpiresAt: DateTime.toEpochMillis(record.absoluteExpiresAt),
      record: Schema.encodeSync(recordJson)(record),
    }),
    encodeRotation: (record) => ({
      digest: record.digest,
      issuedAt: DateTime.toEpochMillis(record.issuedAt),
      expiresAt: DateTime.toEpochMillis(record.expiresAt),
      record: Schema.encodeSync(recordJson)(record),
    }),
    decode: (row) =>
      Schema.decodeEffect(recordJson)(row.record).pipe(Effect.mapError(mappingError)),
    allocateIdSync: () => crypto.randomUUID(),
  },
});
