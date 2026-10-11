import { PersistenceMappingError } from "@yielded/auth-persistence/Adapter";
import { SubjectId } from "@yielded/auth/Schema";
import { type AuthenticationRequirement, SecurityRevision } from "@yielded/auth/Sessions";
import { sql, type SQL } from "drizzle-orm";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { Effect, Schema } from "effect";

// Local subjects are random IDs. Provider email/profile never grants account ownership.
export const subject = sqliteTable("auth_subject", {
  id: text().primaryKey(),
  active: integer({ mode: "boolean" }).notNull(),
  revision: text().notNull(),
  displayName: text().notNull(),
});

export const identifier = sqliteTable(
  "auth_identifier",
  {
    namespace: text().notNull(),
    value: text().notNull(),
    subjectId: text().notNull(),
    verifiedAt: integer().notNull(),
    revision: text().notNull(),
  },
  (t) => [uniqueIndex("auth_identifier_unique").on(t.namespace, t.value)],
);

export const credential = sqliteTable(
  "auth_credential",
  {
    subjectId: text().notNull(),
    credentialId: text().notNull(),
    revision: text().notNull(),
    active: integer({ mode: "boolean" }).notNull(),
  },
  (t) => [uniqueIndex("auth_credential_unique").on(t.subjectId, t.credentialId)],
);

export const emailCredential = sqliteTable(
  "auth_email_credential",
  {
    moduleId: text().notNull(),
    subjectId: text().notNull(),
    credentialId: text().notNull(),
    namespace: text().notNull(),
    value: text().notNull(),
    revision: text().notNull(),
    active: integer({ mode: "boolean" }).notNull(),
  },
  (t) => [
    uniqueIndex("auth_email_id_unique").on(t.moduleId, t.credentialId),
    uniqueIndex("auth_email_identifier_unique").on(t.moduleId, t.namespace, t.value),
  ],
);

export const mappingError = () =>
  PersistenceMappingError.make({
    operation: "travel-auth",
    cause: "Invalid authentication record",
  });

export const decodeMillis = (value: unknown) =>
  Schema.decodeUnknownEffect(Schema.Natural)(value).pipe(Effect.mapError(mappingError));

export const clock = {
  encodeInstant: (millis: number) => millis,
  decodeInstant: Schema.decodeUnknownSync(Schema.Natural),
  engineNowMillis: sql`cast(round((julianday('now') - 2440587.5) * 86400000) as integer)`,
  toMillis: (expression: SQL) => expression,
  fromMillis: (expression: SQL) => expression,
};

export const subjectId = {
  toNative: (id: SubjectId) => Effect.succeed(id),
  toSubject: (id: string) => Schema.decodeEffect(SubjectId)(id).pipe(Effect.mapError(mappingError)),
  equals: (a: string, b: string) => a === b,
};

export const requirement: AuthenticationRequirement = {
  maximumAgeMillis: 300_000,
  alternatives: [
    {
      factors: ["possession"],
      minimumCredentials: 1,
      userVerified: false,
      phishingResistant: false,
    },
  ],
};

export const subjectMapping = {
  table: subject,
  id: "id",
  status: "active",
  securityRevision: "revision",
  isActiveStatus: (value: unknown) => value === true,
  activeStatusValue: true,
  decodeRequirement: () => Effect.succeed(requirement),
  nextSecurityRevisionSync: () => SecurityRevision.make(crypto.randomUUID()),
} as const;

export const credentialMapping = {
  table: credential,
  subjectId: "subjectId",
  credentialId: "credentialId",
  revision: "revision",
  status: "active",
  isActiveStatus: (value: unknown) => value === true,
  activeStatusValue: true,
} as const;
