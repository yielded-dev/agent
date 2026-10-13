import { Effect, Schema } from "effect";

export class AuthStorageError extends Schema.TaggedError<AuthStorageError>()("AuthStorageError", {
  cause: Schema.Defect(),
}) {}

const statements = [
  'create table "auth_subject" ("id" text primary key, "active" integer not null, "revision" text not null, "displayName" text not null)',
  'create table "auth_identifier" ("namespace" text not null, "value" text not null, "subjectId" text not null, "verifiedAt" integer not null, "revision" text not null, constraint "auth_identifier_unique" unique ("namespace", "value"))',
  'create table "auth_credential" ("subjectId" text not null, "credentialId" text not null, "revision" text not null, "active" integer not null, constraint "auth_credential_unique" unique ("subjectId", "credentialId"))',
  'create table "auth_email_credential" ("moduleId" text not null, "subjectId" text not null, "credentialId" text not null, "namespace" text not null, "value" text not null, "revision" text not null, "active" integer not null, constraint "auth_email_id_unique" unique ("moduleId", "credentialId"), constraint "auth_email_identifier_unique" unique ("moduleId", "namespace", "value"))',
  'create table "auth_session" ("id" text primary key, "subjectId" text not null, "digest" text not null, "revision" text not null, "issuedAt" integer not null, "expiresAt" integer not null, "absoluteExpiresAt" integer not null, "record" text not null, constraint "auth_session_digest_unique" unique ("digest"))',
  "create table auth_proof (moduleId text not null, purpose text not null, seriesKey text not null, proofId text not null, binding text not null, verifierKeyId text not null, verifierDigest text not null, issuedAt integer not null, expiresAt integer not null, failedAttempts integer not null, unique(moduleId, purpose, seriesKey), unique(moduleId, proofId))",
  "create table auth_oauth_flow (moduleId text, flowId text, purpose text, generation integer, provider text, callbackId text, issuer text, responseIssuerMode text, subjectId text, stateDigest text, binderVerifier text, binderExpiresAt integer, snapshot text, issuedAt integer, expiresAt integer, unique(moduleId, flowId), unique(stateDigest))",
  "create table auth_oauth_identity (identityKey text primary key, provider text not null, issuer text not null, externalSubject text not null, subjectId text not null)",
  "create table auth_oauth_credential (moduleId text, credentialId text, subjectId text, identityKey text, revision text, active integer, unique(credentialId), unique(identityKey))",
  "create table auth_oauth_intent (moduleId text, reference text, flowId text, identityKey text, snapshot text, expiresAt integer, retentionUntil integer, unique(moduleId, reference), unique(moduleId, flowId))",
];

/** The replacement Auth namespace starts with one atomic initial schema. */
export const initializeAuthStorage = (storage: DurableObjectStorage) =>
  Effect.try({
    try: () =>
      storage.transactionSync(() => {
        const tables = storage.sql
          .exec("select name from sqlite_master where type='table' and name like 'auth_%'")
          .toArray();

        if (tables.length !== 0) {
          const version = Schema.decodeUnknownSync(
            Schema.Array(Schema.Struct({ version: Schema.Int })),
          )(storage.sql.exec("select version from auth_format").toArray());

          return version.length === 1 && version[0]?.version === 1;
        }
        for (const statement of statements) storage.sql.exec(statement);
        storage.sql.exec("create table auth_format (version integer not null)");
        storage.sql.exec("insert into auth_format values (1)");

        return true;
      }),
    catch: (cause) => AuthStorageError.make({ cause }),
  }).pipe(
    Effect.filterOrFail(
      (supported) => supported,
      () => AuthStorageError.make({ cause: "Unsupported auth format" }),
    ),
    Effect.asVoid,
  );
