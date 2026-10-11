import {
  requiredOAuthSignInConstraints,
  requiredOAuthRegistrationConstraints,
  type OAuthSignInMapping,
  type OAuthRegistrationIntentMapping,
  type OAuthRegistrationMapping,
} from "@yielded/auth-persistence-drizzle";
import { SecurityRevision } from "@yielded/auth/Sessions";
import { sql } from "drizzle-orm";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { Effect } from "effect";

import { Registration } from "./contract";
import {
  subject,
  credential,
  subjectMapping,
  clock,
  credentialMapping,
  subjectId,
  requirement,
} from "./schema";

export const oauthFlow = sqliteTable(
  "auth_oauth_flow",
  {
    moduleId: text(),
    flowId: text(),
    purpose: text(),
    generation: integer(),
    provider: text(),
    callbackId: text(),
    issuer: text(),
    responseIssuerMode: text(),
    subjectId: text(),
    stateDigest: text(),
    binderVerifier: text(),
    binderExpiresAt: integer(),
    snapshot: text(),
    issuedAt: integer(),
    expiresAt: integer(),
  },
  (t) => [
    uniqueIndex("auth_oauth_flow_unique").on(t.moduleId, t.flowId),
    uniqueIndex("auth_oauth_state_unique").on(t.stateDigest),
  ],
);

export const oauthIdentity = sqliteTable("auth_oauth_identity", {
  identityKey: text().primaryKey(),
  provider: text().notNull(),
  issuer: text().notNull(),
  externalSubject: text().notNull(),
  subjectId: text().notNull(),
});

export const oauthCredential = sqliteTable(
  "auth_oauth_credential",
  {
    moduleId: text(),
    credentialId: text(),
    subjectId: text(),
    identityKey: text(),
    revision: text(),
    active: integer({ mode: "boolean" }),
  },
  (t) => [
    uniqueIndex("auth_oauth_credential_0").on(t.credentialId),
    uniqueIndex("auth_oauth_credential_1").on(t.identityKey),
  ],
);

export const oauthIntent = sqliteTable(
  "auth_oauth_intent",
  {
    moduleId: text(),
    reference: text(),
    flowId: text(),
    identityKey: text(),
    snapshot: text(),
    expiresAt: integer(),
    retentionUntil: integer(),
  },
  (t) => [
    uniqueIndex("auth_oauth_intent_reference").on(t.moduleId, t.reference),
    uniqueIndex("auth_oauth_intent_flow").on(t.moduleId, t.flowId),
  ],
);

const oauthSubject = {
  ...subjectMapping,
  activeCondition: sql`${subject.active} = 1`,
  decodeAuthenticationRequirement: () => requirement,
};

const oauthAuthority = {
  ...credentialMapping,
  activeCondition: sql`${credential.active} = 1`,
  encodeInsert: (input: {
    subjectId: string;
    credentialId: string;
    revision: SecurityRevision;
  }) => ({ ...input, active: true }),
};

const oauthCredentialMapping = {
  table: oauthCredential,
  moduleId: "moduleId",
  credentialId: "credentialId",
  subjectId: "subjectId",
  identityKey: "identityKey",
  credentialRevision: "revision",
  status: "active",
  isActiveStatus: (value: unknown) => value === true,
  activeCondition: sql`${oauthCredential.active} = 1`,
  removal: "delete",
  encodeInsert: (input: {
    moduleId: string;
    subjectId: string;
    identityKey: string;
    credentialId: string;
    credentialRevision: SecurityRevision;
  }) => ({
    moduleId: input.moduleId,
    subjectId: input.subjectId,
    identityKey: input.identityKey,
    credentialId: input.credentialId,
    revision: input.credentialRevision,
    active: true,
  }),
} as const;

const ownership = {
  table: oauthIdentity,
  identityKey: "identityKey",
  provider: "provider",
  issuer: "issuer",
  externalSubject: "externalSubject",
  subjectId: "subjectId",
  decodeSubjectId: (row: typeof oauthIdentity.$inferSelect) => row.subjectId,
} as const;

const intent = {
  table: oauthIntent,
  moduleId: "moduleId",
  reference: "reference",
  flowId: "flowId",
  identityKey: "identityKey",
  snapshot: "snapshot",
  expiresAt: "expiresAt",
  retentionUntil: "retentionUntil",
  encodeInsert: () => ({}),
} as const;

export const oauthSignInMapping: OAuthSignInMapping<
  typeof subject,
  typeof oauthIdentity,
  typeof oauthCredential,
  typeof credential,
  typeof oauthFlow,
  string
> = {
  subject: oauthSubject,
  subjectId,
  authority: oauthAuthority,
  credential: oauthCredentialMapping,
  clock,
  constraints: requiredOAuthSignInConstraints,
  ownership,
  flow: {
    table: oauthFlow,
    moduleId: "moduleId",
    flowId: "flowId",
    purpose: "purpose",
    generation: "generation",
    provider: "provider",
    callbackId: "callbackId",
    issuer: "issuer",
    responseIssuerMode: "responseIssuerMode",
    subjectId: "subjectId",
    stateDigest: "stateDigest",
    binderVerifier: "binderVerifier",
    binderExpiresAt: "binderExpiresAt",
    snapshot: "snapshot",
    issuedAt: "issuedAt",
    expiresAt: "expiresAt",
    encodeInsert: (input) => ({ ...input }),
  },
};

export const oauthIntentMapping: OAuthRegistrationIntentMapping<
  typeof oauthIdentity,
  typeof oauthIntent,
  string
> = {
  ownership,
  intent,
  clock,
  constraints: {
    intentReference: requiredOAuthRegistrationConstraints.intentReference,
    intentFlow: requiredOAuthRegistrationConstraints.intentFlow,
    ownership: requiredOAuthRegistrationConstraints.ownership,
  },
  eligible: () => sql`1`,
};

export const oauthRegistrationMapping: OAuthRegistrationMapping<
  typeof Registration.Type,
  typeof subject,
  typeof oauthIdentity,
  typeof oauthCredential,
  typeof credential,
  typeof oauthIntent,
  string
> = {
  registration: Registration,
  subject: oauthSubject,
  subjectId,
  authority: oauthAuthority,
  credential: oauthCredentialMapping,
  ownership: {
    ...ownership,
    encodeInsert: ({ identity, identityKey, subjectId }) => ({
      identityKey,
      provider: identity.provider,
      issuer: identity.issuer,
      externalSubject: identity.subject,
      subjectId,
    }),
  },
  intent,
  clock,
  constraints: requiredOAuthRegistrationConstraints,
  inspect: ({ registration }) =>
    Effect.succeed({ fingerprint: registration.displayName, eligible: true }),
  eligibility: { admission: () => sql`1`, postcondition: () => sql`1` },
  allocateSubjectId: Effect.sync(() => crypto.randomUUID()),
  allocateCredentialId: Effect.sync(() => crypto.randomUUID()),
  allocateRevision: Effect.sync(() => SecurityRevision.make(crypto.randomUUID())),
  encodeSubjectInsert: ({ intent, registration }, ids) => ({
    id: ids.subjectId,
    revision: ids.securityRevision,
    active: true,
    displayName: intent.profile?.displayName ?? registration.displayName,
  }),
};
