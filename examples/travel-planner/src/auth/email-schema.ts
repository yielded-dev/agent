import {
  requiredEmailRegistrationConstraints,
  requiredEmailSignInConstraints,
  type EmailRegistrationMapping,
  type EmailSignInMapping,
} from "@yielded/auth-persistence-drizzle";
import { EmailCredentialSnapshot } from "@yielded/auth/Email";
import { TokenDigest } from "@yielded/auth/Schema";
import { SecurityRevision } from "@yielded/auth/Sessions";
import { Effect, Schema, Struct } from "effect";

import { Registration } from "./contract";
import type { subject, credential } from "./schema";
import {
  identifier,
  emailCredential,
  subjectMapping,
  subjectId,
  clock,
  decodeMillis,
  mappingError,
  credentialMapping,
} from "./schema";

const identifierMapping = {
  table: identifier,
  namespace: "namespace",
  value: "value",
  subjectId: "subjectId",
  verifiedAt: "verifiedAt",
  bindingRevision: "revision",
  isCurrent: () => true,
  isMutableTarget: () => false,
  encodeVerifiedInsert: (input: {
    readonly identifier: { readonly namespace: string; readonly value: string };
    readonly subjectId: string;
    readonly verifiedAtMillis: number;
    readonly bindingRevision: SecurityRevision;
  }) => ({
    ...input.identifier,
    subjectId: input.subjectId,
    verifiedAt: input.verifiedAtMillis,
    revision: input.bindingRevision,
  }),
} as const;

const emailCredentialMapping = {
  table: emailCredential,
  moduleId: "moduleId",
  subjectId: "subjectId",
  credentialId: "credentialId",
  identifierNamespace: "namespace",
  identifierValue: "value",
  credentialRevision: "revision",
  status: "active",
  isActiveStatus: (value: unknown) => value === true,
  activeStatusValue: true,
  encodeVerifiedInsert: (input: {
    readonly moduleId: string;
    readonly subjectId: string;
    readonly credentialId: string;
    readonly identifier: { readonly namespace: string; readonly value: string };
    readonly credentialRevision: SecurityRevision;
  }) => ({
    moduleId: input.moduleId,
    subjectId: input.subjectId,
    credentialId: input.credentialId,
    ...input.identifier,
    revision: input.credentialRevision,
    active: true,
  }),
} as const;

export const emailSignInMapping: EmailSignInMapping<
  typeof subject,
  typeof identifier,
  typeof emailCredential,
  typeof credential,
  string
> = {
  subject: subjectMapping,
  subjectId,
  identifier: identifierMapping,
  credential: {
    ...emailCredentialMapping,
    decode: ({ moduleId, subject: account, identifier: address, credential: factor }) =>
      Schema.decodeEffect(EmailCredentialSnapshot.mapFields(Struct.omit(["requirement"])))({
        moduleId,
        identifier: { namespace: address.namespace, value: address.value },
        identifierRevision: address.revision,
        verifiedAtMillis: address.verifiedAt,
        credentialId: factor.credentialId,
        credentialRevision: factor.revision,
        revision: {
          subjectId: account.id,
          securityRevision: account.revision,
          credentials: [{ credentialId: factor.credentialId, revision: factor.revision }],
        },
      }).pipe(Effect.mapError(mappingError)),
  },
  authorityCredential: {
    ...credentialMapping,
    encodeInsert: (input) => ({ ...input, active: true }),
    encodeActivation: (revision) => ({ revision, active: true }),
    encodeRetirement: (revision) => ({ revision, active: false }),
  },
  constraints: requiredEmailSignInConstraints,
  decodeInstant: decodeMillis,
};

const registrationJson = Schema.fromJsonString(Registration);

const fingerprint = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String, Registration])),
);

export const emailRegistrationMapping: EmailRegistrationMapping<
  typeof Registration.Type,
  typeof subject,
  typeof identifier,
  typeof emailCredential,
  typeof credential,
  string
> = {
  mode: "atomic",
  clock,
  subject: subjectMapping,
  subjectId,
  identifier: identifierMapping,
  credential: emailCredentialMapping,
  authorityCredential: {
    ...credentialMapping,
    encodeInsert: (input) => ({ ...input, active: true }),
  },
  constraints: requiredEmailRegistrationConstraints,
  inspect: (input) =>
    Effect.succeed({
      fingerprint: TokenDigest.make(
        fingerprint([input.identifier.namespace, input.identifier.value, input.registration]),
      ),
      eligible: true,
    }),
  inspectSync: (input) => ({
    fingerprint: TokenDigest.make(
      fingerprint([input.identifier.namespace, input.identifier.value, input.registration]),
    ),
    eligible: true,
  }),
  snapshotRegistration: (value) =>
    Schema.decodeEffect(registrationJson)(Schema.encodeSync(registrationJson)(value)).pipe(
      Effect.mapError(mappingError),
    ),
  snapshotRegistrationSync: (value) =>
    Schema.decodeSync(registrationJson)(Schema.encodeSync(registrationJson)(value)),
  allocateCredentialIdSync: () => crypto.randomUUID(),
  allocateRevisionSync: () => SecurityRevision.make(crypto.randomUUID()),
  isIdentifierConflict: () => false,
  isCredentialConflict: () => false,
  provisioning: {
    idMode: "synchronous",
    allocateSubjectIdSync: () => crypto.randomUUID(),
    encodeSubjectInsert: (input, values) => ({
      id: values.nativeSubjectId!,
      active: true,
      revision: values.securityRevision,
      displayName: input.registration.displayName,
    }),
  },
};
