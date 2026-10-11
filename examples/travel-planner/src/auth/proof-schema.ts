import {
  requiredProofConstraints,
  type ProofPersistenceMapping,
} from "@yielded/auth-persistence-drizzle";
import { ProofBinding } from "@yielded/auth/Proofs";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { Schema } from "effect";

import { subject, subjectId, clock } from "./schema";

export const proof = sqliteTable(
  "auth_proof",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    seriesKey: text().notNull(),
    proofId: text().notNull(),
    binding: text().notNull(),
    verifierKeyId: text().notNull(),
    verifierDigest: text().notNull(),
    issuedAt: integer().notNull(),
    expiresAt: integer().notNull(),
    failedAttempts: integer().notNull(),
  },
  (t) => [
    uniqueIndex("auth_proof_series_unique").on(t.moduleId, t.purpose, t.seriesKey),
    uniqueIndex("auth_proof_id_unique").on(t.moduleId, t.proofId),
  ],
);

const binding = Schema.fromJsonString(ProofBinding);

export const proofs = {
  constraints: requiredProofConstraints,
  subjectId,
  subject: { table: subject, id: "id" },
  clock,
  proof: {
    table: proof,
    moduleId: "moduleId",
    purpose: "purpose",
    seriesKey: "seriesKey",
    proofId: "proofId",
    binding: "binding",
    verifierKeyId: "verifierKeyId",
    verifierDigest: "verifierDigest",
    issuedAt: "issuedAt",
    expiresAt: "expiresAt",
    failedAttempts: "failedAttempts",
    encodeInsert: ({ record, seriesKey }) => ({
      moduleId: record.moduleId,
      purpose: record.purpose,
      proofId: record.proofId,
      seriesKey,
      binding: Schema.encodeSync(binding)(record.binding),
      verifierKeyId: record.verifier.keyId,
      verifierDigest: record.verifier.digest,
      issuedAt: 0,
      expiresAt: 0,
      failedAttempts: 0,
    }),
  },
} satisfies ProofPersistenceMapping<typeof proof, typeof subject, string>;
