import { Effect, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { AuthStorageError } from "./storage";

const Links = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      yieldedSubject: Schema.String.check(Schema.isPattern(/^[\x21-\x7e]{1,255}$/)),
      githubSubject: Schema.String.check(Schema.isPattern(/^[0-9]{1,20}$/)),
    }),
  ).check(Schema.isMaxLength(32)),
);

const Owners = Schema.Array(Schema.Struct({ subjectId: Schema.NonEmptyString }));

// Auth beta.12's persisted v1 identity-key encoding. This application migration
// targets that pinned format; never import an adapter's private implementation.
const identityKey = Effect.fnUntraced(function* (issuer: string, subject: string) {
  const fields = ["effect-auth/oauth-identity-key/v1", "yielded", issuer, subject].map((value) =>
    new TextEncoder().encode(value),
  );

  const packed = new Uint8Array(fields.reduce((size, value) => size + 4 + value.length, 0));
  const view = new DataView(packed.buffer);
  let offset = 0;

  for (const value of fields) {
    view.setUint32(offset, value.length, false);
    packed.set(value, offset + 4);
    offset += value.length + 4;
  }

  const digest = yield* Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", packed),
    catch: () => new AuthStorageError({ cause: "Identity key unavailable" }),
  });

  return `v1:${Base64Url.encode(new Uint8Array(digest))}`;
});

/** Operator-approved identities only. No email/profile matching, account merge,
 * credential replacement, or retry that can recreate a subsequently removed link.
 */
export const connectYieldedAccounts = Effect.fn("Auth.connectYieldedAccounts")(function* (
  storage: DurableObjectStorage,
  issuer: string,
  configured: string,
) {
  const links = yield* Schema.decodeEffect(Links)(configured, { reportInput: false }).pipe(
    Effect.mapError(() => new AuthStorageError({ cause: "Invalid Yielded account mapping" })),
  );

  for (const link of links) {
    const key = yield* identityKey(issuer, link.yieldedSubject);

    yield* Effect.try({
      try: () =>
        storage.transactionSync(() => {
          const previous = Schema.decodeUnknownSync(
            Schema.Array(Schema.Struct({ githubSubject: Schema.String })),
          )(
            storage.sql
              .exec(
                "select githubSubject from auth_yielded_account_link where issuer = ? and externalSubject = ?",
                issuer,
                link.yieldedSubject,
              )
              .toArray(),
          );

          if (previous.length > 0) {
            if (previous.length !== 1 || previous[0]?.githubSubject !== link.githubSubject)
              throw new AuthStorageError({ cause: "Yielded account mapping changed" });

            return;
          }

          const owners = Schema.decodeUnknownSync(Owners)(
            storage.sql
              .exec(
                `select t.subjectId from auth_oauth_tuple t join auth_oauth_credential o on o.identityKey = t.identityKey and o.subjectId = t.subjectId
           join auth_credential c on c.credentialId = o.credentialId and c.subjectId = o.subjectId and c.revision = o.revision
           join auth_subject s on s.id = t.subjectId
           where t.provider = 'github' and t.issuer = 'https://github.com/login/oauth' and t.externalSubject = ?
           and t.state = 'Owned' and o.moduleId = 'travel-planner/github' and o.active = 1 and c.active = 1 and s.active = 1`,
                link.githubSubject,
              )
              .toArray(),
          );

          const owner = owners[0];

          if (owners.length !== 1 || owner === undefined)
            throw new AuthStorageError({
              cause: "Expected one active GitHub account for Yielded mapping",
            });
          // A pre-existing tuple (owned, reserved or removed) needs explicit reconciliation.
          if (
            storage.sql.exec("select 1 from auth_oauth_tuple where identityKey = ?", key).toArray()
              .length !== 0
          )
            throw new AuthStorageError({ cause: "Yielded identity already has ownership state" });
          const credentialId = crypto.randomUUID();
          const revision = crypto.randomUUID();

          storage.sql.exec(
            "insert into auth_oauth_tuple (identityKey, provider, issuer, externalSubject, state, version, subjectId) values (?, 'yielded', ?, ?, 'Owned', ?, ?)",
            key,
            issuer,
            link.yieldedSubject,
            crypto.randomUUID(),
            owner.subjectId,
          );
          storage.sql.exec(
            "insert into auth_oauth_credential (moduleId, credentialId, subjectId, identityKey, revision, active) values ('travel-planner/github', ?, ?, ?, ?, 1)",
            credentialId,
            owner.subjectId,
            key,
            revision,
          );
          storage.sql.exec(
            "insert into auth_credential (subjectId, credentialId, revision, active) values (?, ?, ?, 1)",
            owner.subjectId,
            credentialId,
            revision,
          );
          storage.sql.exec(
            "insert into auth_yielded_account_link (issuer, externalSubject, githubSubject, subjectId) values (?, ?, ?, ?)",
            issuer,
            link.yieldedSubject,
            link.githubSubject,
            owner.subjectId,
          );
        }),
      catch: () =>
        new AuthStorageError({
          cause:
            "Yielded account mapping could not be committed; inspect the configured identities",
        }),
    });
  }
});
