import { Context, DateTime, Effect, Result, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

import { FundingError, FundingGrant, GrantFunding, RevokeFunding } from "../funding-domain";
import { AccountId } from "./account";

// GitHub's immutable ID for danieljvdm; neither display names nor registration order grant admin.
export const fundingAdminGithubId = "3450486";
const issuer = "https://github.com/login/oauth";

const unavailable = () =>
  FundingError.make({ message: "Funding access is unavailable. Refresh before retrying." });

export const FundingFailpoint = Context.Reference<{
  readonly hit: (
    point:
      | "schema:before"
      | "schema:after"
      | "grant:before"
      | "grant:after"
      | "revoke:before"
      | "revoke:after",
  ) => Effect.Effect<void, FundingError>;
}>("travel-planner/FundingFailpoint", { defaultValue: () => ({ hit: () => Effect.void }) });

const Identity = Schema.Struct({
  subjectId: AccountId,
  displayName: Schema.String,
});

const Values = Schema.Array(Schema.Struct({ value: Schema.String }));

const Grants = Schema.Array(
  Schema.Struct({
    kind: FundingGrant.fields.kind,
    target: FundingGrant.fields.target,
    value: Schema.String,
  }),
);

const grantJson = Schema.fromJsonString(FundingGrant);

const Email = Schema.String.check(
  Schema.isPattern(/^[^\s@]+@[^\s@]+\.[^\s@]+$/),
  Schema.isMaxLength(320),
);

const GithubLogin = Schema.String.check(
  Schema.isPattern(/^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,37}[a-zA-Z0-9])?$/),
);

const GithubUser = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  login: GithubLogin,
});

/** Auth-owned funding grants refer only to verified, active identities. No credential is returned. */
export const makeFundingStore = Effect.fn("Funding.store")(function* (
  storage: DurableObjectStorage,
) {
  const failpoint = yield* FundingFailpoint;

  const read = <A>(run: () => A) =>
    Effect.try({
      try: run,
      catch: (error) => (Schema.is(FundingError)(error) ? error : unavailable()),
    });

  yield* failpoint.hit("schema:before");

  const supported = yield* read(() =>
    storage.transactionSync(() => {
      const tables = storage.sql
        .exec("select name from sqlite_master where type='table' and name like 'funding_%'")
        .toArray();

      if (tables.length) {
        const rows = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ version: Schema.Int })))(
          storage.sql.exec("select version from funding_format").toArray(),
        );

        return rows.length === 1 && rows[0]?.version === 1;
      }
      storage.sql.exec(
        "create table funding_grant (kind text not null, target text not null, value text not null, primary key (kind, target))",
      );
      storage.sql.exec("create table funding_format (version integer not null)");
      storage.sql.exec("insert into funding_format values (1)");

      return true;
    }),
  );

  if (!supported) return yield* unavailable();
  yield* failpoint.hit("schema:after");

  const identities = Effect.fnUntraced(function* (subjectId: string) {
    const subjects = yield* read(() =>
      Schema.decodeUnknownSync(Schema.Array(Identity))(
        storage.sql
          .exec(
            "select id as subjectId, displayName from auth_subject where id = ? and active = 1",
            subjectId,
          )
          .toArray(),
      ),
    );

    const subject = subjects[0];

    if (!subject) return yield* unavailable();

    const emails = yield* read(() =>
      Schema.decodeUnknownSync(Values)(
        storage.sql
          .exec(
            `select i.value from auth_identifier i join auth_email_credential e on e.subjectId = i.subjectId and e.namespace = i.namespace and e.value = i.value join auth_credential c on c.subjectId = e.subjectId and c.credentialId = e.credentialId where i.subjectId = ? and i.namespace = 'travel-planner/email' and e.active = 1 and c.active = 1 and c.revision = e.revision`,
            subjectId,
          )
          .toArray(),
      ).map((row) => row.value.toLowerCase()),
    );

    const githubIds = yield* read(() =>
      Schema.decodeUnknownSync(Values)(
        storage.sql
          .exec(
            `select t.externalSubject as value from auth_oauth_identity t join auth_oauth_credential o on o.identityKey = t.identityKey and o.subjectId = t.subjectId join auth_credential c on c.subjectId = o.subjectId and c.credentialId = o.credentialId where t.subjectId = ? and t.provider = 'github' and t.issuer = ? and o.active = 1 and c.active = 1 and c.revision = o.revision`,
            subjectId,
            issuer,
          )
          .toArray(),
      ).map((row) => row.value),
    );

    return { ...subject, emails, githubIds, admin: githubIds.includes(fundingAdminGithubId) };
  });

  const grants = Effect.gen(function* () {
    const rows = yield* read(() =>
      Schema.decodeUnknownSync(Grants)(
        storage.sql
          .exec("select kind, target, value from funding_grant order by kind, target")
          .toArray(),
      ),
    );

    return yield* Effect.forEach(rows, (row) =>
      Schema.decodeEffect(grantJson)(row.value).pipe(
        Effect.mapError(unavailable),
        Effect.filterOrFail(
          (grant) => grant.kind === row.kind && grant.target === row.target,
          unavailable,
        ),
      ),
    );
  });

  const allowed = (
    identity: Effect.Success<ReturnType<typeof identities>>,
    entries: ReadonlyArray<FundingGrant>,
  ) =>
    identity.admin ||
    entries.some((grant) =>
      grant.kind === "account"
        ? grant.target === identity.subjectId
        : grant.kind === "github"
          ? identity.githubIds.includes(grant.target)
          : identity.emails.includes(grant.target),
    );

  const status = Effect.fnUntraced(function* (subjectId: string) {
    const identity = yield* identities(subjectId);

    return { admin: identity.admin, allowed: allowed(identity, yield* grants) };
  });

  const requireAdmin = Effect.fnUntraced(function* (subjectId: string) {
    if (!(yield* identities(subjectId)).admin)
      return yield* FundingError.make({ message: "Administrator access required." });
  });

  // Validation is synchronous and read-only. Run it before any mutation; let SQL
  // exceptions escape the callback so transactionSync still rolls back every write.
  const transact = <A>(validate: Effect.Effect<A, FundingError>, mutate: (value: A) => void) =>
    read(() =>
      storage.transactionSync(() => {
        const result = Effect.runSync(Effect.result(validate));

        if (Result.isSuccess(result)) mutate(result.success);

        return result;
      }),
    ).pipe(Effect.flatMap(Effect.fromResult), Effect.asVoid);

  const list = Effect.fn("Funding.list")(function* (actor: string, after?: string) {
    yield* requireAdmin(actor);

    const rows = yield* read(() =>
      Schema.decodeUnknownSync(Schema.Array(Identity))(
        storage.sql
          .exec(
            "select id as subjectId, displayName from auth_subject where active = 1 and id > ? order by id limit 26",
            after ?? "",
          )
          .toArray(),
      ),
    );

    const entries = yield* grants;

    const users = yield* Effect.forEach(
      rows.slice(0, 25),
      Effect.fnUntraced(function* (row) {
        const identity = yield* identities(row.subjectId);

        return { ...identity, allowed: allowed(identity, entries) };
      }),
    );

    return { grants: entries, users, next: rows.length > 25 ? rows[24]!.subjectId : null };
  });

  const grant = Effect.fn("Funding.grant")(function* (
    actor: string,
    input: typeof GrantFunding.Type,
  ) {
    yield* requireAdmin(actor);

    const candidate = yield* Schema.decodeEffect(GrantFunding)(input).pipe(
      Effect.mapError(() => FundingError.make({ message: "Enter a valid funding recipient." })),
    );

    let target = candidate.value.trim();
    let label = target;

    if (candidate.kind === "email") {
      target = yield* Schema.decodeEffect(Email)(target.toLowerCase()).pipe(
        Effect.mapError(() => FundingError.make({ message: "Enter a valid email address." })),
      );
      label = target;
    } else if (candidate.kind === "github") {
      const login = yield* Schema.decodeEffect(GithubLogin)(target.replace(/^@/, "")).pipe(
        Effect.mapError(() => FundingError.make({ message: "Enter a GitHub username." })),
      );

      const user = yield* Effect.gen(function* () {
        const http = yield* HttpClient.HttpClient;

        const response = yield* HttpClient.withScope(http).execute(
          HttpClientRequest.get(`https://api.github.com/users/${encodeURIComponent(login)}`).pipe(
            HttpClientRequest.setHeaders({
              "user-agent": "Elsewhere-funding",
              accept: "application/vnd.github+json",
            }),
          ),
        );

        if (response.status !== 200)
          return yield* FundingError.make({
            message: "Could not find that GitHub user. Check the username and retry.",
          });

        return yield* response.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(GithubUser)));
      }).pipe(
        Effect.scoped,
        Effect.timeout("10 seconds"),
        Effect.mapError(() =>
          FundingError.make({
            message: "Could not verify that GitHub user. Check the username and retry.",
          }),
        ),
        Effect.provide(FetchHttpClient.layer),
      );

      target = String(user.id);
      label = user.login;
    } else {
      target = yield* Schema.decodeEffect(AccountId)(target).pipe(Effect.mapError(unavailable));
      label = (yield* identities(target)).displayName;
    }

    const entry = yield* Schema.decodeEffect(FundingGrant)({
      version: 1,
      kind: candidate.kind,
      target,
      label,
      grantedBy: actor,
      grantedAt: DateTime.formatIso(yield* DateTime.now),
    }).pipe(Effect.mapError(unavailable));

    const encoded = yield* Schema.encodeEffect(grantJson)(entry).pipe(Effect.mapError(unavailable));

    yield* failpoint.hit("grant:before");
    yield* transact(
      Effect.gen(function* () {
        if (!(yield* identities(actor)).admin) return yield* unavailable();
        // Decode existing data before mutation; unknown formats never get overwritten.
        const entries = yield* grants;

        if (
          entries.length >= 1000 &&
          !entries.some((row) => row.kind === entry.kind && row.target === entry.target)
        )
          return yield* FundingError.make({ message: "The funding allowlist is full." });
      }),
      () => {
        storage.sql.exec(
          "insert into funding_grant (kind,target,value) values (?,?,?) on conflict(kind,target) do update set value=excluded.value",
          entry.kind,
          entry.target,
          encoded,
        );
      },
    );
    yield* failpoint.hit("grant:after");

    return entry;
  });

  const revoke = Effect.fn("Funding.revoke")(function* (
    actor: string,
    input: typeof RevokeFunding.Type,
  ) {
    yield* requireAdmin(actor);

    const entry = yield* Schema.decodeEffect(RevokeFunding)(input).pipe(
      Effect.mapError(unavailable),
    );

    yield* failpoint.hit("revoke:before");
    yield* transact(
      Effect.gen(function* () {
        if (!(yield* identities(actor)).admin) return yield* unavailable();
        yield* grants;
      }),
      () => {
        storage.sql.exec(
          "delete from funding_grant where kind = ? and target = ?",
          entry.kind,
          entry.target,
        );
      },
    );
    yield* failpoint.hit("revoke:after");
  });

  return { status, list, grant, revoke };
});
