import { authenticateRequest } from "@yielded/agent-example-auth/cloudflare";
import type { Authentication } from "@yielded/agent-example-auth/server";
import { DurableObject } from "cloudflare:workers";
import { Effect, Schema } from "effect";
import type { Worker } from "effect-cf";
import { type WorkerEnvironment } from "effect-cf";

import { AccountId, type AccountError, type AccountSession } from "./account";

const Lookup = Schema.Struct({
  subjectId: Schema.NonEmptyString,
  githubIds: Schema.Array(Schema.String.check(Schema.isPattern(/^[0-9]{1,20}$/))).check(
    Schema.isMaxLength(32),
  ),
});

const Identity = Schema.Struct({
  accountId: AccountId,
  displayName: Schema.String,
  provider: Schema.Literals(["github", "yielded"]),
  issuer: Schema.String,
  subjectId: Schema.String,
});

const Grant = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literals(["github", "email", "account"]),
  target: Schema.NonEmptyString,
  label: Schema.NonEmptyString,
  grantedBy: AccountId,
  grantedAt: Schema.String,
});

/** Retained namespace for Site's private migration lookup. It never authenticates or writes. */
export class PlannerAuthV2 extends DurableObject<Cloudflare.Env> {
  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (
      request.method !== "POST" ||
      !["/_internal/migration/accounts", "/_internal/migration/grants"].includes(path)
    )
      return new Response("Not found", { status: 404 });

    try {
      const tables = this.ctx.storage.sql
        .exec(
          "select name from sqlite_master where type = 'table' and name in ('auth_format', 'funding_format')",
        )
        .toArray();

      const names = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ name: Schema.String })))(
        tables,
      );

      const format = path.endsWith("/grants") ? "funding_format" : "auth_format";
      const empty = path.endsWith("/grants") ? { grants: [] } : { accounts: [] };

      if (!names.some((row) => row.name === format)) return Response.json(empty);

      const version = Schema.decodeUnknownSync(
        Schema.Array(Schema.Struct({ version: Schema.Int })),
      )(this.ctx.storage.sql.exec(`select version from ${format}`).toArray());

      if (version.length !== 1 || version[0]?.version !== 1)
        return new Response("Archive format is unavailable", { status: 503 });
      if (path.endsWith("/grants")) {
        const rows = Schema.decodeUnknownSync(
          Schema.Array(
            Schema.Struct({ kind: Schema.String, target: Schema.String, value: Schema.String }),
          ),
        )(
          this.ctx.storage.sql
            .exec("select kind, target, value from funding_grant order by kind, target")
            .toArray(),
        );

        const grants = [];

        for (const row of rows) {
          const grant = Schema.decodeSync(Schema.fromJsonString(Grant))(row.value);

          if (grant.kind !== row.kind || grant.target !== row.target)
            return new Response("Invalid archive grant", { status: 503 });
          grants.push(grant);
        }

        return Response.json({ grants }, { headers: { "cache-control": "no-store" } });
      }
      const body = await request.text();

      if (body.length > 4096) return new Response("Request too large", { status: 413 });
      const lookup = Schema.decodeSync(Schema.fromJsonString(Lookup))(body);

      const rows = Schema.decodeUnknownSync(Schema.Array(Identity))(
        this.ctx.storage.sql
          .exec(
            `select s.id as accountId, s.displayName, i.provider, i.issuer, i.externalSubject as subjectId
           from auth_subject s
           join auth_oauth_identity i on i.subjectId = s.id
           join auth_oauth_credential o on o.identityKey = i.identityKey and o.subjectId = s.id
           join auth_credential c on c.credentialId = o.credentialId and c.subjectId = s.id
           where s.active = 1 and o.active = 1 and c.active = 1 and c.revision = o.revision
             and ((i.provider = 'yielded' and i.issuer = ? and i.externalSubject = ?)
               or (i.provider = 'github' and i.issuer = 'https://github.com/login/oauth'))`,
            this.env.AUTH_ISSUER,
            lookup.subjectId,
          )
          .toArray(),
      ).filter((row) => row.provider === "yielded" || lookup.githubIds.includes(row.subjectId));

      const accounts = new Map<
        string,
        {
          accountId: string;
          displayName: string;
          identities: Array<{ provider: "github" | "yielded"; issuer: string; subjectId: string }>;
        }
      >();

      for (const { accountId, displayName, ...identity } of rows) {
        const account = accounts.get(accountId) ?? { accountId, displayName, identities: [] };

        account.identities.push(identity);
        accounts.set(accountId, account);
      }

      return Response.json(
        { accounts: [...accounts.values()] },
        { headers: { "cache-control": "no-store" } },
      );
    } catch {
      return new Response("Archive lookup is unavailable", { status: 503 });
    }
  }
}

export const authenticate: (
  request: Request,
) => Effect.Effect<
  AccountSession,
  AccountError,
  WorkerEnvironment | Authentication | Worker.WorkerContext
> = Effect.fn("Planner.requireSession")(function* (request: Request) {
  const { subjectId, accountId, displayName } = yield* authenticateRequest(request);

  return { subjectId, accountId, displayName };
});
