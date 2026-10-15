import { DurableObject } from "cloudflare:workers";
import { Effect, Redacted, Schema } from "effect";

import { FundingFailpoint, makeFundingStore } from "../../src/auth/funding";
import { initializeAuthStorage } from "../../src/auth/storage";
import { FundingError, GrantFunding, RevokeFunding } from "../../src/funding-domain";
import { credentialForOwner, credentialSourceLayer } from "../../src/server/credentials";

const adminId = "00000000-0000-0000-0000-000000000001";
const emailId = "00000000-0000-0000-0000-000000000002";
const yieldedId = "00000000-0000-0000-0000-000000000003";
const otherIssuerId = "00000000-0000-0000-0000-000000000004";
const yieldedReaderId = "00000000-0000-0000-0000-000000000005";

export class FundingFixture extends DurableObject {
  fetch(request: Request) {
    const url = new URL(request.url);
    const actor = url.searchParams.get("actor") ?? adminId;

    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        yield* initializeAuthStorage(this.ctx.storage);
        const sql = this.ctx.storage.sql;

        if (url.pathname === "/seed") {
          this.ctx.storage.transactionSync(() => {
            for (const [id, name] of [
              [adminId, "Dan"],
              [emailId, "Email reader"],
              [yieldedId, "Yielded owner"],
              [otherIssuerId, "Other issuer"],
            ]) {
              sql.exec("insert into auth_subject values (?,1,?,?)", id, "revision", name);
              sql.exec("insert into auth_credential values (?,?,?,1)", id, id, "revision");
            }
            for (const [id, provider, issuer, externalSubject] of [
              [adminId, "github", "https://github.com/login/oauth", "3450486"],
              [yieldedId, "yielded", "https://auth.yielded.dev", "settings-owner"],
              [otherIssuerId, "yielded", "https://other-issuer.test", "settings-owner"],
            ]) {
              sql.exec(
                "insert into auth_oauth_identity (identityKey,provider,issuer,externalSubject,subjectId) values (?,?,?,?,?)",
                id,
                provider,
                issuer,
                externalSubject,
                id,
              );
              sql.exec(
                "insert into auth_oauth_credential (moduleId,credentialId,subjectId,identityKey,revision,active) values ('travel-planner/oauth',?,?,?,'revision',1)",
                id,
                id,
                id,
              );
            }
            sql.exec(
              "insert into auth_identifier values ('travel-planner/email','reader@gmail.com',?,1,'revision')",
              emailId,
            );
            sql.exec(
              "insert into auth_email_credential values ('travel-planner/email',?,?,'travel-planner/email','reader@gmail.com','revision',1)",
              emailId,
              emailId,
            );
          });
        }
        if (url.pathname === "/register-yielded") {
          this.ctx.storage.transactionSync(() => {
            sql.exec(
              "insert into auth_subject values (?,1,'revision','Yielded reader')",
              yieldedReaderId,
            );
            sql.exec(
              "insert into auth_credential values (?,?,'revision',1)",
              yieldedReaderId,
              yieldedReaderId,
            );
            sql.exec(
              "insert into auth_oauth_identity values (?,'yielded','https://auth.yielded.dev','yielded-reader',?)",
              yieldedReaderId,
              yieldedReaderId,
            );
            sql.exec(
              "insert into auth_oauth_credential values ('travel-planner/oauth',?,?,?,'revision',1)",
              yieldedReaderId,
              yieldedReaderId,
              yieldedReaderId,
            );
          });
        }

        const store = yield* makeFundingStore(this.ctx.storage, {
          issuer: "https://auth.yielded.dev",
          clientSecret: Redacted.make("fixture-yielded-client-secret"),
        });

        if (url.pathname === "/grant") {
          const input = yield* Effect.promise(() => request.json()).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(GrantFunding)),
          );

          return Response.json(yield* store.grant(actor, input));
        }
        if (url.pathname === "/revoke") {
          const input = yield* Effect.promise(() => request.json()).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(RevokeFunding)),
          );

          yield* store.revoke(actor, input);

          return Response.json({ revoked: true });
        }

        return Response.json(yield* store.status(actor));
      }).pipe(
        Effect.provideService(FundingFailpoint, {
          hit: (point) => {
            if (point !== url.searchParams.get("point")) return Effect.void;

            return Effect.fail(FundingError.make({ message: "Injected lost response" }));
          },
        }),
        Effect.timeout("1 second"),
        Effect.catchCause(() =>
          Effect.succeed(Response.json({ error: "Rejected" }, { status: 400 })),
        ),
      ),
    );
  }
}

export default {
  async fetch(request: Request, env: { STORE: DurableObjectNamespace }) {
    const url = new URL(request.url);
    const name = url.searchParams.get("store") ?? "test";

    if (url.pathname === "/resolve") {
      const actor = url.searchParams.get("actor") ?? emailId;

      return Effect.runPromise(
        credentialForOwner(`account-${actor}`).pipe(
          Effect.provide(
            credentialSourceLayer({
              SERVER_OPENAI_KEY: "sk-fixture-server-key-9876",
              PLANNER_THREADS_V3: { getByName: () => ({ modelCredential: async () => "null" }) },
              AUTH_V2: {
                getByName: () => ({
                  fetch: (internal) => {
                    const id = new URL(internal.url).pathname.split("/").at(-1);

                    return env.STORE.getByName(name).fetch(
                      new Request(`https://fixture/status?actor=${id}`),
                    );
                  },
                }),
              },
            }),
          ),
          Effect.match({
            onSuccess: (key) => Response.json({ lastFour: Redacted.value(key).slice(-4) }),
            onFailure: () => Response.json({ denied: true }, { status: 403 }),
          }),
        ),
      );
    }

    return env.STORE.getByName(name).fetch(request);
  },
};
