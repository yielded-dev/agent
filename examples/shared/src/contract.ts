import { OAuthScope } from "@yielded/auth";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as Schema from "effect/Schema";

export const AccountClaims = Schema.Struct({
  accountId: Schema.String.check(Schema.isUUID()),
  displayName: Schema.String,
});

export const Session = Schema.Struct({
  subjectId: Schema.NonEmptyString,
  ...AccountClaims.fields,
  scopes: Schema.Array(OAuthScope.OAuthScope),
  expiresAtMillis: Schema.Natural,
});

export type Session = typeof Session.Type;

export class AccountError extends Schema.TaggedError<AccountError>()("AccountError", {
  code: Schema.Literals(["unauthorized", "unavailable"]),
  message: Schema.String,
}) {}

export const AuthApi = HttpApi.make("ExampleAuth").add(
  HttpApiGroup.make("auth").add(
    HttpApiEndpoint.get("session", "/auth/session", { success: Schema.NullOr(Session) }),
    HttpApiEndpoint.post("refresh", "/auth/refresh", { success: Schema.NullOr(Session) }),
    HttpApiEndpoint.post("logout", "/auth/logout", { success: Schema.Void }),
  ),
);
