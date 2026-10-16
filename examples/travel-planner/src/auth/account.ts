import { Schema } from "effect";

/** Stable application data identity issued by the authentication host. */
export const AccountId = Schema.String.check(
  Schema.isPattern(/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/),
);

export const AccountSession = Schema.Struct({
  subjectId: Schema.NonEmptyString,
  accountId: AccountId,
  displayName: Schema.String,
});

export type AccountSession = typeof AccountSession.Type;

export { AccountError } from "@yielded/agent-example-auth/contract";
