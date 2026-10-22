/**
 * Schema-validated browser credential grants and host-controlled private entry.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const Selector = Schema.NonEmptyString.check(Schema.isMaxLength(2048));

/**
 * Supported credential categories for private browser entry.
 *
 * @category schemas
 * @since 0.1.0
 */
export const CredentialKind = Schema.Literals(["login", "card"]);

/**
 * Permitted roles of credential fields in an authorized form.
 *
 * @category schemas
 * @since 0.1.0
 */
export const CredentialFieldRole = Schema.Literals([
  "username",
  "password",
  "card-name",
  "card-number",
  "card-expiry",
  "card-expiry-month",
  "card-expiry-year",
  "card-security-code",
]);

/**
 * Canonical HTTPS origins, including non-default ports. Grants must match exactly.
 *
 * @category schemas
 * @since 0.1.0
 */
export const CredentialOrigin = Schema.String.check(
  Schema.isMaxLength(2048),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(value);

      return url.protocol === "https:" && url.origin === value && !url.username && !url.password;
    } catch {
      return false;
    }
  }),
);

/**
 * An identified browser field authorized to receive private material.
 *
 * @category schemas
 * @since 0.1.0
 */
export class CredentialTarget extends Schema.Class<CredentialTarget>("CredentialTarget")({
  topOrigin: CredentialOrigin,
  frameOrigin: CredentialOrigin,
  recipientOrigin: CredentialOrigin,
}) {}

/**
 * Selectors identify one native form. Roles explicitly select material; no field inference occurs.
 *
 * @category schemas
 * @since 0.1.0
 */
export class CredentialForm extends Schema.Class<CredentialForm>("CredentialForm")({
  kind: CredentialKind,
  frame: Schema.optionalKey(Schema.Array(Selector).check(Schema.isMaxLength(8))),
  fields: Schema.Array(Schema.Struct({ selector: Selector, role: CredentialFieldRole })).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(8),
  ),
}) {}

/**
 * Private, persistable evidence for an exact page, document and native form controls.
 * Keep it with the host's one-use request, never accept a binding from a model or submitter.
 * A detached document or replaced control requires a fresh user request.
 *
 * @category schemas
 * @since 0.1.0
 */
export class CredentialBinding extends Schema.Class<CredentialBinding>("CredentialBinding")({
  version: Schema.Literal(1),
  pageId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  frameId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  target: CredentialTarget,
  nodes: Schema.Array(Schema.Int.check(Schema.isGreaterThan(0))).check(
    Schema.isMinLength(3),
    Schema.isMaxLength(10),
  ),
  shape: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
}) {}

/**
 * A request to fill the fields identified by a prepared credential binding.
 *
 * @category schemas
 * @since 0.1.0
 */
export class FillCredentialRequest extends Schema.Class<FillCredentialRequest>(
  "FillCredentialRequest",
)({
  ...CredentialForm.fields,
  credential: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  /** Required for delayed user entry; omitted for immediate, host-authorized vault fills. */
  binding: Schema.optionalKey(CredentialBinding),
}) {}

const SecretText = Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(1024)), {
  disallowJsonEncode: true,
});

/**
 * Host-only material. Never return or encode it through a Tool, log, trace, or checkpoint.
 *
 * @category schemas
 * @since 0.1.0
 */
export const LoginCredential = Schema.TaggedStruct("LoginCredential", {
  username: SecretText,
  password: SecretText,
});

/**
 * Security codes are transient; the host must not persist them.
 *
 * @category schemas
 * @since 0.1.0
 */
export const CardCredential = Schema.TaggedStruct("CardCredential", {
  name: SecretText,
  number: Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{12,19}$/)), {
    disallowJsonEncode: true,
  }),
  expiry: SecretText,
  expiryMonth: Schema.Redacted(Schema.String.check(Schema.isPattern(/^(0[1-9]|1[0-2])$/)), {
    disallowJsonEncode: true,
  }),
  expiryYear: Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{4}$/)), {
    disallowJsonEncode: true,
  }),
  securityCode: Schema.optionalKey(
    Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{3,4}$/)), {
      disallowJsonEncode: true,
    }),
  ),
});

/**
 * Supported redacted material for private browser entry.
 *
 * @category schemas
 * @since 0.1.0
 */
export const BrowserCredentialMaterial = Schema.Union([LoginCredential, CardCredential]);
/**
 * Decoded value of the BrowserCredentialMaterial schema.
 *
 * @category models
 * @since 0.1.0
 */
export type BrowserCredentialMaterial = typeof BrowserCredentialMaterial.Type;

/**
 * The host refused or could not supply authorized credential material.
 *
 * @category errors
 * @since 0.1.0
 */
export class CredentialAccessError extends Schema.TaggedError<CredentialAccessError>()(
  "CredentialAccessError",
  {
    reason: Schema.Literals([
      "denied",
      "missing-credential",
      "needs-attention",
      "busy",
      "resolver",
    ]),
  },
) {}

/**
 * Current origin and grant evidence presented to the credential authority.
 *
 * @category models
 * @since 0.1.0
 */
export interface CredentialAccessRequest {
  readonly credential: string;
  readonly kind: typeof CredentialKind.Type;
  readonly target: CredentialTarget;
  readonly roles: ReadonlyArray<typeof CredentialFieldRole.Type>;
}

/**
 * Invocation-owned authority. Derive caller/ownership from the host, not the public credential
 * identifier. Authorize the actual merchant, frame, recipient, roles, and side effects of filling.
 * Repeated checks must consult current grants. Never log or trace resolved material.
 * Bind caller identity and identifier-to-vault-item selection for the whole invocation; an alias
 * changing between resolve and authorize must never authorize different material.
 *
 * @category services
 * @since 0.1.0
 */
export class BrowserCredentialAccess extends Context.Service<
  BrowserCredentialAccess,
  {
    readonly authorize: (
      request: CredentialAccessRequest,
    ) => Effect.Effect<void, CredentialAccessError>;
    readonly resolve: (
      request: CredentialAccessRequest,
    ) => Effect.Effect<BrowserCredentialMaterial, CredentialAccessError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserCredentialAccess") {}

/**
 * How far private field entry progressed before returning.
 *
 * @category schemas
 * @since 0.1.0
 */
export const CredentialDispatch = Schema.Literals([
  "not-dispatched",
  "possibly-dispatched",
  "dispatched",
]);

/**
 * Filled counts acknowledged assignments, not authentication, submission, or website acceptance.
 *
 * @category schemas
 * @since 0.1.0
 */
export class CredentialFillResult extends Schema.Class<CredentialFillResult>(
  "CredentialFillResult",
)({
  dispatch: Schema.Literal("dispatched"),
  filled: Schema.Natural.check(Schema.isLessThanOrEqualTo(8)),
}) {}

/**
 * Content-free evidence. A lost reply never authorizes a retry or proves that no write occurred.
 *
 * @category errors
 * @since 0.1.0
 */
export class CredentialFillError extends Schema.TaggedError<CredentialFillError>()(
  "CredentialFillError",
  {
    reason: Schema.Literals([
      "denied",
      "missing-credential",
      "stale-target",
      "unsupported",
      "needs-attention",
      "busy",
      "provider",
      "resolver",
      "timeout",
    ]),
    dispatch: CredentialDispatch,
    filled: Schema.Natural.check(Schema.isLessThanOrEqualTo(8)),
    cleanup: Schema.Literals(["not-requested", "confirmed", "unconfirmed"]),
  },
) {}
