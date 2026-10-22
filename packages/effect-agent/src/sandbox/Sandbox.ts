/**
 * Shared isolation identity and network-policy schemas for sandbox capabilities.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";

const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Maximum sandbox diagnostic string length: 8,192 UTF-16 code units.
 *
 * @category constants
 * @since 0.1.0
 */
export const SANDBOX_DIAGNOSTIC_MAX_LENGTH = 8 * 1024;

/**
 * The implementation and its isolation posture that executed a request.
 *
 * @category models
 * @since 0.1.0
 */
export class SandboxImplementation extends Schema.Class<SandboxImplementation>(
  "SandboxImplementation",
)({
  isolation: Schema.Literals(["isolated", "unisolated"]),
  identity: BoundedName,
}) {}

/**
 * Explicitly denies all workload network access.
 *
 * @category models
 * @since 0.1.0
 */
export class NetworkDisabled extends Schema.TaggedClass<NetworkDisabled>()("NetworkDisabled", {}) {}

/**
 * Explicitly permits connections only to the named destination and port allowlists.
 *
 * @category models
 * @since 0.1.0
 */
export class NetworkAllowlist extends Schema.TaggedClass<NetworkAllowlist>()("NetworkAllowlist", {
  domains: Schema.Array(BoundedName).check(Schema.isMaxLength(256)),
  ports: Schema.Array(PositiveInt.check(Schema.isLessThanOrEqualTo(65_535))).check(
    Schema.isMaxLength(256),
  ),
}) {}

/**
 * Network access policy supplied to a sandbox implementation.
 *
 * @category schemas
 * @since 0.1.0
 */
export const SandboxNetworkPolicy = Schema.Union([NetworkDisabled, NetworkAllowlist]);
/**
 * Decoded value of {@link SandboxNetworkPolicy}.
 *
 * @category models
 * @since 0.1.0
 */
export type SandboxNetworkPolicy = typeof SandboxNetworkPolicy.Type;
