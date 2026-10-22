/**
 * Validated configuration for Durable Object SQLite storage adapters.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * Default per-value byte bound, below the Durable Object SQLite limit.
 * Values exceeding the configured bound fail before mutation.
 *
 * @category models
 * @since 0.1.0
 */
export const DEFAULT_MAX_STORED_VALUE_BYTES = 1_900_000;

/** The hard schema ceiling for the configurable bound: never at or above the platform limit. */
const MaxStoredValueBytes = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(2_000_000),
);

/**
 * Validated construction configuration consumed by the Durable Object storage Layers. The
 * storage identity itself belongs to the SqlClient Layer (built from `ctx.storage`);
 * duplicating it here could silently diverge from the handle actually in use.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DoStorageConfigValue extends Schema.Class<DoStorageConfigValue>(
  "@effect-agent/storage-cloudflare/DoStorageConfigValue",
)({
  observationPollInterval: ObservationPollInterval,
  /**
   * Maximum bytes for any single stored text value (canonical batch/record JSON, admission
   * input payload, checkpoint JSON). Enforced typed BEFORE any write; must stay under the
   * platform's 2 MB per-value limit.
   */
  maxStoredValueBytes: MaxStoredValueBytes,
  /**
   * Audit stored payload schemas, identities, and batch coverage while opening the store.
   * Per-operation Schema decoding remains enabled; this complete scan is an explicit opt-in.
   */
  verifyOnOpen: Schema.Boolean,
}) {}

/**
 * Explicit Durable Object storage configuration authority.
 *
 * @category services
 * @since 0.1.0
 */
export class DoStorageConfig extends Context.Service<DoStorageConfig, DoStorageConfigValue>()(
  "@effect-agent/storage-cloudflare/DoStorageConfig",
) {}
