import { DEFAULT_MAX_STORED_VALUE_BYTES } from "@yielded/agent-storage-cloudflare/do-storage-config";
import { DeploymentId } from "@yielded/agent/records";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

/**
 * Schema-validated configuration for the Cloudflare durable runtime (deployment spec §4:
 * decoded once during Layer construction, exposed as a typed service; DEPLOY-003). Every
 * cadence is in milliseconds; every bound is finite and checked before any resource opens.
 */

const PositiveMillis = Schema.Int.check(Schema.isGreaterThan(0));

/** Maximum supported auxiliary wave equals the native message policy's five-minute ceiling. */
export const AuxiliaryDispatchMillis = PositiveMillis.check(Schema.isLessThanOrEqualTo(300_000));
const NonNegativeMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Recovery coverage retained after activity; independent of work-start and retry deadlines. */
export const DEFAULT_RECOVERY_WINDOW_MILLIS = 60_000;

/** The supplied Cloudflare durable runtime configuration failed validation (DEPLOY-003). */
export class CloudflarePlatformConfigError extends Schema.TaggedError<CloudflarePlatformConfigError>()(
  "CloudflarePlatformConfigError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * An admission was refused by a host resource limit BEFORE any ledger row existed
 * (deployment spec §8, DEPLOY-007: "Admission has explicit bounded quota and overload
 * behavior... a typed rejection"). This is the DC analogue of `NodeDurableHost`'s
 * `AdmissionClosed` host gate: the port surface stays untouched, the refusal happens in the
 * Thread Object's submit entry point before `DurableAgentRuntime.submit` runs, and
 * nothing was admitted or written.
 */
export class AdmissionLimitExceeded extends Schema.TaggedError<AdmissionLimitExceeded>()(
  "AdmissionLimitExceeded",
  {
    limit: Schema.Literals(["queue-depth", "input-bytes", "database-bytes"]),
    actual: Schema.Int,
    maximum: Schema.Int,
  },
) {
  override get message() {
    return (
      `Admission refused before any ledger row existed: ${this.limit} ${this.actual} exceeds ` +
      `the configured maximum ${this.maximum}. Accepted work is unaffected; retry after the ` +
      "lane drains or raise the limit (DEPLOY-007)."
    );
  }
}

/** The platform's hard per-Object database cap (10 GB, developers.cloudflare.com limits). */
export const CLOUDFLARE_DATABASE_CAP_BYTES = 10_000_000_000;

/** Default database-size admission ceiling: a 1 GB safety margin under the platform cap. */
export const DEFAULT_MAX_DATABASE_BYTES = 9_000_000_000;

/**
 * Explicit bounded admission quotas checked by the Thread Object BEFORE admission
 * (exit gate "resource limits are checked before admission").
 */
export class CloudflareAdmissionLimitsValue extends Schema.Class<CloudflareAdmissionLimitsValue>(
  "@effect-agent/platform-cloudflare/CloudflareAdmissionLimitsValue",
)({
  /** Maximum nonterminal Submissions per Thread lane before new admissions refuse. */
  maxQueueDepthPerLane: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(100_000),
  ),
  /** Maximum encoded input bytes; never above the storage per-value bound. */
  maxInputBytes: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(2_000_000)),
  /** Maximum `ctx.storage.sql.databaseSize` at admission; stays under the 10 GB platform cap. */
  maxDatabaseBytes: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(CLOUDFLARE_DATABASE_CAP_BYTES),
  ),
}) {}

/**
 * Validated Cloudflare durable runtime configuration. The producer identity of one
 * Thread Object is `{producerPrefix}:{threadId}` — stable across incarnations of
 * the same deployment, distinct across deployments. The Object owns its Thread exclusively;
 * scoped local generations reject callbacks from an earlier Run owner.
 */
export class CloudflareDurableRuntimeConfigValue extends Schema.Class<CloudflareDurableRuntimeConfigValue>(
  "@effect-agent/platform-cloudflare/CloudflareDurableRuntimeConfigValue",
)({
  deploymentId: DeploymentId,
  /** Head of the minted producer identity `{producerPrefix}:{threadId}`. */
  producerPrefix: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  /** Recovery-only window. Zero restores immediate idle cancellation and the short guards. */
  recoveryWindowMillis: NonNegativeMillis,
  /**
   * Base delay of exponential backoff when a pass fails or makes no progress.
   * Half this delay (rounded up, at least 1ms) starts alarm-driven work and recovers
   * startup work. It also pre-arms mutations when the recovery window is disabled.
   */
  alarmBackoffBase: PositiveMillis,
  /** Ceiling of exponential backoff after failed or no-progress maintenance passes. */
  alarmBackoffCap: PositiveMillis,
  /** `awaitSettlement` ledger re-check cadence when no wake arrives. */
  settlementPollInterval: PositiveMillis,
  /** Active-Run abort-intent poll cadence. */
  abortPollInterval: PositiveMillis,
  /** Canonical observation poll cadence of the Durable Object store. */
  observationPollInterval: NonNegativeMillis,
  /** One disposable projection backfill wave; independent of required live applyCommitted. */
  projectionDispatchTimeoutMillis: AuxiliaryDispatchMillis,
  /** Per-value byte bound; must stay under the platform's 2 MB SQLite value limit. */
  maxStoredValueBytes: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(2_000_000),
  ),
  /** Opt-in full history and active-state validation while opening the store. */
  verifyOnOpen: Schema.Boolean,
  limits: CloudflareAdmissionLimitsValue,
}) {}

/** Explicit configuration authority for the assembled Cloudflare durable runtime. */
export class CloudflareDurableRuntimeConfig extends Context.Service<
  CloudflareDurableRuntimeConfig,
  CloudflareDurableRuntimeConfigValue
>()("@effect-agent/platform-cloudflare/CloudflareDurableRuntimeConfig") {}

/** Documented production defaults applied by `ThreadObject.layer`. */
export const CLOUDFLARE_RUNTIME_DEFAULTS = {
  recoveryWindowMillis: DEFAULT_RECOVERY_WINDOW_MILLIS,
  alarmBackoffBase: 100,
  alarmBackoffCap: 5_000,
  settlementPollInterval: 500,
  abortPollInterval: 500,
  observationPollInterval: 25,
  projectionDispatchTimeoutMillis: 30_000,
  maxStoredValueBytes: DEFAULT_MAX_STORED_VALUE_BYTES,
  verifyOnOpen: false,
  maxQueueDepthPerLane: 256,
  maxInputBytes: DEFAULT_MAX_STORED_VALUE_BYTES,
  maxDatabaseBytes: DEFAULT_MAX_DATABASE_BYTES,
} as const;
