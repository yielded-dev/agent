import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { messageDeliveryStoreLayer } from "@yielded/agent-storage-sqlite/sqlite-message-delivery-store";
import { scheduleStoreLayer } from "@yielded/agent-storage-sqlite/sqlite-schedule-store";
import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
} from "@yielded/agent-storage-sqlite/sqlite-storage-config";
import { SqliteStorageError } from "@yielded/agent-storage-sqlite/sqlite-storage-error";
import {
  SqliteStorageFailpoint,
  type SqliteStorageFailpointHandler,
} from "@yielded/agent-storage-sqlite/sqlite-storage-failpoint";
import { submissionLedgerLayer } from "@yielded/agent-storage-sqlite/sqlite-submission-ledger";
import {
  exclusiveHostClientLayer,
  exclusiveRunStorageLayer,
  threadStoreLayer,
  storageFailpointLayer,
  type SqliteStorageInitializationError,
} from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import {
  compileRegistrations,
  type AgentRegistration,
  type ResolvedBinding,
} from "@yielded/agent/agent-registration";
import { DurableAgentRuntime, DurableRuntimeConfig } from "@yielded/agent/durable-agent-runtime";
import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointHandler,
} from "@yielded/agent/durable-failpoint";
import { type MessageDeliveryError, MessageDeliveryStore } from "@yielded/agent/message-delivery";
import { DeploymentId, ProducerId } from "@yielded/agent/records";
import {
  CurrentToolFailureObserver,
  RunContextPreparationPassthrough,
  RunToolAuthorization,
  toolFailureObserverLayer,
  type RunContextPreparation,
  type RunCostEstimator,
  type RunToolFailureObserver,
} from "@yielded/agent/run-options";
import { layer as runStorageLayer } from "@yielded/agent/run-storage";
import { ThreadReader } from "@yielded/agent/thread-store";
import { ToolReconciler } from "@yielded/agent/tool-reconciler";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClientService from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import { applicationInvocationContext } from "./internal/application-context.ts";
import { ExclusiveSqliteHost } from "./internal/exclusive-host.ts";
import { NodeWakeSchedulerConfig, nodeWakeSchedulerLayer } from "./NodeWakeScheduler.ts";

const PositiveMillis = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const WorkerConcurrency = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(64),
);

/** The supplied Node durable runtime configuration failed schema validation (DEPLOY-003). */
export class NodePlatformConfigError extends Schema.TaggedError<NodePlatformConfigError>()(
  "NodePlatformConfigError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Validated Node durable runtime configuration (deployment §4: decoded once during Layer
 * construction, exposed as a typed service). Every cadence is in milliseconds and every bound is
 * finite; `workerConcurrency` caps how many worker loops `NodeDurableHost.runWorkers` drives.
 */
export class NodeDurableAgentRuntimeConfigValue extends Schema.Class<NodeDurableAgentRuntimeConfigValue>(
  "@effect-agent/platform-node/NodeDurableAgentRuntimeConfigValue",
)({
  /** SQLite database file backing the Thread Log, Submission Ledger, and delivery obligations. */
  filename: Schema.NonEmptyString,
  deploymentId: DeploymentId,
  producerId: ProducerId,
  /** Finite bound on concurrent worker loops per host (rule 10). */
  workerConcurrency: WorkerConcurrency,
  /** Fallback scan cadence for the Node wake scheduler and independent message delivery recovery. */
  wakeScanInterval: PositiveMillis,
  /** `awaitSettlement` ledger re-check cadence when no wake arrives. */
  settlementPollInterval: PositiveMillis,
  /** Active-Run abort-intent poll cadence. */
  abortPollInterval: PositiveMillis,
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition. */
  busyTimeout: NonNegativeMillis,
  /** SQLite WAL synchronization selected for this host's connection at construction. */
  synchronous: SqliteStorageConfigValue.fields.synchronous,
  /** Canonical observation poll cadence of the SQLite store. */
  observationPollInterval: NonNegativeMillis,
  /** Opt-in full history and active-state validation while opening the store. */
  verifyOnOpen: Schema.Boolean,
}) {}

/** Explicit configuration authority for the assembled Node durable runtime. */
export class NodeDurableAgentRuntimeConfig extends Context.Service<
  NodeDurableAgentRuntimeConfig,
  NodeDurableAgentRuntimeConfigValue
>()("@effect-agent/platform-node/NodeDurableAgentRuntimeConfig") {}

/**
 * Raw (unvalidated) construction options for `NodeDurableAgentRuntime.layer`. Optional fields default
 * to the documented production values; everything is schema-decoded into
 * `NodeDurableAgentRuntimeConfigValue` before any resource opens (deployment §5 gate 1).
 */
export interface NodeDurableAgentRuntimeOptions<
  ContextError = never,
  ContextRequirements = never,
  AuthorizationError = never,
  AuthorizationRequirements = never,
  ReconcilerError = never,
  ReconcilerRequirements = never,
> {
  readonly filename: string;
  readonly deploymentId: string;
  readonly producerId: string;
  /** Default 1; bounded to 1..64. */
  readonly workerConcurrency?: number | undefined;
  /** Milliseconds; default 1000. */
  readonly wakeScanInterval?: number | undefined;
  /** Milliseconds; default 500. */
  readonly settlementPollInterval?: number | undefined;
  /** Milliseconds; default 500. */
  readonly abortPollInterval?: number | undefined;
  /** Deployment-owned pricing authority used by durable cost budgets and settlements. */
  readonly estimateCostMicrousd?: RunCostEstimator | undefined;
  /** Closed trusted Tool failure reporting. Omission masks ambient observers at construction. */
  readonly toolFailureObserver?: RunToolFailureObserver | undefined;
  /** Milliseconds; default 5000. */
  readonly busyTimeout?: number | undefined;
  /**
   * Defaults to FULL. NORMAL can lose acknowledged commits after power loss or an OS crash,
   * potentially repeating external effects. The choice belongs to this host's SQLite connection.
   */
  readonly synchronous?: "FULL" | "NORMAL" | undefined;
  /** Milliseconds; default 25. */
  readonly observationPollInterval?: number | undefined;
  /** Default false. */
  readonly verifyOnOpen?: boolean | undefined;
  /** SQLite adapter fault injection (`ledger:*` / `append:*` locations); default none. */
  readonly storageFailpoint?: SqliteStorageFailpointHandler | undefined;
  /** Coordinator fault injection (`submit:*` / `terminalize:*` locations); default none. */
  readonly runtimeFailpoint?: DurableRuntimeFailpointHandler | undefined;
  /**
   * Reconciliation policy consulted for open ordinary Tool Calls before an Unknown Outcome is
   * recorded (durability §10, DUR-009). Defaults to the fail-closed `ToolReconciler.uncertain`:
   * with no registered policy, every open call stays Unknown and routes to the authorized
   * DUR-017 resolution path. Construction errors and application dependencies remain in the
   * assembled Layer's E and R; the platform supplies Crypto.
   */
  readonly toolReconciler?:
    | Layer.Layer<ToolReconciler, ReconcilerError, ReconcilerRequirements | Crypto.Crypto>
    | undefined;
  /** Host prompt preparation, acquired once with the runtime; default pass-through. */
  readonly runContext?:
    | Layer.Layer<RunContextPreparation, ContextError, ContextRequirements | Crypto.Crypto>
    | undefined;
  /**
   * Independent action-time Tool authority, acquired once with the runtime; default allow-all.
   * Construction errors and application dependencies remain in the assembled Layer's E and R.
   * The platform supplies Crypto to these extension Layers.
   */
  readonly toolAuthorization?:
    | Layer.Layer<
        RunToolAuthorization,
        AuthorizationError,
        AuthorizationRequirements | Crypto.Crypto
      >
    | undefined;
}

/** Built-in construction failures. `layer` also preserves supplied service Layers' errors. */
export type NodeDurableAgentRuntimeInitializationError =
  | NodePlatformConfigError
  | MessageDeliveryError
  | SqliteStorageInitializationError;

/**
 * The services `NodeDurableAgentRuntime.layer` provides. Additional SQLite adapters can use
 * the same client and storage configuration, sharing its serialized connection and Scope.
 */
export type NodeDurableAgentRuntimeServices = Layer.Success<
  ReturnType<typeof NodeDurableAgentRuntime.layer>
>;

const decodeConfigValue = Schema.decodeUnknownEffect(NodeDurableAgentRuntimeConfigValue);

const configFromOptions = (
  options: Omit<
    NodeDurableAgentRuntimeOptions,
    "runContext" | "toolAuthorization" | "toolReconciler"
  >,
): Effect.Effect<NodeDurableAgentRuntimeConfigValue, NodePlatformConfigError> =>
  decodeConfigValue({
    filename: options.filename,
    deploymentId: options.deploymentId,
    producerId: options.producerId,
    workerConcurrency: options.workerConcurrency ?? 1,
    wakeScanInterval: options.wakeScanInterval ?? 1_000,
    settlementPollInterval: options.settlementPollInterval ?? 500,
    abortPollInterval: options.abortPollInterval ?? 500,
    busyTimeout: options.busyTimeout ?? 5_000,
    synchronous: options.synchronous ?? "FULL",
    observationPollInterval: options.observationPollInterval ?? 25,
    verifyOnOpen: options.verifyOnOpen ?? false,
  }).pipe(
    Effect.mapError((error) =>
      NodePlatformConfigError.make({
        message: `Invalid Node durable runtime configuration: ${error.message}`,
        cause: error,
      }),
    ),
  );

/** SQLite storage configuration derived from the single validated Node configuration. */
const sqliteStorageConfigLayer: Layer.Layer<
  SqliteStorageConfig,
  never,
  NodeDurableAgentRuntimeConfig
> = Layer.effect(SqliteStorageConfig)(
  Effect.gen(function* () {
    const config = yield* NodeDurableAgentRuntimeConfig;

    return SqliteStorageConfigValue.make({
      observationPollInterval: config.observationPollInterval,
      busyTimeout: config.busyTimeout,
      synchronous: config.synchronous,
      verifyOnOpen: config.verifyOnOpen,
    });
  }),
);

/** Thread coordinator configuration derived from the single validated Node configuration. */
const durableRuntimeConfigLayer = (
  estimateCostMicrousd: RunCostEstimator | undefined,
): Layer.Layer<DurableRuntimeConfig, never, NodeDurableAgentRuntimeConfig> =>
  Layer.effect(DurableRuntimeConfig)(
    Effect.gen(function* () {
      const config = yield* NodeDurableAgentRuntimeConfig;

      return DurableRuntimeConfig.make({
        deploymentId: config.deploymentId,
        producerId: config.producerId,
        settlementPollInterval: Duration.millis(config.settlementPollInterval),
        abortPollInterval: Duration.millis(config.abortPollInterval),
        ...(estimateCostMicrousd === undefined ? {} : { estimateCostMicrousd }),
      });
    }),
  );

/** Wake fallback-scan cadence derived from the single validated Node configuration. */
const wakeSchedulerConfigLayer: Layer.Layer<
  NodeWakeSchedulerConfig,
  never,
  NodeDurableAgentRuntimeConfig
> = Layer.effect(NodeWakeSchedulerConfig)(
  Effect.gen(function* () {
    const config = yield* NodeDurableAgentRuntimeConfig;

    return { scanInterval: Duration.millis(config.wakeScanInterval) };
  }),
);

/** The managed database stays private even when a caller supplies SQL transformers. */
const privateMessageDeliveryLayer = Layer.effect(MessageDeliveryStore)(
  Effect.gen(function* () {
    const storageContext = (yield* Effect.context<
      | SqlClientService.SqlClient
      | Crypto.Crypto
      | SqliteStorageConfig
      | SqliteStorageFailpoint
      | Scope.Scope
    >()).pipe(
      Context.pick(
        SqlClientService.SqlClient,
        Crypto.Crypto,
        SqliteStorageConfig,
        SqliteStorageFailpoint,
        Scope.Scope,
      ),
    );

    const sql = Context.get(storageContext, SqlClientService.SqlClient);

    const bind = <A, E>(
      effect: Effect.Effect<
        A,
        E,
        | SqlClientService.SqlClient
        | Crypto.Crypto
        | SqliteStorageConfig
        | SqliteStorageFailpoint
        | Scope.Scope
      >,
    ) =>
      Effect.contextWith((live: Context.Context<never>) =>
        Effect.setContext(
          effect,
          Context.merge(
            Context.omit(Statement.CurrentTransformer, sql.transactionService)(live),
            storageContext,
          ),
        ),
      );

    const services = yield* bind(Layer.build(messageDeliveryStoreLayer()));

    const store = Context.get(services, MessageDeliveryStore);

    return MessageDeliveryStore.of({
      limits: store.limits,
      maxStoredValueBytes: store.maxStoredValueBytes,
      insert: (record) => bind(store.insert(record)),
      get: (key) => bind(store.get(key)),
      list: (request) => bind(store.list(request)),
      change: (key, change) => bind(store.change(key, change)),
      due: (now, limit, owner) => bind(store.due(now, limit, owner)),
      nextDeadline: (owner) => bind(store.nextDeadline(owner)),
    });
  }),
);

/**
 * The DN Layer assembly (deployment §12: a Layer-assembly library, not an app entrypoint).
 * `layer(options)` decodes the configuration, opens ONE SQLite database serving both the
 * Thread Log and the Submission Ledger under one scoped owner, wires the Node wake scheduler
 * with its ledger-scan fallback, exposes independent message delivery storage, and defaults
 * the Tool reconciliation policy to the fail-closed
 * `ToolReconciler.uncertain` (override via `options.toolReconciler`), and provides a ready
 * `DurableAgentRuntime` on top. Storage compatibility is
 * verified during construction: an incompatible database file fails the Layer with
 * `SqliteStorageCompatibilityError` before anything is mutated (DEPLOY-008).
 */
export class NodeDurableAgentRuntime {
  /** Validated configuration Layer; fails typed when the supplied options are out of bounds. */
  static configLayer<
    ContextError = never,
    ContextRequirements = never,
    AuthorizationError = never,
    AuthorizationRequirements = never,
    ReconcilerError = never,
    ReconcilerRequirements = never,
  >(
    options: NodeDurableAgentRuntimeOptions<
      ContextError,
      ContextRequirements,
      AuthorizationError,
      AuthorizationRequirements,
      ReconcilerError,
      ReconcilerRequirements
    >,
  ): Layer.Layer<NodeDurableAgentRuntimeConfig, NodePlatformConfigError> {
    return Layer.effect(NodeDurableAgentRuntimeConfig)(configFromOptions(options));
  }

  /** The full DN runtime stack over one SQLite file. */
  static layer<
    ContextError = never,
    ContextRequirements = never,
    AuthorizationError = never,
    AuthorizationRequirements = never,
    ReconcilerError = never,
    ReconcilerRequirements = never,
  >(
    options: NodeDurableAgentRuntimeOptions<
      ContextError,
      ContextRequirements,
      AuthorizationError,
      AuthorizationRequirements,
      ReconcilerError,
      ReconcilerRequirements
    >,
  ) {
    return NodeDurableAgentRuntime.assemble(DurableAgentRuntime.layerWithServices, options);
  }

  /** Own typed executable registrations for every worker using this Node runtime. */
  static layerRegistered<
    const Entries extends ReadonlyArray<AgentRegistration>,
    ContextError = never,
    ContextRequirements = never,
    AuthorizationError = never,
    AuthorizationRequirements = never,
    ReconcilerError = never,
    ReconcilerRequirements = never,
  >(
    registrations: Entries,
    options: NodeDurableAgentRuntimeOptions<
      ContextError,
      ContextRequirements,
      AuthorizationError,
      AuthorizationRequirements,
      ReconcilerError,
      ReconcilerRequirements
    >,
  ) {
    return Layer.unwrap(
      Effect.gen(function* () {
        const bindings = yield* compileRegistrations(registrations);
        const exclusive = yield* ExclusiveSqliteHost;

        // Only the managed Node host owns a private database. Other platforms retain their
        // invocation controls, including the source identity of native maintenance writes.
        const isolate = <A, E>(effect: Effect.Effect<A, E>) =>
          Effect.contextWith((live: Context.Context<never>) =>
            Effect.setContext(effect, applicationInvocationContext(live)),
          );

        return NodeDurableAgentRuntime.layerWithBindings(
          exclusive
            ? bindings.map((binding): ResolvedBinding => ({
                ...binding,
                attempt: (...args) => isolate(binding.attempt(...args)),
                ...(binding.reporting === undefined
                  ? {}
                  : {
                      reporting: binding.reporting.map((report) => ({
                        ...report,
                        prepare: (value) => isolate(report.prepare(value)),
                      })),
                    }),
              }))
            : bindings,
          options,
        );
      }).pipe(Effect.provide(NodeCrypto.layer)),
    );
  }

  /** Construct from registrations already resolved within the enclosing application Scope. */
  static layerWithBindings<
    ContextError = never,
    ContextRequirements = never,
    AuthorizationError = never,
    AuthorizationRequirements = never,
    ReconcilerError = never,
    ReconcilerRequirements = never,
  >(
    bindings: ReadonlyArray<ResolvedBinding>,
    options: NodeDurableAgentRuntimeOptions<
      ContextError,
      ContextRequirements,
      AuthorizationError,
      AuthorizationRequirements,
      ReconcilerError,
      ReconcilerRequirements
    >,
  ) {
    return NodeDurableAgentRuntime.assemble(
      DurableAgentRuntime.layerWithBindings(bindings),
      options,
    );
  }

  private static assemble<
    RuntimeError,
    RuntimeRequirements,
    ContextError,
    ContextRequirements,
    AuthorizationError,
    AuthorizationRequirements,
    ReconcilerError,
    ReconcilerRequirements,
  >(
    runtimeLayer: Layer.Layer<DurableAgentRuntime, RuntimeError, RuntimeRequirements>,
    options: NodeDurableAgentRuntimeOptions<
      ContextError,
      ContextRequirements,
      AuthorizationError,
      AuthorizationRequirements,
      ReconcilerError,
      ReconcilerRequirements
    >,
  ) {
    const assembled = Layer.unwrap(
      Effect.gen(function* () {
        const config = yield* configFromOptions(options);
        const exclusive = yield* ExclusiveSqliteHost;
        const nodeConfigLayer = Layer.succeed(NodeDurableAgentRuntimeConfig)(config);

        // Application extensions are acquired before the private database exists.
        const applicationContext = (yield* Effect.context<never>()).pipe(Context.omit(Scope.Scope));

        const applicationServices = yield* Layer.build(
          Layer.mergeAll(
            options.runContext ?? RunContextPreparationPassthrough,
            options.toolAuthorization ?? RunToolAuthorization.allowAll,
            options.toolReconciler ?? ToolReconciler.uncertain,
            options.toolFailureObserver === undefined
              ? Layer.succeed(CurrentToolFailureObserver)(undefined)
              : toolFailureObserverLayer(options.toolFailureObserver),
          ).pipe(Layer.provide(NodeCrypto.layer)),
        );

        const storageFailpoint = options.storageFailpoint;
        const runtimeFailpoint = options.runtimeFailpoint;

        const clientInfrastructure = Layer.mergeAll(
          sqliteStorageConfigLayer,
          storageFailpointLayer({
            filename: config.filename,
            failpoint:
              storageFailpoint === undefined
                ? undefined
                : (location) =>
                    Effect.scoped(Effect.suspend(() => storageFailpoint(location))).pipe(
                      Effect.setContext(applicationContext),
                    ),
          }),
          SqliteClient.layer({ filename: config.filename, disableWAL: exclusive }).pipe(
            Layer.catch((cause) =>
              Layer.effectContext<
                Layer.Success<ReturnType<typeof SqliteClient.layer>>,
                SqliteStorageError,
                never
              >(
                Effect.fail(
                  SqliteStorageError.make({
                    operation: "open SQLite",
                    message: cause.message,
                    cause,
                  }),
                ),
              ),
            ),
          ),
          NodeCrypto.layer,
        ).pipe(Layer.provide(nodeConfigLayer));

        // The managed host no longer composes its database with application SQL hooks.
        // Only the enclosing resource Scope crosses into storage construction.
        const exclusiveInfrastructure = Layer.effectContext(
          Effect.gen(function* () {
            const scope = yield* Effect.scope;

            return yield* Layer.build(
              Layer.fresh(exclusiveHostClientLayer).pipe(Layer.provideMerge(clientInfrastructure)),
            ).pipe(Effect.setContext(Context.make(Scope.Scope, scope)));
          }),
        );

        const runtimeFailpointLayer =
          runtimeFailpoint === undefined
            ? DurableRuntimeFailpoint.layer
            : Layer.succeed(DurableRuntimeFailpoint)({
                hit: (location) =>
                  Effect.scoped(Effect.suspend(() => runtimeFailpoint(location))).pipe(
                    Effect.setContext(applicationContext),
                  ),
              });

        const ordinaryStorage = runStorageLayer().pipe(
          Layer.provideMerge(Layer.mergeAll(threadStoreLayer, submissionLedgerLayer)),
        );

        const portLayers = Layer.mergeAll(
          ThreadReader.layer(),
          nodeWakeSchedulerLayer,
          exclusive ? privateMessageDeliveryLayer : messageDeliveryStoreLayer(),
          // Custom manual assemblies own schedules; the managed host owns only its runtime.
          exclusive ? Layer.empty : scheduleStoreLayer,
        );

        const ports = exclusive
          ? portLayers.pipe(
              Layer.provideMerge(exclusiveRunStorageLayer),
              Layer.provideMerge(exclusiveInfrastructure),
            )
          : portLayers.pipe(
              Layer.provideMerge(ordinaryStorage),
              Layer.provideMerge(clientInfrastructure),
            );

        return runtimeLayer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(ports, durableRuntimeConfigLayer(options.estimateCostMicrousd)),
          ),
          Layer.provide(Layer.mergeAll(wakeSchedulerConfigLayer, runtimeFailpointLayer)),
          Layer.provideMerge(nodeConfigLayer),
          Layer.provide(Layer.succeedContext(applicationServices)),
        );
      }),
    );

    return assembled;
  }
}
