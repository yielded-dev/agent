/**
 * Versioned Agent and model registrations for durable execution. Registration captures executable
 * services while retaining the replay contracts needed by pending operations.
 *
 * @since 0.1.0
 */

export {
  /**
   * No unique current Binding can execute the stable Agent identity or decode its accepted input.
   *
   * @category errors
   * @since 0.1.0
   */
  BindingUnavailable,
  /**
   * Captured at runtime construction; callers cannot replace routing during an Attempt.
   *
   * @category services
   * @since 0.1.0
   */
  CurrentBindingSelection,
  /**
   * Submission execution/recovery routing for stable identities shared by current Definitions.
   * Worker declarations and peer-messaging endpoints still require unique Agent identities.
   *
   * @category models
   * @since 0.1.0
   */
  type BindingSelection,
  /**
   * Build one exact worker registration from an executable Agent Binding and
   * its already-computed definition digests.
   *
   * `make(agent, digests)` captures the binding plus its worker-requirement
   * Context at Layer/effect construction time. Hosts that start from application
   * version declarations should use `compileRegistrations`; low-level fixtures
   * may supply a previously computed digest triple directly.
   *
   * @category operations
   * @since 0.1.0
   */
  DurableWorkerBinding,
  /**
   * Compile heterogeneous Agent descriptors into exact, dependency-closed worker registrations.
   *
   * @category operations
   * @since 0.1.0
   */
  compileRegistrations,
  /**
   * Exact immutable admission, lineage and delivery evidence; never an executable-code gate.
   *
   * @category operations
   * @since 0.1.0
   */
  definitionDigestsEqual,
  /**
   * Compile current metadata without acquiring executable services.
   *
   * @category operations
   * @since 0.1.0
   */
  compileBindingContracts,
  /**
   * Explicit versions must cover every current Tool. Change a version when handler meaning,
   * durable Step names/codecs, external idempotency keys, authorization, completion projection,
   * or child/report protocols change. JSON Schema cannot detect handler or codec-transform changes.
   *
   * @category models
   * @since 0.1.0
   */
  type ReplayVersions,
  /**
   * Application versions and a model Layer, supplied directly or through an existing Binding.
   *
   * @category utility types
   * @since 0.1.0
   */
  type AgentRegistration,
  /**
   * Trusted claim identity for resolving per-Attempt application services. Not model input.
   *
   * @category models
   * @since 0.1.0
   */
  type AgentAttemptContext,
  /**
   * Executable model Binding accepted by durable registration descriptors.
   *
   * @category models
   * @since 0.1.0
   */
  type ExecutableAgentBinding,
  /**
   * The current registration cannot uniquely supply the accepted Agent's executable binding.
   *
   * @category models
   * @since 0.1.0
   */
  type DurableBindingFailure,
  /**
   * One exact executable registration used by durable claim-time resolution.
   *
   * @category models
   * @since 0.1.0
   */
  type ResolvedBinding,
} from "./internal/agent-registration.ts";
