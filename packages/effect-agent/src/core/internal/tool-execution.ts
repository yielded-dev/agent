import type * as Tool from "effect/ai/Tool";
import * as Context from "effect/Context";

/**
 * Declared external execution class of one Tool, used by the durable runtime
 * to decide which Tool Calls need the prepared/settled uncertainty protocol:
 *
 * - `"readonly"`: the handler performs no external mutation. A crash between
 *   start and settlement is a free re-run; no prepared record is written.
 * - `"idempotent"`: the handler declares an external idempotency contract
 *   (it owns key derivation). Prepared/settled records are written and
 *   recovery may re-execute without reconciliation proof.
 * - `"uncertain"`: no external contract is declared. Prepared/settled records
 *   are written and recovery must reconcile or record an Unknown Outcome.
 *
 * @category models
 * @since 0.1.0
 */
export type ToolExecutionClassValue = "readonly" | "idempotent" | "uncertain";

/**
 * Effect AI Tool annotation key declaring a Tool's execution class, applied
 * with `Tool.annotate(ToolExecutionClass, "...")`.
 *
 * Unannotated Tools default to `"uncertain"`: security and durability
 * decisions are fail-closed, so the framework never infers a safer class
 * from a Tool's shape.
 *
 * @category services
 * @since 0.1.0
 */
export const ToolExecutionClass = Context.Reference<ToolExecutionClassValue>(
  "@effect-agent/engine/ToolExecutionClass",
  { defaultValue: () => "uncertain" },
);

/**
 * Read a Tool's declared execution class, falling back to the fail-closed `"uncertain"` default.
 *
 * @category operations
 * @since 0.1.0
 */
export const getToolExecutionClass = (tool: Tool.Any): ToolExecutionClassValue =>
  Context.get(tool.annotations, ToolExecutionClass);
