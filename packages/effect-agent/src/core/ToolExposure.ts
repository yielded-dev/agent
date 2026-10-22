/**
 * Contracts for bounded Tool exposure, selection, namespaces, and additional programmatic catalogue
 * entries.
 *
 * @since 0.1.0
 */

import type * as Tool from "effect/ai/Tool";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

/**
 * Registered native names only. Empty selection deliberately clears non-pinned Tools.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ToolNames = Schema.Array(
  Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
).check(
  Schema.isMaxLength(1_024),
  Schema.makeFilter((names) => new Set(names).size === names.length, {
    title: "Tool names are unique",
  }),
);

/**
 * One run-scoped replacement, independent of search and of model-visible result text.
 *
 * @category models
 * @since 0.1.0
 */
export class Selection extends Schema.Class<Selection>("@effect-agent/core/ToolExposure/Selection")(
  {
    toolNames: ToolNames,
  },
) {}

/**
 * Exact declarations used by one model request, with the selection that produced them.
 *
 * @category models
 * @since 0.1.0
 */
export class Snapshot extends Schema.Class<Snapshot>("@effect-agent/core/ToolExposure/Snapshot")({
  exposedToolNames: ToolNames,
  selection: Schema.optionalKey(Selection),
}) {}

/**
 * Schema bounding the number of exposed Tools and aggregate schema size in bytes.
 *
 * @category schemas
 * @since 0.1.0
 */
export const Limits = Schema.Struct({
  maxTools: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
  maxSchemaBytes: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(4_194_304)),
});

/**
 * Omission preserves eager exposure. Selection changes take effect only after a complete batch.
 *
 * @category models
 * @since 0.1.0
 */
export interface Configuration {
  readonly initialToolNames?: ReadonlyArray<string> | undefined;
  readonly maxTools?: number | undefined;
  readonly maxSchemaBytes?: number | undefined;
}

/**
 * Trusted grouping metadata; never inferred from a Tool's name.
 *
 * @category services
 * @since 0.1.0
 */
export const ToolNamespace = Context.Reference<string | undefined>(
  "@effect-agent/core/ToolExposure/ToolNamespace",
  { defaultValue: () => undefined },
);

/**
 * An ordinary readonly Tool whose successful result has a `toolNames` field of native names.
 *
 * @category services
 * @since 0.1.0
 */
export const DiscoveryTool = Context.Reference<boolean>(
  "@effect-agent/core/ToolExposure/DiscoveryTool",
  { defaultValue: () => false },
);

/**
 * Remains selected while eligible. Host visibility and inherited grants may hide an explicit
 * pin without failing the Run; discovery, context rollover and required completion stay mandatory.
 *
 * @category services
 * @since 0.1.0
 */
export const PinnedTool = Context.Reference<boolean>("@effect-agent/core/ToolExposure/PinnedTool", {
  defaultValue: () => false,
});

/**
 * Extra documentation candidates executed through their owning native Tool, such as Code Mode.
 *
 * @category models
 * @since 0.1.0
 */
export interface AdditionalCatalogEntry {
  readonly tool: Tool.Any;
  readonly namespace: string;
  readonly method: string;
}

/**
 * Optional declarations of programmatic Tools available for discovery alongside native Tools; the
 * default catalogue is empty.
 *
 * @category services
 * @since 0.1.0
 */
export const AdditionalToolCatalog = Context.Reference<ReadonlyArray<AdditionalCatalogEntry>>(
  "@effect-agent/core/ToolExposure/AdditionalToolCatalog",
  { defaultValue: () => [] },
);

/**
 * The owning Tool description embeds its complete additional catalogue.
 *
 * @category services
 * @since 0.1.0
 */
export const IncludesCatalogDocumentation = Context.Reference<boolean>(
  "@effect-agent/core/ToolExposure/IncludesCatalogDocumentation",
  { defaultValue: () => false },
);
