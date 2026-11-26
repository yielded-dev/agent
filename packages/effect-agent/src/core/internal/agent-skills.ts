import * as Skill from "effect/ai/Skill";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import { PinnedTool } from "../ToolExposure.ts";
import { ToolExecutionClass } from "./tool-execution.ts";
import { utf8ByteLength } from "./utf8.ts";

export const SkillLoadResult = Schema.Struct({
  name: Schema.String,
  instructions: Schema.String,
  compatibility: Schema.optionalKey(Schema.String),
  files: Schema.Array(Schema.String),
});

export const SkillReadResult = Schema.Struct({
  name: Schema.String,
  path: Schema.String,
  content: Schema.String,
  startLine: Schema.Natural,
  endLine: Schema.Natural,
  totalLines: Schema.Natural,
  nextOffset: Schema.NullOr(Schema.Natural),
});

export class SkillLookupError extends Schema.TaggedError<SkillLookupError>()("SkillLookupError", {
  reason: Schema.Literals([
    "SkillNotFound",
    "FileNotFound",
    "UnsupportedFile",
    "InvalidRange",
    "ContentTooLarge",
  ]),
  message: Schema.String,
}) {}

export const SkillTool = Context.Reference<boolean>("@yielded/agent/core/SkillTool", {
  defaultValue: () => false,
});

export const isSkillTool = (tool: Tool.Any): boolean => Context.get(tool.annotations, SkillTool);

const loadParameters = Schema.Struct({ name: Schema.String });

const readParameters = Schema.Struct({
  name: Schema.String,
  path: Schema.String,
  offset: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
});

const loadTool = Tool.make("load_skill", {
  description: "Load the complete instructions and available text-file paths for a named skill.",
  parameters: loadParameters,
  success: SkillLoadResult,
  failure: SkillLookupError,
  failureMode: "return",
})
  .annotate(SkillTool, true)
  .annotate(Tool.Readonly, true)
  .annotate(PinnedTool, true)
  .annotate(ToolExecutionClass, "readonly");

const readTool = Tool.make("read_skill_file", {
  description:
    "Read a skill's text file using a path relative to its root. offset is one-based (default 1); limit caps lines, otherwise return as many complete lines as fit. Continue with nextOffset until null.",
  parameters: readParameters,
  success: SkillReadResult,
  failure: SkillLookupError,
  failureMode: "return",
})
  .annotate(SkillTool, true)
  .annotate(Tool.Readonly, true)
  .annotate(PinnedTool, true)
  .annotate(ToolExecutionClass, "readonly");

export const skillToolkit = Toolkit.make(loadTool, readTool);

const freezeJson = <A extends Schema.Json>(value: A): A => {
  if (Array.isArray(value)) {
    for (const item of value) freezeJson(item);
    Object.freeze(value);
  } else if (value !== null && typeof value === "object") {
    for (const item of Object.values(value)) freezeJson(item);
    Object.freeze(value);
  }

  return value;
};

const frontmatterJson = Schema.fromJsonString(Skill.Frontmatter);
const encodeFrontmatter = Schema.encodeSync(frontmatterJson);
const decodeFrontmatter = Schema.decodeUnknownSync(frontmatterJson);

export const snapshotSkills = (
  skills: ReadonlyArray<Skill.Skill> | undefined,
): ReadonlyArray<Skill.Skill> => {
  const names = new Set<string>();
  const snapshots: Array<Skill.Skill> = [];

  for (const input of skills ?? []) {
    const skill = Schema.decodeSync(Skill.Skill)(input);

    if (names.has(skill.frontmatter.name)) {
      // oxlint-disable-next-line effect/no-untyped-throw -- Agent.make is a synchronous constructor; reject invalid arguments with a tagged error.
      throw new PlatformError.BadArgument({
        module: "Agent",
        method: "make",
        description: `Duplicate skill name: ${skill.frontmatter.name}`,
      });
    }
    names.add(skill.frontmatter.name);
    if (skill.frontmatter["disable-model-invocation"] === true) continue;
    const files: Record<string, string | Uint8Array> = {};

    for (const [path, value] of Object.entries(skill.files)) {
      if (typeof value === "string") {
        Object.defineProperty(files, path, { value, enumerable: true });
      } else {
        const bytes = new Uint8Array(value);

        Object.defineProperty(files, path, {
          get: () => new Uint8Array(bytes),
          enumerable: true,
        });
      }
    }
    const frontmatter = freezeJson(decodeFrontmatter(encodeFrontmatter(skill.frontmatter)));

    const snapshot = Skill.make({
      frontmatter,
      instructions: skill.instructions,
      files,
    });

    // NOTE: class validation materializes Record properties; restore copy-on-read bytes afterward.
    Object.defineProperty(snapshot, "files", { value: Object.freeze(files), enumerable: true });
    Object.defineProperty(snapshot, "frontmatter", { value: frontmatter, enumerable: true });
    snapshots.push(Object.freeze(snapshot));
  }

  return Object.freeze(snapshots);
};

export const withSkillTools = (
  toolkit: Toolkit.Any,
  skills: ReadonlyArray<Skill.Skill>,
): Toolkit.Any => {
  if (skills.length === 0) return toolkit;
  for (const name of [loadTool.name, readTool.name]) {
    if (!Object.hasOwn(toolkit.tools, name)) continue;
    // oxlint-disable-next-line effect/no-untyped-throw -- Agent.make is a synchronous constructor; reject invalid arguments with a tagged error.
    throw new PlatformError.BadArgument({
      module: "Agent",
      method: "make",
      description: `${name} is reserved for Agent skills`,
    });
  }

  return Toolkit.make(...Object.values(toolkit.tools), loadTool, readTool);
};

const missingSkill = () =>
  SkillLookupError.make({
    reason: "SkillNotFound",
    message: "The requested skill is not available.",
  });

const tooLarge = () =>
  SkillLookupError.make({
    reason: "ContentTooLarge",
    message: "The complete skill content does not fit the configured tool-result limit.",
  });

const encodeLoad = Schema.encodeSync(Schema.fromJsonString(SkillLoadResult));
const encodeRead = Schema.encodeSync(Schema.fromJsonString(SkillReadResult));

export const loadSkill = (
  skills: ReadonlyArray<Skill.Skill>,
  name: string,
  maxBytes: number,
): Effect.Effect<typeof SkillLoadResult.Type, SkillLookupError> =>
  Effect.suspend(() => {
    const skill = skills.find((candidate) => candidate.frontmatter.name === name);

    if (skill === undefined) return Effect.fail(missingSkill());

    const result = {
      name,
      instructions: skill.instructions,
      ...(skill.frontmatter.compatibility === undefined
        ? {}
        : { compatibility: skill.frontmatter.compatibility }),
      files: Object.keys(skill.files)
        .filter((path) => typeof skill.files[path] === "string")
        .sort(),
    };

    return utf8ByteLength(encodeLoad(result)) <= maxBytes
      ? Effect.succeed(result)
      : Effect.fail(tooLarge());
  });

export const readSkillFile = (
  skills: ReadonlyArray<Skill.Skill>,
  request: typeof readParameters.Type,
  maxBytes: number,
): Effect.Effect<typeof SkillReadResult.Type, SkillLookupError> =>
  Effect.suspend(() => {
    const skill = skills.find((candidate) => candidate.frontmatter.name === request.name);

    if (skill === undefined) return Effect.fail(missingSkill());
    if (!Object.hasOwn(skill.files, request.path))
      return Effect.fail(
        SkillLookupError.make({
          reason: "FileNotFound",
          message: "The requested path is not a file in this skill bundle.",
        }),
      );
    const content = skill.files[request.path];

    if (typeof content !== "string")
      return Effect.fail(
        SkillLookupError.make({
          reason: "UnsupportedFile",
          message: "Only text skill files can be read.",
        }),
      );
    const offset = request.offset ?? 1;
    const lines = content.split("\n");

    if (
      !Number.isSafeInteger(offset) ||
      offset < 1 ||
      offset > lines.length ||
      (request.limit !== undefined && (!Number.isSafeInteger(request.limit) || request.limit < 1))
    ) {
      return Effect.fail(
        SkillLookupError.make({
          reason: "InvalidRange",
          message: "Offset and limit must be positive integers selecting an existing line.",
        }),
      );
    }
    const available = Math.min(lines.length - offset + 1, request.limit ?? lines.length);

    const resultFor = (count: number): typeof SkillReadResult.Type => ({
      name: request.name,
      path: request.path,
      content: lines.slice(offset - 1, offset - 1 + count).join("\n"),
      startLine: offset,
      endLine: offset + count - 1,
      totalLines: lines.length,
      nextOffset: offset + count <= lines.length ? offset + count : null,
    });

    const complete = resultFor(available);

    if (utf8ByteLength(encodeRead(complete)) <= maxBytes) return Effect.succeed(complete);

    let low = 1;
    let high = available - 1;
    let result: typeof SkillReadResult.Type | undefined;

    while (low <= high) {
      const count = Math.floor((low + high) / 2);
      const candidate = resultFor(count);

      if (utf8ByteLength(encodeRead(candidate)) <= maxBytes) {
        result = candidate;
        low = count + 1;
      } else {
        high = count - 1;
      }
    }

    return result === undefined
      ? Effect.fail(
          SkillLookupError.make({
            reason: "ContentTooLarge",
            message: "The first requested line does not fit the configured tool-result limit.",
          }),
        )
      : Effect.succeed(result);
  });
