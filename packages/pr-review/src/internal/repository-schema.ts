import * as Schema from "effect/Schema";

/** @internal */
export const Revision = Schema.Literals(["base", "head"]);

/** @internal */
export const Path = Schema.NonEmptyString.check(Schema.isMaxLength(512));

/** @internal */
export const Source = Schema.optionalKey(
  Schema.NonEmptyString.check(Schema.isMaxLength(214)),
).annotate({
  description:
    "Omit for the reviewed repository. For dependencies use only an available alias from list_sources; never a URL, repository name, or ref.",
});

/** @internal */
export const ReadFileInput = Schema.Struct({
  source: Source,
  path: Path,
  revision: Revision,
  startLine: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_000_000 })),
  lineCount: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 })),
});

/** @internal */
export const FindFilesInput = Schema.Struct({
  source: Source,
  query: Schema.String.check(Schema.isMaxLength(200)),
  revision: Revision,
});

/** @internal */
export const SearchCodeInput = Schema.Struct({
  source: Source,
  query: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  path: Schema.String.check(Schema.isMaxLength(512)),
  revision: Revision,
  cursor: Schema.Natural.check(Schema.isLessThanOrEqualTo(100_000)),
});
