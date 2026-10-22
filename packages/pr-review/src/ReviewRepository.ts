/**
 * Host-supplied source access and bounded evidence for immutable review revisions.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { Path, ReadFileInput, Revision, Source } from "./internal/repository-schema.ts";

/**
 * A bounded line range at one host-authorized revision. Omit source for the reviewed repository.
 *
 * @category models
 * @since 0.1.0
 */
export interface ReadFileOptions {
  readonly source?: string;
  readonly path: string;
  readonly revision: "base" | "head";
  readonly startLine: number;
  readonly lineCount: number;
}

/**
 * A literal filename search at one host-authorized revision.
 *
 * @category models
 * @since 0.1.0
 */
export interface FindFilesOptions {
  readonly source?: string;
  readonly query: string;
  readonly revision: "base" | "head";
}

/**
 * A paginated literal source search; an empty path searches all authorized files.
 *
 * @category models
 * @since 0.1.0
 */
export interface SearchCodeOptions {
  readonly source?: string;
  readonly query: string;
  readonly path: string;
  readonly revision: "base" | "head";
  readonly cursor: number;
}

/**
 * Host-admitted dependency evidence; base/head refer to the consumer review revisions.
 *
 * @category models
 * @since 0.1.0
 */
export class ReviewDependencySource extends Schema.Class<ReviewDependencySource>(
  "@effect-agent/pr-review/ReviewDependencySource",
)({
  source: Schema.NonEmptyString.check(Schema.isMaxLength(214)),
  revision: Revision,
  commit: Schema.NullOr(Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}$/))),
  status: Schema.Literals(["available", "unavailable"]),
  reason: Schema.String.check(Schema.isMaxLength(300)),
}) {}

/**
 * Host source access failed or the requested evidence range is invalid.
 *
 * @category errors
 * @since 0.1.0
 */
export class ReviewContextError extends Schema.TaggedError<ReviewContextError>()(
  "ReviewContextError",
  { message: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)) },
) {}

/**
 * A bounded source excerpt at one immutable review revision.
 *
 * @category models
 * @since 0.1.0
 */
export class ReviewSource extends Schema.Class<ReviewSource>(
  "@effect-agent/pr-review/ReviewSource",
)({
  source: Source,
  path: Path,
  revision: Revision,
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  totalLines: Schema.Natural,
  content: Schema.String.check(Schema.isMaxLength(20_000)),
}) {
  /**
   * Apply the same line and character bounds in live and frozen-source adapters.
   *
   * @category constructors
   * @since 0.1.0
   */
  static readonly fromText = Effect.fnUntraced(function* (input: ReadFileOptions, text: string) {
    const request = yield* Schema.decodeEffect(ReadFileInput)(input).pipe(
      Effect.mapError(() => ReviewContextError.make({ message: "Invalid source range." })),
    );

    const lines = text.length === 0 ? [] : text.split("\n");

    if (lines.at(-1) === "") lines.pop();
    if (request.startLine > Math.max(1, lines.length)) {
      return yield* ReviewContextError.make({
        message: `startLine ${String(request.startLine)} exceeds the file's ${String(lines.length)} lines.`,
      });
    }

    const content = lines
      .slice(request.startLine - 1, request.startLine - 1 + request.lineCount)
      .join("\n");

    if (content.length > 20_000) {
      return yield* ReviewContextError.make({
        message: "The requested line range exceeds 20,000 characters; request fewer lines.",
      });
    }

    return ReviewSource.make({
      ...(request.source === undefined ? {} : { source: request.source }),
      path: request.path,
      revision: request.revision,
      startLine: request.startLine,
      totalLines: lines.length,
      content,
    });
  });
}

/**
 * A bounded, sorted filename search result with an explicit truncation marker.
 *
 * @category models
 * @since 0.1.0
 */
export class ReviewFileList extends Schema.Class<ReviewFileList>(
  "@effect-agent/pr-review/ReviewFileList",
)({
  paths: Schema.Array(Path).check(Schema.isMaxLength(100)),
  truncated: Schema.Boolean,
}) {}

/**
 * One matching source line and its repository path.
 *
 * @category models
 * @since 0.1.0
 */
export class ReviewSearchMatch extends Schema.Class<ReviewSearchMatch>(
  "@effect-agent/pr-review/ReviewSearchMatch",
)({
  path: Path,
  line: Schema.Int.check(Schema.isGreaterThan(0)),
  content: Schema.String.check(Schema.isMaxLength(500)),
}) {}

/**
 * A page searches twenty authorized files, with at most five matching lines per file.
 *
 * @category models
 * @since 0.1.0
 */
export class ReviewSearchResult extends Schema.Class<ReviewSearchResult>(
  "@effect-agent/pr-review/ReviewSearchResult",
)({
  matches: Schema.Array(ReviewSearchMatch).check(Schema.isMaxLength(100)),
  nextCursor: Schema.optionalKey(Schema.Natural),
  truncated: Schema.Boolean,
  unreadablePaths: Schema.Array(Path).check(Schema.isMaxLength(20)),
}) {}

/**
 * Read-only source access bound by the host to the request's exact two revisions.
 *
 * @category services
 * @since 0.1.0
 */
export class ReviewRepository extends Context.Service<
  ReviewRepository,
  {
    /** Omission authorizes only the reviewed repository; never ambient installed packages. */
    readonly dependencySources?: ReadonlyArray<ReviewDependencySource>;
    readonly readFile: (input: ReadFileOptions) => Effect.Effect<ReviewSource, ReviewContextError>;
    readonly findFiles: (
      input: FindFilesOptions,
    ) => Effect.Effect<ReviewFileList, ReviewContextError>;
    readonly searchCode: (
      input: SearchCodeOptions,
    ) => Effect.Effect<ReviewSearchResult, ReviewContextError>;
  }
>()("@effect-agent/pr-review/ReviewRepository") {}
