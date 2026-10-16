import { ReviewDependencySource } from "@yielded/agent-pr-review/review-repository";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { makeGitHubClient, type RepositorySnapshot, type GitHubApiFailure } from "./github.ts";

const Commit = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}$/));

const Repository = Schema.String.check(
  Schema.isMaxLength(200),
  Schema.isPattern(/^[A-Za-z0-9_-][A-Za-z0-9_.-]*\/[A-Za-z0-9_-][A-Za-z0-9_.-]*$/),
);

/** Trusted workflow configuration, never loaded from PR-controlled files. */
export const DependencySources = Schema.Array(
  Schema.Struct({
    alias: Schema.String.check(
      Schema.isMaxLength(214),
      Schema.isPattern(/^(?:@[a-z0-9_-]+\/)?[a-z0-9_-][a-z0-9._-]*$/),
    ),
    repository: Repository,
    commits: Schema.Array(Commit).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
  }),
).check(Schema.isMaxLength(8));

const Manifest = Schema.Struct({
  catalog: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});

const Pin = Schema.String.check(
  // Bun's empty username is accepted; credentials, other hosts and floating refs are not.
  Schema.isPattern(
    /^git\+https:\/\/(?:@)?github\.com\/[A-Za-z0-9_-][A-Za-z0-9_.-]*\/[A-Za-z0-9_-][A-Za-z0-9_.-]*\.git#[0-9a-f]{40}$/,
  ),
);

export interface DependencySnapshot {
  readonly scope: ReviewDependencySource;
  readonly snapshot?: RepositorySnapshot;
}

/**
 * Select each side from its exact consumer catalog, intersecting it with host authority.
 * Catalog text cannot authorize repositories or commits. No install, checkout or PR code runs.
 * Failed acquisition is retained once for the attempt, with no raw URL/error exposed to the model.
 */
export const loadDependencySources = Effect.fnUntraced(function* (input: {
  readonly configuration: typeof DependencySources.Type;
  readonly base: RepositorySnapshot;
  readonly head: RepositorySnapshot;
  readonly token: Redacted.Redacted<string>;
  readonly apiUrl: string;
}) {
  if (input.configuration.length === 0) return [];

  const catalogAt = Effect.fnUntraced(function* (snapshot: RepositorySnapshot) {
    const entry = snapshot.entry("package.json");

    if (entry?.type !== "blob" || entry.mode === "120000") return Option.none();

    return yield* snapshot
      .readTextFile("package.json")
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Manifest))), Effect.option);
  });

  const manifests = yield* Effect.all({ base: catalogAt(input.base), head: catalogAt(input.head) });
  const sources: Array<DependencySnapshot> = [];

  for (const dependency of input.configuration) {
    // Reuse immutable Git reads only; none of this client's PR operations are invoked.
    const github = yield* makeGitHubClient({
      repository: dependency.repository,
      pullRequest: 1,
      token: input.token,
      apiUrl: input.apiUrl,
    });

    const snapshots = new Map<string, Result.Result<RepositorySnapshot, GitHubApiFailure>>();

    for (const revision of ["base", "head"] as const) {
      const manifest = manifests[revision];

      const value = Option.isSome(manifest)
        ? manifest.value.catalog?.[dependency.alias]
        : undefined;

      let commit: string | null = null;
      let snapshot: RepositorySnapshot | undefined;

      let reason =
        "Root package.json catalog is unavailable or has no supported exact Git pin for this alias. Do not retry source searches.";

      if (value !== undefined && Schema.is(Pin)(value)) {
        const hash = value.lastIndexOf("#");
        const repository = value.slice(value.indexOf("github.com/") + 11, hash - 4);
        const pinned = value.slice(hash + 1);

        if (repository === dependency.repository && dependency.commits.includes(pinned)) {
          commit = pinned;
          let result = snapshots.get(pinned);

          if (result === undefined) {
            result = yield* github.readTreeSnapshot(pinned).pipe(Effect.result);
            snapshots.set(pinned, result);
          }
          if (Result.isSuccess(result)) {
            snapshot = result.success;
            reason =
              "Exact catalog pin admitted by the host. Paths are relative to this dependency repository.";
          } else {
            reason =
              "Authorized pin could not be loaded. Host must check repository Contents read permission, token scope and commit availability. Do not retry or use public web search for private source.";
          }
        } else {
          reason =
            "Catalog repository or commit is not authorized by the host. Do not retry source searches.";
        }
      }
      sources.push({
        scope: ReviewDependencySource.make({
          source: dependency.alias,
          revision,
          commit,
          status: snapshot === undefined ? "unavailable" : "available",
          reason,
        }),
        ...(snapshot === undefined ? {} : { snapshot }),
      });
    }
  }

  return sources;
});
