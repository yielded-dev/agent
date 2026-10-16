import type { R2 } from "effect-cf";
import * as Context from "effect/Context";

/** App tools can declare storage without loading the native Cloudflare entrypoints. */
export class AppBuildBucket extends Context.Service<AppBuildBucket, R2.R2Client>()(
  "trip-app/AppBuildBucket",
) {}
