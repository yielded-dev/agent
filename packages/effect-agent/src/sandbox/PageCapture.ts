import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { SANDBOX_DIAGNOSTIC_MAX_LENGTH, SandboxImplementation } from "./Sandbox.ts";

/**
 * Page capture (capability spec §9.2): one stateless, schema-first rendering
 * pass over a remote or supplied page — navigate (or load raw HTML), render
 * with JavaScript, and return exactly one bounded output. It is a sibling of
 * the command-shaped `Sandbox` and callback-shaped `CodeExecutor` ports: a
 * browser is intrinsically an egress device, so it carries its own explicit
 * capture contract instead of widening the sandbox network policy that every
 * existing adapter rejects.
 *
 * Everything a capture returns is untrusted, attacker-influenced content
 * (security spec §9). Stateful browser sessions, screenshots, PDFs, snapshot
 * bundles, crawling, and accessibility trees are deliberately outside this
 * port's first slice.
 */

const MAX_OUTPUT_BYTES = 8 * 1024 * 1024;
const MAX_LINKS = 4_096;
const MAX_SCRAPE_SELECTORS = 64;
const MAX_SCRAPE_ELEMENTS = 4_096;
const MAX_SCRAPE_ATTRIBUTES = 64;
// Schema `isMaxLength` counts UTF-16 elements: these are transport sanity
// bounds. The authoritative byte budget is `PageCaptureLimits.maxOutputBytes`,
// which adapters enforce on the UTF-8 encoded response.
const BoundedOutputText = Schema.String.check(Schema.isMaxLength(MAX_OUTPUT_BYTES));
const BoundedUrl = Schema.NonEmptyString.check(Schema.isMaxLength(8 * 1024));
const BoundedHtml = Schema.NonEmptyString.check(Schema.isMaxLength(2 * 1024 * 1024));
const BoundedSelector = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const BoundedAttributeName = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const BoundedAttributeValue = Schema.String.check(Schema.isMaxLength(64 * 1_024));
const BoundedPattern = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const BoundedMessage = Schema.String.check(Schema.isMaxLength(SANDBOX_DIAGNOSTIC_MAX_LENGTH));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
/** Bounded by the platform's 60-second browser inactivity ceiling. */
const BoundedTimeoutMillis = PositiveInt.check(Schema.isLessThanOrEqualTo(60_000));

const decodeUrl = Schema.decodeUnknownOption(Schema.URLFromString);

const isCredentialFreeWebUrl = (value: string, allowHttp: boolean): boolean => {
  const parsed = decodeUrl(value);

  if (Option.isNone(parsed)) return false;
  const url = parsed.value;

  return (
    (url.protocol === "https:" || (allowHttp && url.protocol === "http:")) &&
    url.hostname.length > 0 &&
    url.username === "" &&
    url.password === ""
  );
};

/** Absolute, bounded HTTPS navigation URL that never carries embedded credentials. */
export const PageCaptureTargetUrl = BoundedUrl.check(
  Schema.makeFilter((value) => isCredentialFreeWebUrl(value, false), {
    title: "an absolute HTTPS URL without embedded credentials",
  }),
);

export type PageCaptureTargetUrl = typeof PageCaptureTargetUrl.Type;

/** Discovered HTTP(S) links are data, never navigation authority, and carry no credentials. */
export const PageCaptureLinkUrl = BoundedUrl.check(
  Schema.makeFilter((value) => isCredentialFreeWebUrl(value, true), {
    title: "an absolute HTTP or HTTPS URL without embedded credentials",
  }),
);

export type PageCaptureLinkUrl = typeof PageCaptureLinkUrl.Type;

/** Capture a URL after a full render, the common case. */
export class PageUrlTarget extends Schema.TaggedClass<PageUrlTarget>()("PageUrlTarget", {
  url: PageCaptureTargetUrl,
}) {}

/** Render caller-supplied HTML instead of navigating. */
export class PageHtmlTarget extends Schema.TaggedClass<PageHtmlTarget>()("PageHtmlTarget", {
  html: BoundedHtml,
}) {}

/** What the pass renders: a navigated URL or raw HTML. */
export const PageCaptureTarget = Schema.Union([PageUrlTarget, PageHtmlTarget]);
export type PageCaptureTarget = typeof PageCaptureTarget.Type;

/**
 * The rendering engine. `chromium` is the full headless browser; `kitesurf`
 * is a lightweight stateless engine an adapter may support for lower-cost
 * captures. An adapter that cannot honor the requested engine
 * rejects it typed rather than silently substituting the other.
 */
export const PageCaptureEngine = Schema.Literals(["chromium", "kitesurf"]);
export type PageCaptureEngine = typeof PageCaptureEngine.Type;

/** Return the fully rendered HTML document. */
export class CapturePageContent extends Schema.TaggedClass<CapturePageContent>()(
  "CapturePageContent",
  {},
) {}

/** Return the rendered page converted to Markdown. */
export class CapturePageMarkdown extends Schema.TaggedClass<CapturePageMarkdown>()(
  "CapturePageMarkdown",
  {},
) {}

/** Return the hyperlinks discovered on the rendered page. */
export class CapturePageLinks extends Schema.TaggedClass<CapturePageLinks>()("CapturePageLinks", {
  visibleLinksOnly: Schema.optionalKey(Schema.Boolean),
}) {}

/** Return rendered element records grouped by their requested CSS selector. */
export class CapturePageScrape extends Schema.TaggedClass<CapturePageScrape>()(
  "CapturePageScrape",
  {
    selectors: Schema.Array(BoundedSelector).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(MAX_SCRAPE_SELECTORS),
    ),
  },
) {}

/** Exactly one bounded output per pass. */
export const PageCaptureAction = Schema.Union([
  CapturePageContent,
  CapturePageMarkdown,
  CapturePageLinks,
  CapturePageScrape,
]);

export type PageCaptureAction = typeof PageCaptureAction.Type;

/** Wait for one CSS selector to appear before capturing. */
export class PageSelectorWait extends Schema.Class<PageSelectorWait>("PageSelectorWait")({
  selector: BoundedSelector,
  timeoutMillis: Schema.optionalKey(BoundedTimeoutMillis),
}) {}

/** Navigation and readiness options applied before the capture. */
export class PageNavigationOptions extends Schema.Class<PageNavigationOptions>(
  "PageNavigationOptions",
)({
  waitUntil: Schema.optionalKey(
    Schema.Literals(["load", "domcontentloaded", "networkidle0", "networkidle2"]),
  ),
  timeoutMillis: Schema.optionalKey(BoundedTimeoutMillis),
  waitForSelector: Schema.optionalKey(PageSelectorWait),
}) {}

/** The rendered viewport. */
export class PageViewport extends Schema.Class<PageViewport>("PageViewport")({
  width: PositiveInt.check(Schema.isLessThanOrEqualTo(7_680)),
  height: PositiveInt.check(Schema.isLessThanOrEqualTo(7_680)),
}) {}

/**
 * Request egress policy for the render. Rejected resource types are never
 * fetched; when request patterns are given, only matching navigation,
 * redirect, and subresource requests are allowed. Calling capabilities own
 * the capture-target allowlist and project it into this policy so rendered
 * JavaScript cannot escape the same construction-time authority.
 */
export class PageResourcePolicy extends Schema.Class<PageResourcePolicy>("PageResourcePolicy")({
  rejectResourceTypes: Schema.optionalKey(
    Schema.Array(
      Schema.Literals([
        "document",
        "stylesheet",
        "image",
        "media",
        "font",
        "script",
        "texttrack",
        "xhr",
        "fetch",
        "eventsource",
        "websocket",
        "manifest",
        "other",
      ]),
    ).check(Schema.isMaxLength(16)),
  ),
  allowRequestPatterns: Schema.optionalKey(
    Schema.Array(BoundedPattern).check(Schema.isMaxLength(64)),
  ),
}) {}

/** Limits an adapter must either enforce or reject. Bytes are UTF-8 encoded. */
export class PageCaptureLimits extends Schema.Class<PageCaptureLimits>("PageCaptureLimits")({
  maxOutputBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_OUTPUT_BYTES)),
}) {}

/** Schema-first page capture request: one target, one action, one output. */
export class PageCaptureRequest extends Schema.Class<PageCaptureRequest>("PageCaptureRequest")({
  target: PageCaptureTarget,
  action: PageCaptureAction,
  engine: PageCaptureEngine,
  limits: PageCaptureLimits,
  navigation: Schema.optionalKey(PageNavigationOptions),
  viewport: Schema.optionalKey(PageViewport),
  resourcePolicy: Schema.optionalKey(PageResourcePolicy),
}) {}

/** The rendered HTML document. */
export class PageContentCaptured extends Schema.TaggedClass<PageContentCaptured>()(
  "PageContentCaptured",
  {
    html: BoundedOutputText,
  },
) {}

/** The rendered page as Markdown. */
export class PageMarkdownCaptured extends Schema.TaggedClass<PageMarkdownCaptured>()(
  "PageMarkdownCaptured",
  {
    markdown: BoundedOutputText,
  },
) {}

/** The hyperlinks discovered on the rendered page. */
export class PageLinksCaptured extends Schema.TaggedClass<PageLinksCaptured>()(
  "PageLinksCaptured",
  {
    links: Schema.Array(PageCaptureLinkUrl).check(Schema.isMaxLength(MAX_LINKS)),
  },
) {}

/** One bounded HTML attribute returned by a selector scrape. */
export class PageScrapeAttribute extends Schema.Class<PageScrapeAttribute>("PageScrapeAttribute")({
  name: BoundedAttributeName,
  value: BoundedAttributeValue,
}) {}

/** One rendered element returned by a selector scrape. */
export class PageScrapeElement extends Schema.Class<PageScrapeElement>("PageScrapeElement")({
  // Individual fields share the page sanity bound; the adapter enforces the
  // caller's byte budget on the complete response before decoding it.
  text: BoundedOutputText,
  html: BoundedOutputText,
  attributes: Schema.Array(PageScrapeAttribute).check(Schema.isMaxLength(MAX_SCRAPE_ATTRIBUTES)),
  left: Schema.Finite,
  top: Schema.Finite,
  width: Schema.Finite,
  height: Schema.Finite,
}) {}

/** All rendered elements matched by one requested selector. */
export class PageScrapeGroup extends Schema.Class<PageScrapeGroup>("PageScrapeGroup")({
  selector: BoundedSelector,
  results: Schema.Array(PageScrapeElement).check(Schema.isMaxLength(MAX_SCRAPE_ELEMENTS)),
}) {}

const PageScrapeGroups = Schema.Array(PageScrapeGroup).check(
  Schema.isMaxLength(MAX_SCRAPE_SELECTORS),
  Schema.makeFilter(
    (groups) =>
      groups.reduce((total, group) => total + group.results.length, 0) <= MAX_SCRAPE_ELEMENTS,
    { title: "at most 4096 aggregate scraped elements" },
  ),
);

/** Bounded rendered element records, preserving selector grouping. */
export class PageScrapeCaptured extends Schema.TaggedClass<PageScrapeCaptured>()(
  "PageScrapeCaptured",
  {
    groups: PageScrapeGroups,
  },
) {}

/** Exactly one output kind per pass, matching the requested action. */
export const PageCaptureOutput = Schema.Union([
  PageContentCaptured,
  PageMarkdownCaptured,
  PageLinksCaptured,
  PageScrapeCaptured,
]);

export type PageCaptureOutput = typeof PageCaptureOutput.Type;

/** Accounting an adapter observed for one capture. */
export class PageCaptureResourceUse extends Schema.Class<PageCaptureResourceUse>(
  "PageCaptureResourceUse",
)({
  browserMillis: Schema.optionalKey(Schema.Natural),
}) {}

/** The bounded outcome of one capture pass. */
export class PageCaptureResult extends Schema.Class<PageCaptureResult>("PageCaptureResult")({
  implementation: SandboxImplementation,
  output: PageCaptureOutput,
  resourceUse: PageCaptureResourceUse,
}) {}

/**
 * The platform refused the pass for rate or quota reasons. `retryAfterMillis`
 * carries the platform's own backoff hint when one exists; retrying earlier
 * only re-fails.
 */
export class PageCaptureRateLimitedError extends Schema.TaggedError<PageCaptureRateLimitedError>()(
  "PageCaptureRateLimitedError",
  {
    implementation: SandboxImplementation,
    reason: Schema.Literals(["rate", "quota"]),
    retryAfterMillis: Schema.optionalKey(Schema.Natural),
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Navigation, readiness, or capture of the target page failed. */
export class PageCaptureNavigationError extends Schema.TaggedError<PageCaptureNavigationError>()(
  "PageCaptureNavigationError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The adapter cannot honestly honor one requested feature (CAP-010 idiom). */
export class PageCaptureUnsupportedError extends Schema.TaggedError<PageCaptureUnsupportedError>()(
  "PageCaptureUnsupportedError",
  {
    implementation: SandboxImplementation,
    feature: Schema.Literals([
      "engine",
      "action",
      "target",
      "navigation",
      "viewport",
      "resource-policy",
    ]),
    message: BoundedMessage,
  },
) {}

/** The response exceeded the request's output byte budget. */
export class PageCaptureOutputLimitError extends Schema.TaggedError<PageCaptureOutputLimitError>()(
  "PageCaptureOutputLimitError",
  {
    implementation: SandboxImplementation,
    limit: PositiveInt,
    observed: Schema.Natural,
  },
) {}

/** The platform or transport produced a value outside the capture protocol. */
export class PageCaptureProtocolError extends Schema.TaggedError<PageCaptureProtocolError>()(
  "PageCaptureProtocolError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Expected capture failures. Interruption stays an Effect interruption. */
export const PageCaptureError = Schema.Union([
  PageCaptureRateLimitedError,
  PageCaptureNavigationError,
  PageCaptureUnsupportedError,
  PageCaptureOutputLimitError,
  PageCaptureProtocolError,
]);

export type PageCaptureError = typeof PageCaptureError.Type;

/**
 * Stateless page-capture port. One `capture` is one pass: the adapter owns no
 * session, replay, approval, or Thread semantics, identifies its
 * isolation posture honestly (CAP-010), enforces or rejects every requested
 * limit and policy, and returns exactly one bounded output. Capture results
 * are untrusted input to whatever reads them.
 */
export class PageCapture extends Context.Service<
  PageCapture,
  {
    readonly capture: (
      request: PageCaptureRequest,
    ) => Effect.Effect<PageCaptureResult, PageCaptureError>;
  }
>()("@effect-agent/sandbox/PageCapture") {}

/** Type helper for implementations that preserve the public PageCapture contract. */
export type PageCaptureCapture = (
  request: PageCaptureRequest,
) => Effect.Effect<PageCaptureResult, PageCaptureError>;
