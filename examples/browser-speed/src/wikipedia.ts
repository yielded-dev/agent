import { Effect, Schema } from "effect";
import type { Frame } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { Browser } from "./browser.ts";
import { ArticleTitle, LabError, type WikipediaChallenge, type WikiRace } from "./contract.ts";
import { Trace } from "./telemetry.ts";

const normalizeTitle = (title: string) => title.replaceAll("_", " ").trim().replace(/\s+/g, " ");

const articleUrl = (title: string) =>
  `https://en.wikipedia.org/wiki/${encodeURIComponent(normalizeTitle(title).replaceAll(" ", "_"))}`;

const articleTitle = (value: string): string | undefined => {
  try {
    const url = new URL(value);

    if (
      url.origin !== "https://en.wikipedia.org" ||
      url.search ||
      url.hash ||
      !url.pathname.startsWith("/wiki/")
    )
      return;
    const title = normalizeTitle(decodeURIComponent(url.pathname.slice(6)));

    if (!Schema.is(ArticleTitle)(title)) return;

    return title.charAt(0).toUpperCase() + title.slice(1);
  } catch {
    return;
  }
};

const TargetResponse = Schema.Struct({
  query: Schema.Struct({
    pages: Schema.Array(
      Schema.Struct({
        pageid: Schema.optionalKey(Schema.Int),
        ns: Schema.Int,
        title: Schema.String,
        missing: Schema.optionalKey(Schema.Boolean),
      }),
    ),
  }),
});

/** Game setup and arrival evidence only. The agent chooses and follows links through Code Mode. */
export const prepareWikipedia = Effect.fnUntraced(function* (challenge: WikipediaChallenge) {
  const browser = yield* Browser;
  const trace = yield* Trace;

  const resolved = yield* trace.measure(
    "setup",
    "Resolve destination identity",
    browser
      .native((page) =>
        page.evaluate(async (title) => {
          const response = await fetch(
            `/w/api.php?action=query&format=json&formatversion=2&redirects=1&titles=${encodeURIComponent(title)}`,
            { signal: AbortSignal.timeout(8_000) },
          );

          if (!response.ok) return null;
          const value: unknown = await response.json();

          return value;
        }, challenge.target),
      )
      .pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(TargetResponse)),
        Effect.mapError(() =>
          LabError.make({
            code: "browser",
            message: "Could not resolve the Wikipedia destination.",
          }),
        ),
      ),
  );

  const destination = resolved.query.pages[0];

  if (
    destination === undefined ||
    destination.missing ||
    destination.pageid === undefined ||
    destination.ns !== 0
  )
    return yield* LabError.make({
      code: "invalid",
      message: "Choose an existing Wikipedia article as the destination.",
    });

  let race: typeof WikiRace.Type = {
    start: normalizeTitle(challenge.start),
    target: normalizeTitle(destination.title),
    targetUrl: articleUrl(destination.title),
    maxHops: 20,
    path: [],
  };

  const record = (url: string) => {
    if (race.path.at(-1)?.url === url) return;
    const title = articleTitle(url);

    const violation =
      title === undefined
        ? "The route left ordinary English Wikipedia articles."
        : race.path.length > race.maxHops
          ? "The route exceeded 20 hops."
          : undefined;

    race = {
      ...race,
      ...(race.ruleViolation === undefined && violation !== undefined
        ? { ruleViolation: violation }
        : {}),
      path:
        race.path.length > race.maxHops
          ? race.path
          : [...race.path, { title: title ?? url, url, at: trace.now() }],
    };
    trace.update({ race });
  };

  yield* Effect.acquireRelease(
    browser.native(async (page) => {
      const navigated = (frame: Frame) => {
        if (frame === page.mainFrame()) record(frame.url());
      };

      page.on("framenavigated", navigated);
      record(page.url());

      return () => {
        page.off("framenavigated", navigated);
      };
    }),
    (remove) => Effect.sync(remove),
  );

  const verify = trace.measure(
    "verify",
    "Verify Wikipedia arrival",
    Effect.gen(function* () {
      const current = yield* browser.native(async (page) => ({
        url: page.url(),
        canonical: await page.$eval(
          'link[rel="canonical"]',
          (element) => element.getAttribute("href") ?? "",
        ),
      }));

      record(current.url);
      if (race.ruleViolation !== undefined)
        return yield* LabError.make({ code: "invalid", message: race.ruleViolation });
      if (
        race.path.length < 2 ||
        articleTitle(current.url) !== race.target ||
        articleTitle(current.canonical) !== race.target
      )
        return yield* LabError.make({
          code: "invalid",
          message: `The browser has not reached ${race.target}. Continue from the current article using its links.`,
        });
      trace.update({ verifiedAt: trace.now() });
    }),
  );

  return {
    verify,
    prompt: `Play the Wikipedia race from ${race.start} to ${race.target}. Use only links inside the current article body, in the original browser tab. Inspect the actual links with browser_execute and choose your own route. Never use search, enter a URL, go back, open another tab, modify page content, or invent a link. Follow at most ${race.maxHops} links. After each navigation inspect the new article and keep track of the route. Call finish after reaching ${race.target}.`,
  };
});
