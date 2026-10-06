import { Effect, Schema } from "effect";
import type { Protocol } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { Browser } from "./browser.ts";
import { LabError } from "./contract.ts";
import { isCheckout, storeHost, storeUrl } from "./store-policy.ts";
import { Trace } from "./telemetry.ts";

const CheckoutSummary = Schema.Struct({
  url: Schema.String,
  /** The first step (email) is still the active one, so no customer details were submitted. */
  atEmailStep: Schema.Boolean,
  items: Schema.Array(Schema.Struct({ name: Schema.String, quantity: Schema.String })),
});

/** Opens the store with a guard that keeps every page navigation on the store's own host. */
export const openStore = Effect.fnUntraced(function* () {
  const browser = yield* Browser;
  const trace = yield* Trace;

  yield* Effect.acquireRelease(
    browser.native(async (page) => {
      const client = await page.createCDPSession();
      const { frameTree } = await client.send("Page.getFrameTree");

      // Only main-frame documents are checked; Stripe and reCAPTCHA frames load normally.
      const guard = (event: Protocol.Fetch.RequestPausedEvent) => {
        const allowed =
          event.frameId !== frameTree.frame.id || new URL(event.request.url).host === storeHost;

        void (
          allowed
            ? client.send("Fetch.continueRequest", { requestId: event.requestId })
            : client.send("Fetch.failRequest", {
                requestId: event.requestId,
                errorReason: "BlockedByClient",
              })
        ).catch(() => {});
      };

      client.on("Fetch.requestPaused", guard);
      await client.send("Fetch.enable", {
        patterns: [{ urlPattern: "*", resourceType: "Document", requestStage: "Request" }],
      });

      return client;
    }),
    (client) =>
      trace
        .measure(
          "cleanup",
          "Release store guard",
          browser.native(() => client.send("Fetch.disable").then(() => client.detach())),
        )
        .pipe(Effect.catch(() => Effect.void)),
  );

  yield* trace.measure(
    "setup",
    "Open Hedge Coffee store",
    browser.native(async (page) => {
      await page.setViewport({ width: 1100, height: 740 });
      await page.goto(storeUrl, { waitUntil: "domcontentloaded", timeout: 15_000 });

      const product = await page.waitForSelector('a[href*="/store/p/"]', {
        visible: true,
        timeout: 10_000,
      });

      // Start with the bags in view; viewport observations otherwise see only the header.
      await product?.evaluate((node) => node.scrollIntoView({ block: "start" }));
      await product?.dispose();
    }),
  );
});

/** Independent check of the checkout page, read by the host rather than the driver. */
export const verifyCheckout = Effect.gen(function* () {
  const browser = yield* Browser;

  const summary = yield* browser
    .native(async (page) => {
      // The checkout renders its order summary after load; give it time before reading.
      await page
        .waitForFunction(
          () =>
            document.querySelector(
              '[data-test="checkout-summary"] [class*="OrderSummary-cartItem-"]',
            ) !== null,
          { timeout: 10_000 },
        )
        .then((handle) => handle.dispose())
        .catch(() => {});

      return await page.evaluate(() => {
        const box = document.querySelector('[data-test="checkout-summary"]') ?? document;

        return {
          url: location.href,
          atEmailStep:
            document.querySelector('[data-test="customer-info-section-active"]') !== null,
          items: Array.from(box.querySelectorAll('[class*="OrderSummary-cartItem-"]')).map(
            (item) => ({
              name: item.querySelector('[class*="productName"]')?.textContent?.trim() ?? "",
              quantity: item.querySelector('[class*="imageDiv"]')?.textContent?.trim() ?? "",
            }),
          ),
        };
      });
    })
    .pipe(Effect.flatMap(Schema.decodeUnknownEffect(CheckoutSummary)));

  const [item] = summary.items;

  if (!isCheckout(summary.url))
    return { passed: false, message: "The browser is not on the Hedge Coffee checkout page." };
  if (summary.items.length !== 1 || item === undefined || !item.name)
    return {
      passed: false,
      message: `The checkout lists ${summary.items.length} items; the task needs exactly one bag.`,
    };
  if (item.quantity !== "1")
    return { passed: false, message: `The checkout lists ${item.quantity || "?"} × ${item.name}.` };
  if (!summary.atEmailStep)
    return { passed: false, message: "The checkout moved past the email step." };

  return {
    passed: true,
    message: `Verified: the checkout lists 1 × ${item.name}, and no details were entered.`,
  };
}).pipe(
  Effect.mapError(
    () =>
      new LabError({ code: "browser", message: "The checkout order summary could not be read." }),
  ),
);
