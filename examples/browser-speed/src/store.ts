import { Effect, Schema } from "effect";
import type { Protocol } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { Browser } from "./browser.ts";
import { LabError } from "./contract.ts";
import { isCheckout } from "./store-policy.ts";
import { Trace } from "./telemetry.ts";

const CheckoutSummary = Schema.Struct({
  url: Schema.String,
  /** Squarespace step markers: `<step>-section-<state>`, and `payment-options-select-<state>`. */
  sections: Schema.Array(Schema.String),
  items: Schema.Array(Schema.Struct({ name: Schema.String, quantity: Schema.String })),
});

/**
 * Opens a store with a guard that keeps every page navigation on the store's own site: its host
 * without `www.`, and subdomains of it. `product` scrolls the first matching link into view.
 */
export const openStore = Effect.fnUntraced(function* (url: string, product?: string) {
  const browser = yield* Browser;
  const trace = yield* Trace;
  const site = new URL(url).host.replace(/^www\./, "");

  const onSite = (address: string) => {
    try {
      const { host } = new URL(address);

      return host === site || host.endsWith(`.${site}`);
    } catch {
      return false;
    }
  };

  yield* Effect.acquireRelease(
    browser.native(async (page) => {
      const client = await page.createCDPSession();
      const { frameTree } = await client.send("Page.getFrameTree");

      // Only main-frame documents are checked; Stripe and reCAPTCHA frames load normally.
      const guard = (event: Protocol.Fetch.RequestPausedEvent) => {
        const allowed = event.frameId !== frameTree.frame.id || onSite(event.request.url);

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
          browser.native(() => client.send("Fetch.disable")),
        )
        .pipe(
          Effect.catch(() => Effect.void),
          // The CDP session is the lab's own: detaching it ends the guard even behind a fence.
          Effect.ensuring(Effect.promise(() => client.detach().catch(() => {}))),
        ),
  );

  yield* trace.measure(
    "setup",
    "Open store",
    browser.native(async (page) => {
      await page.setViewport({ width: 1100, height: 740 });
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15_000 });
      if (product === undefined) return;

      const link = await page.waitForSelector(product, { visible: true, timeout: 10_000 });

      // Start with the products in view; viewport observations otherwise see only the header.
      await link?.evaluate((node) => node.scrollIntoView({ block: "start" }));
      await link?.dispose();
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
          sections: Array.from(document.querySelectorAll("[data-test]"))
            .map((node) => node.getAttribute("data-test") ?? "")
            .filter((marker) => marker.includes("section") || marker.includes("select")),
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
  const step = (name: string) => summary.sections.find((marker) => marker.startsWith(name)) ?? "";

  if (
    !/visited|complete/.test(step("customer-info")) ||
    !/visited|complete/.test(step("fulfillment"))
  )
    return {
      passed: false,
      message: "The checkout's email and delivery steps were not completed.",
    };
  if (!step("payment").endsWith("active") || !step("review").endsWith("incomplete"))
    return {
      passed: false,
      message: "The checkout is not on its payment step, or it moved past it.",
    };
  if (!browser.cardEntered())
    return { passed: false, message: "The test card was not entered on the payment step." };

  return {
    passed: true,
    message: `Verified: the checkout lists 1 × ${item.name}, the test buyer's email and delivery are complete, and the test card is entered. Payment was never submitted.`,
  };
}).pipe(
  Effect.mapError(
    () =>
      new LabError({ code: "browser", message: "The checkout order summary could not be read." }),
  ),
);
