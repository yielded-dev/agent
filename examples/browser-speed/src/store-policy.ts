import type { AuthorizationContext, Command } from "@yielded/agent-platform-cloudflare/browser-use";
import { BrowserUseError } from "@yielded/agent/browser-use";
import { Effect } from "effect";

/** Hedge Coffee, a real Squarespace store. The lab fills checkout with test data and never pays. */
export const storeUrl = "https://www.hedge.coffee/store";
export const storeHost = "www.hedge.coffee";

export const isCheckout = (url: string | undefined) => {
  try {
    const parsed = new URL(url ?? "");

    return parsed.host === storeHost && parsed.pathname.startsWith("/checkout");
  } catch {
    return false;
  }
};

/** Test data for every checkout field. `example.com` mail is undeliverable by design. */
export const testBuyer = {
  email: "buyer@example.com",
  name: "Test Buyer",
  address: "1 Ferry Building",
  city: "San Francisco",
  state: "CA",
  zip: "94111",
  phone: "415-555-0100",
  card: "4242 4242 4242 4242",
  expiry: "12 / 34",
  cvc: "123",
} as const;

const purchase =
  /\b(purchase|place (your )?order|pay( now)?|pay ?pal|complete (order|purchase)|submit order|buy (it )?now|confirm (order|payment))\b/i;

const marketing = /subscribe|newsletter|mailing list|marketing|email me|text me/i;
const account = /create (an )?account|sign up|register|save my information/i;
const email = /e-?mail/i;
const cardField = /card ?number|expir|security code|\bcvc\b|\bcvv\b/i;

const onCheckout = (url: string | undefined) => {
  try {
    return new URL(url ?? "").pathname.toLowerCase().includes("checkout");
  } catch {
    return false;
  }
};

const refuse = (message: string) =>
  Effect.fail(new BrowserUseError({ code: "invalid", message, dispatch: "not-dispatched" }));

/**
 * Host authorization for real stores: no purchase buttons, no marketing opt-ins, no accounts, the
 * test email only on checkout pages (never in signup popups), and once a card field is filled no
 * further clicks or key presses, so payment is never submitted.
 */
export const makeCheckoutPolicy = () => {
  let cardEntered = false;

  const authorize = (
    command: Command,
    context: AuthorizationContext,
  ): Effect.Effect<void, BrowserUseError> => {
    if (command.kind !== "act") return Effect.void;
    const { kind } = command.action;
    const name = context.target?.name ?? "";

    if (kind === "fill" && cardField.test(name)) {
      cardEntered = true;

      return Effect.void;
    }
    if (cardEntered && kind !== "fill" && kind !== "select")
      return refuse(
        "Card details are entered, so the lab stops here and never submits payment. No input was dispatched; finish the run.",
      );
    if (purchase.test(name))
      return refuse("The lab never places an order. No input was dispatched; finish the run.");
    if (kind === "fill" && email.test(name) && !onCheckout(context.pageUrl))
      return refuse(
        "The lab enters the test email only on checkout pages, never in signup forms. No input was dispatched; close the popup instead.",
      );
    if (account.test(name))
      return refuse(
        "The lab checks out as a guest and never creates an account. No input was dispatched.",
      );
    if (marketing.test(name))
      return refuse(
        "The lab never opts in to marketing. No input was dispatched; leave it unchecked.",
      );

    return Effect.void;
  };

  return { authorize, cardEntered: () => cardEntered };
};
