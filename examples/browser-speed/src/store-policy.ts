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

// In checkout, buttons and links may only move between steps; anything else could submit.
const step = /\b(continue|next|close|dismiss|cancel|edit|change|back|apply)\b/i;

const safeKinds = new Set([
  "checkbox",
  "radio",
  "option",
  "menuitemradio",
  "menuitemcheckbox",
  "switch",
  "tab",
  "combobox",
  "listbox",
  "select",
  "textbox",
  "textarea",
  "spinbutton",
]);

// Enter submits forms and Space activates buttons.
const safeKeys = new Set(["Escape", "Tab", "ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"]);
const digits = (value: string) => value.replace(/\D/g, "");

const cardFields = [
  { field: "number", name: /card ?number/i, value: digits(testBuyer.card) },
  { field: "expiry", name: /expir/i, value: digits(testBuyer.expiry) },
  { field: "cvc", name: /security code|\bcvc\b|\bcvv\b/i, value: testBuyer.cvc },
] as const;

const buyerValues = new Set(
  [
    testBuyer.email,
    testBuyer.name,
    ...testBuyer.name.split(" "),
    testBuyer.address,
    testBuyer.city,
    testBuyer.zip,
    testBuyer.phone,
  ].map((value) => value.toLowerCase()),
);

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
 * Host authorization for real stores. Everywhere: no purchase or express-payment buttons, no
 * marketing opt-ins, no accounts, and the test email only on checkout pages. Once checkout starts
 * (a checkout URL, or a completed fill of a test-buyer detail), buttons and links may only move between steps
 * and Enter and Space are refused, so an order cannot be submitted under any label. Once a card
 * field is authorized, no further clicks or key presses are allowed.
 *
 * `settle` records completed fills: they start checkout, and `cardEntered` is true only after the
 * exact test number, expiry and CVC were each completed in their fields.
 */
export const makeCheckoutPolicy = () => {
  let checkout = false;
  let cardStarted = false;
  const cardRefs = new Map<string, (typeof cardFields)[number]["field"]>();
  const entered = new Set<(typeof cardFields)[number]["field"]>();

  const authorize = (
    command: Command,
    context: AuthorizationContext,
  ): Effect.Effect<void, BrowserUseError> => {
    if (command.kind !== "act") return Effect.void;
    const { action } = command;
    const name = context.target?.name ?? "";

    checkout ||= onCheckout(context.pageUrl);
    if (action.kind === "fill") {
      const card = cardFields.find((candidate) => candidate.name.test(name));

      if (card !== undefined) {
        cardStarted = true;
        if (digits(action.value) === card.value) cardRefs.set(action.ref, card.field);

        return Effect.void;
      }
    }
    if (cardStarted && action.kind !== "fill" && action.kind !== "select")
      return refuse(
        "Card details are entered, so the lab stops here and never submits payment. No input was dispatched; finish the run.",
      );
    if (purchase.test(name))
      return refuse("The lab never places an order. No input was dispatched; finish the run.");
    if (action.kind === "fill" && email.test(name) && !onCheckout(context.pageUrl))
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
    if (checkout && action.kind === "press" && !safeKeys.has(action.key))
      return refuse(
        `In checkout the lab never presses ${action.key}, which can submit the order. No input was dispatched.`,
      );
    if (
      checkout &&
      action.kind === "click" &&
      !safeKinds.has(context.target?.kind ?? "") &&
      context.target?.editable !== true &&
      !step.test(name)
    )
      return refuse(
        "In checkout the lab only clicks buttons that move between steps, such as Continue, so it never submits the order. No input was dispatched; fill the remaining fields instead.",
      );

    return Effect.void;
  };

  /** Records completed fills: test-buyer details start checkout, test card values count as entered. */
  const settle = (
    actions: ReadonlyArray<{
      readonly kind: string;
      readonly ref: string;
      readonly value?: string;
    }>,
    result: { readonly completed: number },
  ) =>
    actions.slice(0, result.completed).forEach((action) => {
      if (action.kind !== "fill") return;
      const field = cardRefs.get(action.ref);

      if (field !== undefined) entered.add(field);
      if (buyerValues.has((action.value ?? "").trim().toLowerCase())) checkout = true;
    });

  return {
    authorize,
    settle,
    cardEntered: () => cardFields.every(({ field }) => entered.has(field)),
  };
};
