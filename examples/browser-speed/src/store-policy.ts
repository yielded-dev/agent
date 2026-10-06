import type { AuthorizationContext, Command } from "@yielded/agent-platform-cloudflare/browser-use";
import { BrowserUseError } from "@yielded/agent/browser-use";
import { Effect } from "effect";

/** A real Squarespace store. The lab adds one bag and stops on checkout; it never pays. */
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

/** Checkout is observation-only: no input there, so no details are entered and nothing is paid. */
export const refuseCheckoutInput = (command: Command, context: AuthorizationContext) =>
  command.kind === "act" && isCheckout(context.pageUrl)
    ? Effect.fail(
        new BrowserUseError({
          code: "invalid",
          message:
            "The checkout page is read-only in this lab. No input was dispatched; the task ends here.",
          dispatch: "not-dispatched",
        }),
      )
    : Effect.void;
