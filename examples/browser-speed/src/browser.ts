import type { BrowserSession } from "@yielded/agent-platform-cloudflare/browser-session";
import * as NativeBrowser from "@yielded/agent-platform-cloudflare/browser-use";
import * as BrowserUse from "@yielded/agent/browser-use";
import { Context, Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";
import type { Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { LabError, defaultChallenge, type RunInput } from "./contract.ts";
import { Trace } from "./telemetry.ts";

export const TaskResult = Schema.Struct({ message: Schema.String });

export const completionTools = Toolkit.make(
  Tool.make("finish", {
    description:
      "Reply with findings supported by observed evidence and source URLs. Correct recoverable code or page inspection errors with a fresh browser observation before finishing; an error is not a reason to ask whether to continue authorized work. Before reporting an action complete, inspect the resulting page or cart and verify it satisfies the user's explicit requirements. Explain any unmet requirement. Ask a question only when information or authorization is missing. The host checks Wikipedia arrival. Other website results are not independently verified.",
    parameters: TaskResult,
    success: TaskResult,
    failure: LabError,
    failureMode: "return",
  }),
);

export const makeBrowser = Effect.fnUntraced(function* (
  session: Pick<BrowserSession, "run">,
  screenshots: boolean,
  image: (value: string) => void,
) {
  const trace = yield* Trace;

  // This lab deliberately grants arbitrary page CDP, including page JavaScript.
  // It is not a URL allowlist, a read-only browser or a payment prevention boundary.
  const controller = yield* NativeBrowser.make(session, {
    authorize: () => Effect.void,
    maxCommands: 2_000,
    maxReturnedBytes: 256 * 1024,
  }).pipe(Effect.mapError((error) => LabError.make({ code: "browser", message: error.message })));

  const native = <A>(action: (page: Page) => Promise<A>) =>
    session.run(Effect.void, action).pipe(
      Effect.mapError((error) =>
        LabError.make({
          code: "browser",
          message: `Browser ${error.reason}; dispatch ${error.dispatch}. Inspect before attempting another action.`,
        }),
      ),
    );

  const capture = (force = false): Effect.Effect<void, LabError> =>
    screenshots || force
      ? trace
          .measure(
            "capture",
            "Screenshot",
            native((page) => page.screenshot({ encoding: "base64", type: "jpeg", quality: 65 })),
          )
          .pipe(
            Effect.tap((value) => Effect.sync(() => image(`data:image/jpeg;base64,${value}`))),
            Effect.asVoid,
          )
      : Effect.void;

  const client: BrowserUse.BrowserCdp["Service"] = {
    send: (request) => trace.measure("action", request.method, controller.client.send(request)),
    tabs: trace.measure("action", "List tabs", controller.client.tabs),
    frames: (request) => trace.measure("action", "List frames", controller.client.frames(request)),
    newTab: trace.measure("action", "New tab", controller.client.newTab),
    closeTab: (request) =>
      trace.measure("action", "Close tab", controller.client.closeTab(request)),
  };

  return {
    native,
    capture,
    layer: Layer.succeed(BrowserUse.BrowserCdp, client),
    prepare: (input: RunInput) =>
      trace.measure(
        "setup",
        "Open starting page",
        native(async (page) => {
          await page.setViewport({ width: 1100, height: 740 });
          await page.goto(
            input.scenario === "chat"
              ? (input.url ?? "about:blank")
              : input.scenario === "wikipedia"
                ? `https://en.wikipedia.org/wiki/${encodeURIComponent((input.wikipedia ?? defaultChallenge).start.trim().replaceAll(" ", "_"))}`
                : input.scenario === "coffee"
                  ? "https://www.hedge.coffee/store"
                  : "about:blank",
            { waitUntil: "domcontentloaded" },
          );
        }),
      ),
  };
});

export class Browser extends Context.Service<
  Browser,
  Effect.Success<ReturnType<typeof makeBrowser>>
>()("browser-speed/Browser") {}
