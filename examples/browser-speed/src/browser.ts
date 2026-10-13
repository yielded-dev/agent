import type { BrowserSession } from "@yielded/agent-platform-cloudflare/browser-session";
import * as NativeBrowser from "@yielded/agent-platform-cloudflare/browser-use";
import * as BrowserUse from "@yielded/agent/browser-use";
import { Context, Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";
import type { Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { Board, LabError, type RunInput } from "./contract.ts";
import { fixtureHtml } from "./fixture.ts";
import { Trace } from "./telemetry.ts";

export const TaskResult = Schema.Struct({ message: Schema.String });

export const completionTools = Toolkit.make(
  Tool.make("finish", {
    description:
      "Finish with a concise account of what happened. For task-board presets the host checks the saved board and returns mismatches for correction. Website results are not independently verified.",
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
    maxCommands: 500,
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
      input.scenario === "browse" && input.url === undefined
        ? Effect.fail(LabError.make({ code: "invalid", message: "Browsing needs a starting URL." }))
        : trace.measure(
            "setup",
            "Open starting page",
            native(async (page) => {
              await page.setViewport({ width: 1100, height: 740 });
              await page.goto(
                input.scenario === "browse"
                  ? (input.url ?? "about:blank")
                  : `data:text/html;charset=utf-8,${encodeURIComponent(fixtureHtml())}`,
                { waitUntil: "domcontentloaded" },
              );
            }),
          ),
    readBoard: trace
      .measure(
        "verify",
        "Read saved task board",
        native((page) => page.$eval("#board-state", (element) => element.textContent ?? "")),
      )
      .pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Board))),
        Effect.mapError(() =>
          LabError.make({ code: "browser", message: "Could not validate the task board." }),
        ),
      ),
  };
});

export class Browser extends Context.Service<
  Browser,
  Effect.Success<ReturnType<typeof makeBrowser>>
>()("browser-speed/Browser") {}
