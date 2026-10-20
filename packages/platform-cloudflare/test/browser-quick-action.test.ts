import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  browserQuickActionCaptureLayer,
  type BrowserQuickActionClient,
} from "@yielded/agent-platform-cloudflare/cloudflare-browser";
import {
  CapturePageMarkdown,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
  type PageCaptureError,
} from "@yielded/agent/page-capture";
import * as WebCapture from "@yielded/agent/web-capture";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vite-plus/test";

interface RecordedCall {
  readonly action: "screenshot" | "content" | "markdown" | "links" | "scrape" | "json";
  readonly options: unknown;
}

/** A scripted binding: hands back the queued responses in order. */
const makeBinding = (responses: ReadonlyArray<Response | Error>) => {
  const calls: Array<RecordedCall> = [];
  let index = 0;

  const respond = (
    action: RecordedCall["action"],
    options: unknown,
  ): Effect.Effect<Response, BrowserQuickActionRpcError> => {
    calls.push({ action, options });
    const next = responses[index];

    index += 1;
    if (next === undefined) {
      return Effect.fail(
        BrowserQuickActionRpcError.make({
          action,
          cause: new Error("no scripted response left"),
        }),
      );
    }

    return next instanceof Error
      ? Effect.fail(BrowserQuickActionRpcError.make({ action, cause: next }))
      : Effect.succeed(next);
  };

  const binding: BrowserQuickActionClient = {
    screenshot: (options) => respond("screenshot", options),
    content: (options) => respond("content", options),
    markdown: (options) => respond("markdown", options),
    links: (options) => respond("links", options),
    scrape: (options) => respond("scrape", options),
    json: (options) => respond("json", options),
  };

  return { binding, calls };
};

const request = (
  action: PageCaptureRequest["action"],
  overrides?: Partial<{
    readonly engine: PageCaptureRequest["engine"];
    readonly maxOutputBytes: number;
    readonly navigation: PageCaptureRequest["navigation"];
    readonly viewport: PageCaptureRequest["viewport"];
    readonly resourcePolicy: PageCaptureRequest["resourcePolicy"];
  }>,
): PageCaptureRequest =>
  PageCaptureRequest.make({
    target: PageUrlTarget.make({ url: "https://docs.example.com/pricing" }),
    action,
    engine: overrides?.engine ?? "chromium",
    limits: PageCaptureLimits.make({ maxOutputBytes: overrides?.maxOutputBytes ?? 128 * 1024 }),
    ...(overrides?.navigation === undefined ? {} : { navigation: overrides.navigation }),
    ...(overrides?.viewport === undefined ? {} : { viewport: overrides.viewport }),
    ...(overrides?.resourcePolicy === undefined
      ? {}
      : { resourcePolicy: overrides.resourcePolicy }),
  });

const captureError = (
  binding: BrowserQuickActionClient,
  input: PageCaptureRequest,
): Promise<PageCaptureError> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const port = yield* PageCapture;

      return yield* port.capture(input).pipe(Effect.flip);
    }).pipe(Effect.provide(captureLayer(binding))),
  );

const captureLayer = (binding: BrowserQuickActionClient): Layer.Layer<PageCapture> =>
  browserQuickActionCaptureLayer().pipe(
    Layer.provide(Layer.succeed(BrowserQuickActionBrowserBinding)(binding)),
  );

describe("Browser Run Quick Action PageCapture adapter", () => {
  // Requested red/green seam: a library capture must accept a multi-megabyte
  // page before the independent model-result budget applies.
  it("returns a complete large page through the default WebCapture tool", async () => {
    const markdown = `# Large reference\n${"reference text\n".repeat(150_000)}end of reference`;

    const { binding } = makeBinding([
      Response.json({ success: true, result: markdown, errors: [] }),
    ]);

    const capture = WebCapture.make("read_page", {
      description: "Read the reference page",
      urls: ["docs.example.com"],
    });

    const tools = Toolkit.make(capture.tool);

    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const toolkit = yield* tools;

        const stream = yield* toolkit.handle("read_page", {
          url: "https://docs.example.com/reference",
          action: "markdown",
        });

        return yield* Stream.runCollect(stream);
      }).pipe(Effect.provide(capture.handlers.pipe(Layer.provide(captureLayer(binding))))),
    );

    expect(results).toHaveLength(1);
    expect(results[0]?.isFailure).toBe(false);
    expect(results[0]?.result).toEqual({
      url: "https://docs.example.com/reference",
      action: "markdown",
      markdown,
    });
  });

  it("returns a large matched element through the default scrape tool", async () => {
    const text = `Introduction\n${"reference text\n".repeat(150_000)}end of reference`;

    const groups = [
      {
        selector: "main",
        results: [{ text, html: text, attributes: [], left: 0, top: 0, width: 800, height: 600 }],
      },
    ];

    const { binding } = makeBinding([Response.json({ success: true, result: groups, errors: [] })]);

    const capture = WebCapture.makeScrape("scrape_page", {
      description: "Read the reference section",
      urls: ["docs.example.com"],
    });

    const tools = Toolkit.make(capture.tool);

    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const toolkit = yield* tools;

        const stream = yield* toolkit.handle("scrape_page", {
          url: "https://docs.example.com/reference",
          selectors: ["main"],
        });

        return yield* Stream.runCollect(stream);
      }).pipe(Effect.provide(capture.handlers.pipe(Layer.provide(captureLayer(binding))))),
    );

    expect(results).toHaveLength(1);
    expect(results[0]?.isFailure).toBe(false);
    expect(results[0]?.result).toEqual({ url: "https://docs.example.com/reference", groups });
  });

  it("stops oversized streams at the first exceeding chunk and releases their reader", async () => {
    let chunksRead = 0;
    let canceled = false;

    const stream = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          chunksRead += 1;
          controller.enqueue(new Uint8Array(512));
        },
        cancel() {
          canceled = true;
        },
      },
      { highWaterMark: 0 },
    );

    const { binding, calls } = makeBinding([new Response(stream)]);

    const error = await captureError(
      binding,
      request(CapturePageMarkdown.make({}), { engine: "kitesurf", maxOutputBytes: 1_024 }),
    );

    expect(error).toMatchObject({
      _tag: "PageCaptureOutputLimitError",
      limit: 1_024,
      observed: 1_536,
    });
    expect(calls).toEqual([
      {
        action: "markdown",
        options: { url: "https://docs.example.com/pricing", browser: "kitesurf" },
      },
    ]);
    expect(chunksRead).toBe(3);
    expect(canceled).toBe(true);
    expect(stream.locked).toBe(false);
  });
});
