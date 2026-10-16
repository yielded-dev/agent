import {
  BrowserCdp,
  BrowserUseError,
  CloseTabRequest,
  FrameInfo,
  FramesRequest,
  SendRequest,
  Tab,
} from "@yielded/agent/browser-use";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type {
  CDPSession,
  Frame,
  Page,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import type { BrowserSession, BrowserSessionError } from "./BrowserSession.ts";
import {
  browserFailure,
  reportBrowserCause,
  reportedBrowserError,
} from "./internal/browser-failure.ts";

export type Command =
  | { readonly kind: "connect" | "tabs" | "new-tab" }
  | { readonly kind: "send"; readonly request: typeof SendRequest.Type }
  | { readonly kind: "frames"; readonly request: typeof FramesRequest.Type }
  | { readonly kind: "close-tab"; readonly request: typeof CloseTabRequest.Type };

/** Current SDK metadata, never a grant supplied by page content or generated code. */
export interface AuthorizationContext {
  /** Absent during initial attachment. */
  readonly pageUrl?: string;
  readonly targetId?: string;
  readonly targetUrl?: string;
  readonly frameUrl?: string;
}

export interface Options<R = never> {
  /**
   * Runs under the BrowserSession lock before every command. Arbitrary CDP/page JavaScript
   * is a broad capability: URL checks cannot prove that code is read-only or constrain egress.
   * The host must authorize that capability and enforce network policy outside page code.
   */
  readonly authorize: (
    command: Command,
    context: AuthorizationContext,
  ) => Effect.Effect<void, BrowserUseError, R>;
  /** Total native operations for this attachment, including authorization refusals. */
  readonly maxCommands: number;
  /** Per-result UTF-8 JSON limit; does not bound the upstream CDP transfer. */
  readonly maxReturnedBytes: number;
  /** Maximum pages discoverable through this attachment. Defaults to 8. */
  readonly maxTabs?: number;
}

const Limits = Schema.Struct({
  maxCommands: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10_000 })),
  maxReturnedBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 8 * 1024 * 1024 })),
  maxTabs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
});

const invalid = (message: string) =>
  BrowserUseError.make({ code: "invalid", message, dispatch: "not-dispatched" });

const decode = <A>(schema: Schema.Codec<A>, input: unknown) =>
  Schema.decodeUnknownEffect(schema)(input).pipe(
    Effect.mapError(() => invalid("Invalid browser command or limits")),
  );

interface AttachedPage {
  readonly page: Page;
  readonly cdp: CDPSession;
  readonly targetId: string;
}

/**
 * A scoped CDP bridge over an existing BrowserSession. No DOM classifier, selectors,
 * implicit waits, input retries, or generated-code execution live in this adapter.
 * CDP connections persist within the attachment (including remote object IDs); pages persist
 * for the host-owned session. Scope release detaches connections without closing pages.
 */
export const make = Effect.fnUntraced(function* <R>(
  session: Pick<BrowserSession, "run">,
  options: Options<R>,
) {
  const limits = yield* decode(Limits, { ...options, maxTabs: options.maxTabs ?? 8 });
  const authority = yield* Effect.context<R>();
  // Code Mode permits concurrent host calls; this native attachment admits one at a time.
  const serial = yield* Semaphore.make(1);

  const authorize = (command: Command, context: AuthorizationContext) =>
    Effect.provideContext(options.authorize(command, context), authority);

  const mapSessionError = (error: BrowserSessionError | BrowserUseError) =>
    error._tag === "BrowserUseError"
      ? error
      : BrowserUseError.make({
          code: "browser",
          message: "Browser command failed; inspect the current state before further input",
          dispatch: error.dispatch,
        });

  let released = false;

  // Register cleanup before creating any native connection, including a partially failed attachment.
  const connections = yield* Effect.acquireRelease(
    Effect.sync(() => new Map<Page | CDPSession, CDPSession>()),
    (connections) =>
      Effect.promise(async (signal) => {
        released = true;
        for (const cdp of connections.values()) {
          if (signal.aborted) break;
          try {
            await cdp.detach();
          } catch {
            // The session may already have terminated on timeout or interruption.
          }
        }
        connections.clear();
      }).pipe(Effect.interruptible, Effect.timeoutOption("1 second"), Effect.asVoid),
  );

  const pages = new Map<string, AttachedPage>();

  const attach = async (page: Page): Promise<AttachedPage> => {
    let cdp = connections.get(page);

    if (cdp === undefined) {
      cdp = await page.createCDPSession();
      connections.set(page, cdp);
    }
    const { targetInfo } = await cdp.send("Target.getTargetInfo");
    const attached = { page, cdp, targetId: targetInfo.targetId };

    pages.set(attached.targetId, attached);

    return attached;
  };

  const primary = yield* session
    .run(authorize({ kind: "connect" }, {}), attach)
    .pipe(Effect.mapError(mapSessionError));

  const context = primary.page.browserContext();
  let calls = 0;

  const tab = async (attached: AttachedPage): Promise<typeof Tab.Type> => {
    const { targetInfo } = await attached.cdp.send("Target.getTargetInfo");

    return Schema.decodeSync(Tab)({
      targetId: targetInfo.targetId,
      url: targetInfo.url,
      title: targetInfo.title,
    });
  };

  const run = Effect.fnUntraced(function* <A>(
    command: Command,
    action: (selected: AttachedPage, frame?: Frame) => Promise<A | BrowserUseError>,
    targetId?: string,
    frameId?: string,
  ) {
    let selected = primary;
    let pageUrl = "";
    let targetUrl = "";
    let frame: Frame | undefined;
    let frameUrl: string | undefined;

    const result = yield* session
      .run(
        Effect.gen(function* () {
          if (released) return yield* Effect.fail(invalid("Browser attachment scope is closed"));
          if (calls++ >= limits.maxCommands)
            return yield* Effect.fail(invalid("Browser command limit reached"));
          selected = targetId === undefined ? primary : (pages.get(targetId) ?? primary);
          if (targetId !== undefined && !pages.has(targetId))
            return yield* Effect.fail(
              invalid("Unknown page; call cdp.tabs({}) to discover current targets"),
            );
          if (selected.page.isClosed())
            return yield* Effect.fail(invalid("The selected page is closed"));
          pageUrl = primary.page.url();
          targetUrl = selected.page.url();
          frame =
            frameId === undefined
              ? undefined
              : selected.page.frames().find((candidate) => candidate._id === frameId);
          if (frameId !== undefined && frame === undefined)
            return yield* Effect.fail(invalid("Unknown frame; call cdp.frames({}) again"));
          frameUrl = frame?.url();
          yield* authorize(command, {
            pageUrl,
            targetId: selected.targetId,
            targetUrl,
            ...(frameUrl === undefined ? {} : { frameUrl }),
          });
        }),
        async () => {
          if (
            primary.page.url() !== pageUrl ||
            selected.page.url() !== targetUrl ||
            selected.page.isClosed() ||
            (frame !== undefined && (frame.detached || frame.url() !== frameUrl))
          )
            return {
              ok: false as const,
              error: invalid("Page changed during authorization; inspect it again"),
            };

          return { ok: true as const, value: await action(selected, frame) };
        },
      )
      .pipe(serial.withPermits(1), Effect.mapError(mapSessionError));

    if (!result.ok) return yield* Effect.fail(result.error);
    // Only host-created failures belong in E; a protocol response with similar fields is data.
    if (result.value instanceof BrowserUseError) return yield* Effect.fail(result.value);

    const encoded = yield* Effect.try({
      try: () => JSON.stringify(result.value),
      catch: () =>
        BrowserUseError.make({
          code: "browser",
          message: "Browser returned a non-JSON result",
          dispatch: "possibly-dispatched",
        }),
    });

    if (
      encoded === undefined ||
      new TextEncoder().encode(encoded).byteLength > limits.maxReturnedBytes
    )
      return yield* Effect.fail(
        BrowserUseError.make({
          code: "browser",
          message:
            "Browser result exceeded its byte limit; request a smaller result without repeating mutations",
          dispatch: "possibly-dispatched",
        }),
      );

    return result.value;
  });

  const client: BrowserCdp["Service"] = {
    send: Effect.fnUntraced(function* (request) {
      const value = yield* decode(SendRequest, request);

      if (
        value.method.startsWith("Browser.") ||
        value.method.startsWith("Target.") ||
        value.method === "Page.close" ||
        value.method === "Page.crash"
      )
        return yield* Effect.fail(
          invalid("Browser lifecycle commands belong to the host; use the tab helpers"),
        );

      const runReport = Effect.runPromiseWith(yield* Effect.context<never>());

      const result: unknown = yield* run(
        { kind: "send", request: value },
        async ({ cdp, page }, frame): Promise<unknown> => {
          let target = cdp;

          if (frame !== undefined) {
            const source = frame.client;
            const originalUrl = primary.page.url();
            const pageUrl = page.url();
            const frameUrl = frame.url();
            const existing = connections.get(source);

            if (existing !== undefined) target = existing;
            else {
              const connection = source.connection();

              if (connection === undefined) return invalid("Frame connection is closed");

              let lookup: Awaited<ReturnType<typeof source.send<"Target.getTargetInfo">>>;

              try {
                lookup = await source.send("Target.getTargetInfo");
              } catch (cause) {
                await runReport(
                  reportBrowserCause("browser-use.frame.target-info", Cause.fail(cause)),
                );

                return reportedBrowserError(
                  BrowserUseError.make({
                    code: "browser",
                    message: "Frame inspection failed before the requested command was sent",
                    dispatch: "not-dispatched",
                  }),
                );
              }

              // Keep arbitrary commands off Puppeteer's own event-tracking session.
              let attached: CDPSession;

              try {
                attached = await connection.createSession(lookup.targetInfo);
              } catch (cause) {
                await runReport(reportBrowserCause("browser-use.frame.attach", Cause.fail(cause)));

                return reportedBrowserError(
                  BrowserUseError.make({
                    code: "browser",
                    message: "Frame attachment failed before the requested command was sent",
                    dispatch: "not-dispatched",
                  }),
                );
              }
              target = attached;
              connections.set(source, target);
            }
            if (
              primary.page.url() !== originalUrl ||
              page.url() !== pageUrl ||
              frame.detached ||
              frame.url() !== frameUrl ||
              frame.client !== source
            )
              return invalid("Frame changed during attachment; inspect it again");
          }

          // Bind the native receiver explicitly; decode the foreign response below.
          try {
            return await Reflect.apply(target.send.bind(target), undefined, [
              value.method,
              value.params,
            ]);
          } catch (cause) {
            // oxlint-disable-next-line effect/no-untyped-throw -- This native Promise preserves existing tab failures and reports sanitized frame stages through BrowserSession.run.
            throw frame === undefined ? cause : browserFailure("browser-use.frame.send", cause);
          }
        },
        value.targetId,
        value.frameId,
      );

      return yield* Schema.decodeUnknownEffect(Schema.Json)(result).pipe(
        Effect.mapError(() =>
          BrowserUseError.make({
            code: "browser",
            message: "Browser returned a non-JSON result",
            dispatch: "possibly-dispatched",
          }),
        ),
      );
    }),
    tabs: Effect.gen(function* () {
      const result = yield* run({ kind: "tabs" }, async () => {
        const current = await context.pages();

        if (current.length > limits.maxTabs) return null;
        const result: Array<typeof Tab.Type> = [];

        for (const page of current) result.push(await tab(await attach(page)));
        for (const [id, attached] of pages) {
          if (attached.page.isClosed()) {
            pages.delete(id);
            connections.delete(attached.page);
          }
        }

        return result;
      });

      if (result === null) return yield* Effect.fail(invalid("Browser tab limit exceeded"));

      return result;
    }),
    frames: Effect.fnUntraced(function* (request) {
      const value = yield* decode(FramesRequest, request);

      const frames = yield* run(
        { kind: "frames", request: value },
        async ({ page }) =>
          page.frames().map((frame) => ({
            frameId: frame._id,
            parentFrameId: frame.parentFrame()?._id ?? null,
            url: frame.url(),
          })),
        value.targetId,
      );

      return yield* decode(Schema.Array(FrameInfo), frames);
    }),
    newTab: Effect.gen(function* () {
      const result = yield* run({ kind: "new-tab" }, async () => {
        if ((await context.pages()).length >= limits.maxTabs) return null;

        return tab(await attach(await context.newPage()));
      });

      if (result === null) return yield* Effect.fail(invalid("Browser tab limit reached"));

      return result;
    }),
    closeTab: Effect.fnUntraced(function* (request) {
      const value = yield* decode(CloseTabRequest, request);

      if (value.targetId === primary.targetId)
        return yield* Effect.fail(invalid("The original page belongs to the host"));
      yield* run(
        { kind: "close-tab", request: value },
        async ({ page }) => {
          await page.close();
          pages.delete(value.targetId);
          connections.delete(page);

          return null;
        },
        value.targetId,
      );
    }),
  };

  return { client, layer: Layer.succeed(BrowserCdp, client) };
});
