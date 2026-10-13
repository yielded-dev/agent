import { ActionResult, Observation } from "@yielded/agent/browser-use";
import { Schema } from "effect";
import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import puppeteer from "puppeteer-core";
import { expect, it } from "vite-plus/test";

const decodeResult = Schema.decodeUnknownSync(
  Schema.Struct({
    observation: Observation,
    clicked: Schema.optionalKey(ActionResult),
    selections: Schema.optionalKey(Schema.String),
    guards: Schema.optionalKey(
      Schema.Array(
        Schema.Struct({
          mutation: Schema.String,
          result: ActionResult,
          count: Schema.String,
        }),
      ),
    ),
  }),
);

// Explicit human request: reproduce a Vue-style clickable card through the actual
// local Cloudflare Browser boundary before changing discovery. The existing native
// suite substitutes the binding; this runs the binding and controller in workerd.
it("discovers clickable cards and revalidates them through workerd and Chromium", async (context) => {
  const executablePath = process.env.BROWSER_TEST_EXECUTABLE;

  if (executablePath === undefined) return context.skip();
  const browser = await puppeteer.launch({ executablePath, headless: true });
  let runtime: Miniflare | undefined;

  try {
    const bundled = await build({
      entryPoints: [`${import.meta.dirname}/browser-clickable-cards-worker.ts`],
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      alias: { crypto: "node:crypto" },
      logLevel: "silent",
    });

    const script = bundled.outputFiles[0]?.text;

    if (script === undefined) throw new Error("Missing browser probe bundle");
    runtime = new Miniflare(
      convertV4MiniflareOptions({
        modules: true,
        script,
        compatibilityDate: "2025-05-01",
        compatibilityFlags: ["nodejs_compat"],
        bindings: { CDP: browser.wsEndpoint().replace("ws:", "http:") },
      }),
    );

    for (const mode of ["default", "jev"]) {
      const response = await runtime.dispatchFetch(`http://probe/?mode=${mode}`);
      const result = decodeResult(await response.json());

      console.log(JSON.stringify({ mode, ...result }));
      expect.soft(response.ok).toBe(true);
      expect.soft(result).toMatchObject({
        observation: {
          controls: [
            { name: "Ginger Chicken Bowl", attributes: { id: "meal" } },
            { name: "Focusable choice" },
            { name: "Nested action" },
          ],
        },
        clicked: { completed: 1, dispatch: "acknowledged" },
        selections: "1",
        guards: [
          "replaced",
          "renamed",
          "cursor",
          "disabled",
          "covered",
          "nested-control",
          "shadow-open",
          "shadow-closed",
        ].map((mutation) => ({
          mutation,
          result: { completed: 0, dispatch: "not-dispatched" },
          count: "0",
        })),
      });
      expect.soft(result.observation.controls).toHaveLength(3);

      // PR #901: independent shadow controls must never be invoked under a wrapper ref.
      const shadowResponse = await runtime.dispatchFetch(
        `http://probe/?mode=${mode}&scenario=shadow`,
      );

      const shadow = Schema.decodeUnknownSync(
        Schema.Struct({
          cases: Schema.Array(
            Schema.Struct({
              mode: Schema.Literals(["open", "closed"]),
              wrapper: Schema.Boolean,
              completed: Schema.Natural,
              count: Schema.String,
            }),
          ),
        }),
      )(await shadowResponse.json());

      expect
        .soft(shadow.cases)
        .toEqual(
          ["open", "closed"].map((mode) => ({ mode, wrapper: false, completed: 0, count: "0" })),
        );

      // PR #901: unrelated descendants cannot consume a targeted control's scan budget.
      for (const scenario of ["options", "filler"]) {
        const response = await runtime.dispatchFetch(
          `http://probe/?mode=${mode}&scenario=${scenario}`,
        );

        const targeted = Schema.decodeUnknownSync(
          Schema.Struct({
            wideHasSave: Schema.Boolean,
            targetedHasSave: Schema.Boolean,
            completed: Schema.Natural,
            count: Schema.String,
          }),
        )(await response.json());

        expect.soft(targeted.wideHasSave).toBe(scenario === "options");
        expect.soft(targeted.targetedHasSave).toBe(true);
        expect.soft(targeted.completed).toBe(1);
        expect.soft(targeted.count).toBe("1");
      }
    }
  } finally {
    try {
      await runtime?.dispose();
    } finally {
      await browser.close();
    }
  }
}, 60_000);
