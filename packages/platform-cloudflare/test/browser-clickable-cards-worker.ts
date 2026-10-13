import { Effect, Schema } from "effect";

import { BrowserSessionError } from "../src/BrowserSession.ts";
import { make } from "../src/BrowserUse.ts";
import { BrowserRunBinding } from "../src/internal/browser-binding.ts";

class ProbeError extends Schema.TaggedError<ProbeError>()("ProbeError", {
  cause: Schema.Defect(),
}) {}

const native = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => ProbeError.make({ cause }) });

const html = `<!doctype html><style>
  .meal-card--selectable { cursor: pointer; width: 320px; padding: 20px; }
  [tabindex] { width: 200px; height: 30px; }
</style><div id="cards">
  <div class="card meal-card meal-card--selectable" id="meal"><div class="card-body"><h4><span><span>Ginger Chicken Bowl</span></span></h4></div></div>
  <div id="plain" class="clickable">Plain content</div>
  <div tabindex="0">Focusable choice</div>
  <div hidden class="meal-card--selectable">Hidden choice</div>
  <div inert><div class="meal-card--selectable">Inert choice</div></div>
  <div class="meal-card--selectable"></div>
  <div class="meal-card--selectable"><button>Nested action</button></div>
</div><output id="selections">0</output><script>
  document.querySelector('#meal').addEventListener('click', () => {
    const output = document.querySelector('#selections');
    output.textContent = String(Number(output.textContent) + 1);
  });
</script>`;

export default {
  fetch(request: Request, env: { CDP: string }) {
    return Effect.runPromise(
      Effect.gen(function* () {
        const binding = yield* BrowserRunBinding;
        const attachment = binding.connect("local-card-probe", "session.connect");

        yield* Effect.addFinalizer(() => attachment.retire.pipe(Effect.orDie));
        const browser = yield* native(() => attachment.browser);
        const page = yield* native(() => browser.newPage());

        yield* Effect.addFinalizer(() => Effect.promise(() => page.close()));
        yield* native(() => page.setViewport({ width: 800, height: 600 }));
        yield* native(() => page.setContent(html));

        const controller = yield* make(
          {
            run: (authorize, action) =>
              authorize.pipe(
                Effect.andThen(
                  Effect.tryPromise({
                    try: () => action(page),
                    catch: () =>
                      BrowserSessionError.make({
                        reason: "provider",
                        dispatch: "possibly-dispatched",
                        cleanup: "not-requested",
                      }),
                  }),
                ),
              ),
          },
          {
            authorize: () => Effect.void,
            observationMode:
              new URL(request.url).searchParams.get("mode") === "jev" ? "jev" : "default",
            maxActions: 20,
            maxReturnedBytes: 256 * 1024,
          },
        );

        const observation = yield* controller.actions.observe;
        const card = observation.controls.find((control) => control.name === "Ginger Chicken Bowl");

        // Keep the failed discovery's real observation as the red-run evidence.
        if (card === undefined) return Response.json({ observation });

        const clicked = yield* controller.actions.act([{ kind: "click", ref: card.ref }]);

        const selections = yield* native(() =>
          page.$eval("#selections", (node) => node.textContent),
        );

        const guards = [];

        for (const mutation of [
          "replaced",
          "renamed",
          "cursor",
          "disabled",
          "covered",
          "nested-control",
        ] as const) {
          yield* native(() => page.setContent(html));
          const before = yield* controller.actions.observe;
          const target = before.controls.find((control) => control.name === "Ginger Chicken Bowl");

          if (target === undefined) return yield* Effect.die("Missing card after reset");
          yield* native(() =>
            page.evaluate((mutation) => {
              const node = document.getElementById("meal");

              if (node === null) return;
              if (mutation === "replaced") node.replaceWith(node.cloneNode(true));
              if (mutation === "renamed") node.textContent = "Different meal";
              if (mutation === "cursor") node.style.cursor = "auto";
              if (mutation === "disabled")
                node.parentElement?.setAttribute("aria-disabled", "true");
              if (mutation === "covered") {
                const cover = document.createElement("div");

                cover.style.cssText = "position:fixed;inset:0;background:white;z-index:10";
                document.body.appendChild(cover);
              }
              if (mutation === "nested-control") {
                const button = document.createElement("button");

                while (node.firstChild !== null) button.appendChild(node.firstChild);
                node.appendChild(button);
              }
            }, mutation),
          );
          const result = yield* controller.actions.act([{ kind: "click", ref: target.ref }]);
          const count = yield* native(() => page.$eval("#selections", (node) => node.textContent));

          guards.push({ mutation, result, count });
        }

        return Response.json({ observation, clicked, selections, guards });
      }).pipe(
        Effect.scoped,
        Effect.provide(
          BrowserRunBinding.layer({
            fetch: () => fetch(env.CDP, { headers: { Upgrade: "websocket" } }),
          }),
        ),
      ),
    );
  },
};
