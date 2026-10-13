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

        const scenario = new URL(request.url).searchParams.get("scenario");

        if (scenario === "shadow") {
          const cases = [];

          for (const mode of ["open", "closed"] as const) {
            yield* native(() => page.setContent(html));
            yield* native(() =>
              page.evaluate((mode) => {
                const card = document.getElementById("meal");

                if (card === null) throw new Error("Missing fixture card");
                const host = document.createElement("span");

                card.style.position = "relative";
                host.style.cssText = "position:absolute;inset:0";
                const root = host.attachShadow({ mode });

                root.innerHTML =
                  '<button style="width:100%;height:100%">Independent action</button>';
                const button = root.querySelector("button");

                if (button === null) throw new Error("Missing fixture button");
                button.addEventListener("click", (event) => {
                  event.stopPropagation();
                  const counter = document.getElementById("selections");

                  if (counter === null) throw new Error("Missing fixture counter");
                  counter.textContent = "1";
                });
                card.appendChild(host);
              }, mode),
            );
            const observed = yield* controller.actions.observe;

            const wrapper = observed.controls.find(
              (control) => control.name === "Ginger Chicken Bowl",
            );

            const clicked =
              wrapper === undefined
                ? undefined
                : yield* controller.actions.act([{ kind: "click", ref: wrapper.ref }]);

            const count = yield* native(() =>
              page.$eval("#selections", (node) => node.textContent),
            );

            cases.push({
              mode,
              wrapper: wrapper !== undefined,
              completed: clicked?.completed ?? 0,
              count,
            });
          }

          return Response.json({ cases });
        }
        if (scenario === "options" || scenario === "filler") {
          yield* native(() =>
            page.setContent(
              `<!doctype html>${
                scenario === "options"
                  ? `<select style="width:100px">${"<option>Unrelated option</option>".repeat(10_000)}</select>`
                  : "<div></div>".repeat(10_000)
              }<button id="save">Save</button><output id="selections">0</output><script>document.getElementById("save").onclick=()=>document.getElementById("selections").textContent="1";</script>`,
            ),
          );
          const wide = yield* controller.actions.observe;
          const targeted = yield* controller.control.inspect({ selector: "#save" });
          const target = targeted.controls.find((control) => control.name === "Save");

          const clicked =
            target === undefined
              ? undefined
              : yield* controller.actions.act([{ kind: "click", ref: target.ref }]);

          const count = yield* native(() => page.$eval("#selections", (node) => node.textContent));

          return Response.json({
            wideHasSave: wide.controls.some((control) => control.name === "Save"),
            targetedHasSave: target !== undefined,
            completed: clicked?.completed ?? 0,
            count,
          });
        }

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
          "shadow-open",
          "shadow-closed",
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
              if (mutation === "shadow-open" || mutation === "shadow-closed") {
                const host = document.createElement("span");

                node.style.position = "relative";
                host.style.cssText = "position:absolute;inset:0";

                const root = host.attachShadow({
                  mode: mutation === "shadow-open" ? "open" : "closed",
                });

                root.innerHTML =
                  '<button style="width:100%;height:100%">Independent action</button>';
                const button = root.querySelector("button");

                if (button === null) throw new Error("Missing fixture button");
                button.addEventListener("click", (event) => {
                  event.stopPropagation();
                  const counter = document.getElementById("selections");

                  if (counter === null) throw new Error("Missing fixture counter");
                  counter.textContent = "1";
                });
                node.appendChild(host);
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
