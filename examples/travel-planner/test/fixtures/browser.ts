import { WebCapture } from "@yielded/agent";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";

import { ReadTravelPage } from "../../src/research.ts";

export const FixtureBrowserLive = Toolkit.make(ReadTravelPage).toLayer({
  read_travel_page: () =>
    Effect.fail(
      WebCapture.WebCaptureFailure.make({
        errorTag: "TestBrowser",
        message: "Browser research is replaced in tests.",
      }),
    ),
});
