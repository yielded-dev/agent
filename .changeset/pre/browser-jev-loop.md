---
"@yielded/agent": minor
"@yielded/agent-platform-cloudflare": minor
---

Add `BrowserUse.runJev` to drive a page with a native `DecisionModel`, and `act(actions, { observe: false })` for hosts that read the next page themselves. Cloudflare native browser actions are faster and now work through payment frames, navigations and Wrangler-bundled Workers.

BEHAVIOR CHANGE: `grounding: "decision"`, `mode: "plan"`, `selectTargets`, `TargetAction`, `act_ref` and the optional `BrowserActions.latestObservation` are removed. Use `BrowserUse.make({ mode })` for a model agent or `BrowserUse.runJev` for decision-model control.
