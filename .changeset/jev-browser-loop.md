---
"@yielded/agent": minor
"@yielded/agent-platform-cloudflare": patch
---

Add `BrowserUse.runJev` to drive a page with a native `DecisionModel` and write field text with a `LanguageModel`, without an agent. Cloudflare Jev observations now include the page metrics it uses for scrolling.

BEHAVIOR CHANGE: `grounding: "decision"`, `mode: "plan"`, `selectTargets`, `TargetAction`, `act_ref` and the optional `BrowserActions.latestObservation` are removed. Use `BrowserUse.make({ mode })` for a model agent or `BrowserUse.runJev` for decision-model control.
