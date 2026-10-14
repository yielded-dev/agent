import { PageCaptureEngine } from "@yielded/agent/page-capture";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/http-api";

export const BrowserEngine = PageCaptureEngine;
export const browserCommandTimeoutMillis = 60_000;

export const Scenario = Schema.Literals(["chat", "wikipedia", "coffee"]);

export type Scenario = typeof Scenario.Type;

export const ArticleTitle = Schema.NonEmptyString.check(
  Schema.isMaxLength(180),
  Schema.isPattern(/^[^:#?[\]{}|<>\\\p{Cc}]+$/u),
);

export const WikipediaChallenge = Schema.Struct({ start: ArticleTitle, target: ArticleTitle });
export type WikipediaChallenge = typeof WikipediaChallenge.Type;
export const defaultChallenge: WikipediaChallenge = { start: "Mars", target: "Nelson Mandela" };

export const racePrompt = (challenge: WikipediaChallenge) =>
  `Starting at the Wikipedia page for ${challenge.start}, get to ${challenge.target}.`;

export const WikiHop = Schema.Struct({
  title: Schema.String,
  url: Schema.String,
  at: Schema.Finite,
  via: Schema.optionalKey(Schema.Struct({ label: Schema.String, url: Schema.String })),
});

export const WikiRace = Schema.Struct({
  start: Schema.String,
  target: Schema.String,
  targetUrl: Schema.String,
  maxHops: Schema.Natural,
  path: Schema.Array(WikiHop),
  ruleViolation: Schema.optionalKey(Schema.String),
});

export const coffeeDemo = {
  id: "coffee",
  title: "Check out a bag of coffee",
  detail: "Hedge Coffee · prepare checkout, stop before payment",
  prompt:
    "On https://www.hedge.coffee/store, find one bag of coffee, add it to the cart and open checkout as a guest. Fill the checkout with the test buyer. Leave marketing boxes unchecked. Stop before submitting an order or payment.",
} as const;

export const StartUrl = Schema.String.check(
  Schema.makeFilter((value) => URL.canParse(value), { expected: "a valid URL" }),
  Schema.isMaxLength(2_000),
  Schema.isPattern(/^https?:\/\//),
);

export const ModelId = Schema.Literals([
  "gpt-6-astra",
  "gpt-6.1-sol",
  "gpt-6-luna",
  "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
]);

export type ModelId = typeof ModelId.Type;
export const Reasoning = Schema.Literals(["none", "low", "medium", "high", "xhigh", "max"]);
export const ServiceTier = Schema.Literals(["ultrafast", "fast", "default"]);

export const modelChoices: ReadonlyArray<{
  id: ModelId;
  label: string;
  /** The model rejects reasoning effort "none"; runs use "low" instead. */
  requiresReasoning?: true;
  supportsUltrafast?: true;
}> = [
  { id: "gpt-6-astra", label: "GPT-6 Astra", requiresReasoning: true, supportsUltrafast: true },
  { id: "gpt-6.1-sol", label: "GPT-6.1 Sol", requiresReasoning: true, supportsUltrafast: true },
  { id: "gpt-6-luna", label: "GPT-6 Luna" },
  { id: "@cf/meta/llama-3.3-70b-instruct-fp8-fast", label: "Llama 3.3 · Workers AI" },
];

export const requiresReasoning = (model: string) =>
  modelChoices.some((choice) => choice.id === model && choice.requiresReasoning === true);

export const supportsUltrafast = (model: string) =>
  modelChoices.some((choice) => choice.id === model && choice.supportsUltrafast === true);

export const serviceTierForModel = (model: string, tier: typeof ServiceTier.Type) =>
  tier === "ultrafast" && !supportsUltrafast(model) ? "fast" : tier;

export const RunInput = Schema.Struct({
  id: Schema.String.check(Schema.isUUID()),
  scenario: Scenario,
  /** Continue this owner's retained Thread and browser; omitted for a fresh conversation. */
  chatId: Schema.optionalKey(Schema.String.check(Schema.isUUID())),
  url: Schema.optionalKey(StartUrl),
  wikipedia: Schema.optionalKey(WikipediaChallenge),
  engine: Schema.optionalKey(BrowserEngine),
  prompt: Schema.String.check(Schema.isMaxLength(8_000)),
  screenshots: Schema.Boolean,
  liveView: Schema.Boolean,
  model: Schema.optionalKey(ModelId),
  reasoning: Schema.optionalKey(Reasoning),
  serviceTier: Schema.optionalKey(ServiceTier),
});

export type RunInput = typeof RunInput.Type;

export const Phase = Schema.Literals(["setup", "model", "action", "capture", "verify", "cleanup"]);

export type Phase = typeof Phase.Type;

export const Span = Schema.Struct({
  id: Schema.Natural,
  phase: Phase,
  name: Schema.String,
  start: Schema.Finite,
  duration: Schema.NullOr(Schema.Finite),
  outcome: Schema.Literals(["running", "success", "failure", "interrupted", "defect"]),
  turn: Schema.Natural,
  inputTokens: Schema.optionalKey(Schema.Natural),
  outputTokens: Schema.optionalKey(Schema.Natural),
  reasoningTokens: Schema.optionalKey(Schema.Natural),
  reasoningSummary: Schema.optionalKey(Schema.String),
  serviceTier: Schema.optionalKey(Schema.String),
  bytes: Schema.optionalKey(Schema.Natural),
  model: Schema.optionalKey(Schema.String),
  error: Schema.optionalKey(Schema.String),
});

export type Span = typeof Span.Type;

export const Report = Schema.Struct({
  version: Schema.Literal(3),
  fixture: Schema.Literals(["wikipedia-race-v1", "website"]),
  input: RunInput,
  model: Schema.String,
  browserVersion: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
  browserUserAgent: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(512))),
  browserRevision: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
  commandTimeoutMillis: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  startedAt: Schema.String,
  status: Schema.Literals(["running", "passed", "failed", "unverified", "cancelled"]),
  message: Schema.String,
  spans: Schema.Array(Span),
  elapsed: Schema.Finite,
  // All timestamps share the admission-relative Worker clock.
  readyAt: Schema.NullOr(Schema.Finite),
  finishedAt: Schema.NullOr(Schema.Finite),
  verifiedAt: Schema.NullOr(Schema.Finite),
  firstActionAt: Schema.NullOr(Schema.Finite),
  cleanup: Schema.Literals(["pending", "closed", "retained", "failed"]),
  race: Schema.optionalKey(WikiRace),
  finalUrl: Schema.optionalKey(Schema.String),
});

export type Report = typeof Report.Type;

export const ModelApi = Schema.Literals(["responses", "chat-completions"]);

// Viewing capabilities and images are transient: never part of exported benchmark reports.
export const Snapshot = Schema.Struct({
  ready: Schema.Boolean,
  busy: Schema.Boolean,
  canClose: Schema.Boolean,
  model: Schema.String,
  browserConfigured: Schema.Boolean,
  agentConfigured: Schema.Boolean,
  models: Schema.Array(
    Schema.Struct({ id: ModelId, label: Schema.String, configured: Schema.Boolean }),
  ),
  public: Schema.Boolean,
  report: Schema.NullOr(Report),
  liveViewUrl: Schema.NullOr(Schema.String),
  image: Schema.NullOr(Schema.String),
  notice: Schema.NullOr(Schema.String),
});

export class LabError extends Schema.TaggedError<LabError>()(
  "LabError",
  {
    code: Schema.Literals([
      "configuration",
      "busy",
      "invalid",
      "browser",
      "expired",
      "storage",
      "interrupted",
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 400 },
) {}

const headers = {
  "x-lab-session": Schema.String.check(Schema.isUUID()),
};

/** Visitor-owned provider keys for one run. The lab never stores or reports them. */
export const VisitorKey = Schema.String.check(Schema.isPattern(/^[\x21-\x7e]{16,512}$/));

const visitorKeyHeaders = {
  "x-lab-openai-key": Schema.optionalKey(VisitorKey),
};

/** A travel planner account signed in on this origin. Funded accounts run on the lab's keys. */
export const Account = Schema.Struct({ displayName: Schema.String, funded: Schema.Boolean });
export type Account = typeof Account.Type;

export const LabApi = HttpApi.make("BrowserSpeedLab").add(
  HttpApiGroup.make("lab").add(
    HttpApiEndpoint.get("account", "/api/account", { headers, success: Schema.NullOr(Account) }),
    HttpApiEndpoint.get("snapshot", "/api/snapshot", {
      headers,
      success: Snapshot,
      error: LabError,
    }),
    HttpApiEndpoint.post("run", "/api/run", {
      headers: { ...headers, ...visitorKeyHeaders },
      payload: RunInput,
      success: Report,
      error: LabError,
    }),
    HttpApiEndpoint.post("stop", "/api/stop", { headers, success: Schema.Void, error: LabError }),
    HttpApiEndpoint.post("close", "/api/close", { headers, success: Schema.Void, error: LabError }),
  ),
);
