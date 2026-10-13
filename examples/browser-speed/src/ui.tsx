import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Predicate, Schema } from "effect";
import { AsyncResult } from "effect/reactivity";
import {
  ArrowUpRight,
  BookOpen,
  Check,
  ChevronDown,
  Circle,
  Download,
  KeyRound,
  LogIn,
  Play,
  Square,
  UserRound,
  X,
} from "lucide-react";
import { useRef, useState } from "react";

import {
  ArticleTitle,
  BrowserEngine,
  defaultChallenge,
  defaultShopItem,
  ModelId,
  modelChoices,
  requiresReasoning,
  serviceTierForModel,
  supportsUltrafast,
  racePrompt,
  Reasoning,
  scenarios,
  ShopRequest,
  shopPrompt,
  shopTask,
  storeTask,
  ServiceTier,
  StartUrl,
  type Phase,
  type Report,
  type Scenario,
} from "./contract.ts";
import {
  accountAtom,
  cohort,
  comparisons,
  controlAtom,
  errorMessage,
  exportAtom,
  flowStarted,
  fromReady,
  historyAtom,
  keysAtom,
  percentile,
  progressAtom,
  runAtom,
  runOutcome,
  saveKeysAtom,
  selectedAtom,
  signInUrl,
  snapshotAtom,
  validKey,
  verifiedMillis,
  watchingAtom,
  type ProviderKeys,
} from "./state.ts";

const hostOf = (url: string) => {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
};

const seconds = (value: number | null | undefined) =>
  value === null || value === undefined ? "—" : (value / 1000).toFixed(2);

const benchmarkReportUrl: unknown = import.meta.env.VITE_BROWSER_BENCHMARK_REPORT_URL;
const docsUrl = "https://yielded.dev/agent/guide/browser/";
const sourceUrl = "https://github.com/yielded-dev/agent/tree/main/examples/browser-speed";

const phaseNames: Record<Phase, string> = {
  setup: "Setup",
  model: "Model turn",
  action: "Browser action",
  capture: "Screenshot",
  verify: "Verify",
  cleanup: "Cleanup",
};

const tasks: ReadonlyArray<{ id: Scenario; title: string; detail: string }> = [
  {
    id: "wikipedia",
    title: "Wikipedia race",
    detail: "Find a route between two articles",
  },
  { id: "browse", title: "Browse a website", detail: "Your URL and goal · open-ended" },
  ...scenarios.map(({ id, title, detail }) => ({ id, title, detail })),
  { id: storeTask.id, title: storeTask.title, detail: storeTask.detail },
  shopTask,
  { id: "custom", title: "Your own task", detail: "Free-form on the task board · not verified" },
];

const keyLinks = {
  openai: "https://platform.openai.com/api-keys",
};

const Mark = () => (
  <svg className="mark" viewBox="0 0 32 32" aria-hidden="true">
    <rect width="32" height="32" rx="8" />
    <g fill="none" strokeWidth="3.2" strokeLinecap="round">
      <path d="M16 8.5v15" />
      <path d="m9.5 12.25 13 7.5" />
      <path d="m22.5 12.25-13 7.5" />
    </g>
  </svg>
);

const Steps = ({ report }: { report: Report | null }) => {
  const [expanded, setExpanded] = useState<number | null>(null);

  const extent = Math.max(
    1,
    report?.elapsed ?? 1,
    ...(report?.spans.map((span) => span.start + (span.duration ?? 0)) ?? []),
  );

  const ready = report?.readyAt ?? null;

  return (
    <section className="section" aria-labelledby="steps-title">
      <div className="section-head">
        <h2 id="steps-title">Every step</h2>
        <p>
          The full trace of the selected run, including browser preparation. The flow clock starts
          at the marker, when the starting page is ready.
        </p>
      </div>
      {!report?.spans.length ? (
        <div className="empty">
          Run a task to see each model call, browser action and screenshot here.
        </div>
      ) : (
        <div className="card steps">
          <div className="legend">
            {(["model", "action", "capture", "verify", "setup", "cleanup"] as const).map(
              (phase) => (
                <span key={phase}>
                  <i className={`dot phase-${phase}`} />
                  {phaseNames[phase]}
                </span>
              ),
            )}
          </div>
          <ol className="step-list">
            {report.spans.map((span) => {
              const failed = ["failure", "defect"].includes(span.outcome);

              return (
                <li
                  key={span.id}
                  className={ready !== null && span.start < ready ? "before-ready" : ""}
                >
                  <button
                    className="step"
                    onClick={() => setExpanded(expanded === span.id ? null : span.id)}
                    aria-expanded={expanded === span.id}
                  >
                    <span className="step-name">
                      <i className={`dot phase-${span.phase}`} />
                      <span>
                        {span.name.startsWith("LanguageModel.")
                          ? `Model · ${span.model ?? span.name.slice("LanguageModel.".length)}`
                          : span.name === "Native browser actions"
                            ? "Browser action"
                            : span.name}
                      </span>
                    </span>
                    <span className="track" aria-hidden="true">
                      {ready !== null && (
                        <span
                          className="ready-mark"
                          style={{ left: `${(ready / extent) * 100}%` }}
                        />
                      )}
                      <span
                        className={`bar phase-${span.phase} ${span.outcome === "running" ? "running" : ""}`}
                        style={{
                          left: `${(span.start / extent) * 100}%`,
                          width: `${Math.max(0.4, ((span.duration ?? extent - span.start) / extent) * 100)}%`,
                        }}
                      />
                    </span>
                    <span className={`step-ms mono ${failed ? "bad" : ""}`}>
                      {span.duration === null ? "…" : `${Math.round(span.duration)} ms`}
                    </span>
                  </button>
                  {expanded === span.id && (
                    <div className="step-detail">
                      <p>
                        {phaseNames[span.phase]} · {span.outcome}
                        {span.model === undefined ? "" : ` · ${span.model}`}
                        {span.inputTokens === undefined ? "" : ` · ${span.inputTokens} in`}
                        {span.outputTokens === undefined ? "" : ` · ${span.outputTokens} out`}
                        {span.reasoningTokens === undefined
                          ? ""
                          : ` · ${span.reasoningTokens} reasoning`}
                        {span.serviceTier === undefined ? "" : ` · tier ${span.serviceTier}`}
                        {` · turn ${span.turn} · starts at ${seconds(span.start)} s`}
                        {span.bytes === undefined ? "" : ` · ${span.bytes.toLocaleString()} bytes`}
                      </p>
                      {span.error && <p className="bad">{span.error}</p>}
                      {span.reasoningSummary && <p>{span.reasoningSummary}</p>}
                    </div>
                  )}
                </li>
              );
            })}
          </ol>
        </div>
      )}
    </section>
  );
};

const KeyField = ({
  id,
  label,
  hint,
  href,
  value,
  onChange,
}: {
  id: keyof typeof keyLinks;
  label: string;
  hint: string;
  href: string;
  value: string;
  onChange: (value: string) => void;
}) => (
  <label className="key-field" htmlFor={`key-${id}`}>
    <span className="key-label">
      {label}
      <a href={href} target="_blank" rel="noreferrer">
        Get a key <ArrowUpRight size={12} />
      </a>
    </span>
    <input
      id={`key-${id}`}
      type="password"
      autoComplete="off"
      spellCheck={false}
      value={value}
      placeholder="Not set"
      aria-invalid={value.trim() !== "" && !validKey(value)}
      onChange={(event) => onChange(event.target.value)}
    />
    <span className={`key-hint ${value.trim() !== "" && !validKey(value) ? "bad" : ""}`}>
      {value.trim() !== "" && !validKey(value) ? "This doesn’t look like an API key." : hint}
    </span>
  </label>
);

export const App = () => {
  const [scenario, setScenario] = useState<Scenario>("wikipedia");
  const [wikiStart, setWikiStart] = useState(defaultChallenge.start);
  const [wikiTarget, setWikiTarget] = useState(defaultChallenge.target);
  const [prompt, setPrompt] = useState("");
  const [url, setUrl] = useState("https://example.com");
  const [goal, setGoal] = useState("Read the page and tell me what it is for.");
  const [shopUrl, setShopUrl] = useState("");
  const [shopItem, setShopItem] = useState(defaultShopItem);
  const [model, setModel] = useState<ModelId | "all">("gpt-6-luna");
  const [engine, setEngine] = useState<typeof BrowserEngine.Type | "all">("chromium");
  const [reasoning, setReasoning] = useState<typeof Reasoning.Type>("none");
  const [serviceTier, setServiceTier] = useState<typeof ServiceTier.Type>("fast");
  const [screenshots, setScreenshots] = useState(false);
  const [liveView, setLiveView] = useState(true);
  const [repetitions, setRepetitions] = useState(1);
  const [draft, setDraft] = useState<ProviderKeys | null>(null);
  const keysDialog = useRef<HTMLDialogElement>(null);
  const keys = useAtomValue(keysAtom);
  const saveKeys = useAtomSet(saveKeysAtom);
  const [run, start] = useAtom(runAtom);
  const [control, act] = useAtom(controlAtom);
  const snapshot = useAtomValue(snapshotAtom);
  const history = useAtomValue(historyAtom);
  const watching = useAtomValue(watchingAtom);
  const progress = useAtomValue(progressAtom);
  const [selected, select] = useAtom(selectedAtom);
  const download = useAtomSet(exportAtom);
  const remote = AsyncResult.isSuccess(snapshot) ? snapshot.value : null;
  const selectedSample = history.find(({ report }) => report.input.id === selected);
  const report = selectedSample?.report ?? remote?.report ?? history.at(-1)?.report ?? null;
  const displayedScenario = report?.input.scenario ?? scenario;
  const busy = watching || remote?.busy === true;
  const connected = remote !== null;
  const publicLab = remote?.public ?? false;
  const accountResult = useAtomValue(accountAtom);
  const account = AsyncResult.isSuccess(accountResult) ? accountResult.value : null;
  const funded = account?.funded === true;
  const failure = errorMessage(run) ?? errorMessage(control) ?? errorMessage(snapshot);

  const isWiki = scenario === "wikipedia";
  const isStore = scenario === "coffee" || scenario === "shop";
  const shop = { url: shopUrl.trim(), item: shopItem.trim() };
  const validShop = Schema.is(ShopRequest)(shop);
  const effectiveReasoning = reasoning === "none" && requiresReasoning(model) ? "low" : reasoning;

  const effectiveServiceTier =
    model === "all" ? serviceTier : serviceTierForModel(model, serviceTier);

  const effectivePrompt = isWiki
    ? racePrompt({ start: wikiStart.trim(), target: wikiTarget.trim() })
    : scenario === "browse"
      ? goal
      : scenario === "custom"
        ? prompt
        : scenario === "shop"
          ? shopPrompt(shop)
          : isStore
            ? storeTask.prompt
            : (scenarios.find((preset) => preset.id === scenario)?.prompt ?? "");

  const validChallenge =
    Schema.is(ArticleTitle)(wikiStart.trim()) &&
    Schema.is(ArticleTitle)(wikiTarget.trim()) &&
    wikiStart.trim() !== wikiTarget.trim();

  const availableModels = modelChoices.filter(
    (choice) =>
      remote?.models.some((candidate) => candidate.id === choice.id && candidate.configured) ||
      (!choice.id.startsWith("@cf/") && (funded || validKey(keys.openai))),
  );

  const modelReady =
    model === "all"
      ? availableModels.length > 0
      : availableModels.some((choice) => choice.id === model);

  // Real merchants: the public lab runs store tasks only for allowlisted accounts.
  const needsAccount = isStore && publicLab;

  const needs = [
    ...(needsAccount ? [{ label: "Funded account", ok: funded }] : []),
    {
      label:
        model === "all"
          ? "Available models"
          : model.startsWith("@cf/")
            ? "Workers AI (lab key)"
            : "OpenAI",
      ok: modelReady,
    },
  ];

  const canRun =
    connected &&
    remote.browserConfigured &&
    !busy &&
    !control.waiting &&
    (!isWiki || validChallenge) &&
    (scenario !== "shop" || validShop) &&
    (scenario !== "browse" || Schema.is(StartUrl)(url.trim())) &&
    effectivePrompt.trim().length > 0 &&
    effectivePrompt.length <= 2000 &&
    needs.every((need) => need.ok);

  const matches = report ? cohort(history, report) : [];
  const comparison = report ? comparisons(history, report) : [];

  const verified = matches.flatMap(({ report }) => {
    const time = verifiedMillis(report);

    return report.status === "passed" && time !== null ? [time] : [];
  });

  const started = matches.filter(({ report }) => flowStarted(report)).length;

  const preparing =
    !selected && busy && (Predicate.isNullish(remote?.report?.readyAt) || !remote?.busy);

  const showingActive = !selected && busy;
  const live = !selected && remote?.liveViewUrl;
  const captured = !selected && remote?.image;
  const lastHop = report?.race?.path.at(-1);
  const shopAddress = report?.input.shop?.url ?? (scenario === "shop" ? shop.url : "");

  const flow = preparing
    ? null
    : report
      ? (verifiedMillis(report) ?? fromReady(report, report.finishedAt ?? report.elapsed))
      : null;

  const count = (phase: Phase) => report?.spans.filter((span) => span.phase === phase).length ?? 0;

  const openKeys = () => {
    setDraft(keys);
    keysDialog.current?.showModal();
  };

  const startRun = () =>
    start({
      scenario,
      prompt: effectivePrompt,
      ...(isWiki ? { wikipedia: { start: wikiStart.trim(), target: wikiTarget.trim() } } : {}),
      ...(scenario === "shop" ? { shop } : {}),
      ...(scenario === "browse" ? { url: url.trim() } : {}),
      screenshots,
      liveView: liveView && engine === "chromium",
      repetitions,
      ...(engine === "all" ? { compareEngines: ["chromium", "kitesurf"] } : { engine: engine }),
      ...(!model.startsWith("@cf/")
        ? { reasoning: effectiveReasoning, serviceTier: effectiveServiceTier }
        : {}),
      ...(model === "all"
        ? { compareModels: availableModels.map((choice) => choice.id) }
        : { model }),
    });

  const keyCount = validKey(keys.openai) ? 1 : 0;

  return (
    <div className="page">
      <header className="top">
        <a className="brand" href="https://yielded.dev/agent/">
          <Mark />
          <span>
            <span className="crumbs">
              yielded <i>/</i> agent <i>/</i>{" "}
            </span>
            <b>browser-use</b>
          </span>
        </a>
        <nav className="top-links" aria-label="Project">
          <a href={docsUrl}>
            <BookOpen size={15} /> Docs
          </a>
          <a href={sourceUrl}>
            Source <ArrowUpRight size={14} />
          </a>
          {account ? (
            <span
              className="account"
              title={
                account.funded
                  ? "Your account is allowlisted: runs use the lab’s keys where you haven’t added your own."
                  : "This account isn’t allowlisted. Add your own keys to run."
              }
            >
              <UserRound size={15} /> <span className="account-name">{account.displayName}</span>
              {account.funded && <small>lab keys</small>}
            </span>
          ) : (
            publicLab && (
              <a className="account" href={signInUrl}>
                <LogIn size={15} /> Sign in
              </a>
            )
          )}
          <button className={`keys-button ${keyCount ? "has-keys" : ""}`} onClick={openKeys}>
            <KeyRound size={15} />{" "}
            {keyCount ? `${keyCount} key${keyCount === 1 ? "" : "s"}` : "Add keys"}
          </button>
        </nav>
      </header>

      <section className="hero">
        <p className="kicker mono">Code Mode · live in a Cloudflare browser</p>
        <h1>Watch an agent drive a real browser.</h1>
        <p className="lede">
          Race across Wikipedia, work through a task board, or give the agent a website and a goal.
          Watch it write browser code and compare models, settings, and every step along the way.
        </p>
      </section>

      <main className="stage">
        <aside className="controls" aria-label="Run setup">
          <fieldset className="group">
            <legend>Task</legend>
            <div className="task-list">
              {tasks.map((task) => (
                <label key={task.id} className={`task ${scenario === task.id ? "selected" : ""}`}>
                  <input
                    type="radio"
                    name="task"
                    value={task.id}
                    checked={scenario === task.id}
                    disabled={busy}
                    onChange={() => setScenario(task.id)}
                  />
                  <span className="task-title">{task.title}</span>
                  <span className="task-detail">{task.detail}</span>
                </label>
              ))}
            </div>
            {isWiki ? (
              <div className="race-inputs">
                <label>
                  <span>From</span>
                  <input
                    value={wikiStart}
                    disabled={busy}
                    maxLength={180}
                    onChange={(event) => setWikiStart(event.target.value)}
                  />
                </label>
                <span className="arrow" aria-hidden="true">
                  →
                </span>
                <label>
                  <span>To</span>
                  <input
                    value={wikiTarget}
                    disabled={busy}
                    maxLength={180}
                    onChange={(event) => setWikiTarget(event.target.value)}
                  />
                </label>
              </div>
            ) : scenario === "browse" ? (
              <div className="browse-inputs">
                <label>
                  <span>Starting URL</span>
                  <input
                    type="url"
                    value={url}
                    disabled={busy}
                    maxLength={2000}
                    onChange={(event) => setUrl(event.target.value)}
                  />
                </label>
                <label>
                  <span>Your goal</span>
                  <textarea
                    className="prompt"
                    value={goal}
                    disabled={busy}
                    maxLength={2000}
                    onChange={(event) => setGoal(event.target.value)}
                  />
                </label>
              </div>
            ) : scenario === "shop" ? (
              <div className="shop-inputs">
                <label>
                  <span>Store link</span>
                  <input
                    value={shopUrl}
                    disabled={busy}
                    maxLength={500}
                    inputMode="url"
                    placeholder="https://store.example/"
                    onChange={(event) => setShopUrl(event.target.value)}
                  />
                </label>
                <label>
                  <span>Item</span>
                  <input
                    value={shopItem}
                    disabled={busy}
                    maxLength={120}
                    onChange={(event) => setShopItem(event.target.value)}
                  />
                </label>
              </div>
            ) : scenario === "custom" ? (
              <textarea
                className="prompt"
                aria-label="Your task"
                placeholder="Describe a change to the task board, for example: Mark every task assigned to Jordan as Done."
                value={prompt}
                disabled={busy}
                maxLength={2000}
                onChange={(event) => setPrompt(event.target.value)}
              />
            ) : (
              <p className="prompt-preview">{effectivePrompt}</p>
            )}
          </fieldset>

          <p className="fine">
            {isWiki
              ? "The host verifies arrival and records page navigations. Link-only racing rules are not enforced."
              : isStore
                ? "The agent is instructed to stop before payment. There is no hard payment guard; real purchases are possible."
                : scenario === "browse"
                  ? "The agent can take real actions on this website. Results are agent-reported, not independently verified."
                  : scenario === "custom"
                    ? "Custom task outcomes are not independently verified."
                    : "The host compares the saved board with the preset. Page code can modify the fixture; this is not tamper-resistant proof."}
          </p>

          {needs.length > 0 && (
            <div className="needs">
              <span>Needs</span>
              {needs.map((need) => (
                <span key={need.label} className={need.ok ? "ok" : "missing"}>
                  {need.ok ? <Check size={13} /> : <X size={13} />} {need.label}
                </span>
              ))}
              {funded && <small className="needs-note">lab keys</small>}
              {needsAccount && !funded ? (
                account === null ? (
                  <a className="link-button" href={signInUrl}>
                    Sign in
                  </a>
                ) : (
                  <small className="needs-note">this account isn’t allowlisted</small>
                )
              ) : (
                needs.some((need) => !need.ok) && (
                  <button className="link-button" onClick={openKeys}>
                    Add keys
                  </button>
                )
              )}
            </div>
          )}

          <div className="run-row">
            {busy ? (
              <button
                className="primary stop"
                onClick={() => act("stop")}
                disabled={control.waiting}
              >
                <Square size={13} fill="currentColor" /> Stop run
                {progress.total > 1 ? ` · ${progress.current}/${progress.total}` : ""}
              </button>
            ) : (
              <button className="primary" disabled={!canRun} onClick={startRun}>
                <Play size={15} fill="currentColor" /> {isWiki ? "Start race" : "Run task"}
              </button>
            )}
            <button
              className="ghost"
              disabled={!remote?.canClose || control.waiting}
              onClick={() => act("close")}
            >
              Close browser
            </button>
          </div>

          <details className="settings">
            <summary>
              Run settings <ChevronDown size={15} />
            </summary>
            <div className="settings-grid">
              <label>
                <span>Browser</span>
                <select
                  value={engine}
                  disabled={busy}
                  onChange={(event) =>
                    setEngine(
                      event.target.value === "all"
                        ? "all"
                        : Schema.decodeUnknownSync(BrowserEngine)(event.target.value),
                    )
                  }
                >
                  <option value="chromium">Chromium</option>
                  <option value="kitesurf">Kitesurf · experimental</option>
                  <option value="all">Compare both</option>
                </select>
              </label>
              <>
                <label>
                  <span>Model</span>
                  <select
                    value={model}
                    disabled={busy}
                    onChange={(event) =>
                      setModel(
                        event.target.value === "all"
                          ? "all"
                          : Schema.decodeUnknownSync(ModelId)(event.target.value),
                      )
                    }
                  >
                    {modelChoices.map((choice) => (
                      <option
                        key={choice.id}
                        value={choice.id}
                        disabled={!availableModels.some((value) => value.id === choice.id)}
                      >
                        {choice.label}
                      </option>
                    ))}
                    <option value="all" disabled={availableModels.length < 2}>
                      Compare available models
                    </option>
                  </select>
                </label>
                <label>
                  <span>OpenAI speed</span>
                  <select
                    value={effectiveServiceTier}
                    disabled={busy || model.startsWith("@cf/")}
                    onChange={(event) =>
                      setServiceTier(Schema.decodeUnknownSync(ServiceTier)(event.target.value))
                    }
                  >
                    <option
                      value="ultrafast"
                      disabled={model !== "all" && !supportsUltrafast(model)}
                    >
                      Ultrafast
                    </option>
                    <option value="fast">Fast</option>
                    <option value="default">Standard</option>
                  </select>
                </label>
                <label>
                  <span>Reasoning</span>
                  <select
                    value={effectiveReasoning}
                    disabled={busy || model.startsWith("@cf/")}
                    onChange={(event) =>
                      setReasoning(Schema.decodeUnknownSync(Reasoning)(event.target.value))
                    }
                  >
                    {Reasoning.literals
                      .filter((effort) => effort !== "none" || !requiresReasoning(model))
                      .map((effort) => (
                        <option key={effort} value={effort}>
                          {effort}
                        </option>
                      ))}
                  </select>
                </label>
              </>
              <label>
                <span>Repetitions</span>
                <select
                  value={repetitions}
                  disabled={busy}
                  onChange={(event) => setRepetitions(Number(event.target.value))}
                >
                  <option value={1}>1 run</option>
                  <option value={3}>3 runs</option>
                  <option value={10}>10 runs</option>
                </select>
              </label>
            </div>
            {model === "all" && serviceTier === "ultrafast" && (
              <p className="fine">
                Astra and Sol use Ultrafast; Luna uses Fast. Each run records its requested and
                served tier.
              </p>
            )}
            <div className="toggles">
              <label>
                <input
                  type="checkbox"
                  checked={liveView && engine === "chromium"}
                  disabled={busy || engine !== "chromium"}
                  onChange={(event) => setLiveView(event.target.checked)}
                />
                Live view <small>Chromium only</small>
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={screenshots}
                  disabled={busy}
                  onChange={(event) => setScreenshots(event.target.checked)}
                />
                Screenshot after each code call
              </label>
            </div>
            {engine !== "chromium" && (
              <p className="fine">
                Kitesurf is Cloudflare’s experimental lightweight engine. It has no live view, no
                native dialogs (the task board needs them), and large pages such as Wikipedia can
                exhaust its memory. Comparing browsers disables live view for both.
              </p>
            )}
          </details>
        </aside>

        <section className="viewer" aria-label="Browser">
          <div className="hud">
            <div className="clock">
              <span
                className={`clock-value mono ${showingActive && !preparing ? "ticking" : flow === null ? "idle" : ""}`}
              >
                {flow === null ? "0.00" : seconds(flow)}
                <small>s</small>
              </span>
              <span className="clock-label">
                {showingActive
                  ? preparing
                    ? "Preparing the browser · clock starts when the page is ready"
                    : "Page ready → now"
                  : report?.verifiedAt !== null && report?.verifiedAt !== undefined
                    ? "Page ready → verified"
                    : report
                      ? "Page ready → stopped"
                      : "Page ready → verified"}
              </span>
            </div>
            <div className="hud-stats">
              <span>
                <b className="mono">{count("model")}</b> model calls
              </span>
              <span>
                <b className="mono">{count("action")}</b> actions
              </span>
              <span className={`status ${showingActive ? "running" : (report?.status ?? "idle")}`}>
                {report?.status === "passed" && !showingActive ? (
                  <Check size={13} />
                ) : (
                  <Circle size={9} fill="currentColor" />
                )}
                {showingActive
                  ? preparing
                    ? "preparing"
                    : "running"
                  : report
                    ? report.race && report.status === "passed"
                      ? "arrival verified"
                      : runOutcome(report)
                    : "ready"}
              </span>
            </div>
          </div>

          <div className="browser">
            <div className="chrome">
              <span className="lights" aria-hidden="true">
                <i />
                <i />
                <i />
              </span>
              <span className="address mono">
                {displayedScenario === "wikipedia" || report?.race
                  ? (lastHop?.url.replace("https://", "") ?? "en.wikipedia.org")
                  : displayedScenario === "browse"
                    ? hostOf(report?.finalUrl ?? report?.input.url ?? url)
                    : shopAddress
                      ? hostOf(shopAddress)
                      : displayedScenario === "coffee"
                        ? "www.hedge.coffee"
                        : "task-board · synthetic data"}
              </span>
              <span className={`view-label mono ${live ? "live" : ""}`}>
                {live
                  ? "LIVE"
                  : captured
                    ? "SCREENSHOT"
                    : showingActive
                      ? "CONNECTING"
                      : selected
                        ? "SAVED"
                        : "IDLE"}
              </span>
            </div>
            <div className="screen">
              {live ? (
                <iframe
                  key={live}
                  title="Read-only view of the agent’s browser"
                  src={live}
                  referrerPolicy="no-referrer"
                />
              ) : captured ? (
                <img src={captured} alt="Latest screenshot of the agent’s browser" />
              ) : (
                <div className="screen-empty">
                  <p>
                    {showingActive
                      ? "Connecting to the Cloudflare browser…"
                      : selected
                        ? "Saved run selected. Inspect its route, report and timeline below. Screenshots and live view are transient."
                        : isWiki
                          ? `A Cloudflare browser opens ${wikiStart || "the start article"} on Wikipedia and tries to reach ${wikiTarget || "the destination"}. Arrival is verified; link-only racing rules are not enforced.`
                          : isStore
                            ? "The agent opens the store, finds the item and prepares checkout. It is instructed to stop before payment, but there is no hard payment guard. Results are agent-reported."
                            : scenario === "browse"
                              ? "A Cloudflare browser opens your URL and works toward your goal using Code Mode."
                              : "A Cloudflare browser opens a small task board. Preset runs compare the saved board with the requested result."}
                  </p>
                </div>
              )}
            </div>
          </div>
          {live && (
            <a className="fine-link" href={live} target="_blank" rel="noreferrer">
              Open live view in a new tab <ArrowUpRight size={13} />
            </a>
          )}

          {report?.race && (
            <div className="route-summary">
              <p className="fine">
                {report.race.start} → {report.race.target} ·{" "}
                {Math.max(0, report.race.path.length - 1)} navigations
                {report.status === "passed" ? " · arrival verified" : " · arrival not verified"}.
                Link-only rules are not enforced.
              </p>
              {report.race.ruleViolation && <p className="bad">{report.race.ruleViolation}</p>}
              <ol className="route" aria-label="Route">
                {report.race.path.map((hop, index) => (
                  <li
                    key={`${index}-${hop.url}`}
                    title={hop.via ? `Via ${hop.via.label}` : undefined}
                  >
                    <a href={hop.url} target="_blank" rel="noreferrer">
                      {hop.title}
                    </a>
                    <small className="mono">
                      {index === 0 ? "start" : `${seconds(fromReady(report, hop.at))} s`}
                    </small>
                  </li>
                ))}
              </ol>
            </div>
          )}

          {(failure || remote?.notice || busy || report) && (
            <div
              className={`message ${failure || report?.status === "failed" ? "bad" : ""}`}
              role="status"
            >
              {failure ??
                (selected ? report?.message : remote?.notice) ??
                (busy
                  ? preparing
                    ? "Preparing the starting page."
                    : (report?.message ?? "Running.")
                  : (report?.message ?? ""))}
            </div>
          )}
          {connected && !remote.browserConfigured && (
            <div className="message bad" role="alert">
              This lab has no Cloudflare browser configured.
            </div>
          )}
        </section>
      </main>

      {report && (
        <section className="section" aria-label="Run details">
          <div className="stats">
            <div>
              <span>Total elapsed</span>
              <b className="mono">{seconds(report.elapsed)} s</b>
            </div>
            <div>
              <span>Page preparation</span>
              <b className="mono">{seconds(report.readyAt)} s</b>
            </div>
            <div>
              <span>First action after ready</span>
              <b className="mono">{seconds(fromReady(report, report.firstActionAt))} s</b>
            </div>
            <div>
              <span>Cleanup</span>
              <b className="mono">{report.cleanup}</b>
            </div>
          </div>
          {selectedSample && (
            <p className="fine">
              Client round trip: {seconds(selectedSample.clientElapsedMillis)} s
            </p>
          )}
          {report.board.length > 0 && (
            <details className="card board-result">
              <summary>Saved task board · {report.board.length} tasks</summary>
              <div className="table-wrap">
                <table aria-label="Saved task board">
                  <thead>
                    <tr>
                      <th>Task</th>
                      <th>Assignee</th>
                      <th>Priority</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.board.map((task) => (
                      <tr key={task.id}>
                        <td>{task.title}</td>
                        <td>{task.assignee}</td>
                        <td>{task.priority}</td>
                        <td>{task.status}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          )}
        </section>
      )}
      <Steps key={report?.input.id ?? "empty"} report={report} />

      <section className="section" aria-labelledby="compare-title">
        <div className="section-head">
          <h2 id="compare-title">Compare runs</h2>
          <p>
            Runs in this tab, grouped by task and settings. Medians use verified runs only; failures
            count toward the started total. Wikipedia verification confirms arrival only. Website
            outcomes are agent-reported; their history is retained without verified-time rankings.
            {typeof benchmarkReportUrl === "string" && benchmarkReportUrl.length > 0 && (
              <>
                {" "}
                <a href={benchmarkReportUrl}>Published comparison</a>
              </>
            )}
          </p>
          <button className="ghost small" disabled={!history.length} onClick={() => download()}>
            <Download size={14} /> Export JSON
          </button>
        </div>
        {history.length ? (
          <>
            <div className="stats">
              <div>
                <span>Verified</span>
                <b className="mono">
                  {verified.length}/{started}
                </b>
              </div>
              <div>
                <span>Median</span>
                <b className="mono">{seconds(percentile(verified, 0.5))} s</b>
              </div>
              <div>
                <span>p95</span>
                <b className="mono">
                  {verified.length >= 20 ? `${seconds(percentile(verified, 0.95))} s` : "—"}
                </b>
                {verified.length < 20 && <small>after 20 verified runs</small>}
              </div>
              <div>
                <span>Preparation failures</span>
                <b className="mono">
                  {
                    matches.filter(
                      ({ report }) => !flowStarted(report) && report.status !== "running",
                    ).length
                  }
                </b>
              </div>
            </div>
            {comparison.length > 1 && (
              <div className="table-wrap card">
                <table aria-label="Comparison">
                  <thead>
                    <tr>
                      <th>Model</th>
                      <th>Browser</th>
                      <th>Reasoning</th>
                      <th>Tier asked / served</th>
                      <th className="num">Verified</th>
                      <th className="num">Median</th>
                      {report?.race && <th className="num">Hops</th>}
                    </tr>
                  </thead>
                  <tbody>
                    {comparison.map((group) => (
                      <tr key={group.key}>
                        <td>
                          {modelChoices.find((value) => value.id === group.model)?.label ??
                            group.model}
                        </td>
                        <td>{group.engine === "kitesurf" ? "Kitesurf" : "Chromium"}</td>
                        <td>{group.reasoning}</td>
                        <td>
                          {group.serviceTier} / {group.servedTier}
                        </td>
                        <td className="num mono">
                          {group.passed}/{group.started}
                        </td>
                        <td className="num mono">{seconds(group.median)} s</td>
                        {report?.race && <td className="num mono">{group.hops ?? "—"}</td>}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <div className="table-wrap card">
              <table aria-label="Run history">
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Model</th>
                    <th>Browser</th>
                    <th className="num">Work time</th>
                    <th>Outcome</th>
                    <th>
                      <span className="sr-only">Inspect</span>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {[...history].reverse().map(({ report: row }) => (
                    <tr key={row.input.id} className={selected === row.input.id ? "selected" : ""}>
                      <td>
                        {row.input.scenario === "wikipedia"
                          ? `${row.race?.start ?? row.input.wikipedia?.start ?? defaultChallenge.start} → ${row.race?.target ?? row.input.wikipedia?.target ?? defaultChallenge.target}${row.race ? ` · ${Math.max(0, row.race.path.length - 1)} hops` : ""}`
                          : row.input.scenario === "browse"
                            ? hostOf(row.input.url ?? "Website")
                            : (tasks.find((value) => value.id === row.input.scenario)?.title ??
                              "Your own task")}
                      </td>
                      <td>
                        {modelChoices.find((value) => value.id === row.model)?.label ?? row.model}
                      </td>
                      <td>{row.input.engine === "kitesurf" ? "Kitesurf" : "Chromium"}</td>
                      <td className="num mono">
                        {seconds(
                          verifiedMillis(row) ?? fromReady(row, row.finishedAt ?? row.elapsed),
                        )}{" "}
                        s
                      </td>
                      <td>
                        <span className={`outcome ${row.status}`}>
                          {row.race && row.status === "passed"
                            ? "arrival verified"
                            : runOutcome(row)}
                        </span>
                      </td>
                      <td>
                        <button
                          className="icon"
                          aria-label={selected === row.input.id ? "Show latest run" : "Inspect run"}
                          onClick={() => select(selected === row.input.id ? null : row.input.id)}
                        >
                          <ArrowUpRight size={15} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <div className="empty">No runs yet. Your results appear here, side by side.</div>
        )}
      </section>

      <footer className="foot">
        <span>
          <Mark /> yielded.dev
        </span>
        <span>
          Runs use a real Cloudflare browser and real model calls, on your keys or, for allowlisted
          accounts, the lab’s.
        </span>
      </footer>

      <dialog
        ref={keysDialog}
        className="keys-dialog"
        aria-labelledby="keys-title"
        onClose={() => setDraft(null)}
      >
        {draft && (
          <form
            method="dialog"
            onSubmit={() => {
              saveKeys(draft);
            }}
          >
            <h2 id="keys-title">Your API keys</h2>
            <p>
              Keys stay in this browser. Each run sends them over HTTPS to this lab’s Worker, which
              uses them for that run only. They aren’t stored on the server or included in reports.
            </p>
            {publicLab && account === null && (
              <p>
                Allowlisted? <a href={signInUrl}>Sign in</a> to run on the lab’s keys instead.
              </p>
            )}
            <KeyField
              id="openai"
              label="OpenAI"
              hint="Runs the selected OpenAI model using Code Mode."
              href={keyLinks.openai}
              value={draft.openai}
              onChange={(openai) => setDraft({ ...draft, openai })}
            />
            <label className="remember">
              <input
                type="checkbox"
                checked={draft.remember}
                onChange={(event) => setDraft({ ...draft, remember: event.target.checked })}
              />
              Remember on this device
              <small>Otherwise keys are forgotten when you close this tab.</small>
            </label>
            <div className="dialog-actions">
              <button
                type="button"
                className="ghost"
                onClick={() => setDraft({ openai: "", remember: false })}
              >
                Clear all
              </button>
              <span />
              <button type="button" className="ghost" onClick={() => keysDialog.current?.close()}>
                Cancel
              </button>
              <button
                type="submit"
                className="primary"
                disabled={[draft.openai].some((value) => value.trim() !== "" && !validKey(value))}
              >
                Save keys
              </button>
            </div>
          </form>
        )}
      </dialog>
    </div>
  );
};
