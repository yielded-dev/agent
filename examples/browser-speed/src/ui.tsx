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
  Play,
  Square,
  X,
} from "lucide-react";
import { useRef, useState } from "react";

import {
  type Driver,
  ArticleTitle,
  BrowserEngine,
  defaultChallenge,
  Mode,
  ModelId,
  modelChoices,
  racePrompt,
  Reasoning,
  scenarios,
  ServiceTier,
  type Phase,
  type Report,
  type Scenario,
} from "./contract.ts";
import {
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
  snapshotAtom,
  submittedAtom,
  validKey,
  verifiedMillis,
  watchingAtom,
  type ProviderKeys,
} from "./state.ts";

const seconds = (value: number | null | undefined) =>
  value === null || value === undefined ? "—" : (value / 1000).toFixed(2);

const benchmarkReportUrl: unknown = import.meta.env.VITE_BROWSER_BENCHMARK_REPORT_URL;
const docsUrl = "https://yielded.dev/agent/guide/browser/#let-jev-drive-the-browser";
const sourceUrl = "https://github.com/yielded-dev/agent/tree/main/examples/browser-speed";

const phaseNames: Record<Phase, string> = {
  setup: "Setup",
  model: "Model turn",
  decision: "Jev decision",
  action: "Browser action",
  observation: "Read page",
  wait: "Wait",
  capture: "Screenshot",
  verify: "Verify",
  cleanup: "Cleanup",
};

const tasks: ReadonlyArray<{ id: Scenario; title: string; detail: string }> = [
  {
    id: "wikipedia",
    title: "Wikipedia race",
    detail: "Follow real article links to a destination",
  },
  ...scenarios.map(({ id, title, detail }) => ({ id, title, detail })),
  { id: "custom", title: "Your own task", detail: "Free-form on the task board · not verified" },
];

const keyLinks = {
  typesafe: "https://docs.typesafe.ai/introduction/quickstart",
  openrouter: "https://openrouter.ai/keys",
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
        <div className="empty">Run a task to see each decision, action and page read here.</div>
      ) : (
        <div className="card steps">
          <div className="legend">
            {(
              ["decision", "model", "action", "observation", "wait", "verify", "setup"] as const
            ).map((phase) => (
              <span key={phase}>
                <i className={`dot phase-${phase}`} />
                {phaseNames[phase]}
              </span>
            ))}
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
                        {span.candidateCount === undefined
                          ? ""
                          : ` · ${span.candidateCount} candidates`}
                        {span.questionCount === undefined
                          ? ""
                          : ` · ${span.questionCount} questions`}
                        {span.bytes === undefined ? "" : ` · ${span.bytes.toLocaleString()} bytes`}
                      </p>
                      {span.error && <p className="bad">{span.error}</p>}
                      {span.reasoningSummary && <p>{span.reasoningSummary}</p>}
                      {span.choices?.map((choice) => (
                        <p key={`${choice.target}/${choice.ref}`}>
                          {choice.target} → #{choice.ref} · {(choice.probability * 100).toFixed(1)}%
                        </p>
                      ))}
                      {span.decisionDistributions
                        ?.filter((value) => Math.abs(value.reportedMass - 1) > 1e-6)
                        .map((value) => (
                          <p key={value.question}>
                            Jev reported {(value.reportedProbability * 100).toFixed(1)}% for #
                            {value.ref}; its distribution totaled {value.reportedMass.toFixed(2)}{" "}
                            and was normalized. The choice is unchanged.
                          </p>
                        ))}
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
  const [driver, setDriver] = useState<typeof Driver.Type>("jev");
  const [mode, setMode] = useState<typeof Mode.Type>("batched");
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
  const submitted = useAtomValue(submittedAtom);
  const progress = useAtomValue(progressAtom);
  const [selected, select] = useAtom(selectedAtom);
  const download = useAtomSet(exportAtom);
  const remote = AsyncResult.isSuccess(snapshot) ? snapshot.value : null;
  const selectedSample = history.find(({ report }) => report.input.id === selected);
  const report = selectedSample?.report ?? remote?.report ?? history.at(-1)?.report ?? null;
  const busy = watching || remote?.busy === true;
  const connected = remote !== null;
  const publicLab = remote?.public ?? false;
  const failure = errorMessage(run) ?? errorMessage(control) ?? errorMessage(snapshot);

  const isWiki = scenario === "wikipedia";
  const jev = driver === "jev";
  const scripted = !jev && !publicLab && mode === "scripted";

  const effectiveMode: typeof Mode.Type =
    jev || isWiki ? "agent" : scripted ? "scripted" : mode === "scripted" ? "batched" : mode;

  const effectivePrompt = isWiki
    ? racePrompt({ start: wikiStart, target: wikiTarget })
    : scenario === "custom"
      ? prompt
      : (scenarios.find((preset) => preset.id === scenario)?.prompt ?? "");

  const validChallenge =
    Schema.is(ArticleTitle)(wikiStart.trim()) &&
    Schema.is(ArticleTitle)(wikiTarget.trim()) &&
    wikiStart.trim() !== wikiTarget.trim();

  // A visitor key or the lab's own key (private labs only) satisfies each requirement.
  const hasTypesafe = validKey(keys.typesafe) || remote?.jevConfigured === true;

  const hasFieldText =
    validKey(keys.openrouter) || validKey(keys.openai) || (remote?.jevTextModel ?? null) !== null;

  const availableModels = modelChoices.filter(
    (choice) =>
      remote?.models.some((candidate) => candidate.id === choice.id && candidate.configured) ||
      (!choice.id.startsWith("@cf/") && validKey(keys.openai)),
  );

  const modelReady =
    model === "all"
      ? availableModels.length > 0
      : availableModels.some((choice) => choice.id === model);

  const needs = jev
    ? [
        { label: "TypeSafe", ok: hasTypesafe },
        ...(isWiki ? [] : [{ label: "OpenRouter or OpenAI", ok: hasFieldText }]),
      ]
    : scripted
      ? []
      : [{ label: model.startsWith("@cf/") ? "Workers AI (lab key)" : "OpenAI", ok: modelReady }];

  const canRun =
    connected &&
    remote.browserConfigured &&
    !busy &&
    !control.waiting &&
    (!isWiki || validChallenge) &&
    effectivePrompt.trim() !== "" &&
    (scripted ? scenario !== "custom" : needs.every((need) => need.ok));

  const matches = report ? cohort(history, report) : [];
  const comparison = report ? comparisons(history, report) : [];

  const verified = matches.flatMap(({ report }) => {
    const time = verifiedMillis(report);

    return report.status === "passed" && time !== null ? [time] : [];
  });

  const started = matches.filter(({ report }) => flowStarted(report)).length;
  const preparing = busy && (Predicate.isNullish(remote?.report?.readyAt) || !remote?.busy);
  const live = !selected && remote?.liveViewUrl;
  const captured = !selected && remote?.image;
  const lastHop = report?.race?.path.at(-1);

  const flow = report
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
      mode: effectiveMode,
      driver,
      prompt: effectivePrompt,
      ...(isWiki ? { wikipedia: { start: wikiStart.trim(), target: wikiTarget.trim() } } : {}),
      screenshots,
      liveView: liveView && engine !== "kitesurf",
      repetitions,
      ...(engine === "all" ? { compareEngines: ["chromium", "kitesurf"] } : { engine }),
      ...(!jev && !scripted && !model.startsWith("@cf/") ? { reasoning, serviceTier } : {}),
      ...(jev || scripted
        ? {}
        : model === "all"
          ? { compareModels: availableModels.map((choice) => choice.id) }
          : { model }),
    });

  const keyCount = [keys.typesafe, keys.openrouter, keys.openai].filter(validKey).length;

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
          <button className={`keys-button ${keyCount ? "has-keys" : ""}`} onClick={openKeys}>
            <KeyRound size={15} />{" "}
            {keyCount ? `${keyCount} key${keyCount === 1 ? "" : "s"}` : "Add keys"}
          </button>
        </nav>
      </header>

      <section className="hero">
        <p className="kicker mono">BrowserUse.runJev · live in a Cloudflare browser</p>
        <h1>Watch Jev drive a real browser.</h1>
        <p className="lede">
          Jev picks each action and its target from what’s on screen, in one decision request per
          step. Race it against a model agent on Wikipedia or a task board, and see where the time
          goes.
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

          <fieldset className="group">
            <legend>Driver</legend>
            <div className="segmented" role="radiogroup" aria-label="Driver">
              {(
                [
                  ["jev", "Jev", "One decision per step"],
                  ["model", "Model agent", "An LLM calls browser tools"],
                ] as const
              ).map(([value, label, detail]) => (
                <label key={value} className={driver === value ? "selected" : ""}>
                  <input
                    type="radio"
                    name="driver"
                    value={value}
                    checked={driver === value}
                    disabled={busy}
                    onChange={() => setDriver(value)}
                  />
                  <strong>{label}</strong>
                  <small>{detail}</small>
                </label>
              ))}
            </div>
          </fieldset>

          {needs.length > 0 && (
            <div className="needs">
              <span>Needs</span>
              {needs.map((need) => (
                <span key={need.label} className={need.ok ? "ok" : "missing"}>
                  {need.ok ? <Check size={13} /> : <X size={13} />} {need.label}
                </span>
              ))}
              {needs.some((need) => !need.ok) && (
                <button className="link-button" onClick={openKeys}>
                  Add keys
                </button>
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
              {!jev && (
                <>
                  <label>
                    <span>Model</span>
                    <select
                      value={model}
                      disabled={busy || scripted}
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
                    <span>Execution</span>
                    <select
                      value={effectiveMode}
                      disabled={busy || isWiki}
                      onChange={(event) =>
                        setMode(Schema.decodeUnknownSync(Mode)(event.target.value))
                      }
                    >
                      <option value="agent">One action per call</option>
                      <option value="batched">Batched actions</option>
                      {!publicLab && <option value="scripted">Scripted baseline</option>}
                    </select>
                  </label>
                  <label>
                    <span>OpenAI speed</span>
                    <select
                      value={serviceTier}
                      disabled={busy || scripted || model.startsWith("@cf/")}
                      onChange={(event) =>
                        setServiceTier(Schema.decodeUnknownSync(ServiceTier)(event.target.value))
                      }
                    >
                      <option value="fast">Fast</option>
                      <option value="default">Standard</option>
                    </select>
                  </label>
                  <label>
                    <span>Reasoning</span>
                    <select
                      value={reasoning}
                      disabled={busy || scripted || model.startsWith("@cf/")}
                      onChange={(event) =>
                        setReasoning(Schema.decodeUnknownSync(Reasoning)(event.target.value))
                      }
                    >
                      {Reasoning.literals.map((effort) => (
                        <option key={effort} value={effort}>
                          {effort}
                        </option>
                      ))}
                    </select>
                  </label>
                </>
              )}
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
            <div className="toggles">
              <label>
                <input
                  type="checkbox"
                  checked={liveView && engine !== "kitesurf"}
                  disabled={busy || engine === "kitesurf"}
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
                Screenshot after each action
              </label>
            </div>
            {engine !== "chromium" && (
              <p className="fine">
                Kitesurf is Cloudflare’s experimental lightweight engine. It has no live view, no
                native dialogs (the task board needs them), and large pages such as Wikipedia can
                exhaust its memory.
              </p>
            )}
          </details>
        </aside>

        <section className="viewer" aria-label="Browser">
          <div className="hud">
            <div className="clock">
              <span
                className={`clock-value mono ${busy && !preparing ? "ticking" : flow === null ? "idle" : ""}`}
              >
                {flow === null ? "0.00" : seconds(flow)}
                <small>s</small>
              </span>
              <span className="clock-label">
                {busy
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
                <b className="mono">{count("decision")}</b> Jev decisions
              </span>
              <span>
                <b className="mono">{count("model")}</b> model calls
              </span>
              <span>
                <b className="mono">{count("action")}</b> actions
              </span>
              <span className={`status ${busy ? "running" : (report?.status ?? "idle")}`}>
                {report?.status === "passed" && !busy ? (
                  <Check size={13} />
                ) : (
                  <Circle size={9} fill="currentColor" />
                )}
                {busy
                  ? preparing
                    ? "preparing"
                    : "running"
                  : report
                    ? runOutcome(report)
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
                {isWiki || report?.race
                  ? (lastHop?.url.replace("https://", "") ?? "en.wikipedia.org")
                  : "task-board · synthetic data"}
              </span>
              <span className={`view-label mono ${live ? "live" : ""}`}>
                {live ? "LIVE" : captured ? "SCREENSHOT" : busy ? "CONNECTING" : "IDLE"}
              </span>
            </div>
            <div className="screen">
              {live ? (
                <iframe
                  key={live}
                  title="Read-only view of the agent’s browser"
                  src={live}
                  referrerPolicy="no-referrer"
                  allow="clipboard-read; clipboard-write"
                />
              ) : captured ? (
                <img src={captured} alt="Latest screenshot of the agent’s browser" />
              ) : (
                <div className="screen-empty">
                  <p>
                    {busy
                      ? "Connecting to the Cloudflare browser…"
                      : isWiki
                        ? `A Cloudflare browser opens ${wikiStart || "the start article"} on Wikipedia and follows article links to ${wikiTarget || "the destination"}. No search, no typed URLs.`
                        : "A Cloudflare browser opens a small task board. The run passes only if the saved board exactly matches the request."}
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
            <ol className="route" aria-label="Route">
              {report.race.path.map((hop, index) => (
                <li key={`${index}-${hop.url}`}>
                  <a href={hop.url} target="_blank" rel="noreferrer">
                    {hop.title}
                  </a>
                  <small className="mono">
                    {index === 0 ? "start" : `${seconds(fromReady(report, hop.at))} s`}
                  </small>
                </li>
              ))}
            </ol>
          )}

          {(failure || remote?.notice || submitted) && (
            <div
              className={`message ${failure || report?.status === "failed" ? "bad" : ""}`}
              role="status"
            >
              {failure ??
                remote?.notice ??
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

      <Steps report={report} />

      <section className="section" aria-labelledby="compare-title">
        <div className="section-head">
          <h2 id="compare-title">Compare runs</h2>
          <p>
            Runs in this tab, grouped by task and settings. Medians use verified runs only; failures
            count toward the started total.
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
                      <th>Driver</th>
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
                        <td>{group.driver === "jev" ? "Jev" : "Model agent"}</td>
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
                    <th>Driver</th>
                    <th>Model</th>
                    <th>Browser</th>
                    <th className="num">Flow</th>
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
                          ? `${row.race?.start ?? row.input.wikipedia?.start ?? defaultChallenge.start} → ${row.race?.target ?? row.input.wikipedia?.target ?? defaultChallenge.target}${row.race ? ` · ${row.race.path.length - 1} hops` : ""}`
                          : (tasks.find((value) => value.id === row.input.scenario)?.title ??
                            "Your own task")}
                      </td>
                      <td>
                        {row.input.driver === "jev"
                          ? "Jev"
                          : row.input.mode === "scripted"
                            ? "Scripted"
                            : `Model · ${row.input.mode === "batched" ? "batched" : "single"}`}
                      </td>
                      <td>
                        {modelChoices.find((value) => value.id === row.model)?.label ?? row.model}
                      </td>
                      <td>{row.input.engine === "kitesurf" ? "Kitesurf" : "Chromium"}</td>
                      <td className="num mono">{seconds(verifiedMillis(row))} s</td>
                      <td>
                        <span className={`outcome ${row.status}`}>{runOutcome(row)}</span>
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

      <section className="section use" aria-labelledby="use-title">
        <div className="section-head">
          <h2 id="use-title">Use it in your agent</h2>
          <p>
            The Jev loop on this page is <code>BrowserUse.runJev</code> from{" "}
            <code>@yielded/agent</code>. Provide a page adapter, a Jev decision model and a small
            model for field text.
          </p>
        </div>
        <pre className="code card">
          <code>
            <span className="tok-k">const</span> result = <span className="tok-k">yield</span>*{" "}
            BrowserUse.<span className="tok-f">runJev</span>({"{"} goal:{" "}
            <span className="tok-s">"Create a task called Ship demo."</span> {"}"});{"\n"}
            <span className="tok-c">
              {'// result.stop is why it ended; "done" is Jev\'s claim, so verify the outcome.'}
            </span>
          </code>
        </pre>
        <a className="fine-link" href={docsUrl}>
          Read the guide <ArrowUpRight size={13} />
        </a>
      </section>

      <footer className="foot">
        <span>
          <Mark /> yielded.dev
        </span>
        <span>Runs use a real Cloudflare browser and real model calls with your keys.</span>
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
            <KeyField
              id="typesafe"
              label="TypeSafe"
              hint="Runs Jev’s decisions."
              href={keyLinks.typesafe}
              value={draft.typesafe}
              onChange={(typesafe) => setDraft({ ...draft, typesafe })}
            />
            <KeyField
              id="openrouter"
              label="OpenRouter"
              hint="Mercury 2.5 writes Jev’s field text. Fastest option."
              href={keyLinks.openrouter}
              value={draft.openrouter}
              onChange={(openrouter) => setDraft({ ...draft, openrouter })}
            />
            <KeyField
              id="openai"
              label="OpenAI"
              hint="Runs the model agent; also writes Jev’s field text if OpenRouter isn’t set."
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
                onClick={() =>
                  setDraft({ typesafe: "", openrouter: "", openai: "", remember: false })
                }
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
                disabled={[draft.typesafe, draft.openrouter, draft.openai].some(
                  (value) => value.trim() !== "" && !validKey(value),
                )}
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
