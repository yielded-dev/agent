import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option, Predicate, Redacted, Schema } from "effect";
import { AsyncResult } from "effect/reactivity";
import {
  ArrowUpRight,
  BookOpen,
  Check,
  ChevronDown,
  Circle,
  Download,
  Hand,
  KeyRound,
  LogIn,
  Play,
  Send,
  MessageSquare,
  Square,
  UserRound,
  X,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Streamdown } from "streamdown";

import {
  ArticleTitle,
  BrowserEngine,
  defaultChallenge,
  coffeeDemo,
  ModelId,
  modelChoices,
  requiresReasoning,
  serviceTierForModel,
  supportsUltrafast,
  racePrompt,
  Reasoning,
  ServiceTier,
  StartUrl,
  type Phase,
  type PrivateEntry,
  type Report,
  type Scenario,
  type Span,
} from "./contract.ts";
import {
  accountAtom,
  submitPrivateEntryAtom,
  cancelPrivateEntryAtom,
  chatDraftAtom,
  chatIdAtom,
  chatTurnsAtom,
  sendChatAtom,
  cohort,
  comparisons,
  controlAtom,
  errorMessage,
  exportAtom,
  downloadTraceAtom,
  viewAtom,
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
  type ChatTurn,
} from "./state.ts";

// Native provider citation IDs have no meaning outside their response; source links carry the references.
const formatReply = (text: string) =>
  text.replace(/\uE200cite\uE202.*?\uE201|\\ue200cite\\ue202.*?\\ue201/gu, "").trim();

const Reply = ({ text, streaming = false }: { text: string; streaming?: boolean }) => (
  <Streamdown
    className="chat-reply"
    mode={streaming ? "streaming" : "static"}
    isAnimating={streaming}
    controls={false}
    skipHtml
    rehypePlugins={[]}
    allowedElements={[
      "p",
      "a",
      "strong",
      "em",
      "del",
      "h1",
      "h2",
      "h3",
      "h4",
      "h5",
      "h6",
      "ul",
      "ol",
      "li",
      "blockquote",
      "pre",
      "code",
      "hr",
      "br",
      "table",
      "thead",
      "tbody",
      "tr",
      "th",
      "td",
    ]}
    urlTransform={(url) => Option.getOrUndefined(Schema.decodeOption(StartUrl)(url))}
    components={{
      a: ({ href, children }) => (
        <a href={href} target="_blank" rel="noopener noreferrer">
          {children}
        </a>
      ),
    }}
  >
    {formatReply(text)}
  </Streamdown>
);

const activityName = (span: Span) => {
  if (span.name.startsWith("Web search"))
    return span.name.replace(/^Web search(?: · )?/, "") || "Searching the web";
  switch (span.name) {
    case "Page.navigate":
      return "Navigate page";
    case "Runtime.evaluate":
      return "Inspect or interact with page";
    case "Page.captureScreenshot":
      return "Capture page";
    default:
      return span.name;
  }
};

const PrivateEntryForm = ({ entry, busy }: { entry: PrivateEntry; busy: boolean }) => {
  const [submission, submit] = useAtom(submitPrivateEntryAtom);
  const [cancellation, cancel] = useAtom(cancelPrivateEntryAtom);
  const waiting = busy || submission.waiting || cancellation.waiting;
  const failure = errorMessage(submission) ?? errorMessage(cancellation);

  return (
    <section className="handoff-card private-entry" aria-label="Private sign-in details">
      <div className="private-entry-heading">
        <p className="private-entry-target">{entry.target.topOrigin}</p>
        <button
          className="private-entry-skip"
          disabled={waiting}
          type="button"
          aria-label="Skip private entry"
          title="Skip private entry"
          onClick={() => cancel(entry.id)}
        >
          <X size={16} aria-hidden="true" />
        </button>
      </div>
      {entry.target.frameOrigin !== entry.target.topOrigin && (
        <p className="private-entry-target">Form: {entry.target.frameOrigin}</p>
      )}
      {entry.target.recipientOrigin !== entry.target.topOrigin && (
        <p className="private-entry-target">Sends to: {entry.target.recipientOrigin}</p>
      )}
      {entry.status === "expired" ? (
        <p className="handoff-hint">
          Private entry expired. Skip it to continue or request a fresh form.
        </p>
      ) : (
        <form
          autoComplete="off"
          onSubmit={(event) => {
            event.preventDefault();
            const data = new FormData(event.currentTarget);
            const username = data.get("username");
            const password = data.get("password");

            if (typeof username !== "string" || typeof password !== "string") return;
            submit({
              id: entry.id,
              username: Redacted.make(username),
              password: Redacted.make(password),
            });
            event.currentTarget.reset();
          }}
        >
          <label>
            <span className="sr-only">Username or email</span>
            <input
              name="username"
              placeholder="Username or email"
              autoComplete="off"
              required
              maxLength={1024}
              disabled={waiting}
            />
          </label>
          <label>
            <span className="sr-only">Password</span>
            <input
              name="password"
              type="password"
              placeholder="Password"
              autoComplete="off"
              required
              maxLength={1024}
              disabled={waiting}
            />
          </label>
          <button className="ghost" disabled={waiting} type="submit">
            {waiting ? "Filling…" : "Fill sign-in details"}
          </button>
        </form>
      )}
      {failure && (
        <p className="chat-status bad" role="alert">
          {failure}
        </p>
      )}
    </section>
  );
};

const privateEntryMessage = (entry: PrivateEntry) => {
  switch (entry.status) {
    case "filled":
      return "Details filled. Finish signing in on the website.";
    case "uncertain":
      return "Couldn't confirm the fill. Check the website before continuing.";
    case "failed":
      return "Couldn't fill the form. Sign in on the website.";
    case "expired":
      return "Private entry expired.";
    case "cancelled":
    case "pending":
      return null;
  }
};

const HandoffCard = ({ report }: { report: Report }) => {
  const snapshot = useAtomValue(snapshotAtom);
  const turns = useAtomValue(chatTurnsAtom);
  const watching = useAtomValue(watchingAtom);
  const control = useAtomValue(controlAtom);
  const [view, changeView] = useAtom(viewAtom);
  const remote = Option.getOrNull(AsyncResult.value(snapshot));
  const latest = turns.at(-1)?.id === report.input.id;

  const connected =
    AsyncResult.isSuccess(snapshot) &&
    remote?.ready === true &&
    remote.chatId === (report.input.chatId ?? report.input.id);

  const entry = remote?.privateEntry?.runId === report.input.id ? remote.privateEntry : null;

  const pendingEntry =
    latest && connected && (entry?.status === "pending" || entry?.status === "expired");

  const humanControl = connected && remote.viewMode === "control";
  const busy = watching || remote?.busy === true || control.waiting;
  const failure = latest ? errorMessage(view) : null;

  if (report.handoff === undefined) return null;
  if (pendingEntry) return <PrivateEntryForm key={entry.id} entry={entry} busy={busy} />;

  return (
    <section className="handoff-card" aria-label={report.handoff.title}>
      <div className="handoff-heading">
        <h3>{report.handoff.title}</h3>
        {latest && connected && (
          <button
            type="button"
            className="ghost"
            disabled={busy || view.waiting}
            onClick={() => changeView(humanControl ? "watch" : "control")}
          >
            {view.waiting ? "Connecting…" : humanControl ? "Return to agent" : "Take over"}
          </button>
        )}
      </div>
      <p>{(latest && entry && privateEntryMessage(entry)) || report.handoff.instructions}</p>
      {(!latest || !connected || busy) && (
        <p className="handoff-hint">
          {!latest
            ? "See the latest message to continue."
            : !connected
              ? remote?.ready === false
                ? "Browser session ended. Start a new chat."
                : "Checking the browser connection…"
              : "Available when the agent finishes."}
        </p>
      )}
      {failure && (
        <p className="chat-status bad" role="alert">
          {failure}
        </p>
      )}
    </section>
  );
};

const ChatResponse = ({ turn }: { turn: ChatTurn }) => {
  const { report } = turn;
  const active = turn.pending && (report === undefined || report.status === "running");
  const completed = report?.status === "passed" || report?.status === "unverified";
  const output = report?.modelOutput ?? [];
  const actions = report?.spans.filter((span) => span.phase === "action") ?? [];
  const lastText = output.findLast((entry) => entry.kind === "text");

  const timeline = [
    ...output
      .filter(
        (entry) =>
          !(
            completed &&
            entry === lastText &&
            formatReply(entry.text) === formatReply(report.message)
          ),
      )
      .map((entry) => ({ type: "output" as const, at: entry.at, entry })),
    ...actions.slice(-20).map((span) => ({ type: "activity" as const, at: span.start, span })),
  ].sort((a, b) => a.at - b.at);

  return (
    <div className="chat-message from-agent" aria-busy={active}>
      <b>Agent</b>
      {actions.length > 20 && (
        <p className="chat-note">
          Showing the latest 20 actions. Earlier actions are in run details.
        </p>
      )}
      {timeline.map((item) => {
        if (item.type === "activity") {
          const { span } = item;
          const outcome = span.outcome === "running" && !active ? "incomplete" : span.outcome;

          return (
            <div className={`chat-activity ${outcome}`} key={`action-${span.id}`}>
              <Circle size={8} fill="currentColor" aria-hidden="true" />
              <span>
                {span.name.startsWith("Web search") ? "Search" : "Browser"} · {activityName(span)}
              </span>
              <small>{outcome === "success" ? "done" : outcome}</small>
            </div>
          );
        }
        const { entry } = item;

        const streaming =
          active &&
          !entry.complete &&
          report?.spans.some((span) => span.id === entry.spanId && span.outcome === "running");

        return (
          <div className={`chat-output ${entry.kind}`} key={`output-${entry.id}`}>
            {entry.kind === "reasoning-summary" && (
              <span className="chat-label">Returned reasoning summary</span>
            )}
            <Reply text={entry.text} streaming={streaming} />
          </div>
        );
      })}
      {report?.outputTruncated && (
        <p className="chat-note">Live output reached its display limit.</p>
      )}
      {completed && (
        <div className="chat-final">
          {report.handoff === undefined && <span className="chat-label">Final answer</span>}
          <Reply text={report.message} />
          {report.handoff !== undefined && <HandoffCard report={report} />}
        </div>
      )}
      {active && (
        <p className="chat-status" role="status">
          {report?.message ?? "Connecting to the agent…"}
        </p>
      )}
      {turn.pending && turn.connectionLost && (
        <p className="chat-status" role="status">
          Run response interrupted. Checking run status… Any text above may be partial. You can stop
          this wait; the message will not be sent again automatically.
        </p>
      )}
      {turn.error ? (
        <p className="chat-status bad" role="status">
          {report === undefined && !turn.connectionLost
            ? "Message not sent. "
            : "Response interrupted. Any text above may be partial. "}
          {turn.error}
        </p>
      ) : report?.status === "cancelled" ? (
        <p className="chat-status" role="status">
          Stopped. Any text above is partial; no actions will be replayed.
        </p>
      ) : report?.status === "failed" ? (
        <p className="chat-status bad" role="status">
          Run failed. Any text above is partial. See run details for the diagnostic.
        </p>
      ) : null}
    </div>
  );
};

const hostOf = (url: string) => {
  try {
    return new URL(url).host || url;
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

const modes: ReadonlyArray<{ id: Scenario; title: string; detail: string }> = [
  {
    id: "chat",
    title: "Chat with the agent",
    detail: "Any website. Your instructions. Keep the conversation going.",
  },
  { id: "wikipedia", title: "Wikipedia race", detail: "Find a route between two articles" },
  coffeeDemo,
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

const RunSteps = ({ report }: { report: Report }) => {
  const [expanded, setExpanded] = useState<number | null>(null);

  const extent = Math.max(
    1,
    report?.elapsed ?? 1,
    ...(report?.spans.map((span) => span.start + (span.duration ?? 0)) ?? []),
  );

  const ready = report?.readyAt ?? null;

  return (
    <div className="turn-steps">
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
    </div>
  );
};

const Steps = ({ reports }: { reports: ReadonlyArray<Report> }) => (
  <section className="section" aria-labelledby="steps-title">
    <div className="section-head">
      <h2 id="steps-title">Every step</h2>
      <p>
        All messages in this conversation, including browser preparation. Each timeline starts when
        its message was sent; the marker shows when the page was ready.
      </p>
    </div>
    {reports.length === 0 && <div className="empty">Send a message to see its trace here.</div>}
    {reports.map((report, index) => (
      <div key={report.input.id} className="conversation-trace">
        <h3>
          Message {index + 1}{" "}
          <small className="mono">
            {seconds(report.elapsed)} s · {runOutcome(report)}
          </small>
        </h3>
        <p className="trace-prompt">{report.input.prompt}</p>
        <RunSteps report={report} />
      </div>
    ))}
  </section>
);

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
  const [scenario, setScenario] = useState<Scenario>("chat");
  const [wikiStart, setWikiStart] = useState(defaultChallenge.start);
  const [wikiTarget, setWikiTarget] = useState(defaultChallenge.target);
  const [url, setUrl] = useState("");
  const [draftMessage, setDraftMessage] = useAtom(chatDraftAtom);
  const chatId = useAtomValue(chatIdAtom);
  const chatTurns = useAtomValue(chatTurnsAtom);
  const transcript = useRef<HTMLDivElement>(null);
  const followOutput = useRef(true);
  const [showLatest, setShowLatest] = useState(false);

  useEffect(() => {
    if (followOutput.current)
      transcript.current?.scrollTo({ top: transcript.current.scrollHeight });
  }, [chatTurns]);
  const [chatResult, sendChat] = useAtom(sendChatAtom);
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
  const [viewResult, changeView] = useAtom(viewAtom);
  const [traceResult, downloadTrace] = useAtom(downloadTraceAtom);
  const snapshot = useAtomValue(snapshotAtom);
  const history = useAtomValue(historyAtom);
  const watching = useAtomValue(watchingAtom);
  const progress = useAtomValue(progressAtom);
  const [selected, select] = useAtom(selectedAtom);
  const download = useAtomSet(exportAtom);
  const remote = Option.getOrNull(AsyncResult.value(snapshot));
  const selectedSample = history.find(({ report }) => report.input.id === selected);
  const latestReport = remote?.report ?? history.at(-1)?.report ?? null;

  const report =
    selectedSample?.report ??
    (scenario === "chat" && latestReport?.input.id !== chatTurns.at(-1)?.id ? null : latestReport);

  const displayedScenario = report?.input.scenario ?? scenario;
  const busy = watching || remote?.busy === true;
  const connected = remote !== null;
  const publicLab = remote?.public ?? false;
  const accountResult = useAtomValue(accountAtom);
  const account = AsyncResult.isSuccess(accountResult) ? accountResult.value : null;
  const funded = account?.funded === true;

  const failure =
    (chatTurns.length ? errorMessage(chatResult) : null) ??
    errorMessage(run) ??
    errorMessage(control) ??
    errorMessage(snapshot);

  const chatControlFailure =
    errorMessage(control) ?? errorMessage(viewResult) ?? errorMessage(snapshot);

  const isWiki = scenario === "wikipedia";
  const isChat = scenario === "chat";
  const isStore = scenario === "coffee";
  const chatEnded = chatTurns.length > 0 && chatId === null && !busy;
  const humanControl = remote?.viewMode === "control";

  const privateEntryPending =
    remote?.privateEntry?.status === "pending" || remote?.privateEntry?.status === "expired";

  const conversationId = report?.input.chatId ?? report?.input.id ?? chatId;

  const reportsById = new Map(
    history
      .filter(
        ({ report: value }) =>
          isChat &&
          !selected &&
          value.input.scenario === "chat" &&
          (value.input.chatId ?? value.input.id) === conversationId,
      )
      .map(({ report: value }) => [value.input.id, value]),
  );

  if (report) reportsById.set(report.input.id, report);
  const inspectedReports = [...reportsById.values()];
  const conversation = isChat && !selected;
  const totalElapsed = inspectedReports.reduce((sum, value) => sum + value.elapsed, 0);
  const preparationElapsed = inspectedReports.reduce((sum, value) => sum + (value.readyAt ?? 0), 0);

  const effectiveReasoning = reasoning === "none" && requiresReasoning(model) ? "low" : reasoning;

  const effectiveServiceTier =
    model === "all" ? serviceTier : serviceTierForModel(model, serviceTier);

  const effectivePrompt = isWiki
    ? racePrompt({ start: wikiStart.trim(), target: wikiTarget.trim() })
    : isChat
      ? draftMessage
      : coffeeDemo.prompt;

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

  const missingNeeds = needs.filter((need) => !need.ok);

  const canRun =
    !privateEntryPending &&
    !humanControl &&
    !viewResult.waiting &&
    connected &&
    remote.browserConfigured &&
    !busy &&
    !control.waiting &&
    (!isWiki || validChallenge) &&
    (!isChat ||
      (!chatEnded && (!url.trim() || chatId !== null || Schema.is(StartUrl)(url.trim())))) &&
    effectivePrompt.trim().length > 0 &&
    effectivePrompt.length <= 8000 &&
    missingNeeds.length === 0;

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
  const captured = !selected && report !== null && remote?.image;
  const lastHop = report?.race?.path.at(-1);

  const flow = preparing
    ? null
    : report
      ? (verifiedMillis(report) ?? fromReady(report, report.finishedAt ?? report.elapsed))
      : null;

  const count = (phase: Phase) =>
    inspectedReports.reduce(
      (sum, value) => sum + value.spans.filter((span) => span.phase === phase).length,
      0,
    );

  const conversationFlow = conversation
    ? inspectedReports.reduce(
        (sum, value) => sum + (fromReady(value, value.finishedAt ?? value.elapsed) ?? 0),
        0,
      )
    : flow;

  const openKeys = () => {
    setDraft(keys);
    keysDialog.current?.showModal();
  };

  const startRun = () =>
    start({
      scenario,
      prompt: effectivePrompt,
      ...(isWiki ? { wikipedia: { start: wikiStart.trim(), target: wikiTarget.trim() } } : {}),
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

  const submitChat = () =>
    sendChat({
      ...(chatId === null && url.trim() ? { url: url.trim() } : {}),
      screenshots,
      liveView,
      engine: "chromium",
      model: model === "all" ? "gpt-6-luna" : model,
      ...(!model.startsWith("@cf/")
        ? { reasoning: effectiveReasoning, serviceTier: effectiveServiceTier }
        : {}),
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

      <h1 className="sr-only">Browser use</h1>

      <main className="stage">
        <section className="controls" aria-label="Run setup">
          <div className="setup-row">
            <fieldset className="group">
              <legend className="sr-only">Demo</legend>
              <div className="mode-list">
                {modes.map((mode) => (
                  <label
                    key={mode.id}
                    title={mode.detail}
                    className={`mode ${scenario === mode.id ? "selected" : ""}`}
                  >
                    <input
                      className="sr-only"
                      type="radio"
                      name="mode"
                      value={mode.id}
                      checked={scenario === mode.id}
                      disabled={busy}
                      onChange={() => {
                        setScenario(mode.id);
                        if (mode.id === "chat") {
                          setEngine("chromium");
                          if (model === "all") setModel("gpt-6-luna");
                        }
                      }}
                    />
                    <span className="mode-title">{mode.title}</span>
                  </label>
                ))}
              </div>
            </fieldset>
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
            ) : isChat ? (
              <div className="browse-inputs">
                <label>
                  <span className="sr-only">
                    Starting URL <small>optional</small>
                  </span>
                  <input
                    type="url"
                    value={url}
                    disabled={busy || chatTurns.length > 0}
                    maxLength={2000}
                    placeholder="Starting URL (optional)"
                    onChange={(event) => setUrl(event.target.value)}
                  />
                </label>
              </div>
            ) : null}

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
              ) : !isChat ? (
                <button className="primary" disabled={!canRun} onClick={startRun}>
                  <Play size={15} fill="currentColor" /> {isWiki ? "Start race" : "Start demo"}
                </button>
              ) : null}
              <button
                className="ghost"
                disabled={
                  watching ||
                  (remote?.busy && !remote.canClose) ||
                  (!remote?.canClose && !chatTurns.length) ||
                  control.waiting
                }
                onClick={() => act("close")}
              >
                {isChat ? "New chat" : "Close browser"}
              </button>
            </div>
          </div>
          <div className="setup-meta">
            <p className="fine">
              {isWiki
                ? "The host verifies arrival and records page navigations. Link-only racing rules are not enforced."
                : isStore
                  ? "The agent is instructed to stop before payment. There is no hard payment guard; real purchases are possible."
                  : "Real browser actions. Agent-reported results. Follow up in the same browser."}
            </p>

            {missingNeeds.length > 0 && (
              <div className="needs">
                <span>Needs</span>
                {missingNeeds.map((need) => (
                  <span key={need.label} className="missing">
                    <X size={13} /> {need.label}
                  </span>
                ))}
                {needsAccount && !funded ? (
                  account === null ? (
                    <a className="link-button" href={signInUrl}>
                      Sign in
                    </a>
                  ) : (
                    <small className="needs-note">this account isn’t allowlisted</small>
                  )
                ) : (
                  <button className="link-button" onClick={openKeys}>
                    Add keys
                  </button>
                )}
              </div>
            )}

            <p className="fine budget-note">
              120 model turns · 300 tool calls · 10 minutes per {isChat ? "message" : "run"}
            </p>
          </div>
          {isStore && <p className="prompt-preview">{effectivePrompt}</p>}

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
                  <option value="kitesurf" disabled={isChat}>
                    Kitesurf · experimental
                  </option>
                  <option value="all" disabled={isChat}>
                    Compare both
                  </option>
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
                    <option value="all" disabled={isChat || availableModels.length < 2}>
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
                  value={isChat ? 1 : repetitions}
                  disabled={busy || isChat}
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
                Kitesurf is Cloudflare’s experimental lightweight engine. It has no live view, and
                large pages such as Wikipedia can exhaust its memory. Comparing browsers disables
                live view for both.
              </p>
            )}
          </details>
        </section>

        <div className="hud">
          <div className="clock">
            <span
              className={`clock-value mono ${showingActive && !preparing ? "ticking" : flow === null ? "idle" : ""}`}
            >
              {conversationFlow === null ? "0.00" : seconds(conversationFlow)}
              <small>s</small>
            </span>
            <span className="clock-label">
              {conversation
                ? "Conversation work time · excludes waiting for you"
                : showingActive
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

        <div className={`workspace ${isChat ? "with-chat" : ""}`}>
          {isChat && (
            <section className="chat-panel card" aria-label="Chat">
              <div className="chat-heading">
                <MessageSquare size={17} /> <h2>Chat</h2>
                <span className="mono">{chatId ? "Browser connected" : "Open-ended browsing"}</span>
              </div>
              <div
                className="chat-transcript"
                ref={transcript}
                role="log"
                aria-label="Conversation"
                aria-live="polite"
                onScroll={(event) => {
                  const element = event.currentTarget;

                  followOutput.current =
                    element.scrollHeight - element.scrollTop - element.clientHeight < 48;
                  setShowLatest(!followOutput.current);
                }}
              >
                {chatTurns.length === 0 ? (
                  <div className="chat-empty">
                    <p>What would you like to explore?</p>
                    <span>
                      Ask it to research, compare, or work through a website. You can refine the
                      goal as you go.
                    </span>
                  </div>
                ) : (
                  chatTurns.map((turn) => (
                    <div className="chat-turn" key={turn.id}>
                      <div className="chat-message from-user">
                        <b>You</b>
                        <p>{turn.prompt}</p>
                      </div>
                      <ChatResponse turn={turn} />
                    </div>
                  ))
                )}
              </div>
              {showLatest && (
                <button
                  className="ghost small chat-latest"
                  onClick={() => {
                    followOutput.current = true;
                    setShowLatest(false);
                    transcript.current?.scrollTo({ top: transcript.current.scrollHeight });
                  }}
                >
                  Jump to latest <ChevronDown size={14} />
                </button>
              )}
              <form
                className="chat-composer"
                onSubmit={(event) => {
                  event.preventDefault();
                  if (canRun) submitChat();
                }}
              >
                <label className="sr-only" htmlFor="chat-message">
                  Message the agent
                </label>
                <textarea
                  id="chat-message"
                  value={draftMessage}
                  placeholder={
                    chatEnded ? "Start a new chat to continue" : "Ask the agent to do something…"
                  }
                  maxLength={8000}
                  rows={3}
                  onChange={(event) => setDraftMessage(event.target.value)}
                  onKeyDown={(event) => {
                    if (
                      event.key === "Enter" &&
                      !event.shiftKey &&
                      !event.nativeEvent.isComposing
                    ) {
                      event.preventDefault();
                      if (canRun) event.currentTarget.form?.requestSubmit();
                    }
                  }}
                />
                <div className="composer-actions">
                  <span>
                    {privateEntryPending
                      ? "Complete or skip private entry to continue."
                      : humanControl
                        ? "You control the browser. Return to the agent when finished."
                        : chatEnded
                          ? "This chat ended. Choose New chat to start again."
                          : busy
                            ? "Working in the browser. You can draft your next message."
                            : "Enter to send · Shift+Enter for a new line"}
                  </span>
                  <button className="primary" type="submit" disabled={!canRun}>
                    <Send size={15} /> Send
                  </button>
                </div>
              </form>
            </section>
          )}
          <section className="viewer" aria-label="Browser">
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
                    : displayedScenario === "chat"
                      ? hostOf(report?.finalUrl ?? report?.input.url ?? (url || "about:blank"))
                      : "www.hedge.coffee"}
                </span>
                <span className={`view-label mono ${live ? "live" : ""}`}>
                  {live
                    ? humanControl
                      ? "YOU HAVE CONTROL"
                      : "LIVE"
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
                    title={
                      humanControl ? "Control the browser" : "Read-only view of the agent’s browser"
                    }
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
                              : "Send a message in chat to open a website and start exploring."}
                    </p>
                  </div>
                )}
              </div>
            </div>
            {isChat && !selected && remote?.ready && (
              <div className="viewer-controls">
                <button
                  className="ghost small"
                  disabled={busy || viewResult.waiting}
                  title={busy ? "Available when the agent finishes this message" : undefined}
                  onClick={() => changeView(humanControl ? "watch" : "control")}
                >
                  <Hand size={14} />{" "}
                  {viewResult.waiting
                    ? "Connecting…"
                    : humanControl
                      ? "Return to agent"
                      : "Take over"}
                </button>
                {!humanControl && (
                  <button
                    className="ghost small"
                    disabled={busy || viewResult.waiting}
                    onClick={() => changeView("watch")}
                  >
                    Reconnect live view
                  </button>
                )}
              </div>
            )}
            {humanControl && (
              <p className="fine">
                Use the browser above, then return control and tell the agent what changed.
              </p>
            )}
            {live && !humanControl && (
              <a className="fine-link" href={live} target="_blank" rel="noreferrer">
                Open live view in a new tab <ArrowUpRight size={13} />
              </a>
            )}
          </section>
        </div>
      </main>

      {report?.race && (
        <div className="route-summary">
          <p className="fine">
            {report.race.start} → {report.race.target} · {Math.max(0, report.race.path.length - 1)}{" "}
            navigations
            {report.status === "passed" ? " · arrival verified" : " · arrival not verified"}.
            Link-only rules are not enforced.
          </p>
          {report.race.ruleViolation && <p className="bad">{report.race.ruleViolation}</p>}
          <ol className="route" aria-label="Route">
            {report.race.path.map((hop, index) => (
              <li key={`${index}-${hop.url}`} title={hop.via ? `Via ${hop.via.label}` : undefined}>
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

      {(!isChat || remote?.notice || chatControlFailure) &&
        (failure || remote?.notice || busy || report) && (
          <div
            className={`message ${failure || report?.status === "failed" ? "bad" : ""}`}
            role="status"
          >
            {(isChat ? (chatControlFailure ?? remote?.notice) : failure) ??
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

      <details className="run-inspector">
        <summary>
          {conversation ? "Conversation details and traces" : "Run details and comparisons"}
        </summary>
        {report && (
          <section className="section" aria-label="Run details">
            {!conversation && (
              <p className={report.status === "failed" ? "message bad" : "message"}>
                {report.message}
              </p>
            )}
            <div className="trace-actions">
              <button
                className="ghost small"
                disabled={busy || traceResult.waiting}
                onClick={() => downloadTrace(inspectedReports.map((value) => value.input.id))}
              >
                <Download size={14} />{" "}
                {traceResult.waiting
                  ? "Downloading…"
                  : conversation
                    ? "Download conversation trace"
                    : "Download debug trace"}
              </button>
              {errorMessage(traceResult) && <p className="bad">{errorMessage(traceResult)}</p>}
              <span className="fine">
                Saved automatically. Includes page content and model messages; review before
                sharing.
              </span>
            </div>
            <div className="stats">
              <div>
                <span>{conversation ? "Total active time" : "Total elapsed"}</span>
                <b className="mono">{seconds(totalElapsed)} s</b>
              </div>
              <div>
                <span>Page preparation</span>
                <b className="mono">{seconds(preparationElapsed)} s</b>
              </div>
              <div>
                <span>{conversation ? "Messages" : "First action after ready"}</span>
                <b className="mono">
                  {conversation
                    ? inspectedReports.length
                    : `${seconds(fromReady(report, report.firstActionAt))} s`}
                </b>
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
          </section>
        )}
        <Steps reports={inspectedReports} />

        <section className="section" aria-labelledby="compare-title">
          <div className="section-head">
            <h2 id="compare-title">Compare runs</h2>
            <p>
              Runs in this tab, grouped by goal and settings. Medians use verified runs only;
              failures count toward the started total. Wikipedia verification confirms arrival only.
              Website outcomes are agent-reported; their history is retained without verified-time
              rankings.
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
                      <th>Goal</th>
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
                      <tr
                        key={row.input.id}
                        className={selected === row.input.id ? "selected" : ""}
                      >
                        <td title={row.input.prompt}>
                          {row.input.scenario === "wikipedia"
                            ? `${row.race?.start ?? row.input.wikipedia?.start ?? defaultChallenge.start} → ${row.race?.target ?? row.input.wikipedia?.target ?? defaultChallenge.target}${row.race ? ` · ${Math.max(0, row.race.path.length - 1)} hops` : ""}`
                            : row.input.scenario === "chat"
                              ? row.input.prompt
                              : coffeeDemo.title}
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
                            aria-label={
                              selected === row.input.id ? "Show latest run" : "Inspect run"
                            }
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
      </details>

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
