import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Schema } from "effect";
import { AsyncResult } from "effect/reactivity";
import { ArrowUpRight, Download, Globe, Play, Square, X } from "lucide-react";
import { useState } from "react";

import {
  BrowserEngine,
  ModelId,
  Reasoning,
  Scenario,
  ServiceTier,
  StartUrl,
  modelChoices,
  scenarios,
} from "./contract.ts";
import {
  accountAtom,
  controlAtom,
  errorMessage,
  exportAtom,
  fromReady,
  historyAtom,
  keysAtom,
  runAtom,
  runOutcome,
  saveKeysAtom,
  selectedAtom,
  signInUrl,
  snapshotAtom,
  validKey,
  watchingAtom,
} from "./state.ts";

const seconds = (value: number | null) => (value === null ? "—" : `${(value / 1000).toFixed(1)}s`);

export function App() {
  const snapshotResult = useAtomValue(snapshotAtom);
  const snapshot = AsyncResult.isSuccess(snapshotResult) ? snapshotResult.value : undefined;
  const accountResult = useAtomValue(accountAtom);
  const account = AsyncResult.isSuccess(accountResult) ? accountResult.value : null;
  const watching = useAtomValue(watchingAtom);
  const history = useAtomValue(historyAtom);
  const [selected, select] = useAtom(selectedAtom);
  const [runResult, run] = useAtom(runAtom);
  const [controlResult, control] = useAtom(controlAtom);
  const exportRuns = useAtomSet(exportAtom);
  const keys = useAtomValue(keysAtom);
  const saveKeys = useAtomSet(saveKeysAtom);
  const [scenario, setScenario] = useState<typeof Scenario.Type>("browse");
  const [url, setUrl] = useState("https://example.com");
  const [goal, setGoal] = useState("Read the page and tell me what it is for.");
  const [engine, setEngine] = useState<typeof BrowserEngine.Type>("chromium");
  const [model, setModel] = useState<typeof ModelId.Type>("gpt-6-luna");
  const [reasoning, setReasoning] = useState<typeof Reasoning.Type>("none");
  const [tier, setTier] = useState<typeof ServiceTier.Type>("fast");
  const [screenshots, setScreenshots] = useState(true);
  const [liveView, setLiveView] = useState(true);
  const busy = watching || snapshot?.busy === true;

  const report = selected
    ? history.find((entry) => entry.report.input.id === selected)?.report
    : snapshot?.report;

  const openai = !model.startsWith("@cf/");

  const configured =
    snapshot?.models.find((candidate) => candidate.id === model)?.configured ||
    (openai && (validKey(keys.openai) || account?.funded));

  const canRun =
    !busy &&
    snapshot?.browserConfigured &&
    configured &&
    (scenario !== "browse" || (Schema.is(StartUrl)(url) && goal.trim().length > 0));

  const error =
    errorMessage(runResult) ?? errorMessage(controlResult) ?? errorMessage(snapshotResult);

  return (
    <div className="shell">
      <header className="masthead">
        <a className="brand" href="/browser-use/">
          ✳ yielded <span>/ browser lab</span>
        </a>
        <nav>
          <a href="https://yielded.dev/agent/">
            Documentation <ArrowUpRight size={14} />
          </a>
          {account ? (
            <span>
              {account.displayName}
              {account.funded ? " · funded" : ""}
            </span>
          ) : (
            <a href={signInUrl}>Sign in</a>
          )}
        </nav>
      </header>
      <main>
        <div className="intro">
          <p className="eyebrow">A URL. A goal. A real browser.</p>
          <h1>Put the browser to work.</h1>
          <p>Watch an agent inspect pages, write code, and take action across the web.</p>
        </div>
        <div className="workspace">
          <form
            className="controls"
            onSubmit={(event) => {
              event.preventDefault();
              if (canRun)
                run({
                  scenario,
                  ...(scenario === "browse" ? { url } : {}),
                  prompt: goal,
                  engine,
                  model,
                  ...(openai ? { reasoning, serviceTier: tier } : {}),
                  screenshots,
                  liveView,
                });
            }}
          >
            <label>
              Task
              <select
                value={scenario}
                disabled={busy}
                onChange={(event) =>
                  setScenario(Schema.decodeUnknownSync(Scenario)(event.target.value))
                }
              >
                <option value="browse">Browse a website</option>
                {scenarios.map((preset) => (
                  <option key={preset.id} value={preset.id}>
                    {preset.title} · task board
                  </option>
                ))}
              </select>
            </label>
            {scenario === "browse" ? (
              <>
                <label>
                  Starting URL
                  <input
                    type="url"
                    required
                    maxLength={2000}
                    value={url}
                    disabled={busy}
                    onChange={(event) => setUrl(event.target.value)}
                  />
                </label>
                <label>
                  Your goal
                  <textarea
                    required
                    maxLength={2000}
                    value={goal}
                    disabled={busy}
                    onChange={(event) => setGoal(event.target.value)}
                    rows={5}
                  />
                </label>
              </>
            ) : (
              <p className="preset">{scenarios.find((preset) => preset.id === scenario)?.prompt}</p>
            )}
            <label>
              Model
              <select
                value={model}
                disabled={busy}
                onChange={(event) =>
                  setModel(Schema.decodeUnknownSync(ModelId)(event.target.value))
                }
              >
                {modelChoices.map((choice) => (
                  <option key={choice.id} value={choice.id}>
                    {choice.label}
                  </option>
                ))}
              </select>
            </label>
            <details>
              <summary>Run settings</summary>
              <label>
                Browser
                <select
                  value={engine}
                  disabled={busy}
                  onChange={(event) =>
                    setEngine(Schema.decodeUnknownSync(BrowserEngine)(event.target.value))
                  }
                >
                  <option value="chromium">Chromium</option>
                  <option value="kitesurf">Kitesurf</option>
                </select>
              </label>
              {openai && (
                <div className="two-columns">
                  <label>
                    Reasoning
                    <select
                      value={reasoning}
                      disabled={busy}
                      onChange={(event) =>
                        setReasoning(Schema.decodeUnknownSync(Reasoning)(event.target.value))
                      }
                    >
                      {["none", "low", "medium", "high", "xhigh", "max"].map((value) => (
                        <option key={value}>{value}</option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Processing
                    <select
                      value={tier}
                      disabled={busy}
                      onChange={(event) =>
                        setTier(Schema.decodeUnknownSync(ServiceTier)(event.target.value))
                      }
                    >
                      <option value="fast">Fast</option>
                      <option value="default">Standard</option>
                    </select>
                  </label>
                </div>
              )}
              <label className="check">
                <input
                  type="checkbox"
                  checked={screenshots}
                  disabled={busy}
                  onChange={(event) => setScreenshots(event.target.checked)}
                />{" "}
                Capture after each code call
              </label>
              <label className="check">
                <input
                  type="checkbox"
                  checked={liveView}
                  disabled={busy}
                  onChange={(event) => setLiveView(event.target.checked)}
                />{" "}
                Read-only live view
              </label>
            </details>
            <details open={snapshot?.public && !account?.funded && !validKey(keys.openai)}>
              <summary>Your OpenAI key</summary>
              <label>
                API key
                <input
                  type="password"
                  autoComplete="off"
                  placeholder="sk-…"
                  value={keys.openai}
                  onChange={(event) => saveKeys({ ...keys, openai: event.target.value })}
                />
              </label>
              <label className="check">
                <input
                  type="checkbox"
                  checked={keys.remember}
                  onChange={(event) => saveKeys({ ...keys, remember: event.target.checked })}
                />{" "}
                Remember on this device
              </label>
              <p className="fine">Sent to the Worker for this run. Excluded from reports.</p>
            </details>
            <p className="fine">
              The agent can run arbitrary page JavaScript and perform real actions, including
              purchases. There is no payment or website guard. Use goals and sites you are
              authorized to operate.
            </p>
            {!snapshot?.browserConfigured && snapshot && (
              <p className="notice">The Worker needs browser credentials before it can run.</p>
            )}
            {!configured && snapshot?.browserConfigured && (
              <p className="notice">Configure this model or add your OpenAI key to run.</p>
            )}
            <div className="buttons">
              <button className="primary" type="submit" disabled={!canRun}>
                <Play size={15} />
                {busy ? "Running…" : "Start browsing"}
              </button>
              {busy && (
                <button type="button" onClick={() => control("stop")}>
                  <Square size={14} />
                  Stop run
                </button>
              )}
              {snapshot?.canClose && (
                <button type="button" onClick={() => control("close")}>
                  <X size={14} />
                  Close browser
                </button>
              )}
            </div>
          </form>
          <section className="results" aria-label="Browser and results">
            <div className="browser-bar">
              <span>
                <Globe size={15} />
                {report?.finalUrl ?? (busy ? "Browser session" : "Browser preview")}
              </span>
              <span className="badge">{report ? runOutcome(report) : "Ready"}</span>
            </div>
            <div className="preview">
              {!selected && snapshot?.liveViewUrl ? (
                <iframe
                  title="Read-only browser live view"
                  src={snapshot.liveViewUrl}
                  referrerPolicy="no-referrer"
                />
              ) : !selected && snapshot?.image ? (
                <img src={snapshot.image} alt="Latest browser screenshot" />
              ) : (
                <div className="empty">
                  <Globe size={40} />
                  <h2>{selected ? "Saved run" : "Your next session starts here."}</h2>
                  <p>
                    {selected
                      ? "Screenshots are transient. The report and trace are below."
                      : "Choose a website and tell the agent what you want done."}
                  </p>
                </div>
              )}
            </div>
            {error && (
              <p className="notice error" role="alert">
                {error}
              </p>
            )}
            {snapshot?.notice && <p className="notice">{snapshot.notice}</p>}
            {report && (
              <div className="report" aria-live="polite">
                <p>{report.message}</p>
                <div className="metrics">
                  <span>
                    Elapsed <strong>{seconds(report.elapsed)}</strong>
                  </span>
                  <span>
                    Work time{" "}
                    <strong>
                      {seconds(fromReady(report, report.finishedAt ?? report.elapsed))}
                    </strong>
                  </span>
                  <span>
                    Verified <strong>{seconds(fromReady(report, report.verifiedAt))}</strong>
                  </span>
                  <span>
                    Cleanup <strong>{report.cleanup}</strong>
                  </span>
                </div>
                <details open>
                  <summary>Execution trace · {report.spans.length} operations</summary>
                  <div className="trace">
                    {report.spans.map((span) => (
                      <div className="trace-row" key={span.id}>
                        <span className="phase">{span.phase}</span>
                        <span>
                          {span.name}
                          {span.error && <small>{span.error}</small>}
                        </span>
                        <span>{span.outcome === "running" ? "…" : seconds(span.duration)}</span>
                      </div>
                    ))}
                  </div>
                </details>
                {report.board.length > 0 && (
                  <details>
                    <summary>Saved task board</summary>
                    <pre>{JSON.stringify(report.board, null, 2)}</pre>
                    <p className="fine">
                      Compared with the preset by the host. Page JavaScript can modify this fixture;
                      this is not tamper-resistant proof.
                    </p>
                  </details>
                )}
              </div>
            )}
          </section>
        </div>
        {history.length > 0 && (
          <section className="history">
            <div className="history-heading">
              <h2>This session</h2>
              <button onClick={() => exportRuns()}>
                <Download size={14} />
                Export JSON
              </button>
            </div>
            <div className="history-list">
              <button className={!selected ? "active" : ""} onClick={() => select(null)}>
                Latest run
              </button>
              {history.toReversed().map(({ report: entry }) => (
                <button
                  className={selected === entry.input.id ? "active" : ""}
                  key={entry.input.id}
                  onClick={() => select(entry.input.id)}
                >
                  <span>
                    {entry.input.scenario === "browse" ? entry.input.url : entry.input.scenario}
                  </span>
                  <small>
                    {runOutcome(entry)} · {seconds(entry.elapsed)}
                  </small>
                </button>
              ))}
            </div>
          </section>
        )}
      </main>
      <footer>
        Code Mode + CDP · Cloudflare Browser Run{" "}
        <span>Website outcomes are agent reports, not independent verification.</span>
      </footer>
    </div>
  );
}
