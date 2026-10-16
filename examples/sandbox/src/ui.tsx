import {
  RegistryContext,
  useAtom,
  useAtomMount,
  useAtomSet,
  useAtomValue,
} from "@effect/atom-react";
import { accountLifetime, auth, sessionObservation } from "@yielded/agent-example-auth/client";
import {
  Button,
  Conversation,
  ConversationContent,
  ConversationScrollButton,
  Message,
  MessageResponse,
  Reasoning,
  Tool,
} from "@yielded/ui";
import { AsyncResult } from "effect/reactivity";
import { Check, LoaderCircle } from "lucide-react";
import { useMemo } from "react";

import { type Entry, type ShellCommand } from "./contract";
import { signIn } from "./location";
import { newWorkspace, workspace, workspaceId } from "./state";

const workspaceDescription =
  "Workspace files expire after 30 minutes idle. Conversation history is kept.";

const settingControl =
  "mt-1 w-full min-w-0 rounded-md border border-border bg-background px-2 py-1.5 text-sm text-foreground";

const Brand = () => (
  <a className="brand" href="/">
    ✳ yielded <span>/ agent</span>
  </a>
);

export function App() {
  useAtomMount(sessionObservation);
  const session = useAtomValue(auth.session);
  const account = useAtomValue(accountLifetime);
  const signOut = useAtomSet(auth.signOut);
  const login = useAtomSet(signIn);

  if (
    !AsyncResult.isSuccess(session) ||
    !session.value ||
    !session.value.scopes.some((scope) => scope === "agent:sandbox") ||
    !AsyncResult.isSuccess(account) ||
    account.value.subject !== session.value.subjectId
  )
    return (
      <main className="gate">
        <Brand />
        <h1>A workspace for your next idea.</h1>
        <p>Give the agent a coding task. Follow its commands and inspect the files.</p>
        {AsyncResult.isFailure(session) ? (
          <p role="alert">
            Could not check your session.{" "}
            <Button variant="ghost" onClick={() => login()}>
              Sign in again
            </Button>
          </p>
        ) : AsyncResult.isSuccess(session) &&
          (!session.value || !session.value.scopes.some((scope) => scope === "agent:sandbox")) ? (
          <Button onClick={() => login()}>
            {session.value ? "Sign in again to open Sandbox →" : "Sign in to start →"}
          </Button>
        ) : (
          <p role="status">Checking your session…</p>
        )}
        <p className="muted">{workspaceDescription}</p>
      </main>
    );

  return (
    <RegistryContext.Provider value={account.value.registry}>
      <Account
        key={account.value.generation}
        subject={session.value.subjectId}
        signOut={() => signOut()}
      />
    </RegistryContext.Provider>
  );
}

function Account({ subject, signOut }: { subject: string; signOut: () => void }) {
  const id = useAtomValue(workspaceId(subject));
  const create = useAtomSet(newWorkspace);

  if (id === null)
    return (
      <main className="gate">
        <Brand />
        <h1>This workspace link is invalid.</h1>
        <p>Open a saved workspace link or start a new one.</p>
        <Button onClick={() => create(subject)}>New workspace</Button>
      </main>
    );

  return <AccountWorkspace key={id} id={id} subject={subject} signOut={signOut} />;
}

function AccountWorkspace({
  id,
  subject,
  signOut,
}: {
  id: string;
  subject: string;
  signOut: () => void;
}) {
  // Atom.family caches bundles weakly; retain this workspace while it is mounted.
  const atoms = useMemo(() => workspace(id), [id]);
  const state = useAtomValue(atoms.state);
  const create = useAtomSet(newWorkspace);

  return (
    <div className="app">
      <header>
        <Brand />
        <Button variant="outline" onClick={signOut}>
          Sign out
        </Button>
      </header>
      <div className="heading">
        <div>
          <small>SANDBOX / CODING WORKSPACE</small>
          <h1>Make something work.</h1>
        </div>
        <Button
          variant="outline"
          disabled={
            state.active !== null || state.pending !== null || state.admissionRequestId !== null
          }
          onClick={() => create(subject)}
        >
          ＋ New workspace
        </Button>
      </div>
      <p className="muted">{workspaceDescription}</p>
      <Workspace key={id} id={id} />
    </div>
  );
}

function Workspace({ id }: { id: string }) {
  const atoms = workspace(id);

  useAtomMount(atoms.events);
  const commandEvents = useAtomValue(atoms.commandEvents);
  const state = useAtomValue(atoms.state);
  const [draft, setDraft] = useAtom(atoms.draft);
  const [sent, send] = useAtom(atoms.send);
  const [stopped, stop] = useAtom(atoms.stop);
  const reconnectCommand = useAtomSet(atoms.reconnectCommand);
  const expired = useAtomValue(atoms.expired);
  const settings = useAtomValue(atoms.settings);
  const changeSettings = useAtomSet(atoms.changeSettings);

  const { entries, results } = useMemo(() => {
    const calls = new Set(state.entries.filter((entry) => entry.kind === "tool").map(toolKey));

    const results = new Map(
      state.entries
        .filter((entry) => entry.kind === "result")
        .map((entry) => [toolKey(entry), entry]),
    );

    return {
      entries: state.entries.filter(
        (entry) => entry.kind !== "result" || !calls.has(toolKey(entry)),
      ),
      results,
    };
  }, [state.entries]);

  const timeline = useMemo(
    () =>
      [
        ...entries.map((entry) => ({
          position: entry.sequence ?? Number.parseInt(entry.id, 10),
          entry,
          command: undefined,
        })),
        ...state.commands.map((command) => ({
          position: command.afterSequence + 0.5,
          entry: undefined,
          command,
        })),
      ].sort((a, b) => a.position - b.position),
    [entries, state.commands],
  );

  const completed = state.entries.findLast(
    (entry) => entry.kind === "status" && Object.hasOwn(completionLabels, entry.text),
  );

  const busy =
    expired ||
    state.active !== null ||
    state.pending !== null ||
    state.admissionRequestId !== null ||
    state.uncertain;

  return (
    <main className="workbench">
      <section className="conversation" aria-label="Coding conversation">
        <div className="bar">
          <h2>Agent</h2>
          <small role="status">
            {state.connection === "Live" ? "● " : "○ "}
            {state.connection}
          </small>
        </div>
        <p className="sr-only" role="status" aria-live="polite" aria-atomic="true">
          {completed && <span key={completed.id}>{completionLabels[completed.text]}</span>}
        </p>
        <Conversation className="transcript" aria-label="Workspace transcript" aria-live="off">
          <ConversationContent className="transcript-content">
            {state.entries.length === 0 && state.commands.length === 0 && !state.pending && (
              <div className="welcome">
                <small>FULL FILESYSTEM + BASH</small>
                <h2>From a prompt to working files.</h2>
                <p>Ask the agent to write code, run commands, or explore a problem.</p>
                <Button
                  variant="outline"
                  onClick={() =>
                    setDraft(
                      "Build a small Python CLI that summarizes a text file. Create sample input and run it.",
                    )
                  }
                >
                  Try building a Python CLI →
                </Button>
              </div>
            )}
            {state.entries.length >= 500 && (
              <p className="muted">Showing the latest 500 entries.</p>
            )}
            {timeline.map(({ entry, command }) =>
              entry ? (
                <TranscriptEntry
                  key={entry.id}
                  entry={entry}
                  result={results.get(toolKey(entry))}
                  active={state.active !== null && entry.runId === state.runId}
                />
              ) : command ? (
                <CommandEntry key={`command:${command.requestId}`} command={command} />
              ) : null,
            )}
            {state.pending &&
              !state.commands.some((command) => command.requestId === state.pending?.requestId) && (
                <Message from="user">
                  <small>
                    You ·{" "}
                    {state.uncertain
                      ? "Checking delivery"
                      : state.pending.submissionId
                        ? "Accepted; awaiting history"
                        : "Sending…"}
                  </small>
                  <p className="whitespace-pre-wrap">{state.pending.text}</p>
                </Message>
              )}
            {state.drafts.map((entry) => (
              <TranscriptEntry key={entry.id} entry={entry} streaming />
            ))}
            {state.active && state.drafts.length === 0 && (
              <p className="flex items-center gap-2 text-xs text-muted-foreground" role="status">
                <LoaderCircle className="size-3.5 motion-safe:animate-spin" aria-hidden="true" />
                Working…
              </p>
            )}
          </ConversationContent>
          <ConversationScrollButton />
        </Conversation>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            send();
          }}
        >
          {expired && (
            <p className="error" role="alert">
              This workspace has expired and its files are no longer available. Your conversation is
              still here. Start a new workspace to run code again.
            </p>
          )}
          {state.error && (
            <p className="error" role="alert">
              {state.error}
            </p>
          )}
          {AsyncResult.isFailure(commandEvents) && !state.pending?.command && (
            <p className="error" role="alert">
              Could not load command history.{" "}
              <Button
                variant="ghost"
                type="button"
                disabled={commandEvents.waiting}
                onClick={() => reconnectCommand()}
              >
                Reconnect
              </Button>
            </p>
          )}
          {AsyncResult.isFailure(sent) && (
            <p className="error" role="alert">
              Could not send. Check your prompt and try again.
            </p>
          )}
          <fieldset
            className="mb-3 grid grid-cols-2 gap-2 disabled:opacity-50 sm:grid-cols-[minmax(0,1.4fr)_1fr_1fr]"
            disabled={busy || draft.trimStart().startsWith("!")}
          >
            <legend className="sr-only">Model settings</legend>
            <label htmlFor="model" className="col-span-2 min-w-0 sm:col-span-1">
              Model
              <select
                id="model"
                className={settingControl}
                value={settings.model}
                onChange={(event) => changeSettings({ model: event.target.value })}
              >
                <option value="gpt-6.1-sol">GPT-6.1 Sol</option>
                <option value="gpt-6-astra">GPT-6 Astra</option>
              </select>
            </label>
            <label htmlFor="reasoning" className="min-w-0">
              Reasoning
              <select
                id="reasoning"
                className={settingControl}
                value={settings.reasoningEffort}
                onChange={(event) => changeSettings({ reasoningEffort: event.target.value })}
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
                <option value="xhigh">Extra high</option>
                <option value="max">Maximum</option>
              </select>
            </label>
            <label htmlFor="priority" className="min-w-0">
              Priority
              <select
                id="priority"
                className={settingControl}
                value={settings.serviceTier}
                onChange={(event) => changeSettings({ serviceTier: event.target.value })}
              >
                <option value="ultrafast">Ultrafast</option>
                <option value="fast">Fast</option>
                <option value="default">Standard</option>
              </select>
            </label>
          </fieldset>
          <label htmlFor="prompt">
            {draft.trimStart().startsWith("!") ? "Run a command" : "Your coding task"}
          </label>
          <textarea
            id="prompt"
            placeholder="Ask the agent, or use ! to run a command"
            value={draft}
            maxLength={8000}
            disabled={expired}
            onChange={(event) => setDraft(event.target.value)}
            rows={3}
            onKeyDown={(event) => {
              if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
                event.preventDefault();
                send();
              }
            }}
          />
          <div className="bar">
            <small>
              {state.pending?.command
                ? state.admissionRequestId
                  ? "Sending command…"
                  : "Command is running"
                : state.active
                  ? "Agent is working"
                  : state.reviewRequired
                    ? "Review required"
                    : state.uncertain
                      ? "Awaiting delivery status"
                      : "! runs Bash · ⌘ / Ctrl + Enter to send"}
            </small>
            {state.pending?.command && state.uncertain ? (
              <Button variant="outline" type="button" onClick={() => reconnectCommand()}>
                Reconnect
              </Button>
            ) : state.pending && state.uncertain && !state.active && !state.reviewRequired ? (
              <Button
                variant="outline"
                type="button"
                disabled={expired || sent.waiting || !!state.admissionRequestId}
                onClick={() => send()}
              >
                Retry delivery
              </Button>
            ) : state.active || state.pending?.command ? (
              <Button
                variant="outline"
                type="button"
                disabled={
                  stopped.waiting ||
                  !!state.admissionRequestId ||
                  (state.pending?.command &&
                    !state.commands.some(
                      (command) => command.requestId === state.pending?.requestId,
                    ))
                }
                onClick={() => stop()}
              >
                {stopped.waiting ? "Stopping…" : "■ Stop"}
              </Button>
            ) : (
              <Button variant="default" disabled={busy || !draft.trim()} type="submit">
                {draft.trimStart().startsWith("!") ? "Run ↵" : "Send ↑"}
              </Button>
            )}
          </div>
        </form>
      </section>
      <Files id={id} />
    </main>
  );
}

function CommandEntry({ command }: { command: ShellCommand }) {
  const output = command.output;

  const label = {
    queued: "Preparing…",
    running: "Running…",
    failed: command.failure === "workspace-expired" ? "Workspace expired" : "Could not start",
    unknown: "Outcome unknown",
    completed: output?.timedOut ? "Timed out" : `Exit ${output?.exitCode ?? "—"}`,
  }[command.status];

  return (
    <article className="rounded-lg border border-border bg-muted/30 p-4" aria-label="Shell command">
      <div className="mb-3 flex items-center justify-between gap-3 text-xs text-muted-foreground">
        <span>Shell · /workspace</span>
        <span role="status">{label}</span>
      </div>
      <pre className="overflow-x-auto whitespace-pre-wrap break-words font-mono text-sm">
        $ {command.script}
      </pre>
      {command.failure && (
        <p className="mt-3 border-t border-border pt-3 text-sm text-destructive">
          {command.failure === "workspace-expired"
            ? "The temporary workspace is no longer available. This command was not run. Start a new workspace."
            : "The command could not start before its deadline. It was not run."}
        </p>
      )}
      {output && (
        <div className="mt-3 border-t border-border pt-3">
          {output.stdout && (
            <pre className="overflow-x-auto whitespace-pre-wrap break-words font-mono text-sm">
              {output.stdout}
            </pre>
          )}
          {output.stderr && (
            <pre className="overflow-x-auto whitespace-pre-wrap break-words font-mono text-sm text-destructive">
              {output.stderr}
            </pre>
          )}
          {!output.stdout && !output.stderr && (
            <p className="text-xs text-muted-foreground">No output.</p>
          )}
          {output.truncated && (
            <p className="mt-2 text-xs text-muted-foreground">Output was truncated.</p>
          )}
        </div>
      )}
    </article>
  );
}

const completionLabels: Readonly<Record<string, string>> = {
  Completed: "The agent’s response is ready.",
  Stopped: "The agent has stopped.",
  Failed: "The agent could not finish. Check the conversation for details.",
  Unknown: "An action needs review before the agent can continue.",
};

// Provider call IDs may repeat across Turns; canonical result IDs do not.
const toolKey = (entry: Entry) => entry.toolResultId ?? entry.id;

const toolTitles: Readonly<Record<string, string>> = {
  list_files: "List files",
  read_file: "Read file",
  write_file: "Write file",
  edit_file: "Edit file",
  search: "Search files",
  bash: "Run Bash",
};

function TranscriptEntry({
  entry,
  result,
  active = false,
  streaming = false,
}: {
  readonly entry: Entry;
  readonly result?: Entry;
  readonly active?: boolean;
  readonly streaming?: boolean;
}) {
  if (entry.kind === "tool" || entry.kind === "result") {
    const outcome = result ?? (entry.kind === "result" ? entry : undefined);

    return (
      <Tool
        title={
          Object.hasOwn(toolTitles, entry.name ?? "")
            ? toolTitles[entry.name ?? ""]
            : (entry.name ?? "Tool")
        }
        status={
          outcome ? (outcome.failed ? "failed" : "complete") : active ? "running" : "incomplete"
        }
        input={entry.kind === "tool" ? entry.text : undefined}
        output={outcome?.text}
      />
    );
  }
  if (entry.kind === "reasoning") return <Reasoning streaming={streaming}>{entry.text}</Reasoning>;
  if (entry.kind === "status")
    return (
      <p
        className={`flex items-center gap-2 text-xs ${entry.failed ? "text-destructive" : "text-muted-foreground"}`}
      >
        {entry.text === "Completed" && <Check className="size-3.5" aria-hidden="true" />}
        {entry.text}
      </p>
    );

  return (
    <Message from={entry.kind === "user" ? "user" : "assistant"}>
      <p className="mb-2 text-xs font-medium text-muted-foreground">
        {entry.kind === "user" ? "You" : "Yielded Agent"}
      </p>
      {entry.kind === "user" ? (
        <p className="whitespace-pre-wrap">{entry.text}</p>
      ) : (
        <MessageResponse streaming={streaming}>{entry.text}</MessageResponse>
      )}
    </Message>
  );
}

function Files({ id }: { id: string }) {
  const atoms = workspace(id);
  const [directory, setDirectory] = useAtom(atoms.directory);
  const [selected, setSelected] = useAtom(atoms.selectedFile);
  const files = useAtomValue(atoms.files);
  const file = useAtomValue(atoms.file);
  const refresh = useAtomSet(atoms.refresh);

  return (
    <section className="files" aria-label="Workspace files">
      <div className="bar">
        <h2>
          Workspace <small title={id}>/ {id.slice(0, 8)}</small>
        </h2>
        <Button variant="outline" onClick={() => refresh(undefined)}>
          ↻ Refresh
        </Button>
      </div>
      <div className="file-browser">
        <nav aria-label="File browser">
          <div className="directory">
            <Button
              variant="outline"
              disabled={!directory}
              onClick={() => setDirectory(directory.split("/").slice(0, -1).join("/"))}
              aria-label="Parent directory"
            >
              ↑
            </Button>
            <span title={directory || "/"}>{directory || "/"}</span>
          </div>
          {files.waiting && (
            <p className="muted" role="status">
              Loading files…
            </p>
          )}
          {AsyncResult.isFailure(files) && (
            <p className="error" role="alert">
              Could not load files. Refresh to retry.
            </p>
          )}
          {AsyncResult.isSuccess(files) && files.value.entries.length === 0 && (
            <p className="muted">
              {files.value.expired ? "This workspace's files have expired." : "No files here yet."}
            </p>
          )}
          {AsyncResult.isSuccess(files) &&
            files.value.entries.map((item) => (
              <Button
                variant="ghost"
                key={item.path}
                className="file-item block h-auto text-left font-mono text-xs aria-[current=true]:bg-muted aria-[current=true]:text-primary"
                title={item.path}
                aria-current={!item.directory && selected === item.path ? "true" : undefined}
                onClick={() => (item.directory ? setDirectory(item.path) : setSelected(item.path))}
              >
                {item.directory ? "▸ " : "· "}
                {item.path.split("/").at(-1)}
              </Button>
            ))}
        </nav>
        <div className="viewer">
          <div className="file-tab">{selected ?? "File viewer"}</div>
          {selected === null ? (
            <p className="viewer-message muted">
              Select a file to read its contents.
              <br />
              Files update as the agent works.
            </p>
          ) : file.waiting ? (
            <p className="viewer-message" role="status">
              Loading file…
            </p>
          ) : AsyncResult.isFailure(file) ? (
            <p className="viewer-message error" role="alert">
              Could not read this file. It may have moved or expired. Refresh to retry.
            </p>
          ) : (
            AsyncResult.isSuccess(file) &&
            file.value && (
              <>
                {file.value.truncated && <p className="muted">Showing a truncated file.</p>}
                <pre>
                  <code>{file.value.content || "(empty file)"}</code>
                </pre>
              </>
            )
          )}
        </div>
      </div>
      <footer>Read-only file viewer · Workspace files are temporary</footer>
    </section>
  );
}
