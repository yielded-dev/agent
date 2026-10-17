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
import { DateTime } from "effect";
import { AsyncResult } from "effect/reactivity";
import { ArrowLeft, ArrowUpRight, Check, FolderCode, LoaderCircle } from "lucide-react";
import { useEffect, useId, useMemo, useRef } from "react";

import { type Entry, type PreviewSession, type ShellCommand } from "./contract";
import { signIn } from "./location";
import { location, newWorkspace, workspace, workspaceList } from "./state";

const workspaceDescription =
  "Files are saved after work settles. Processes stop after 30 minutes idle; conversation history is kept.";

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
  const route = useAtomValue(location);
  const navigate = useAtomSet(location);

  return (
    <div className="app">
      <header>
        <Brand />
        <div className="flex flex-wrap items-center gap-2">
          {route.page === "workspace" ? (
            <WorkspaceActions key={route.id} id={route.id} />
          ) : (
            <CreateWorkspace />
          )}
          <Button variant="outline" size="sm" onClick={signOut}>
            Sign out
          </Button>
        </div>
      </header>
      {route.page === "workspace" ? (
        <Workspace key={route.id} id={route.id} />
      ) : route.page === "list" ? (
        <WorkspaceList subject={subject} />
      ) : (
        <main className="gate">
          <h1>This workspace link is invalid.</h1>
          <Button onClick={() => navigate(null)}>Your workspaces</Button>
        </main>
      )}
    </div>
  );
}

function CreateWorkspace({ disabled = false }: { disabled?: boolean }) {
  const [result, create] = useAtom(newWorkspace);

  return (
    <div>
      <Button
        variant="outline"
        size="sm"
        disabled={disabled || result.waiting}
        onClick={() => create()}
      >
        {result.waiting ? "Creating…" : "＋ New workspace"}
      </Button>
      {AsyncResult.isFailure(result) && !result.waiting && (
        <p className="error" role="alert">
          Could not create the workspace. Try again.
        </p>
      )}
    </div>
  );
}

function WorkspaceList({ subject }: { subject: string }) {
  const atoms = useMemo(() => workspaceList(subject), [subject]);
  const result = useAtomValue(atoms.page);
  const cursors = useAtomValue(atoms.cursors);
  const next = useAtomSet(atoms.next);
  const previous = useAtomSet(atoms.previous);
  const refresh = useAtomSet(atoms.refresh);

  return (
    <main className="mx-auto max-w-4xl pb-10">
      <div className="mt-7">
        <small>SANDBOX</small>
        <h1>Your workspaces</h1>
      </div>
      <p className="mt-4 text-sm text-muted-foreground">{workspaceDescription}</p>
      <div className="my-6 flex items-center justify-between gap-3">
        <h2>{cursors.length === 0 ? "Recent workspaces" : "Earlier workspaces"}</h2>
        <Button variant="ghost" size="sm" disabled={result.waiting} onClick={() => refresh()}>
          Refresh
        </Button>
      </div>
      {AsyncResult.isFailure(result) ? (
        <p className="error" role="alert">
          Could not load your workspaces. Refresh to try again.
        </p>
      ) : !AsyncResult.isSuccess(result) ? (
        <p className="text-muted-foreground" role="status">
          Loading your workspaces…
        </p>
      ) : result.value.entries.length === 0 ? (
        <div className="rounded-xl border border-dashed border-border px-6 py-14 text-center">
          <FolderCode className="mx-auto mb-4 size-7 text-muted-foreground" />
          <h2>No workspaces yet</h2>
          <p className="mt-2 text-sm text-muted-foreground">
            Start a workspace to build something with the agent.
          </p>
        </div>
      ) : (
        <ul className="divide-y divide-border overflow-hidden rounded-xl border border-border">
          {result.value.entries.map((entry) => (
            <li key={entry.id}>
              <a
                className="flex items-center gap-4 px-5 py-5 no-underline hover:bg-muted"
                href={`/sandbox/${entry.id}`}
              >
                <FolderCode className="size-5 shrink-0 text-muted-foreground" />
                <div className="min-w-0 flex-1">
                  <p className="truncate font-medium">{entry.title ?? "Untitled workspace"}</p>
                  <p className="mt-1 flex flex-wrap gap-x-3 text-xs text-muted-foreground">
                    <span className="font-mono">{entry.id.slice(0, 8)}</span>
                    <span>
                      Saved{" "}
                      {DateTime.formatLocal(DateTime.makeUnsafe(entry.addedAt), {
                        dateStyle: "medium",
                        timeStyle: "short",
                      })}
                    </span>
                  </p>
                </div>
                <ArrowUpRight className="size-4 shrink-0 text-muted-foreground" />
              </a>
            </li>
          ))}
        </ul>
      )}
      {(cursors.length > 0 || (AsyncResult.isSuccess(result) && result.value.next !== null)) && (
        <div className="mt-5 flex justify-between gap-3">
          <Button
            variant="outline"
            disabled={cursors.length === 0 || result.waiting}
            onClick={() => previous()}
          >
            Newer
          </Button>
          <Button
            variant="outline"
            disabled={
              !AsyncResult.isSuccess(result) || result.value.next === null || result.waiting
            }
            onClick={() => next()}
          >
            Older
          </Button>
        </div>
      )}
    </main>
  );
}

function WorkspaceActions({ id }: { id: string }) {
  // Atom.family caches bundles weakly; retain this workspace while it is mounted.
  const atoms = useMemo(() => workspace(id), [id]);
  const state = useAtomValue(atoms.state);
  const navigate = useAtomSet(location);
  const busy = state.active !== null || state.pending !== null || state.admissionRequestId !== null;

  return (
    <>
      <Button variant="ghost" size="sm" disabled={busy} onClick={() => navigate(null)}>
        <ArrowLeft className="size-3.5" /> Your workspaces
      </Button>
      <CreateWorkspace disabled={busy} />
    </>
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
  const [recovery, recover] = useAtom(atoms.recover);
  const reconnectCommand = useAtomSet(atoms.reconnectCommand);
  const expired = useAtomValue(atoms.expired);
  const saved = useAtomValue(atoms.saved);
  const fileOperation = useAtomValue(atoms.fileOperation);
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
    !state.historyReady ||
    expired ||
    fileOperation !== "idle" ||
    state.reviewRequired ||
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
              {saved !== undefined && saved.savedAt !== null
                ? "This workspace has expired. Restore its last saved files from the file browser. Your conversation is kept."
                : "This workspace expired without saved files. Its old files cannot be recovered. Your conversation is still here; start a new workspace to run code."}
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
          {AsyncResult.isFailure(recovery) && (
            <p role="alert">Recovery could not be checked. Try again.</p>
          )}
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
                      : !state.historyReady
                        ? "Loading history…"
                        : fileOperation === "saving"
                          ? "Saving files…"
                          : fileOperation === "restoring"
                            ? saved?.restoreFailed
                              ? "Restore needs a retry"
                              : "Restoring files…"
                            : "! runs Bash · ⌘ / Ctrl + Enter to send"}
            </small>
            {state.reviewRequired ? (
              <Button
                variant="outline"
                type="button"
                disabled={recovery.waiting}
                onClick={() => recover()}
              >
                {recovery.waiting ? "Checking recovery…" : "Check recovery"}
              </Button>
            ) : state.pending?.command && state.uncertain ? (
              <Button variant="outline" type="button" onClick={() => reconnectCommand()}>
                Reconnect
              </Button>
            ) : state.pending && state.uncertain && !state.active && !state.reviewRequired ? (
              <Button
                variant="outline"
                type="button"
                disabled={
                  expired || fileOperation !== "idle" || sent.waiting || !!state.admissionRequestId
                }
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
            ? "The workspace had expired. This command was not run. Restore saved files, if available, or start a new workspace."
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
  const saved = useAtomValue(atoms.saved);
  const expired = useAtomValue(atoms.expired);
  const fileOperation = useAtomValue(atoms.fileOperation);
  const state = useAtomValue(atoms.state);
  const [restored, restore] = useAtom(atoms.restore);
  const [savedResult, save] = useAtom(atoms.save);
  const [preparedPreview, preparePreview] = useAtom(atoms.preparePreview);
  const preview = useAtomValue(atoms.preview);
  const fileView = useAtomValue(atoms.fileView);
  const changeFileView = useAtomSet(atoms.changeFileView);
  const refresh = useAtomSet(atoms.refresh);
  const listing = AsyncResult.getOrElse(files, () => null);
  const savedAt = saved?.savedAt ?? null;
  const hasSavedFiles = savedAt !== null;
  const restoreFailed = saved?.restoreFailed && !restored.waiting;
  const saveFailed = saved?.saveFailed && !savedResult.waiting;

  const canPreview =
    selected !== null && /\.html?$/i.test(selected) && listing !== null && !expired;

  const canSave =
    listing !== null &&
    !expired &&
    fileOperation === "idle" &&
    (!hasSavedFiles || saveFailed) &&
    state.historyReady &&
    !state.active &&
    !state.pending &&
    !state.admissionRequestId &&
    !state.reviewRequired &&
    !state.uncertain;

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
      {listing !== null && (
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3 text-xs">
          <div className="min-w-0">
            <p className="text-muted-foreground">
              {savedAt !== null
                ? `Last saved ${DateTime.formatLocal(DateTime.makeUnsafe(savedAt), {
                    dateStyle: "medium",
                    timeStyle: "short",
                  })}`
                : "No saved files yet"}
            </p>
            {fileOperation !== "idle" && !restoreFailed && (
              <p className="mt-1 flex items-center gap-2 text-muted-foreground" role="status">
                <LoaderCircle className="size-3.5 motion-safe:animate-spin" aria-hidden="true" />
                {fileOperation === "restoring" ? "Restoring files…" : "Saving files…"}
              </p>
            )}
            {restoreFailed && (
              <p className="mt-1 text-destructive" role="alert">
                Files could not be restored. Retry restore to continue.
              </p>
            )}
            {saveFailed && fileOperation === "idle" && (
              <p className="mt-1 text-destructive" role="alert">
                The latest files could not be saved.
              </p>
            )}
            {expired && fileOperation === "idle" && AsyncResult.isFailure(restored) && (
              <p className="mt-1 text-destructive" role="alert">
                Could not confirm restoration. Refresh to check its status, then retry.
              </p>
            )}
            {canSave && AsyncResult.isFailure(savedResult) && (
              <p className="mt-1 text-destructive" role="alert">
                Could not confirm the save. Refresh to check its status, then retry.
              </p>
            )}
            {expired && hasSavedFiles && state.reviewRequired && (
              <p className="mt-1 text-muted-foreground">
                Restoring files leaves operations awaiting review.
              </p>
            )}
          </div>
          {expired && hasSavedFiles ? (
            <Button
              variant="outline"
              size="sm"
              disabled={
                restored.waiting ||
                savedResult.waiting ||
                (fileOperation !== "idle" && !restoreFailed)
              }
              onClick={() => restore()}
            >
              {fileOperation === "restoring" && !restoreFailed
                ? "Restoring…"
                : restoreFailed || AsyncResult.isFailure(restored)
                  ? "Retry restore"
                  : "Restore workspace"}
            </Button>
          ) : canSave ? (
            <Button variant="outline" size="sm" onClick={() => save()}>
              {saveFailed || AsyncResult.isFailure(savedResult) ? "Retry save" : "Save files"}
            </Button>
          ) : null}
        </div>
      )}
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
              {files.value.expired
                ? hasSavedFiles
                  ? "Restore this workspace to browse its saved files."
                  : "These expired files cannot be recovered; no saved files are available."
                : "No files here yet."}
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
          <div className="file-tab shrink-0">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <span
                className="min-w-0 flex-1 truncate max-sm:basis-full"
                title={selected ?? undefined}
              >
                {selected ?? "File viewer"}
              </span>
              {canPreview && (
                <div className="flex items-center gap-2">
                  <div
                    className="flex rounded-md border border-border p-0.5"
                    role="group"
                    aria-label="File view"
                  >
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs aria-pressed:bg-muted"
                      aria-pressed={fileView === "source"}
                      onClick={() => changeFileView("source")}
                    >
                      Source
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs aria-pressed:bg-muted"
                      aria-pressed={fileView === "preview"}
                      disabled={fileOperation !== "idle"}
                      onClick={() => changeFileView("preview")}
                    >
                      Preview
                    </Button>
                  </div>
                  {preview !== null && (
                    <form
                      className="shrink-0 border-0 bg-transparent p-0"
                      action={preview.url}
                      method="post"
                      target="_blank"
                      rel="noopener"
                    >
                      <input type="hidden" name="token" value={preview.token} />
                      <input type="hidden" name="expiresAt" value={preview.expiresAt} />
                      <input type="hidden" name="path" value={selected} />
                      <Button
                        type="submit"
                        variant="ghost"
                        size="sm"
                        disabled={fileOperation !== "idle"}
                        aria-label="Open preview in a new tab"
                        title="Open preview in a new tab"
                      >
                        <ArrowUpRight className="size-4" aria-hidden="true" />
                      </Button>
                    </form>
                  )}
                </div>
              )}
            </div>
          </div>
          {canPreview && fileView === "preview" ? (
            preview !== null ? (
              <FilePreview preview={preview} path={selected} />
            ) : (
              <div className="viewer-message space-y-3 text-center">
                {preparedPreview.waiting ? (
                  <p role="status">Preparing preview…</p>
                ) : (
                  <>
                    <p role={AsyncResult.isFailure(preparedPreview) ? "alert" : "status"}>
                      {AsyncResult.isFailure(preparedPreview)
                        ? "Could not prepare preview. Try again."
                        : AsyncResult.isSuccess(preparedPreview)
                          ? "Preview expired. Refresh to open it again."
                          : "Open a preview of this file."}
                    </p>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={fileOperation !== "idle"}
                      onClick={() => preparePreview()}
                    >
                      {AsyncResult.isFailure(preparedPreview)
                        ? "Retry preview"
                        : AsyncResult.isSuccess(preparedPreview)
                          ? "Refresh preview"
                          : "Prepare preview"}
                    </Button>
                  </>
                )}
              </div>
            )
          ) : selected === null ? (
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
      <footer>Read-only file viewer · Files saved after work settles</footer>
    </section>
  );
}

function FilePreview({ preview, path }: { preview: typeof PreviewSession.Type; path: string }) {
  const frameName = useId();
  const form = useRef<HTMLFormElement>(null);

  useEffect(() => {
    form.current?.requestSubmit();
  }, [preview, path]);

  return (
    <>
      <iframe
        className="min-h-0 w-full flex-1 border-0 bg-white"
        name={frameName}
        title={`Preview of ${path}`}
        sandbox="allow-scripts allow-same-origin"
      />
      <form ref={form} action={preview.url} method="post" target={frameName} hidden>
        <input type="hidden" name="token" value={preview.token} />
        <input type="hidden" name="expiresAt" value={preview.expiresAt} />
        <input type="hidden" name="path" value={path} />
      </form>
    </>
  );
}
