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

import { type Entry } from "./contract";
import { newWorkspace, workspace, workspaceId } from "./state";

const signIn = "/auth/login?return=%2Fsandbox%2F";
const workspaceDescription = "Files live in a temporary, isolated workspace.";

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
            Could not check your session. <a href={signIn}>Sign in again</a>
          </p>
        ) : AsyncResult.isSuccess(session) &&
          (!session.value || !session.value.scopes.some((scope) => scope === "agent:sandbox")) ? (
          <a className="primary" href={signIn}>
            {session.value ? "Sign in again to open Sandbox →" : "Sign in to start →"}
          </a>
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
  const state = useAtomValue(atoms.state);
  const [draft, setDraft] = useAtom(atoms.draft);
  const [sent, send] = useAtom(atoms.send);
  const [stopped, stop] = useAtom(atoms.stop);

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

  const completed = state.entries.findLast(
    (entry) => entry.kind === "status" && Object.hasOwn(completionLabels, entry.text),
  );

  const busy =
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
            {state.entries.length === 0 && !state.pending && (
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
            {entries.map((entry) => (
              <TranscriptEntry
                key={entry.id}
                entry={entry}
                result={results.get(toolKey(entry))}
                active={state.active !== null && entry.runId === state.runId}
              />
            ))}
            {state.pending && (
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
          {state.error && (
            <p className="error" role="alert">
              {state.error}
            </p>
          )}
          {AsyncResult.isFailure(sent) && (
            <p className="error" role="alert">
              Could not send. Check your prompt and try again.
            </p>
          )}
          <label htmlFor="prompt">Your coding task</label>
          <textarea
            id="prompt"
            placeholder="What would you like to build?"
            value={draft}
            maxLength={8000}
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
              {state.active
                ? "Agent is working"
                : state.reviewRequired
                  ? "Review required"
                  : state.uncertain
                    ? "Awaiting delivery status"
                    : "⌘ / Ctrl + Enter to send"}
            </small>
            {state.active ? (
              <Button
                variant="outline"
                type="button"
                disabled={stopped.waiting}
                onClick={() => stop()}
              >
                {stopped.waiting ? "Stopping…" : "■ Stop"}
              </Button>
            ) : (
              <Button variant="default" disabled={busy || !draft.trim()} type="submit">
                Send ↑
              </Button>
            )}
          </div>
        </form>
      </section>
      <Files id={id} />
    </main>
  );
}

const completionLabels: Readonly<Record<string, string>> = {
  Completed: "The agent’s response is ready.",
  Stopped: "The agent has stopped.",
  Failed: "The agent could not finish. Check the conversation for details.",
  Unknown: "An action needs review before the agent can continue.",
};

const toolKey = (entry: Entry) =>
  entry.toolCallId ? JSON.stringify([entry.runId, entry.toolCallId]) : entry.id;

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
          {AsyncResult.isSuccess(files) && files.value.length === 0 && (
            <p className="muted">No files here yet.</p>
          )}
          {AsyncResult.isSuccess(files) &&
            files.value.map((item) => (
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
