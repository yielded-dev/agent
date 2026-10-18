import { Dialog } from "@base-ui/react/dialog";
import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Button } from "@yielded/ui";
import { DateTime } from "effect";
import { AsyncResult } from "effect/reactivity";
import { Activity, X } from "lucide-react";
import { useMemo } from "react";

import { DIAGNOSTIC_PAGE_SIZE } from "./contract";
import { workspace } from "./state";

export function WorkspaceDiagnostics({ id }: { id: string }) {
  return (
    <Dialog.Root>
      <Dialog.Trigger render={<Button variant="ghost" size="sm" />}>
        <Activity className="size-3.5" /> Diagnostics
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop className="fixed inset-0 z-40 bg-black/60" />
        <Dialog.Popup className="fixed inset-3 z-50 mx-auto flex max-w-4xl flex-col overflow-hidden rounded-xl border border-border bg-background text-foreground shadow-xl sm:inset-8">
          <div className="flex items-start justify-between gap-4 border-b border-border p-5">
            <div>
              <Dialog.Title className="font-semibold">Workspace diagnostics</Dialog.Title>
              <Dialog.Description className="mt-1 text-sm text-muted-foreground">
                Saved activity, read on demand. Tool times include startup and recording.
              </Dialog.Description>
            </div>
            <Dialog.Close
              render={<Button variant="ghost" size="icon" />}
              aria-label="Close diagnostics"
            >
              <X className="size-4" />
            </Dialog.Close>
          </div>
          <DiagnosticsPage id={id} />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function DiagnosticsPage({ id }: { id: string }) {
  const atoms = useMemo(() => workspace(id), [id]);
  const result = useAtomValue(atoms.diagnostics);
  const setAfter = useAtomSet(atoms.diagnosticAfter);
  const refresh = useAtomSet(atoms.refreshDiagnostics);
  const [copied, copy] = useAtom(atoms.copyDiagnostics);
  const page = AsyncResult.isSuccess(result) ? result.value : undefined;

  return (
    <>
      <div className="flex flex-wrap items-center gap-2 border-b border-border px-5 py-3">
        <Button variant="outline" size="sm" disabled={result.waiting} onClick={() => refresh()}>
          Refresh latest
        </Button>
        <Button
          variant="ghost"
          size="sm"
          disabled={!page || result.waiting || copied.waiting}
          onClick={() => copy()}
        >
          {AsyncResult.isSuccess(copied) ? "Copied" : "Copy page"}
        </Button>
        <span className="ml-auto font-mono text-xs text-muted-foreground">{id}</span>
      </div>
      {AsyncResult.isFailure(copied) && (
        <p className="px-5 pt-3 text-sm text-destructive" role="alert">
          Could not copy diagnostics.
        </p>
      )}
      <div className="min-h-0 flex-1 overflow-y-auto px-5" aria-busy={result.waiting}>
        {AsyncResult.isFailure(result) ? (
          <p className="py-5 text-destructive" role="alert">
            Could not load diagnostics. Refresh to try again.
          </p>
        ) : !page ? (
          <p className="py-5 text-muted-foreground" role="status">
            Loading saved activity…
          </p>
        ) : page.rows.length === 0 ? (
          <p className="py-5 text-muted-foreground">No saved activity in this page.</p>
        ) : (
          <ol className="divide-y divide-border">
            {page.rows.map((row) => (
              <li key={row.id} className="py-3">
                <details>
                  <summary className="cursor-pointer text-sm">
                    <span className="mr-3 font-mono text-xs text-muted-foreground">
                      {DateTime.formatLocal(DateTime.makeUnsafe(row.at), {
                        hour: "2-digit",
                        minute: "2-digit",
                        second: "2-digit",
                      })}
                    </span>
                    <span className="font-medium">{row.label}</span>
                    <span
                      className={`ml-3 ${row.failed ? "text-destructive" : "text-muted-foreground"}`}
                    >
                      {row.status}
                      {row.durationMs === undefined
                        ? ""
                        : ` · ${(row.durationMs / 1000).toFixed(1)}s`}
                    </span>
                  </summary>
                  <div className="mt-3 space-y-2 rounded-lg bg-muted p-3 text-xs">
                    {row.detail && (
                      <pre className="whitespace-pre-wrap break-all font-mono">{row.detail}</pre>
                    )}
                    {(row.inputTokens !== undefined || row.outputTokens !== undefined) && (
                      <p>
                        Tokens: {row.inputTokens?.toLocaleString() ?? "—"} input ·{" "}
                        {row.outputTokens?.toLocaleString() ?? "—"} output
                      </p>
                    )}
                    {row.runId && <p className="break-all font-mono">Run: {row.runId}</p>}
                    {row.toolCallId && (
                      <p className="break-all font-mono">Tool call: {row.toolCallId}</p>
                    )}
                    {row.responseIds?.map((responseId) => (
                      <p key={responseId} className="break-all font-mono">
                        Response: {responseId}
                      </p>
                    ))}
                  </div>
                </details>
              </li>
            ))}
          </ol>
        )}
      </div>
      {page && (
        <div className="flex items-center justify-between gap-3 border-t border-border p-4 text-xs text-muted-foreground">
          <Button
            variant="ghost"
            size="sm"
            disabled={page.after === 0 || result.waiting}
            onClick={() => setAfter(Math.max(0, page.after - DIAGNOSTIC_PAGE_SIZE))}
          >
            Older
          </Button>
          <span>
            {page.through > page.after
              ? `Records ${page.after + 1}–${page.through}`
              : "No newer records"}
          </span>
          <Button
            variant="ghost"
            size="sm"
            disabled={!page.hasMore || result.waiting}
            onClick={() => setAfter(page.through)}
          >
            Newer
          </Button>
        </div>
      )}
    </>
  );
}
