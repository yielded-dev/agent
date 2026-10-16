import { Collapsible } from "@base-ui/react/collapsible";
import { Brain, Check, ChevronDown, CircleAlert, LoaderCircle, Minus, Wrench } from "lucide-react";

import { cn } from "./class-names.ts";
import { MessageResponse } from "./message.tsx";

export type ToolStatus = "running" | "complete" | "failed" | "incomplete";

const statusLabels = {
  running: "Running",
  complete: "Completed",
  failed: "Failed",
  incomplete: "Not completed",
};

const statusIcons = {
  running: LoaderCircle,
  complete: Check,
  failed: CircleAlert,
  incomplete: Minus,
};

/** One call and its result share a single disclosure. Values are rendered as text. */
export function Tool({
  title,
  status,
  input,
  output,
  className,
}: {
  readonly title: string;
  readonly status: ToolStatus;
  readonly input?: string;
  readonly output?: string;
  readonly className?: string;
}) {
  const Icon = statusIcons[status];
  const expandable = input !== undefined || output !== undefined;

  const header = (
    <>
      <Wrench className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
      <span className="min-w-0 flex-1 truncate text-left">{title}</span>
      <span
        className={cn(
          "flex shrink-0 items-center gap-1.5 text-xs text-muted-foreground",
          status === "failed" && "text-destructive",
        )}
      >
        <Icon
          className={cn("size-3.5", status === "running" && "motion-safe:animate-spin")}
          aria-hidden="true"
        />
        {statusLabels[status]}
      </span>
      {expandable && (
        <ChevronDown
          className="size-3.5 shrink-0 text-muted-foreground transition-transform group-data-[panel-open]:rotate-180"
          aria-hidden="true"
        />
      )}
    </>
  );

  return (
    <Collapsible.Root
      data-slot="tool"
      className={cn("min-w-0 rounded-[var(--radius,0.5rem)] border border-border", className)}
    >
      {expandable ? (
        <Collapsible.Trigger
          className="group flex w-full items-center gap-2.5 rounded-[var(--radius,0.5rem)] px-3 py-2.5 text-sm hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
          aria-label={`${title}: ${statusLabels[status]}`}
        >
          {header}
        </Collapsible.Trigger>
      ) : (
        <div className="flex items-center gap-2.5 px-3 py-2.5 text-sm">{header}</div>
      )}
      <Collapsible.Panel className="overflow-hidden">
        {input !== undefined && <ToolValue label="Input" value={input} />}
        {output !== undefined && (
          <ToolValue label={status === "failed" ? "Error" : "Output"} value={output} />
        )}
      </Collapsible.Panel>
    </Collapsible.Root>
  );
}

function ToolValue({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div className="min-w-0 border-t border-border p-3">
      <p className="mb-2 text-xs font-medium text-muted-foreground">{label}</p>
      <pre className="max-h-72 overflow-auto rounded-[var(--radius,0.5rem)] bg-muted p-3 font-mono text-xs leading-6 whitespace-pre-wrap [overflow-wrap:anywhere]">
        {value}
      </pre>
    </div>
  );
}

/** Shows only text supplied by a reasoning-capable provider. */
export function Reasoning({
  children,
  streaming = false,
  className,
}: {
  readonly children: string;
  readonly streaming?: boolean;
  readonly className?: string;
}) {
  return (
    <Collapsible.Root
      defaultOpen={streaming}
      data-slot="reasoning"
      className={cn("min-w-0 text-muted-foreground", className)}
    >
      <Collapsible.Trigger className="group flex items-center gap-2 rounded-[var(--radius,0.5rem)] py-1 text-xs hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring">
        <Brain
          className={cn("size-3.5", streaming && "motion-safe:animate-pulse")}
          aria-hidden="true"
        />
        {streaming ? "Thinking…" : "Reasoning summary"}
        <ChevronDown
          className="size-3.5 transition-transform group-data-[panel-open]:rotate-180"
          aria-hidden="true"
        />
      </Collapsible.Trigger>
      <Collapsible.Panel className="mt-2 overflow-hidden border-l-2 border-border pl-4">
        <MessageResponse streaming={streaming} className="text-muted-foreground">
          {children}
        </MessageResponse>
      </Collapsible.Panel>
    </Collapsible.Root>
  );
}
