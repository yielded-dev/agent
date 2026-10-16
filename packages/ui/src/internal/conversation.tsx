/*! Adapted from Vercel AI Elements. Copyright 2023 Vercel, Inc. Apache-2.0; see NOTICE. */
import { ArrowDown } from "lucide-react";
import type { ComponentProps } from "react";
import { StickToBottom, useStickToBottomContext } from "use-stick-to-bottom";

import { Button } from "./button.tsx";
import { cn } from "./class-names.ts";

/** Follows new output until the reader scrolls up. */
export function Conversation({ className, ...props }: ComponentProps<typeof StickToBottom>) {
  return (
    <StickToBottom
      className={cn("relative min-h-0 flex-1 overflow-hidden", className)}
      initial="instant"
      resize="instant"
      role="log"
      {...props}
    />
  );
}

export function ConversationContent({
  className,
  ...props
}: ComponentProps<typeof StickToBottom.Content>) {
  return <StickToBottom.Content className={cn("flex flex-col gap-5 p-6", className)} {...props} />;
}

export function ConversationScrollButton() {
  const { isAtBottom, scrollToBottom } = useStickToBottomContext();

  return (
    !isAtBottom && (
      <Button
        className="absolute bottom-4 left-1/2 -translate-x-1/2 rounded-full shadow-sm"
        onClick={() => {
          void scrollToBottom();
        }}
        size="icon"
        type="button"
        variant="outline"
        aria-label="Scroll to latest message"
      >
        <ArrowDown className="size-4" aria-hidden="true" />
      </Button>
    )
  );
}
