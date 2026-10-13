import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
  beforeLoad: () => {
    // oxlint-disable-next-line effect/no-untyped-throw -- TanStack Router consumes thrown redirects from beforeLoad.
    throw redirect({
      to: "/conversations/$conversationId",
      params: { conversationId: crypto.randomUUID() },
      replace: true,
    });
  },
});
