import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/travel/")({
  beforeLoad: () => {
    // oxlint-disable-next-line effect/no-untyped-throw -- TanStack Router consumes thrown redirects from beforeLoad.
    throw redirect({
      to: "/travel/conversations/$conversationId",
      params: { conversationId: crypto.randomUUID() },
      replace: true,
    });
  },
});
