import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { Login, LoginLoading } from "../auth/login";

export const Route = createFileRoute("/auth/yielded/callback")({
  head: () => ({ meta: [{ title: "Sign in · yielded agent" }] }),
  component: () => (
    <ClientOnly fallback={<LoginLoading step="callback" />}>
      <Login callback />
    </ClientOnly>
  ),
});
