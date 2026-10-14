import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { Login, LoginLoading } from "../auth/login";

export const Route = createFileRoute("/auth/login")({
  head: () => ({ meta: [{ title: "Sign in · yielded agent" }] }),
  component: () => (
    <ClientOnly fallback={<LoginLoading />}>
      <Login />
    </ClientOnly>
  ),
});
