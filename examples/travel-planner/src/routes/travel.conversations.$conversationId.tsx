import { ClientOnly, createFileRoute, notFound } from "@tanstack/react-router";
import * as Schema from "effect/Schema";

import { ConversationId } from "../domain";
import { Planner } from "../planner";

export const Route = createFileRoute("/travel/conversations/$conversationId")({
  beforeLoad: ({ params }) => {
    // oxlint-disable-next-line effect/no-untyped-throw -- TanStack Router consumes thrown notFound responses from beforeLoad.
    if (!Schema.is(ConversationId)(params.conversationId)) throw notFound();
  },
  component: Conversation,
});

function Conversation() {
  const { conversationId } = Route.useParams();

  return (
    <ClientOnly
      fallback={
        <main className="empty">
          <span className="wordmark">
            elsewhere<span>↗</span>
          </span>
          <h1>Your next trip awaits.</h1>
          <p>Loading your conversation…</p>
        </main>
      }
    >
      <Planner conversationId={conversationId} />
    </ClientOnly>
  );
}
