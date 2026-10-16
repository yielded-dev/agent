import { Option, Schema } from "effect";
import { Atom } from "effect/reactivity";

import { WorkspaceId } from "./contract";

const decodeWorkspacePath = Schema.decodeUnknownOption(
  Schema.TemplateLiteralParser(["/sandbox/", WorkspaceId, Schema.Literals(["", "/"])]),
);

export const workspaceIdFromPath = (pathname: string): string | null =>
  Option.match(decodeWorkspacePath(pathname), {
    onNone: () => null,
    onSome: ([, id]) => id.toLowerCase(),
  });

export const signIn = Atom.fnSync(() => {
  const pathname = window.location.pathname === "/sandbox" ? "/sandbox/" : window.location.pathname;
  const query = new URLSearchParams({ return: pathname + window.location.search });

  window.location.assign(`/auth/login?${query}`);
});
