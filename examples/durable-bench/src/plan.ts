/** Shared workload from https://github.com/clavia-labs/durable-bench. */
export const CYCLE = [1, 1, 0] as const;

export const MEASURED_TOOLS = 8;

export interface Turn {
  readonly id: string;
  readonly text: string;
}

export interface Message {
  readonly role: "user" | "assistant" | "tool";
  readonly text: string;
  readonly calls?: readonly number[];
}

export type Step = { readonly call: number } | { readonly answer: string };

export interface Stats {
  readonly bytes: number;
  readonly tables: Readonly<Record<string, number>>;
  readonly checkpoint?: number;
}

export const turn = (id: string, tools: number): Turn => ({
  id,
  text: `turn ${id} tools=${tools}`,
});

export const history = (from: number, to: number): readonly Turn[] =>
  Array.from({ length: to - from }, (_, index) => {
    const tools = CYCLE[(from + index) % CYCLE.length];

    return turn(`h${from + index}`, tools === undefined ? 0 : tools);
  });

/** 256-byte tool results, with every 97th record expanded to 8 KiB. */
export const payload = (n: number): string => {
  const head = `record ${n}: `;
  const width = n % 97 === 0 ? 8192 : 256;

  return head + "x".repeat(Math.max(0, width - head.length));
};

/**
 * One scripted model step. `call` is the 1-based count of tool results in the
 * whole transcript. The answer names how many lookups this user turn requested.
 */
export const next = (context: readonly Message[]): Step => {
  const last = context.findLastIndex((message) => message.role === "user");
  const source = last < 0 ? undefined : context[last];
  const planned = Number(/tools=(\d+)/.exec(source?.text ?? "")?.[1] ?? 0);
  const total = context.filter((message) => message.role === "tool").length;
  const done = context.slice(last + 1).filter((message) => message.role === "tool").length;

  return done < planned ? { call: total + 1 } : { answer: `done after ${done} lookups` };
};

export const fingerprint = async (context: readonly Message[]): Promise<string> => {
  const bytes = new TextEncoder().encode(
    JSON.stringify(context.map((message) => [message.role, message.text, message.calls ?? []])),
  );

  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));

  return Array.from(digest.slice(0, 8), (byte) => byte.toString(16).padStart(2, "0")).join("");
};
