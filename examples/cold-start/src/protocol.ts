/** The forwarding Worker deliberately has no runtime framework imports. */
export interface Env {
  TOKEN: string;
  PROVIDER_URL: string;
  VERSION: { id: string };
  LOCATION_HINT: DurableObjectLocationHint;
  THREAD: {
    getByName(
      name: string,
      options: { locationHint: DurableObjectLocationHint },
    ): {
      probe(sample: string): Promise<ReadableStream<Uint8Array>>;
      touch(): Promise<unknown>;
    };
    idFromName(name: string): { toString(): string };
  };
}

export const encode = (type: string, value: unknown) =>
  new TextEncoder().encode(JSON.stringify({ type, value }) + "\n");

export const lines = (consume: (line: string) => void) => {
  const decoder = new TextDecoder();
  let pending = "";

  const drain = () => {
    let end: number;

    while ((end = pending.indexOf("\n")) !== -1) {
      consume(pending.slice(0, end));
      pending = pending.slice(end + 1);
    }
  };

  return {
    write(bytes: Uint8Array) {
      pending += decoder.decode(bytes, { stream: true });
      drain();
    },
    finish() {
      pending += decoder.decode();
      drain();
      if (pending) consume(pending);
      pending = "";
    },
  };
};
