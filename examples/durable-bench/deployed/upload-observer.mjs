import { appendFileSync } from "node:fs";

// Capture only upload evidence; never retain headers, bindings, or account identifiers.
const original = globalThis.fetch;

globalThis.fetch = async (input, init) => {
  const url = new URL(input instanceof Request ? input.url : String(input));

  const name = url.pathname.match(
    /\/workers\/scripts\/(cold-storage-fresh-[a-z0-9-]+)(?:\/versions)?$/,
  )?.[1];

  const method = init?.method ?? (input instanceof Request ? input.method : "GET");
  const observe = name && ["PUT", "POST"].includes(method);

  const files =
    observe && init?.body instanceof FormData
      ? Array.from(init.body.entries())
          .filter(([field, value]) => field !== "metadata" && value instanceof Blob)
          .map(([field, value]) => ({ field, bytes: value.size }))
      : [];

  try {
    const response = await original(input, init);

    if (observe) {
      const data = await response.clone().json();

      appendFileSync(
        process.env.COLD_STORAGE_UPLOADS,
        JSON.stringify({
          at: new Date().toISOString(),
          name,
          build: process.env.DURABLE_BENCH_BUILD,
          status: response.status,
          success: data.success,
          files,
          startup_time_ms: data.result?.startup_time_ms ?? null,
          errors: (data.errors ?? []).map(({ code }) => code),
        }) + "\n",
        { mode: 0o600 },
      );
    }

    return response;
  } catch (error) {
    if (observe)
      appendFileSync(
        process.env.COLD_STORAGE_UPLOADS,
        JSON.stringify({
          at: new Date().toISOString(),
          name,
          build: process.env.DURABLE_BENCH_BUILD,
          status: null,
          success: false,
          startup_time_ms: null,
          errors: ["transport-or-observer"],
        }) + "\n",
        { mode: 0o600 },
      );
    throw error;
  }
};
