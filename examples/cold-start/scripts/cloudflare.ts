import {
  Cause,
  Clock,
  Config,
  Console,
  Context,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Path,
  Redacted,
  Schedule,
  Schema,
} from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";

import { type Upload, State, type Bundle } from "./evidence.ts";
import { BenchError, check, hash, nonce, read, save, workspace } from "./platform.ts";

const Namespaces = Schema.Array(
  Schema.Struct({
    id: Schema.String,
    name: Schema.String,
    script: Schema.optionalKey(Schema.String),
  }),
);

const Workers = Schema.Array(Schema.Struct({ id: Schema.String }));

const Deployments = Schema.Struct({
  deployments: Schema.Array(
    Schema.Struct({
      versions: Schema.Array(Schema.Struct({ version_id: Schema.String })),
    }),
  ),
});

const Envelope = Schema.Struct({ success: Schema.Boolean, result: Schema.Unknown });

const Uploaded = Schema.Struct({
  version_id: Schema.optionalKey(Schema.NullOr(Schema.String)),
  startup_time_ms: Schema.optionalKey(Schema.NullOr(Schema.Finite)),
});

interface RequestOptions {
  readonly method?: "GET" | "POST" | "PUT" | "DELETE";
  readonly json?: Schema.Json;
  readonly form?: FormData;
}

const connect = Effect.gen(function* () {
  const account = Redacted.value(yield* Config.Redacted("CLOUDFLARE_ACCOUNT_ID"));
  const token = Redacted.value(yield* Config.Redacted("CLOUDFLARE_API_TOKEN"));
  const client = yield* HttpClient.HttpClient;
  let lastApi = 0;

  const redact = (text: string) =>
    text.replaceAll(account, "[redacted]").replaceAll(token, "[redacted]");

  const request = Effect.fnUntraced(
    function* (url: string, bearer: string, options: RequestOptions = {}) {
      let request = HttpClientRequest.make(options.method ?? "GET")(url, {
        headers: { authorization: `Bearer ${bearer}`, "user-agent": "durable-bench/1.0" },
      });

      if (options.json !== undefined)
        request = HttpClientRequest.bodyJsonUnsafe(request, options.json);
      if (options.form) request = HttpClientRequest.bodyFormData(request, options.form);
      const response = yield* client.execute(request);

      return {
        status: response.status,
        bytes: new Uint8Array(yield* response.arrayBuffer),
        retryAfter: response.headers["retry-after"] ?? null,
      };
    },
    Effect.timeoutOrElse({
      duration: "90 seconds",
      orElse: () => BenchError.make({ message: "HTTP request timed out; outcome may be unknown" }),
    }),
    Effect.mapError(() =>
      BenchError.make({ message: "HTTP transport failed; outcome may be unknown" }),
    ),
  );

  const maybeApi = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(route: string, schema: S, options: RequestOptions = {}) {
    for (let attempt = 0; ; attempt++) {
      const now = yield* Clock.currentTimeMillis;

      yield* Effect.sleep(Math.max(0, 1000 - (now - lastApi)));
      lastApi = yield* Clock.currentTimeMillis;

      const response = yield* request(
        `https://api.cloudflare.com/client/v4/accounts/${account}/${route}`,
        token,
        options,
      );

      if (response.status === 404) return Option.none<S["Type"]>();
      const text = new TextDecoder().decode(response.bytes);

      if (response.status < 200 || response.status >= 300) {
        if (
          (options.method ?? "GET") === "GET" &&
          attempt < 6 &&
          [408, 429, 500, 502, 503, 504].includes(response.status)
        ) {
          const retryAfter = Number(response.retryAfter);

          yield* Effect.sleep(
            Math.max(2 ** attempt, Number.isFinite(retryAfter) ? retryAfter : 0) * 1000,
          );
          continue;
        }

        return yield* BenchError.make({
          message: redact(
            `Cloudflare ${options.method ?? "GET"} ${route}: HTTP ${response.status} ${text}`,
          ).slice(0, 1600),
        });
      }

      const envelope = yield* Schema.decodeEffect(Schema.fromJsonString(Envelope))(text).pipe(
        Effect.mapError(() => BenchError.make({ message: "Invalid Cloudflare API envelope" })),
      );

      yield* check(envelope.success, "Cloudflare API rejected the operation");

      return Option.some(
        yield* Schema.decodeUnknownEffect(schema)(envelope.result).pipe(
          Effect.mapError(() =>
            BenchError.make({ message: `Invalid Cloudflare API result for ${route}` }),
          ),
        ),
      );
    }
  });

  const api = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(route: string, schema: S, options: RequestOptions = {}) {
    const result = yield* maybeApi(route, schema, options);

    if (Option.isNone(result))
      return yield* BenchError.make({ message: `Cloudflare resource not found: ${route}` });

    return result.value;
  });

  const namespaces = Effect.gen(function* () {
    const rows = new Map<string, (typeof Namespaces.Type)[number]>();

    for (let page = 1; page <= 100; page++) {
      const batch = yield* api(
        `workers/durable_objects/namespaces?per_page=1000&page=${page}`,
        Namespaces,
      );

      for (const row of batch) rows.set(row.id, row);
      if (batch.length < 1000) return [...rows.values()];
    }

    return yield* BenchError.make({ message: "Namespace pagination did not finish" });
  });

  const version = Effect.fnUntraced(function* (name: string) {
    const result = yield* maybeApi(`workers/scripts/${name}/deployments`, Deployments);

    if (Option.isNone(result)) return null;
    const versions = result.value.deployments[0]?.versions;

    return versions?.length === 1 ? versions[0]!.version_id : null;
  });

  const profile = (name: string, version: string, body: Schema.Json) =>
    request(
      `https://api.cloudflare.com/client/v4/accounts/${account}/workers/workers/${name}/versions/${version}/profile`,
      token,
      { method: "POST", json: body },
    );

  return { api, maybeApi, namespaces, version, request, profile, redact };
});

export class Cloudflare extends Context.Service<Cloudflare, Effect.Success<typeof connect>>()(
  "cold-start/Cloudflare",
) {
  static readonly layer = Layer.effect(Cloudflare, connect);
}

export const makeRun = Effect.fnUntraced(function* (directory: string, existing = false) {
  const cf = yield* Cloudflare;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const out = path.resolve(directory);
  let prefix: string;
  let names: string[];
  const uploads: Record<string, typeof Upload.Type> = {};
  const token = (yield* nonce) + (yield* nonce);
  let domain = "";

  const redact = (text: string) => {
    text = cf.redact(text).replaceAll(token, "[redacted]");

    return domain ? text.replaceAll(domain, "[redacted]") : text;
  };

  if (existing) {
    const state = yield* read(path.join(out, "state.json"), State);

    prefix = state.prefix;
    names = [...state.names];
  } else {
    prefix = "press-cs-" + (yield* nonce).slice(0, 10);
    names = [];
    yield* fs.makeDirectory(path.dirname(out), { recursive: true, mode: 0o700 });
    yield* fs.makeDirectory(out, { mode: 0o700 });
    yield* save(path.join(out, "state.json"), { prefix, names });
    for (const folder of ["src", "scripts", "third-party/src"]) {
      for (const file of yield* fs.readDirectory(path.join(workspace, folder))) {
        const source = path.join(workspace, folder, file);

        if ((yield* fs.stat(source)).type !== "File") continue;
        const destination = path.join(out, "source", folder, file);

        yield* fs.makeDirectory(path.dirname(destination), { recursive: true, mode: 0o700 });
        yield* fs.copyFile(source, destination);
      }
    }
    for (const file of yield* fs.readDirectory(path.join(workspace, "dist"))) {
      if (file.endsWith(".map"))
        yield* fs.copyFile(path.join(workspace, "dist", file), path.join(out, file));
    }
  }
  yield* check(
    /^press-cs-[a-f0-9]{10}$/.test(prefix) &&
      names.every((name) => name.startsWith(prefix + "-") && /^[a-z0-9-]+$/.test(name)),
    "Refusing resources outside the exact run prefix",
  );

  const capacity = Effect.fnUntraced(function* (additional: number) {
    const rows = yield* cf.namespaces;

    const owned = rows.filter(
      (row) => row.script?.startsWith("press-cs-") || row.name.startsWith("press-cs-"),
    );

    const state = {
      total: rows.length,
      owned: owned.length,
      additional,
      spareAfter: 500 - rows.length - additional,
    };

    yield* fs.writeFileString(path.join(out, "capacity.jsonl"), JSON.stringify(state) + "\n", {
      flag: "a",
      mode: 0o600,
    });
    yield* check(
      state.spareAfter >= 100 && owned.length + additional <= 40,
      "Need >=100 spare namespaces and <=40 owned by press-cs-",
    );
  });

  const upload = Effect.fnUntraced(function* (
    suffix: string,
    bundle: typeof Bundle.Type,
    klass?: "Thread" | "EmptyObject" | "Driver",
    host?: string,
    observable = false,
  ) {
    const name = prefix + "-" + suffix;

    yield* check(/^[a-z0-9-]+$/.test(suffix), "Invalid Worker suffix");
    const additional = Number(klass !== undefined && host === undefined && !names.includes(name));

    yield* capacity(additional);

    const bindings: Schema.Json[] = [
      { type: "secret_text", name: "TOKEN", text: token },
      { type: "version_metadata", name: "VERSION" },
      { type: "plain_text", name: "LOCATION_HINT", text: "wnam" },
      { type: "plain_text", name: "PREFIX", text: prefix },
      { type: "plain_text", name: "SUBDOMAIN", text: domain },
      {
        type: "plain_text",
        name: "PROVIDER_URL",
        text: `https://${prefix}-provider.${domain}.workers.dev/v1`,
      },
    ];

    if (klass)
      bindings.push({
        type: "durable_object_namespace",
        name: klass === "Driver" ? "DRIVER" : "THREAD",
        class_name: klass,
        ...(host ? { script_name: host } : {}),
      });

    const metadata = {
      main_module: "worker.mjs",
      compatibility_date: "2026-08-18",
      compatibility_flags: ["nodejs_compat", "global_fetch_strictly_public"],
      bindings,
      observability: { enabled: observable, head_sampling_rate: observable ? 1 : 0 },
      ...(additional
        ? { migrations: { new_tag: "v1", steps: [{ new_sqlite_classes: [klass!] }] } }
        : {}),
    };

    const base = yield* fs.readFileString(path.join(workspace, "dist", bundle + ".mjs"));
    const source = base + `\n// sample ${name} ${yield* nonce}\n`;
    const sourceMap = yield* fs.readFileString(path.join(workspace, "dist", bundle + ".mjs.map"));
    const form = new FormData();

    form.append(
      "metadata",
      new Blob([JSON.stringify(metadata)], { type: "application/json" }),
      "metadata.json",
    );
    form.append(
      "worker.mjs",
      new Blob([source], { type: "application/javascript+module" }),
      "worker.mjs",
    );
    form.append(
      bundle + ".mjs.map",
      new Blob([sourceMap], { type: "application/source-map" }),
      bundle + ".mjs.map",
    );
    if (!names.includes(name)) {
      names.push(name);
      // Retain intent before mutation, including an ambiguous upload failure.
      yield* save(path.join(out, "state.json"), { prefix, names });
    }
    const result = yield* cf.api(`workers/scripts/${name}`, Uploaded, { method: "PUT", form });
    let version = result.version_id ?? null;

    for (let attempt = 0; !version && attempt < 20; attempt++) {
      version = yield* cf.version(name);
      if (!version) yield* Effect.sleep("2 seconds");
    }
    if (!version) return yield* BenchError.make({ message: "Uploaded version is not visible" });
    uploads[name] = {
      version,
      sha256: hash(source),
      bundleSha256: hash(base),
      bundle,
      startupMs: result.startup_time_ms ?? null,
      uploadedAt: (yield* Clock.currentTimeMillis) / 1000,
      host: host ?? null,
    };
    yield* fs.writeFileString(path.join(out, name + ".mjs"), source, { mode: 0o600 });
    yield* save(path.join(out, "uploads.json"), uploads);
    yield* cf.api(`workers/scripts/${name}/subdomain`, Schema.Unknown, {
      method: "POST",
      json: { enabled: true, previews_enabled: false },
    });
    yield* Console.error("Uploaded", suffix);

    return name;
  });

  const invokeText = Effect.fnUntraced(function* (
    name: string,
    route: string,
    body?: Schema.Json,
    failurePath?: string,
  ) {
    yield* check(names.includes(name), "Refusing a Worker outside this run");

    const response = yield* cf.request(`https://${name}.${domain}.workers.dev${route}`, token, {
      method: body === undefined ? "GET" : "POST",
      ...(body === undefined ? {} : { json: body }),
    });

    const text = new TextDecoder().decode(response.bytes);

    if (response.status < 200 || response.status >= 300) {
      if (failurePath)
        yield* save(failurePath, {
          error: `Target HTTP ${response.status}`,
          responseBody: redact(text),
        });

      return yield* BenchError.make({
        message: `Target HTTP ${response.status}: ${redact(text).slice(0, 2000)}`,
      });
    }

    return text;
  });

  const invoke = Effect.fnUntraced(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(name: string, route: string, schema: S, body?: Schema.Json, failurePath?: string) {
    const text = yield* invokeText(name, route, body, failurePath);

    // Preserve the response before decoding, including unexpected fields or malformed evidence.
    if (failurePath) yield* fs.writeFileString(failurePath, text, { mode: 0o600 });

    return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text).pipe(
      Effect.mapError(() => BenchError.make({ message: "Worker returned invalid evidence" })),
    );
  });

  const ready = (name: string) =>
    invokeText(name, "/health").pipe(
      Effect.retry({ times: 39, schedule: Schedule.spaced("3 seconds") }),
    );

  const cleanup = Effect.gen(function* () {
    const errors: { attempt: number; name?: string; error?: string; inventoryError?: string }[] =
      [];

    let pending = [...names].reverse();

    for (let attempt = 0; attempt < 10; attempt++) {
      for (const name of pending) {
        const result = yield* Effect.exit(
          cf.maybeApi(`workers/scripts/${name}?force=true`, Schema.Unknown, { method: "DELETE" }),
        );

        if (Exit.isFailure(result))
          errors.push({ attempt, name, error: redact(Cause.pretty(result.cause)) });
      }

      const inventory = yield* Effect.exit(
        Effect.gen(function* () {
          const workers = (yield* cf.api("workers/scripts", Workers))
            .filter((row) => names.includes(row.id))
            .map((row) => row.id);

          const namespaces = (yield* cf.namespaces).filter(
            (row) => row.script !== undefined && names.includes(row.script),
          );

          const versions: string[] = [];

          for (const name of names) {
            const result = yield* cf.maybeApi(
              `workers/scripts/${name}/versions`,
              Schema.Struct({ items: Schema.Array(Schema.Unknown) }),
            );

            if (Option.isSome(result) && result.value.items.length) versions.push(name);
          }

          return { workers, namespaces, versions };
        }),
      );

      if (Exit.isFailure(inventory)) {
        errors.push({ attempt, inventoryError: redact(Cause.pretty(inventory.cause)) });
        yield* save(path.join(out, "cleanup.json"), { verified: false, deleteHistory: errors });
      } else {
        const { workers, namespaces, versions } = inventory.value;
        const verified = workers.length + namespaces.length + versions.length === 0;

        yield* save(path.join(out, "cleanup.json"), {
          workers,
          namespaces: namespaces.map((row) => row.id),
          versions,
          deleteHistory: errors,
          verified,
        });
        if (verified) {
          yield* Console.error(
            "Teardown verified: zero Workers, namespaces, and uploaded versions",
          );

          return;
        }
        pending = [
          ...new Set([
            ...workers,
            ...versions,
            ...namespaces.flatMap((row) => (row.script ? [row.script] : [])),
          ]),
        ];
      }
      yield* Effect.sleep("3 seconds");
    }

    return yield* BenchError.make({
      message: "Teardown incomplete; rerun cleanup with the same output directory",
    });
  }).pipe(Effect.uninterruptible);

  const initialize = Effect.gen(function* () {
    domain = (yield* cf.api(
      "workers/subdomain",
      Schema.Struct({ subdomain: Schema.NonEmptyString }),
    )).subdomain;
  });

  return {
    out,
    prefix,
    uploads,
    redact,
    initialize,
    upload,
    invoke,
    invokeText,
    ready,
    cleanup,
  };
});

export type Run = Effect.Success<ReturnType<typeof makeRun>>;

/** Release runs on success, failure and interruption; cleanup failures remain typed. */
export const withRun = <A, E, R>(out: string, use: (run: Run) => Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(makeRun(out), use, (run, exit) =>
    (Exit.isFailure(exit)
      ? save(`${run.out}/failure.json`, { error: run.redact(Cause.pretty(exit.cause)) })
      : Effect.void
    ).pipe(Effect.onExit(() => run.cleanup)),
  );
