import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

import { NodeServices } from "@effect/platform-node";
import { it } from "@effect/vitest";
import {
  Cause,
  Clock,
  Config,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Redacted,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { Evidence, type Receipt, RunId, WorkerFailure } from "../src/proof.ts";
import { Failure, GateFailure, fail, request as checkoutRequest } from "./request.ts";

const Commit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));

const WorkerBindings = Schema.Struct({
  CHECKOUT_RUN_ID: RunId,
  CHECKOUT_TOKEN: Schema.String,
  CHECKOUT_PASSWORD: Schema.String,
  CHECKOUT_MODEL: Schema.String,
  OPENAI_API_KEY: Schema.String,
  CLOUDFLARE_ACCOUNT_ID: Schema.String,
  BROWSER_RENDERING_API_TOKEN: Schema.String,
});

const AccountSubdomain = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({ subdomain: Schema.String }),
});

const safeFailure = <E>(cause: Cause.Cause<E>) => {
  const error = Cause.findErrorOption(cause);

  return Option.isSome(error) && Schema.is(GateFailure)(error.value)
    ? error.value
    : fail("interrupted-or-unexpected");
};

const Report = Schema.Struct({
  version: Schema.Literal(4),
  runId: RunId,
  workerName: Schema.String,
  sourceCommit: Commit,
  model: Schema.String,
  dispatched: Schema.Boolean,
  passed: Schema.Boolean,
  evidence: Schema.NullOr(Evidence),
  failure: Schema.NullOr(Failure),
  cleanupFailure: Schema.NullOr(Failure),
  cleanup: Schema.Literals(["pending", "browsers-closed", "confirmed", "failed"]),
  timings: Schema.Struct({
    totalMillis: Schema.Natural,
    checkoutMillis: Schema.Natural,
    cleanupMillis: Schema.Natural,
  }),
});

// Independent oracle: never derive expected values from the receiver or its receipt schema.
const expectedReceipt = {
  buyer: "buyer@example.test",
  product: "everyday-shirt",
  color: "blue",
  size: "M",
  quantity: 1,
  address: "123 Test Street, San Francisco, CA 94107, US",
  shipping: "standard",
  subtotal: 3400,
  shippingCents: 500,
  tax: 312,
  total: 4212,
  currency: "USD",
  paid: true,
} satisfies typeof Receipt.Type;

const receiptMatches = (evidence: typeof Evidence.Type) =>
  isDeepStrictEqual(evidence.receipt, expectedReceipt);

// Persist only known fixture values. A malformed receipt or free-form failure is never published.
const publicEvidence = (evidence: typeof Evidence.Type): typeof Evidence.Type => ({
  phase: evidence.phase,
  started: evidence.started,
  attempts: evidence.attempts,
  receipt: receiptMatches(evidence) ? evidence.receipt : null,
  closed: evidence.closed,
  scrapeAttempts: evidence.scrapeAttempts,
  loginRequests: evidence.loginRequests,
  browserDiagnostics: evidence.browserDiagnostics,
  failure:
    evidence.failure === null
      ? null
      : Option.getOrElse(
          Schema.decodeUnknownOption(WorkerFailure)(evidence.failure),
          () => "worker-failure",
        ),
});

const proof = Effect.gen(function* () {
  const totalStart = yield* Clock.monotonicTimeNanos;
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const client = yield* HttpClient.HttpClient;

  const run = yield* Config.schema(RunId, "CHECKOUT_RUN_ID").pipe(
    Effect.mapError(() => fail("configuration:CHECKOUT_RUN_ID")),
  );

  const cleanupOnly = yield* Config.Boolean("CHECKOUT_CLEANUP").pipe(
    Config.withDefault(false),
    Effect.mapError(() => fail("configuration:CHECKOUT_CLEANUP")),
  );

  const expectedCommit = yield* Config.schema(Commit, "CHECKOUT_EXPECTED_SHA").pipe(
    Effect.mapError(() => fail("configuration:CHECKOUT_EXPECTED_SHA")),
  );

  const git = Effect.fnUntraced(function* (args: ReadonlyArray<string>) {
    return (yield* spawner
      .string(ChildProcess.make("git", args))
      .pipe(Effect.mapError(() => fail("revision:git")))).trim();
  });

  const sourceCommit = yield* git(["rev-parse", "HEAD"]);

  if (sourceCommit !== expectedCommit) return yield* fail("revision:sha-mismatch");
  if ((yield* git(["status", "--porcelain"])) !== "") return yield* fail("revision:dirty");
  if (yield* Config.Boolean("ALCHEMY_TEST_DEV").pipe(Config.withDefault(false)))
    return yield* fail("configuration:hosted-required");

  // Validate bindings before provisioning anything, including during recovery.
  for (const name of [
    "CHECKOUT_MODEL",
    "CHECKOUT_TOKEN",
    "CHECKOUT_PASSWORD",
    "OPENAI_API_KEY",
    "CLOUDFLARE_ACCOUNT_ID",
    "CLOUDFLARE_API_TOKEN",
    "BROWSER_RENDERING_API_TOKEN",
  ]) {
    const value = yield* Config.Redacted(name).pipe(
      Effect.mapError(() => fail("configuration:" + name)),
    );

    if (Redacted.value(value).length === 0) return yield* fail("configuration:" + name);
  }

  const subdomain = yield* Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/)),
    "CLOUDFLARE_WORKERS_SUBDOMAIN",
  ).pipe(Effect.mapError(() => fail("configuration:CLOUDFLARE_WORKERS_SUBDOMAIN")));

  const model = yield* Config.NonEmptyString("CHECKOUT_MODEL");
  const token = yield* Config.Redacted("CHECKOUT_TOKEN");
  const cloudflareToken = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");
  const account = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");
  const workerName = "ea-checkout-" + run;
  const origin = "https://" + workerName + "." + subdomain + ".workers.dev";
  const directory = ".checkout-proof/" + run;
  const reportPath = directory + "/report.json";
  const accountUrl = "https://api.cloudflare.com/client/v4/accounts/" + account;
  const workerUrl = accountUrl + "/workers/scripts/" + workerName;
  let report: typeof Report.Type;

  const workerStatus = client
    .execute(HttpClientRequest.get(workerUrl).pipe(HttpClientRequest.bearerToken(cloudflareToken)))
    .pipe(
      Effect.map((response) => response.status),
      Effect.timeout("30 seconds"),
      Effect.mapError(() => fail("worker-status")),
    );

  // Recovery must confirm closure on the same account whose Worker it retires.
  yield* Effect.gen(function* () {
    const response = yield* client.execute(
      HttpClientRequest.get(accountUrl + "/workers/subdomain").pipe(
        HttpClientRequest.bearerToken(cloudflareToken),
      ),
    );

    if (response.status !== 200) return yield* fail("configuration:subdomain", response.status);

    const observed = yield* response.text.pipe(
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(AccountSubdomain))),
    );

    if (observed.result.subdomain !== subdomain)
      return yield* fail("configuration:origin-mismatch");
  }).pipe(
    Effect.timeout("30 seconds"),
    Effect.mapError((error) =>
      Schema.is(GateFailure)(error) ? error : fail("configuration:subdomain"),
    ),
  );

  const deploy = Effect.gen(function* () {
    const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "checkout-worker-" });
    const secretsPath = temporary + "/bindings.json";

    const bindings = yield* Schema.encodeEffect(Schema.fromJsonString(WorkerBindings))({
      CHECKOUT_RUN_ID: run,
      CHECKOUT_TOKEN: Redacted.value(token),
      CHECKOUT_PASSWORD: Redacted.value(yield* Config.Redacted("CHECKOUT_PASSWORD")),
      CHECKOUT_MODEL: model,
      OPENAI_API_KEY: Redacted.value(yield* Config.Redacted("OPENAI_API_KEY")),
      CLOUDFLARE_ACCOUNT_ID: account,
      BROWSER_RENDERING_API_TOKEN: Redacted.value(
        yield* Config.Redacted("BROWSER_RENDERING_API_TOKEN"),
      ),
    });

    yield* fs.writeFileString(secretsPath, bindings, { mode: 0o600 });

    // A single upload prevents the DO from serving an earlier empty bootstrap class.
    const child = yield* ChildProcess.make(
      "vp",
      [
        "exec",
        "wrangler",
        "deploy",
        "--config",
        fileURLToPath(new URL("../wrangler.jsonc", import.meta.url)),
        "--name",
        workerName,
        "--secrets-file",
        secretsPath,
        "--no-autoconfig",
      ],
      {
        cwd: fileURLToPath(new URL("..", import.meta.url)),
        env: {
          CI: "true",
          CLOUDFLARE_ACCOUNT_ID: account,
          CLOUDFLARE_API_TOKEN: Redacted.value(cloudflareToken),
          WRANGLER_SEND_METRICS: "false",
          WRANGLER_WRITE_LOGS: "false",
        },
        extendEnv: true,
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      },
    );

    yield* Stream.runDrain(child.all);
    if ((yield* child.exitCode) !== 0) return yield* fail("deployment:wrangler");
  }).pipe(
    Effect.scoped,
    Effect.timeout("5 minutes"),
    Effect.mapError((error) => (Schema.is(GateFailure)(error) ? error : fail("deployment"))),
  );

  const save = Effect.fnUntraced(
    function* () {
      const json = yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report);

      yield* fs.writeFileString(reportPath + ".tmp", json, { mode: 0o600 });
      yield* fs.rename(reportPath + ".tmp", reportPath);
    },
    Effect.mapError(() => fail("report-write")),
  );

  const measure = Effect.fnUntraced(function* <A, E, R>(
    phase: "checkoutMillis" | "cleanupMillis",
    effect: Effect.Effect<A, E, R>,
  ) {
    if (cleanupOnly) return yield* effect;
    const start = yield* Clock.monotonicTimeNanos;

    return yield* effect.pipe(
      Effect.onExit(() =>
        Effect.gen(function* () {
          const millis = Number((yield* Clock.monotonicTimeNanos) - start) / 1_000_000;

          report = { ...report, timings: { ...report.timings, [phase]: Math.round(millis) } };
          yield* save();
        }),
      ),
    );
  });

  const request = (operation: "run" | "evidence" | "close") =>
    checkoutRequest({ operation, origin, token, runId: run });

  // Fresh routes may be temporarily unavailable. Only reads and idempotent closure may repeat.
  const repeatableRequest = Effect.fnUntraced(function* (operation: "evidence" | "close") {
    let lastFailure: GateFailure | undefined;

    return yield* request(operation).pipe(
      Effect.timeout("10 seconds"),
      Effect.tapError((error) =>
        Effect.sync(() => {
          if (Schema.is(GateFailure)(error)) lastFailure = error;
        }),
      ),
      Effect.retry({
        schedule: Schedule.spaced("2 seconds"),
        while: (error) =>
          error._tag === "TimeoutError" ||
          error.stage === operation + ":transport-or-timeout" ||
          (error.stage === operation &&
            error.status !== null &&
            (error.status === 404 || (error.status >= 500 && error.status <= 599))),
      }),
      Effect.timeout("1 minute"),
      Effect.mapError((error) =>
        error._tag === "TimeoutError"
          ? fail(operation + ":retry-timeout", lastFailure?.status, lastFailure?.response)
          : error,
      ),
    );
  });

  if (cleanupOnly) {
    report = yield* fs.readFileString(reportPath).pipe(
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))),
      Effect.mapError(() => fail("recovery:report")),
    );
    if (
      report.runId !== run ||
      report.workerName !== workerName ||
      report.sourceCommit !== sourceCommit
    )
      return yield* fail("recovery:identity-mismatch");
  } else {
    if (yield* fs.exists(reportPath)) return yield* fail("run-id:already-recorded");
    const status = yield* workerStatus;

    if (status !== 404) return yield* fail("worker-name:unavailable", status);
    yield* fs.makeDirectory(directory, { recursive: true });
    report = {
      version: 4,
      runId: run,
      workerName,
      sourceCommit,
      model,
      dispatched: false,
      passed: false,
      evidence: null,
      failure: null,
      cleanupFailure: null,
      cleanup: "pending",
      timings: { totalMillis: 0, checkoutMillis: 0, cleanupMillis: 0 },
    };
    yield* save();
  }

  const retire = measure(
    "cleanupMillis",
    Effect.gen(function* () {
      const status = yield* workerStatus;

      if (status === 200) {
        // Closing is idempotent; unlike /run, explicit recovery may repeat it.
        const closed = yield* repeatableRequest("close");

        report = { ...report, evidence: publicEvidence(closed.evidence) };
        yield* save();
        if (!closed.evidence.closed) return yield* fail("cleanup:closure-unconfirmed");
        const observed = yield* repeatableRequest("evidence");

        report = { ...report, evidence: publicEvidence(observed.evidence) };
        yield* save();
        if (!observed.evidence.closed) return yield* fail("cleanup:closure-unconfirmed");
      } else if (status !== 404) {
        return yield* fail("cleanup:worker-status", status);
      } else if (report.dispatched && report.evidence?.closed !== true) {
        return yield* fail("cleanup:owner-absent-without-closure");
      }
      // Persist closure before teardown. A failed close preserves the owner and its alarm.
      report = { ...report, cleanup: "browsers-closed" };
      yield* save();

      const removed = yield* client
        .execute(
          HttpClientRequest.delete(workerUrl).pipe(
            HttpClientRequest.setUrlParam("force", "true"),
            HttpClientRequest.bearerToken(cloudflareToken),
          ),
        )
        .pipe(Effect.mapError(() => fail("cleanup:destroy")));

      if (removed.status !== 200 && removed.status !== 404)
        return yield* fail("cleanup:destroy", removed.status);
      const absent = yield* workerStatus;

      if (absent !== 404) return yield* fail("cleanup:worker-remains", absent);
      report = { ...report, cleanup: "confirmed" };
      yield* save();
    }).pipe(
      Effect.timeout("5 minutes"),
      Effect.onExit((exit) =>
        Exit.isFailure(exit)
          ? Effect.gen(function* () {
              const failure = safeFailure(exit.cause);

              report = {
                ...report,
                passed: false,
                cleanup: "failed",
                failure: report.failure ?? failure,
                cleanupFailure: failure,
              };
              yield* save();
            })
          : Effect.void,
      ),
    ),
  );

  const checkout = Effect.gen(function* () {
    yield* deploy;
    // Wait for the fresh namespace's authenticated receiver before the one-shot dispatch.
    yield* repeatableRequest("evidence").pipe(
      Effect.filterOrFail(
        ({ evidence }) =>
          !evidence.started &&
          evidence.attempts === 0 &&
          evidence.receipt === null &&
          evidence.closed &&
          evidence.scrapeAttempts === 0 &&
          evidence.loginRequests === 0 &&
          evidence.failure === null,
        () => fail("readiness:owner-not-pristine"),
      ),
      Effect.mapError((error) => fail("readiness", error.status, error.response)),
    );
    // Conservatively persist before sending, including throughout edge-only admission retries.
    report = { ...report, dispatched: true };
    yield* save();
    const executed = yield* measure("checkoutMillis", request("run")).pipe(Effect.exit);

    // Keep the one-shot outcome authoritative even if the subsequent observation is unavailable.
    if (Exit.isFailure(executed)) {
      if (safeFailure(executed.cause).stage === "run:not-dispatched") {
        report = { ...report, dispatched: false };
        yield* save();
      }

      return yield* Effect.failCause(executed.cause);
    }
    const runEvidence = publicEvidence(executed.value.evidence);

    report = { ...report, evidence: runEvidence };
    yield* save();
    if (executed.value.status !== 200)
      return yield* fail(
        `run:${runEvidence.phase}:${runEvidence.failure}`,
        executed.value.status,
        executed.value.response,
      );
    const observed = yield* repeatableRequest("evidence");

    report = { ...report, evidence: publicEvidence(observed.evidence) };
    yield* save();
    const evidence = observed.evidence;

    if (
      !evidence.started ||
      !evidence.closed ||
      evidence.attempts !== 1 ||
      !receiptMatches(evidence)
    )
      return yield* fail("receipt:mismatch");
    if (evidence.scrapeAttempts < 1 || evidence.scrapeAttempts > 3 || evidence.failure !== null)
      return yield* fail("checkout:worker-failure");
    if (evidence.loginRequests < 2) return yield* fail("readiness:login-not-recovered");
  });

  yield* checkout.pipe(
    Effect.when(Effect.succeed(!cleanupOnly)),
    Effect.onExit((exit) =>
      Exit.isFailure(exit)
        ? Effect.gen(function* () {
            report = { ...report, passed: false, failure: safeFailure(exit.cause) };
            yield* save();
          })
        : Effect.void,
    ),
    Effect.onExit(() => retire),
    Effect.onExit(() =>
      Effect.gen(function* () {
        if (!cleanupOnly) {
          const millis = Number((yield* Clock.monotonicTimeNanos) - totalStart) / 1_000_000;

          report = { ...report, timings: { ...report.timings, totalMillis: Math.round(millis) } };
        }
        yield* save();
      }),
    ),
  );
  if (!cleanupOnly) {
    const evidence = report.evidence;

    if (
      report.cleanup !== "confirmed" ||
      evidence === null ||
      !evidence.closed ||
      !evidence.started ||
      evidence.attempts !== 1 ||
      !receiptMatches(evidence) ||
      evidence.failure !== null
    ) {
      const failure = fail("checkout:incomplete");

      report = { ...report, failure };
      yield* save();

      return yield* failure;
    }
    report = { ...report, passed: true };
    yield* save();
  }
}).pipe(
  Effect.catchCause((cause) => Effect.fail(safeFailure(cause))),
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
);

it.live("submits once and verifies the independent receipt and browser closure", () =>
  proof.pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
);
