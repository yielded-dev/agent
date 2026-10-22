import {
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionRequest,
  CodeExecutor,
  CodeHostCallFailure,
  CodeHostCallSuccess,
  type CodeExecutionError,
  type CodeExecutionResult,
  type CodeHostCall,
  type CodeHostCallResult,
} from "@yielded/agent/code-executor";
import {
  NetworkAllowlist,
  NetworkDisabled,
  type SandboxImplementation,
} from "@yielded/agent/sandbox";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

/**
 * Reusable `CodeExecutor` behavior checks (TEST-015). These checks exercise values, errors,
 * host calls, limits and interruption; they do not prove isolation. Actual ambient network
 * denial and synchronous CPU runaway termination require checks against the isolated adapter.
 * Implementation metadata is compared with the adapter's declaration, not independently verified.
 *
 * Cases assume the live `Clock` (the wall-clock case uses a short real deadline) and take one
 * fresh executor pass per case, so a suite may share one executor Layer across cases.
 */
export class CodeExecutorConformanceViolation extends Schema.TaggedError<CodeExecutorConformanceViolation>()(
  "CodeExecutorConformanceViolation",
  {
    caseName: Schema.String,
    message: Schema.String,
  },
) {}

export interface CodeExecutorConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<void, CodeExecutorConformanceViolation, CodeExecutor>;
}

export interface CodeExecutorConformanceOptions {
  /** The declared implementation metadata the adapter must stamp on results and errors. */
  readonly implementation: SandboxImplementation;
}

const baseLimits = CodeExecutionLimits.make({
  maxSourceBytes: 64 * 1024,
  maxWallTime: Duration.seconds(10),
  maxLogBytes: 16 * 1024,
  maxResultBytes: 64 * 1024,
  maxHostCalls: 8,
  maxHostCallArgumentBytes: 16 * 1024,
  maxHostCallResultBytes: 32 * 1024,
});

const warehouseNamespace = CodeExecutionNamespace.make({
  name: "warehouse",
  methods: ["query", "count"],
});

const makeRequest = (
  source: string,
  overrides?: {
    readonly limits?: CodeExecutionLimits;
    readonly namespaces?: ReadonlyArray<CodeExecutionNamespace>;
    readonly network?: CodeExecutionRequest["network"];
  },
): CodeExecutionRequest =>
  CodeExecutionRequest.make({
    language: "javascript",
    source,
    namespaces: overrides?.namespaces ?? [],
    network: overrides?.network ?? NetworkDisabled.make(),
    limits: overrides?.limits ?? baseLimits,
  });

const unusedHost: CodeExecutionHost["Service"] = {
  call: () =>
    Effect.die(
      // oxlint-disable-next-line effect/no-native-error -- Conformance assertion preserves its native defect when an unexpected host call escapes.
      new Error("this conformance case expected no host call to reach the CodeExecutionHost"),
    ),
};

const respondingHost = (
  respond: (call: CodeHostCall) => CodeHostCallResult,
): { readonly host: CodeExecutionHost["Service"]; readonly calls: Array<CodeHostCall> } => {
  const calls: Array<CodeHostCall> = [];

  return {
    calls,
    host: {
      call: (call) =>
        Effect.sync(() => {
          calls.push(call);

          return respond(call);
        }),
    },
  };
};

const runPass = (
  request: CodeExecutionRequest,
  host: CodeExecutionHost["Service"],
): Effect.Effect<CodeExecutionResult, CodeExecutionError, CodeExecutor> =>
  Effect.gen(function* () {
    const executor = yield* CodeExecutor;

    return yield* executor
      .execute(request)
      .pipe(Effect.provideService(CodeExecutionHost, CodeExecutionHost.of(host)));
  }).pipe(Effect.scoped);

const violation = (caseName: string, message: string) =>
  CodeExecutorConformanceViolation.make({ caseName, message });

const preview = (value: unknown): string => {
  try {
    return JSON.stringify(value)?.slice(0, 200) ?? String(value).slice(0, 200);
  } catch {
    return String(value).slice(0, 200);
  }
};

const expectSuccess = (
  caseName: string,
  request: CodeExecutionRequest,
  host: CodeExecutionHost["Service"],
  check: (result: CodeExecutionResult) => string | undefined,
): Effect.Effect<void, CodeExecutorConformanceViolation, CodeExecutor> =>
  runPass(request, host).pipe(
    Effect.mapError((error) =>
      violation(caseName, `expected success, got ${error._tag}: ${preview(error)}`),
    ),
    Effect.flatMap((result) => {
      const complaint = check(result);

      return complaint === undefined ? Effect.void : Effect.fail(violation(caseName, complaint));
    }),
  );

const expectFailure = (
  caseName: string,
  request: CodeExecutionRequest,
  host: CodeExecutionHost["Service"],
  tag: CodeExecutionError["_tag"],
  check?: (error: CodeExecutionError) => string | undefined,
): Effect.Effect<void, CodeExecutorConformanceViolation, CodeExecutor> =>
  runPass(request, host).pipe(
    Effect.flip,
    Effect.mapError((result) =>
      violation(caseName, `expected ${tag}, but the pass succeeded with ${preview(result.value)}`),
    ),
    Effect.flatMap((error) => {
      if (error._tag !== tag) {
        return Effect.fail(
          violation(caseName, `expected ${tag}, got ${error._tag}: ${preview(error)}`),
        );
      }
      const complaint = check?.(error);

      return complaint === undefined ? Effect.void : Effect.fail(violation(caseName, complaint));
    }),
  );

export const codeExecutorConformanceCases = (
  options: CodeExecutorConformanceOptions,
): ReadonlyArray<CodeExecutorConformanceCase> => {
  const implementation = options.implementation;

  return [
    {
      name: "TEST-015 executes bounded JSON computation and returns the program value",
      run: expectSuccess(
        "TEST-015 executes bounded JSON computation and returns the program value",
        makeRequest(
          "async () => { const xs = [1, 2, 3].map((n) => n * 2); return { xs, sum: xs.reduce((a, b) => a + b, 0) }; }",
        ),
        unusedHost,
        (result) => {
          if (JSON.stringify(result.value) !== JSON.stringify({ xs: [2, 4, 6], sum: 12 })) {
            return `unexpected program value ${preview(result.value)}`;
          }
          if (
            result.implementation.isolation !== implementation.isolation ||
            result.implementation.identity !== implementation.identity
          ) {
            return `result implementation ${preview(result.implementation)} does not match the declared ${preview(implementation)}`;
          }

          return undefined;
        },
      ),
    },
    {
      name: "TEST-015 routes host calls through the CodeExecutionHost in program order",
      run: Effect.gen(function* () {
        const caseName =
          "TEST-015 routes host calls through the CodeExecutionHost in program order";

        const { host, calls } = respondingHost((call) =>
          call.method === "query"
            ? CodeHostCallSuccess.make({ value: { rows: [1, 2, 3] } })
            : CodeHostCallSuccess.make({ value: 3 }),
        );

        const result = yield* runPass(
          makeRequest(
            "async () => { const q = await warehouse.query({ sql: 'select' }); const c = await warehouse.count({ table: 't' }); return { rows: q.rows, count: c }; }",
            { namespaces: [warehouseNamespace] },
          ),
          host,
        ).pipe(
          Effect.mapError((error) =>
            violation(caseName, `expected success, got ${error._tag}: ${preview(error)}`),
          ),
        );

        if (JSON.stringify(result.value) !== JSON.stringify({ rows: [1, 2, 3], count: 3 })) {
          return yield* violation(caseName, `unexpected value ${preview(result.value)}`);
        }
        const observed = calls.map((call) => `${call.namespace}.${call.method}`);

        if (JSON.stringify(observed) !== JSON.stringify(["warehouse.query", "warehouse.count"])) {
          return yield* violation(caseName, `unexpected host call order ${preview(observed)}`);
        }
        if (result.resourceUse.hostCalls !== 2) {
          return yield* violation(
            caseName,
            `expected 2 accounted host calls, got ${result.resourceUse.hostCalls}`,
          );
        }
      }),
    },
    {
      name: "TEST-015 a caught failed host call lets the program branch on the envelope",
      run: expectSuccess(
        "TEST-015 a caught failed host call lets the program branch on the envelope",
        makeRequest(
          "async () => { try { await warehouse.query({ sql: 'x' }); return 'unreachable'; } catch (envelope) { return { caught: envelope }; } }",
          { namespaces: [warehouseNamespace] },
        ),
        respondingHost(() =>
          CodeHostCallFailure.make({ error: { _tag: "ToolInputError", message: "bad input" } }),
        ).host,
        (result) =>
          JSON.stringify(result.value) ===
          JSON.stringify({ caught: { _tag: "ToolInputError", message: "bad input" } })
            ? undefined
            : `the envelope did not round-trip: ${preview(result.value)}`,
      ),
    },
    {
      name: "TEST-015 an uncaught failed host call fails the program with the envelope",
      run: expectFailure(
        "TEST-015 an uncaught failed host call fails the program with the envelope",
        makeRequest("async () => warehouse.query({ sql: 'x' })", {
          namespaces: [warehouseNamespace],
        }),
        respondingHost(() =>
          CodeHostCallFailure.make({ error: { _tag: "PolicyDenied", message: "denied" } }),
        ).host,
        "CodeProgramFailedError",
        (error) =>
          error._tag === "CodeProgramFailedError" &&
          error.reason === "rejected" &&
          JSON.stringify(error.thrown) ===
            JSON.stringify({ _tag: "PolicyDenied", message: "denied" })
            ? undefined
            : `unexpected failure detail ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 fails typed on syntactically invalid source",
      run: expectFailure(
        "TEST-015 fails typed on syntactically invalid source",
        makeRequest("async () => {"),
        unusedHost,
        "CodeSourceError",
        (error) => {
          if (error._tag !== "CodeSourceError" || error.reason !== "invalid") {
            return `expected reason invalid, got ${preview(error)}`;
          }
          if (
            error.implementation === undefined ||
            error.implementation.isolation !== implementation.isolation ||
            error.implementation.identity !== implementation.identity
          ) {
            return `error implementation ${preview(error.implementation)} does not match the declared ${preview(implementation)}`;
          }

          return undefined;
        },
      ),
    },
    {
      name: "TEST-015 fails typed when the expression is not one async function",
      run: expectFailure(
        "TEST-015 fails typed when the expression is not one async function",
        makeRequest("1 + 1"),
        unusedHost,
        "CodeSourceError",
        (error) =>
          error._tag === "CodeSourceError" && error.reason === "not-a-function"
            ? undefined
            : `expected reason not-a-function, got ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 fails typed on source larger than the declared byte limit",
      run: expectFailure(
        "TEST-015 fails typed on source larger than the declared byte limit",
        makeRequest(`async () => "${"x".repeat(2_000)}"`, {
          limits: CodeExecutionLimits.make({ ...baseLimits, maxSourceBytes: 256 }),
        }),
        unusedHost,
        "CodeSourceError",
        (error) =>
          error._tag === "CodeSourceError" && error.reason === "oversized"
            ? undefined
            : `expected reason oversized, got ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 terminates a never-settling program at the wall-clock deadline",
      run: expectFailure(
        "TEST-015 terminates a never-settling program at the wall-clock deadline",
        makeRequest("async () => { await new Promise(() => {}); return 1; }", {
          limits: CodeExecutionLimits.make({ ...baseLimits, maxWallTime: Duration.millis(250) }),
        }),
        unusedHost,
        "CodeExecutionTimeoutError",
      ),
    },
    {
      name: "TEST-015 fails typed when console output exceeds its byte budget",
      run: expectFailure(
        "TEST-015 fails typed when console output exceeds its byte budget",
        makeRequest(
          "async () => { for (let i = 0; i < 64; i += 1) { console.log('x'.repeat(256)); } return 1; }",
          { limits: CodeExecutionLimits.make({ ...baseLimits, maxLogBytes: 2_048 }) },
        ),
        unusedHost,
        "CodeOutputLimitError",
        (error) =>
          error._tag === "CodeOutputLimitError" && error.surface === "logs"
            ? undefined
            : `expected surface logs, got ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 fails typed when the final result exceeds its byte budget",
      run: expectFailure(
        "TEST-015 fails typed when the final result exceeds its byte budget",
        makeRequest("async () => 'y'.repeat(4096)", {
          limits: CodeExecutionLimits.make({ ...baseLimits, maxResultBytes: 1_024 }),
        }),
        unusedHost,
        "CodeOutputLimitError",
        (error) =>
          error._tag === "CodeOutputLimitError" && error.surface === "result"
            ? undefined
            : `expected surface result, got ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 fails typed when host calls exceed the executor cap",
      run: Effect.gen(function* () {
        const caseName = "TEST-015 fails typed when host calls exceed the executor cap";
        const { host, calls } = respondingHost(() => CodeHostCallSuccess.make({ value: null }));

        yield* expectFailure(
          caseName,
          makeRequest(
            "async () => { await warehouse.query({}); await warehouse.query({}); await warehouse.query({}); return 1; }",
            {
              namespaces: [warehouseNamespace],
              limits: CodeExecutionLimits.make({ ...baseLimits, maxHostCalls: 2 }),
            },
          ),
          host,
          "CodeHostCallLimitError",
        );
        // The cap is enforced before dispatch: the over-limit call must never
        // have reached the host, or an unauthorized side effect already ran.
        if (calls.length !== 2) {
          return yield* violation(
            caseName,
            `expected exactly 2 dispatched host calls under a cap of 2, observed ${calls.length}`,
          );
        }
      }),
    },
    {
      name: "TEST-015 fails typed on a host outcome outside the protocol schema",
      run: expectFailure(
        "TEST-015 fails typed on a host outcome outside the protocol schema",
        makeRequest("async () => warehouse.query({})", { namespaces: [warehouseNamespace] }),
        {
          // Deliberately violate the compile-time host contract to verify that an adapter
          // independently decodes the runtime protocol boundary.
          call: () => Effect.succeed({ bogus: true } as unknown as CodeHostCallResult),
        },
        "CodeExecutionProtocolError",
      ),
    },
    {
      name: "TEST-015 surfaces an uncaught program throw with its bounded log capture",
      run: expectFailure(
        "TEST-015 surfaces an uncaught program throw with its bounded log capture",
        makeRequest(
          "async () => { console.log('before the failure'); throw new Error('deliberate'); }",
        ),
        unusedHost,
        "CodeProgramFailedError",
        (error) =>
          error._tag === "CodeProgramFailedError" &&
          error.reason === "threw" &&
          error.logs.some((line) => line.includes("before the failure"))
            ? undefined
            : `expected a threw failure carrying the log capture, got ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 fails typed when the program returns a non-JSON value",
      run: expectFailure(
        "TEST-015 fails typed when the program returns a non-JSON value",
        makeRequest("async () => (() => 1)"),
        unusedHost,
        "CodeProgramFailedError",
        (error) =>
          error._tag === "CodeProgramFailedError" && error.reason === "non-json-result"
            ? undefined
            : `expected reason non-json-result, got ${preview(error)}`,
      ),
    },
    {
      name: "CAP-015 rejects a network allowlist it cannot enforce with a typed unsupported error",
      run: expectFailure(
        "CAP-015 rejects a network allowlist it cannot enforce with a typed unsupported error",
        makeRequest("async () => 1", {
          network: NetworkAllowlist.make({ domains: ["example.com"], ports: [443] }),
        }),
        unusedHost,
        "CodeExecutorUnsupportedError",
        (error) =>
          error._tag === "CodeExecutorUnsupportedError" && error.feature === "network"
            ? undefined
            : `expected feature network, got ${preview(error)}`,
      ),
    },
    {
      name: "TEST-015 interruption reaches an in-flight host call",
      run: Effect.gen(function* () {
        const caseName = "TEST-015 interruption reaches an in-flight host call";
        const started = yield* Deferred.make<void>();
        const witness = { hostCallInterrupted: false };

        const host: CodeExecutionHost["Service"] = {
          call: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  witness.hostCallInterrupted = true;
                }),
              ),
            ),
        };

        const fiber = yield* runPass(
          makeRequest("async () => warehouse.query({})", { namespaces: [warehouseNamespace] }),
          host,
        ).pipe(Effect.forkChild);

        // Guard against a broken adapter that settles the pass without ever
        // reaching the host: the case must report a violation, not hang.
        const winner = yield* Effect.raceFirst(
          Deferred.await(started).pipe(Effect.as("started" as const)),
          Fiber.join(fiber).pipe(Effect.exit, Effect.as("exited" as const)),
        );

        if (winner === "exited") {
          return yield* violation(
            caseName,
            "the pass settled before any host call reached the CodeExecutionHost",
          );
        }
        yield* Fiber.interrupt(fiber);
        if (!witness.hostCallInterrupted) {
          return yield* violation(
            caseName,
            "interrupting the pass did not interrupt the in-flight host call",
          );
        }
      }),
    },
  ];
};
