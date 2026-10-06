import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { NodeDurableAgentRuntime } from "@yielded/agent-platform-node/node-durable-agent-runtime";
import * as SqliteThreadStore from "@yielded/agent-storage-sqlite/sqlite-thread-store";
import {
  ObligationThresholds,
  RecoveryExplanation,
  RetryCommand,
  renderRecoveryExplanation,
  type IntegrityReport,
  type ObligationReport,
} from "@yielded/agent/admin";
import { DurableAgentRuntime, type RecoveryReport } from "@yielded/agent/durable-agent-runtime";
import { ThreadId, SubmissionId } from "@yielded/agent/identifiers";
import {
  reencodeThread,
  ThreadArchive,
  ThreadImport,
  ThreadImportRejected,
  ThreadImportResult,
} from "@yielded/agent/thread-import";
import {
  ThreadExport,
  MAX_THREAD_EXPORT_PAGE_BYTES,
  streamExport,
} from "@yielded/agent/thread-store";
import { Console, Effect, FileSystem, Option, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/cli";

/**
 * P7 operator CLI (plan §3): explain | verify | retry | wake | obligations over the DN
 * platform Layers. The stack opens the SAME SQLite file the host uses — run it against a live
 * deployment's database only for the read-only subcommands (explain/verify/obligations), and
 * prefer a quiesced host for `retry`. No worker loop starts and no startup recovery pass runs:
 * the Layer assembles the coordinator only, so read-only subcommands perform zero writes
 * (opening a brand-new file would create empty schema tables — point `--database` at an
 * existing deployment file).
 */

class InvalidIdentifier extends Schema.TaggedError<InvalidIdentifier>()("InvalidIdentifier", {
  kind: Schema.String,
  value: Schema.String,
}) {
  override get message() {
    return `${this.value} is not a valid ${this.kind}`;
  }
}

class MissingSelector extends Schema.TaggedError<MissingSelector>()("MissingSelector", {
  message: Schema.String,
}) {}

class IntegrityViolation extends Schema.TaggedError<IntegrityViolation>()("IntegrityViolation", {
  threadId: Schema.String,
  failed: Schema.Array(Schema.String),
}) {
  override get message() {
    return `Integrity verification failed for ${this.threadId}: ${this.failed.join("; ")}`;
  }
}

const decodeThreadId = (value: string) =>
  Schema.decodeEffect(ThreadId)(value).pipe(
    Effect.mapError(() => InvalidIdentifier.make({ kind: "ThreadId", value })),
  );

const decodeSubmissionId = (value: string) =>
  Schema.decodeEffect(SubmissionId)(value).pipe(
    Effect.mapError(() => InvalidIdentifier.make({ kind: "SubmissionId", value })),
  );

const encodeExplanation = Schema.encodeEffect(RecoveryExplanation);

const database = Flag.File("database").pipe(
  Flag.withDescription("SQLite database file of the DN deployment (Thread Log + ledger)."),
);

const json = Flag.Boolean("json").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Print Schema-encoded JSON instead of operator text."),
);

/** The admin CLI writes with its own producer identity; epoch fencing stays authoritative. */
const runtimeLayerFor = (filename: string) =>
  NodeDurableAgentRuntime.layer({
    filename,
    deploymentId: "durable-admin",
    producerId: "durable-admin",
  });

const admin = CliCommand.make("durable-admin").pipe(
  CliCommand.withSharedFlags({ database }),
  CliCommand.withDescription(
    "Administrative operations over a DN durable deployment: explain recovery state, verify " +
      "thread integrity, re-drive one recovery decision, nudge a lane, and report aged " +
      "settlement obligations.",
  ),
);

const withRuntime = <A, E>(effect: Effect.Effect<A, E, DurableAgentRuntime>) =>
  Effect.gen(function* () {
    const { database: filename } = yield* admin;

    return yield* effect.pipe(Effect.provide(runtimeLayerFor(filename)));
  });

const printExplanation = (explanation: RecoveryExplanation, asJson: boolean) =>
  asJson
    ? encodeExplanation(explanation).pipe(
        Effect.flatMap((encoded) => Console.log(JSON.stringify(encoded, null, 2))),
      )
    : Console.log(`${renderRecoveryExplanation(explanation)}\n`);

const explainCommand = CliCommand.make(
  "explain",
  {
    submission: Flag.String("submission").pipe(
      Flag.optional,
      Flag.withDescription("Explain one Submission by identity."),
    ),
    thread: Flag.String("thread").pipe(
      Flag.optional,
      Flag.withDescription("Explain every nonterminal Submission of one Thread lane."),
    ),
    json,
  },
  ({ thread, json: asJson, submission }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        if (submission._tag === "Some") {
          const submissionId = yield* decodeSubmissionId(submission.value);
          const explanation = yield* runtime.explain(submissionId);

          return yield* printExplanation(explanation, asJson);
        }
        if (thread._tag === "Some") {
          const threadId = yield* decodeThreadId(thread.value);
          const explanations = yield* runtime.explainThread(threadId);

          if (asJson) {
            const encoded = yield* Schema.encodeEffect(Schema.Array(RecoveryExplanation))(
              explanations,
            );

            return yield* Console.log(JSON.stringify(encoded, null, 2));
          }
          if (explanations.length === 0) {
            return yield* Console.log(`No nonterminal Submissions on thread ${threadId}.`);
          }

          return yield* Effect.forEach(
            explanations,
            (explanation) => printExplanation(explanation, false),
            { discard: true },
          );
        }

        return yield* MissingSelector.make({
          message: "Pass --submission <id> or --thread <id>.",
        });
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Read-only recovery explanation: classifier decision, meaning, and predicted disposition. " +
      "Performs zero writes.",
  ),
);

const renderReport = (report: IntegrityReport): ReadonlyArray<string> => [
  `Thread ${report.threadId}: ${report.recordCount} records, tail ${report.tailSequence}, ${report.submissionCount} submissions`,
  ...report.checks.map(
    (check) =>
      `  ${check.status === "passed" ? "PASS" : check.status === "failed" ? "FAIL" : "SKIP"} ${check.name}${
        check.detail === undefined ? "" : ` — ${check.detail}`
      }`,
  ),
  report.ok ? "OK — no integrity check failed." : "FAILED — see the checks above.",
];

const verifyCommand = CliCommand.make(
  "verify",
  {
    thread: Flag.String("thread").pipe(Flag.withDescription("Thread to verify.")),
  },
  ({ thread }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const threadId = yield* decodeThreadId(thread);
        const report = yield* runtime.verify(threadId);

        yield* Effect.forEach(renderReport(report), (line) => Console.log(line), {
          discard: true,
        });
        if (!report.ok) {
          return yield* IntegrityViolation.make({
            threadId: thread,
            failed: report.checks
              .filter((check) => check.status === "failed")
              .map((check) => check.name),
          });
        }
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Read-only integrity verification: typed per-check results, never a repair. Exits non-zero " +
      "when any check fails.",
  ),
);

const renderRetry = (report: RecoveryReport): string =>
  `Submission ${report.submissionId}: decision ${report.decision._tag} → disposition ${report.disposition}`;

const retryCommand = CliCommand.make(
  "retry",
  {
    submission: Flag.String("submission").pipe(
      Flag.withDescription("Submission whose classified recovery decision is re-driven."),
    ),
    author: Flag.String("author").pipe(
      Flag.withDescription("Mandatory audit author recorded with the operator action (SEC-011)."),
    ),
    reason: Flag.String("reason").pipe(
      Flag.withDescription("Mandatory audit reason recorded with the operator action (SEC-011)."),
    ),
  },
  ({ author, reason, submission }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const submissionId = yield* decodeSubmissionId(submission);

        const command = yield* Schema.decodeEffect(RetryCommand)({
          submissionId,
          author,
          reason,
        }).pipe(
          Effect.mapError(() =>
            InvalidIdentifier.make({ kind: "RetryCommand", value: `${author}/${reason}` }),
          ),
        );

        const report = yield* runtime.retry(command);

        yield* Console.log(renderRetry(report));
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Audited single-Submission re-drive of the classified recovery decision. Refuses typed for " +
      "settled work and for lanes owned by resolveUnknown/resolveApproval.",
  ),
);

const wakeCommand = CliCommand.make(
  "wake",
  {
    thread: Flag.String("thread").pipe(Flag.withDescription("Thread lane to nudge.")),
  },
  ({ thread }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const threadId = yield* decodeThreadId(thread);

        yield* runtime.wake(threadId);
        yield* Console.log(
          `Wake hint sent for ${threadId} (droppable by contract; workers' ledger scans stay authoritative).`,
        );
      }),
    ),
).pipe(CliCommand.withDescription("Send the documented operator liveness nudge to one lane."));

const renderObligations = (report: ObligationReport): ReadonlyArray<string> =>
  report.entries.length === 0
    ? ["No nonterminal Submissions — every accepted obligation is settled."]
    : report.entries.map(
        (entry) =>
          `${entry.severity.toUpperCase().padEnd(7)} ${entry.blockedOn.padEnd(15)} age=${String(entry.ageSeconds).padStart(6)}s ${entry.submissionId} (${entry.state}) on ${entry.threadId}`,
      );

const obligationsCommand = CliCommand.make(
  "obligations",
  {
    agingSeconds: Flag.Int("aging-seconds").pipe(
      Flag.withDefault(300),
      Flag.withDescription("Age (seconds) at which an obligation is reported aging."),
    ),
    overdueSeconds: Flag.Int("overdue-seconds").pipe(
      Flag.withDefault(3_600),
      Flag.withDescription("Age (seconds) at which an obligation is reported overdue."),
    ),
  },
  ({ agingSeconds, overdueSeconds }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const thresholds = yield* Schema.decodeEffect(ObligationThresholds)({
          agingSeconds,
          overdueSeconds,
        }).pipe(
          Effect.mapError(() =>
            InvalidIdentifier.make({
              kind: "ObligationThresholds",
              value: `${agingSeconds}/${overdueSeconds}`,
            }),
          ),
        );

        const report = yield* runtime.scanObligations(thresholds);

        yield* Effect.forEach(renderObligations(report), (line) => Console.log(line), {
          discard: true,
        });
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Scan-based settlement-obligation report (DUR-017/OPS-001): every nonterminal Submission " +
      "with what it is blocked on, its age, and a threshold-classified severity.",
  ),
);

/** Each source page pins the same canonical tail and independent fact revision. */
const sourcePages = (filename: string, threadId: ThreadId) =>
  streamExport(
    {
      export: (request) => SqliteThreadStore.exportThread({ filename }, request),
    },
    { threadId },
  );

/** NDJSON framing bounds an unfinished line before decoding its Schema. */
const archivePages = (fs: FileSystem.FileSystem, input: string) =>
  fs.stream(input).pipe(
    Stream.decodeText(),
    Stream.concat(Stream.succeed("\n")),
    Stream.mapAccumEffect(
      () => "",
      (pending, chunk) =>
        Effect.gen(function* () {
          const parts = chunk.split("\n");
          const first = parts.shift() ?? "";

          if (
            new TextEncoder().encode(pending).byteLength +
              new TextEncoder().encode(first).byteLength >
            MAX_THREAD_EXPORT_PAGE_BYTES
          )
            return yield* ThreadImportRejected.make({
              reason: "invalid-archive",
              message: "Archive page exceeds its byte bound",
            });
          const combined = pending + first;

          if (parts.length === 0) return [combined, []] as const;
          const remainder = parts.pop() ?? "";
          const lines = [combined, ...parts];

          for (const line of [...lines, remainder])
            if (new TextEncoder().encode(line).byteLength > MAX_THREAD_EXPORT_PAGE_BYTES)
              return yield* ThreadImportRejected.make({
                reason: "invalid-archive",
                message: "Archive page exceeds its byte bound",
              });

          return [remainder, lines.filter((line) => line.length > 0)] as const;
        }),
    ),
    Stream.mapEffect((line) =>
      Schema.decodeEffect(Schema.fromJsonString(ThreadArchive))(line).pipe(
        Effect.mapError(() =>
          ThreadImportRejected.make({
            reason: "invalid-archive",
            message: "Invalid current-format archive page",
          }),
        ),
      ),
    ),
  );

const exportCommand = CliCommand.make(
  "export",
  {
    thread: Flag.String("thread"),
    output: Flag.String("output").pipe(
      Flag.withDescription("New archive file; existing files are refused."),
    ),
  },
  ({ thread, output }) =>
    Effect.gen(function* () {
      const { database: filename } = yield* admin;
      const fs = yield* FileSystem.FileSystem;
      const threadId = yield* decodeThreadId(thread);

      yield* Effect.scoped(
        Effect.gen(function* () {
          const file = yield* fs.open(output, { flag: "wx" });
          let complete = false;
          let records = 0;

          yield* Effect.addFinalizer(() =>
            complete ? Effect.void : fs.remove(output).pipe(Effect.ignore),
          );
          yield* Stream.runForEach(sourcePages(filename, threadId), (page) =>
            Effect.gen(function* () {
              const text = yield* Schema.encodeEffect(Schema.fromJsonString(ThreadExport))(page);

              yield* file.writeAll(new TextEncoder().encode(`${text}\n`));
              records += page.records.length;
            }),
          );
          complete = true;
          yield* Console.log(`Exported ${records} records to ${output}`);
        }),
      );
    }),
).pipe(
  CliCommand.withDescription(
    "Back up one quiesced Thread, including immutable admission facts; never change the source.",
  ),
);

const importCommand = CliCommand.make(
  "import",
  {
    input: Flag.File("input").pipe(
      Flag.withDescription("Thread archive to restore into --database."),
    ),
  },
  ({ input }) =>
    Effect.gen(function* () {
      const { database: filename } = yield* admin;
      const fs = yield* FileSystem.FileSystem;

      const result = yield* Effect.flatMap(ThreadImport, (target) =>
        target.import(archivePages(fs, input)),
      ).pipe(Effect.provide(SqliteThreadStore.layer({ filename })));

      yield* Console.log(
        yield* Schema.encodeEffect(Schema.fromJsonString(ThreadImportResult))(result),
      );
    }),
).pipe(
  CliCommand.withDescription(
    "Restore an archive into an empty Thread and rebuild its ledger; refuse existing work.",
  ),
);

const reencodeCommand = CliCommand.make(
  "reencode",
  {
    thread: Flag.String("thread"),
    target: Flag.String("target").pipe(
      Flag.withDescription("Destination SQLite file containing no work for this Thread."),
    ),
  },
  ({ thread, target: filename }) =>
    Effect.gen(function* () {
      const { database: source } = yield* admin;
      const threadId = yield* decodeThreadId(thread);
      const fs = yield* FileSystem.FileSystem;

      if (yield* fs.exists(filename)) {
        const from = yield* fs.stat(source);
        const to = yield* fs.stat(filename);

        if (
          (yield* fs.realPath(source)) === (yield* fs.realPath(filename)) ||
          (from.dev === to.dev &&
            Option.isSome(from.ino) &&
            Option.isSome(to.ino) &&
            from.ino.value === to.ino.value)
        ) {
          return yield* ThreadImportRejected.make({
            threadId,
            reason: "target-not-empty",
            message: "Source and destination name the same database",
          });
        }
      }

      const result = yield* reencodeThread(sourcePages(source, threadId)).pipe(
        Effect.provide(SqliteThreadStore.layer({ filename })),
      );

      yield* Console.log(
        yield* Schema.encodeEffect(Schema.fromJsonString(ThreadImportResult))(result),
      );
    }),
).pipe(
  CliCommand.withDescription(
    "Export a quiesced source and import its current-format records into an empty destination Thread.",
  ),
);

const command = admin.pipe(
  CliCommand.withSubcommands([
    explainCommand,
    verifyCommand,
    retryCommand,
    wakeCommand,
    obligationsCommand,
    exportCommand,
    importCommand,
    reencodeCommand,
  ]),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
