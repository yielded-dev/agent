import {
  DurableAlarmError,
  ThreadHostMaintenance,
  ThreadMutationGate,
} from "@yielded/agent-platform-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-cloudflare/cloudflare-bindings";
import { SqlStorageOwner } from "@yielded/agent/sql-storage-owner";
import { AdmissionPolicyError, SubmissionAdmissionFence } from "@yielded/agent/submission-ledger";
import { ThreadMaterialization, ThreadStore, ThreadTailRequest } from "@yielded/agent/thread-store";
import { ThreadWorkDiscovery } from "@yielded/agent/thread-work";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import { Clock, Context, Effect, Layer, Option, Schedule, Schema, Stream, Struct } from "effect";
import { DurableObjectState } from "effect-cf";
import { SqlClient } from "effect/sql";

import type { CommandInput } from "./contract.ts";
import { CommandRejected, ShellCommand } from "./contract.ts";
import { Allocation, allocateCommand, executeCommand, readCommandOutput } from "./tools.ts";
import { Workspace } from "./workspace.ts";

const lane = "sandbox-shell";
const activeKey = "shell-active";
const prefix = "shell-command:";
const commandKey = (id: string) => `${prefix}${id}`;
const limit = 100;
const timeoutMs = 30_000;

const SavedCommand = Schema.Struct({
  ...ShellCommand.fields,
  deadlineAt: Schema.Finite,
  allocation: Schema.optionalKey(Allocation),
  stopRequested: Schema.optionalKey(Schema.Boolean),
});

type SavedCommand = typeof SavedCommand.Type;

const publicCommand = (command: SavedCommand): ShellCommand =>
  Struct.omit(command, ["deadlineAt", "allocation", "stopRequested"]);

const busy = () =>
  CommandRejected.make({
    status: 409,
    message: "Wait for the current workspace operation to finish.",
  });

const terminal = (command: ShellCommand) =>
  command.status === "completed" || command.status === "failed" || command.status === "unknown";

const stoppedOutput = {
  stdout: "",
  stderr: "",
  exitCode: 130,
  timedOut: false,
  truncated: false,
  nextOffset: null,
};

// The native ledger checks this inside fresh admission's transaction, after replay lookup.
// Command admission writes the same key under the same SQL transaction owner.
export const commandAdmission = Layer.effect(
  SubmissionAdmissionFence,
  Effect.gen(function* () {
    const { storage } = yield* DurableObjectState.DurableObjectState;

    return SubmissionAdmissionFence.of({
      check: (request) =>
        storage.kv.get(activeKey).pipe(
          Effect.mapError((cause) =>
            AdmissionPolicyError.make({
              reason: "unavailable",
              code: "sandbox-shell-state",
              cause,
            }),
          ),
          Effect.flatMap((active) =>
            request.admissionFence !== undefined
              ? Effect.fail(
                  AdmissionPolicyError.make({ reason: "refused", code: "unsupported-policy" }),
                )
              : active === undefined
                ? Effect.void
                : Effect.fail(
                    AdmissionPolicyError.make({ reason: "occupied", code: "sandbox-shell-active" }),
                  ),
          ),
        ),
    });
  }),
);

const make = Effect.gen(function* () {
  const { storage } = yield* DurableObjectState.DurableObjectState;
  const { threadId } = yield* ThreadObjectIdentity;
  const mutations = yield* ThreadMutationGate;
  const work = yield* ThreadWorkDiscovery;
  const store = yield* ThreadStore;
  const wake = yield* WakeScheduler;
  const owner = yield* SqlStorageOwner;
  const sql = yield* SqlClient.SqlClient;

  if (owner === undefined)
    return yield* Effect.die("Sandbox commands require the SQL storage owner.");

  const transaction = <A, E, R>(body: Effect.Effect<A, E, R>) =>
    owner.transaction(body).pipe(Effect.provideService(SqlClient.SqlClient, sql));

  const read = Effect.fnUntraced(function* (id: string) {
    const saved = yield* storage.kv.get(commandKey(id));

    return saved === undefined ? null : yield* Schema.decodeUnknownEffect(SavedCommand)(saved);
  });

  const save = (command: SavedCommand) => storage.kv.put(commandKey(command.requestId), command);

  const list = Effect.gen(function* () {
    const values = yield* storage.kv.list({ prefix, limit });

    const commands = yield* Effect.forEach(values, ([, value]) =>
      Schema.decodeUnknownEffect(SavedCommand)(value),
    );

    return commands.map(publicCommand).sort((a, b) => a.createdAt - b.createdAt);
  });

  const watch = Stream.unwrap(
    Effect.gen(function* () {
      // Subscribe before reading so admission between the snapshot and waiting cannot be lost.
      const changed = yield* wake.subscribe(threadId);
      const commands = yield* list;

      return Stream.succeed(commands).pipe(
        Stream.concat(Stream.fromEffect(changed).pipe(Stream.drain)),
      );
    }),
  ).pipe(Stream.scoped, Stream.repeat(Schedule.forever));

  const accept = Effect.fnUntraced(function* (input: typeof CommandInput.Type) {
    const accepted = yield* mutations.withMutation(
      transaction(
        Effect.gen(function* () {
          const existing = yield* read(input.requestId);

          if (existing !== null) {
            if (existing.script !== input.script)
              return yield* CommandRejected.make({
                status: 409,
                message: "This command ID already belongs to a different command.",
              });

            return publicCommand(existing);
          }
          if ((yield* storage.kv.get(activeKey)) !== undefined) return yield* busy();
          yield* store.materialize(ThreadMaterialization.make({ threadId }));
          const pending = yield* work.page({ threadId, limit: 1 });

          if (pending.entries.length > 0 || pending.cursor !== undefined) return yield* busy();
          if ((yield* storage.kv.list({ prefix, limit })).length >= limit)
            return yield* CommandRejected.make({
              status: 429,
              message: "This workspace has reached its command limit. Start a new workspace.",
            });

          const { tailSequence: afterSequence } = yield* store.inspectTail(
            ThreadTailRequest.make({ threadId }),
          );

          const createdAt = yield* Clock.currentTimeMillis;

          const command: SavedCommand = {
            ...input,
            afterSequence,
            createdAt,
            deadlineAt: createdAt + timeoutMs,
            status: "queued",
          };

          yield* save(command);
          yield* storage.kv.put(activeKey, input.requestId);
          yield* mutations.recordProgress([lane]);

          return publicCommand(command);
        }),
      ),
      { invalidatesRecovery: false, lanes: [lane] },
    );

    yield* wake.notify(threadId, "progress");

    return accepted;
  });

  const update = (command: SavedCommand, complete = false) =>
    transaction(
      Effect.gen(function* () {
        const latest = yield* read(command.requestId);
        const saved = latest?.stopRequested ? { ...command, stopRequested: true } : command;

        yield* save(saved);
        if (complete) yield* storage.kv.delete(activeKey);
        yield* mutations.recordProgress([lane]);

        return saved;
      }),
    ).pipe(Effect.tap(() => wake.notify(threadId, "progress")));

  const run = Effect.gen(function* () {
    const id = yield* storage.kv.get(activeKey);

    if (id === undefined) return Option.none<number>();
    let command = yield* read(yield* Schema.decodeUnknownEffect(Schema.String)(id));

    if (command === null || command.status === "unknown") return Option.none<number>();

    let allocation = command.allocation;

    if (allocation === undefined) {
      if (command.stopRequested || (yield* Clock.currentTimeMillis) >= command.deadlineAt) {
        yield* update(
          command.stopRequested
            ? { ...command, status: "completed", output: stoppedOutput }
            : { ...command, status: "failed", failure: "not-started" },
          true,
        );

        return Option.none<number>();
      }
      const allocated = yield* allocateCommand(command.deadlineAt).pipe(Effect.result);

      if (allocated._tag === "Failure") {
        // Inspect only after failed allocation; ordinary command startup adds no RPC.
        const inspection = yield* (yield* Workspace).inspect.pipe(Effect.result);

        if (inspection._tag === "Success" && inspection.success.expired) {
          yield* update({ ...command, status: "failed", failure: "workspace-expired" }, true);

          return Option.none<number>();
        }

        return yield* allocated.failure;
      }
      allocation = allocated.success;

      // A stop can arrive during allocation. Keep its durable intent when retaining the ticket.
      command = yield* update({ ...command, allocation, status: "running" });
    }
    const sandbox = yield* (yield* Workspace).open;

    if (command.output === undefined) {
      const outcome = yield* (
        command.stopRequested
          ? Effect.gen(function* () {
              yield* sandbox.signalProcess(allocation.processId, 9);

              return yield* readCommandOutput(allocation);
            })
          : executeCommand(["bash", "--noprofile", "--norc", "-c", command.script], allocation)
      ).pipe(Effect.result);

      if (outcome._tag === "Failure") {
        if ((yield* Clock.currentTimeMillis) < command.deadlineAt + 10_000)
          return yield* outcome.failure;
        // Preserve the exact receipt and block new writes when execution cannot be established.
        yield* update({ ...command, status: "unknown" });

        return Option.none<number>();
      }
      command = { ...command, output: outcome.success };
      yield* update(command);
    }
    // Once output is retained, retries can only acknowledge this ticket, never execute again.
    yield* sandbox.acknowledgeProcess(allocation.processId);
    yield* update({ ...command, status: "completed" }, true);

    return Option.none<number>();
  }).pipe(
    Effect.mapError((cause) =>
      DurableAlarmError.make({
        operation: "sandbox-shell",
        message: "The retained command will be recovered with its original process receipt.",
        cause,
      }),
    ),
  );

  const wait = Effect.fnUntraced(function* (id: string) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        while (true) {
          const changed = yield* wake.subscribe(threadId);
          const command = yield* read(id);

          if (command === null || terminal(command))
            return command === null ? null : publicCommand(command);
          yield* changed;
        }
      }),
    ).pipe(Effect.timeout("50 seconds"));
  });

  const stop = Effect.fnUntraced(function* (id: string) {
    const command = yield* mutations.withMutation(
      transaction(
        Effect.gen(function* () {
          const command = yield* read(id);

          if (command === null || terminal(command)) return command;
          yield* save({ ...command, stopRequested: true });
          yield* mutations.recordProgress([lane]);

          return command;
        }),
      ),
      { invalidatesRecovery: false, lanes: [lane] },
    );

    if (command?.allocation && !terminal(command)) {
      const sandbox = yield* (yield* Workspace).open;

      yield* sandbox.signalProcess(command.allocation.processId, 9);
    }

    return command !== null;
  });

  return { accept, list, watch, wait, stop, run };
});

export class ShellCommands extends Context.Service<ShellCommands, Effect.Success<typeof make>>()(
  "sandbox-example/ShellCommands",
) {
  static readonly layer = Layer.effect(ShellCommands, make);
}

export const commandMaintenance = Layer.effect(
  ThreadHostMaintenance,
  Effect.gen(function* () {
    const commands = yield* ShellCommands;
    const workspace = yield* Workspace;

    return {
      lanes: [
        {
          id: lane,
          dispatchTimeoutMillis: 45_000,
          run: commands.run.pipe(Effect.provideService(Workspace, workspace)),
        },
      ],
    };
  }),
);
