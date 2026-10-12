---
title: Sandbox execution
description: Choose a scoped execution capability and understand what its adapter actually isolates.
---

<a id="sandbox-execution"></a>

A sandbox capability describes an operation and its resource limits. The adapter determines which
isolation guarantees it can enforce. Choose `Sandbox` for known processes, [Code Mode](/guide/code-mode/)
for generated JavaScript over allowlisted tools, and [browser services](/guide/browser/) for web interaction.

## Run a trusted local process

```ts twoslash
// @types: node
import { LocalSandbox } from "@yielded/agent-sandbox-local";
import { NetworkDisabled, Sandbox, SandboxRequest } from "@yielded/agent/sandbox";
import { Duration, Effect, Stream } from "effect";

const program = Effect.gen(function* () {
  const sandbox = yield* Sandbox;
  return yield* sandbox
    .execute(
      SandboxRequest.make({
        runtime: { kind: "unisolated-process", identity: "local-process" },
        command: process.execPath,
        args: ["-e", "console.log('ready')"],
        cwd: process.cwd(),
        environment: { allow: [] },
        mounts: [],
        network: NetworkDisabled.make({}),
        limits: { maxOutputBytes: 16_384, maxWallTime: Duration.seconds(10) },
        secretHandles: [],
        artifactRules: [],
      }),
    )
    .pipe(Stream.runCollect);
}).pipe(Effect.provide(LocalSandbox.layer), Effect.scoped);
```

Install `@yielded/agent-sandbox-local` and run `program` from your Effect entry point. The event
stream identifies process start, stdout/stderr output, and exit. A nonzero exit emits its exit
event before failing with `SandboxExitError`.

## Limits and local adapter boundaries

The local adapter is **unisolated**. `NetworkDisabled` is only a request marker here: it does not
prevent the child from opening network connections. Use this adapter only for trusted commands,
never model-generated commands or sensitive credentials.

It passes only allowlisted environment names and bounds combined output bytes and elapsed time.
It rejects requested mounts, CPU/memory limits, secret handles, and artifact collection because it
cannot enforce them. Unsupported requests fail before process startup.

Stream finalization owns process cleanup. A crash after a process changes an external system
can leave an uncertain outcome; restarting the process is not recovery.

## Use the other services deliberately

`CodeExecutor` exposes a narrow host-call capability to one generated program; it does not grant
a shell. Browser services own network and page lifetimes separately. Neither turns local process
execution into an isolated sandbox.

The [local adapter example](https://github.com/yielded-dev/agent/tree/main/packages/sandbox-local)
contains setup and executable contract cases. Keep authentication and resource authorization in
your host regardless of the adapter selected.
