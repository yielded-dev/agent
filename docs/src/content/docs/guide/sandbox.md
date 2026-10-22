---
title: Sandbox execution
description: Choose a scoped execution capability and understand what its adapter actually isolates.
---

<a id="sandbox-execution"></a>

Execution capabilities describe the work and its limits. Their adapters determine which
isolation guarantees they can enforce. Use [Code Mode](/guide/code-mode/) for generated
JavaScript over allowlisted tools and [browser services](/guide/browser/) for web interaction.

## Choose the execution boundary

`CodeExecutor` gives one generated program a narrow host-call capability; it does not grant
a shell. The Cloudflare adapter runs each pass in a fresh Dynamic Worker with ambient network
access disabled. Calls into application tools still require the host's current authorization.

Browser adapters own network access and page lifetimes separately. Their configuration controls
which destinations and browser capabilities a session can use. Keep credentials and resource
authorization in the host.

For trusted local commands, compose Effect's `ChildProcess` and `ChildProcessSpawner` in your
application. A child process is not an isolation boundary. Run untrusted commands only through
an adapter that enforces the required filesystem, network, credential, and resource restrictions.

## Understand limits and cleanup

The shared `SandboxImplementation` schema identifies an adapter's isolation posture. Network
policy values describe requested restrictions; the selected adapter must enforce or reject them.
A policy value alone does not isolate execution.

Keep execution within its owning Effect Scope so interruption releases local resources.
External effects can outlive that Scope: a crash after a write can leave an uncertain outcome.
Use the tool's reconciliation contract before retrying; restarting a process or program is not
recovery. See [Code Mode partial outcomes](/guide/code-mode/#partial-outcomes).
