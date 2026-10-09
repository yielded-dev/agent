# Follow-up: Attempt ownership and oversized identity recovery

Both findings in [automated review 5473378123](https://github.com/yielded-dev/agent/pull/829#pullrequestreview-5473378123) apply to product commit `15b3b93bb895037febf3bacafd371db712b99b29`.

- `makeAttempt` accepted a Publisher service instance. It now yields that port inside its Effect operation, which is acquired through the existing Scope. The no-op fast path stays unchanged.
- An oversized terminal identity could leave the preview hub disabled after its Attempt ended. Terminal frames that exceed the existing bounds are now dropped without disabling a finished Attempt. Oversized live events still fail closed for the offending Attempt's lifetime; no wire bound was widened.

The product change is limited to two source files. The temporary regression probe and its results live only on this evidence branch. These are local correctness checks, not new latency measurements; the deployed timing evidence above remains tied to its recorded source and bundles. This follow-up created no Cloudflare resources.

Before changing the implementation, the scoped failure inventory covered reopening after a real accepted long-ID Submission, shared-hub recovery with and without text, continued fail-closed behavior during the offending Attempt, and preservation of default/no-op acquisition and scoped cleanup. General lifecycle behavior is covered by the existing runtime and Cloudflare suites.

| Probe | Before | After |
| --- | --- | --- |
| A real 220-character Thread ID mints a 257-character Submission; after canonical settlement, the public client opens `watchText` in the same Object | `HostProtocolError` | Receives `Reset` |
| A shared hub's oversized Attempt ends without text; a short-ID sibling opens a preview | `HostProtocolError` | Opens and cancels successfully |
| Oversized live text disables previews until its Attempt ends; a short-ID sibling then opens a preview | Still `HostProtocolError` after the end | Fails closed while active, then opens and cancels successfully |

The first probe uses the existing workerd Thread Object, real SQLite ledger, registered scripted provider, production alarms and public Cloudflare client. The two narrow lifecycle probes compose the actual Attempt operation and hub Layer to exercise shared-incarnation memory without adding a test-only Worker binding or copying business logic.

All three cases failed at the intended `HostProtocolError` before the fix and passed afterward. [Before](before.txt), [after](after.txt), and [validation receipt](validation.json) retain the outcomes. An initial invocation from the workspace root selected no files; the corrected package task below produced the meaningful red/green results. No production submission was retried.

To reproduce, check out either the reviewed commit or the fixed product commit recorded in `validation.json`, install the frozen dependencies, and copy [review-live-text.test.ts](review-live-text.test.ts) to `packages/platform-cloudflare/test/review-live-text.test.ts`. Run with Node 24.20.0 on `PATH`:

```sh
vp run -F @yielded/agent-platform-cloudflare test --project workerd test/review-live-text.test.ts
```

Remove the temporary test after the probe. The required `vp run ready` gate uses CI's Node 24.20.0, a frozen install with install scripts disabled followed by `vp run patch:tsgo`, and an isolated loopback PostgreSQL 18 database. No vendored `third-party/node_modules` or evidence files are present in the product checkout. Source hashes and cleanup are recorded in the validation receipt.
