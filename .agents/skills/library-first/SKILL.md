---
name: library-first
description: Build or extract reusable libraries, including packages inside application repositories. Use when a capability is intended for reuse across projects or an existing shared library is changing.
---

# Library first

Build the smallest reusable capability that serves the current caller. A module or workspace package inside the application is a valid home, even with one consumer. Extend an existing library when it owns the capability. Defer a separate repository, publication, configuration, and extension points until a real consumer needs them.

Put the current reusable workflow behind a small public API. Own the invariants that workflow actually promises; do not expand its scope while extracting it. Applications supply product policy and resources through explicit contracts; the library must not import its consuming app.

Apply the higher quality bar to the behavior the library actually promises. Keep that contract narrow, implement it correctly, and verify it through a real consumer. Add failure, cleanup, concurrency, or recovery handling when the supported workflow requires it or a real bug demonstrates the need. Do not front-load defensive frameworks or broad test matrices to make a new library seem stable. Prefer a lean first release with explicit limits; harden it through small fixes and focused regression tests as actual cases appear. Reuse existing proof and follow repository testing policy.

App-specific composition can move faster while preserving required behavior. Critical paths in both app and library code require [performance evidence](../measure-performance/SKILL.md). Reusable verification tooling should share the real operation across app commands, tests, and CI; use [feedback-loops](../feedback-loops/SKILL.md) when that tooling is missing or slow.

- Designing or implementing the reusable API: [boundaries](references/boundaries.md).
- Changing contracts, replacing duplicates, or extracting a package: [evolution](references/evolution.md).

Finish with the boundary actually used, relevant checks passing, and a concise explanation of guarantees, host responsibilities, and limitations.
