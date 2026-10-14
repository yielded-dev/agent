# Review a substantial PR

Use this review pass when taking over an existing PR, changing lifecycle behavior, or adding public API surface. Adapt its depth to the diff. The implementer can perform these checks, but an independent reviewer is useful when available and authorized.

- Reconstruct the intended contract from the issue, base behavior, repository conventions, and current maintainer feedback. Treat the existing PR implementation as a proposal to verify, even if it already has passing tests.
- Read the aggregate diff. Ask whether each added public API, helper, comment, and test is necessary for the observable contract. Prefer a local guard over a new public mechanism when both provide the same behavior.
- Assert behavior at the affected boundary. Avoid spies and counters that require one implementation mechanism when several correct implementations exist. Where practical, run new regression tests against the old implementation or remove the new behavior temporarily to check that each test detects the failure it claims to cover. A supporting contract test need not fail on the base branch.
- Check less common but supported uses for behavior changes outside the main example, such as unmounted or one-shot reads. Decide whether each change follows the intended contract; document material consumer-visible changes.
- Report actionable findings tied to behavior or unnecessary scope. After corrections, review the revised diff and rerun relevant checks. A clean review is evidence about the reviewed commit, not a guarantee of maintainer approval. Finish comment and changeset wording after behavior and scope settle.
